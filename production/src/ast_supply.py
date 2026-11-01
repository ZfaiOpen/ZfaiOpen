# ast_supply.py — U7·AST 供给(tree-sitter 语法树边界切分 + 符号抽取·20261026)
# ═══════════════════════════════════════════════════════════════════════════════
# 病根: 现有切片/指纹全是正则级——spanvec.split_spans 按段落/diff/tool 标记切, 代码函数
#       常被 \n\n 贪心装箱腰斩(半个函数一片, 嵌入语义破碎); knife13 symbols 正则只认
#       py def/class + js function/const, go/rust/java 方法签名全部漏采。代码检索共识
#       (Aider repo-map / cAST): AST 边界切片优于纯文本切割——函数/类/方法是完整语义单元。
# 处方: tree-sitter 解析 → 语法树节点边界供给:
#   ast_spans(code, lang)   函数/类/方法=完整单元切分, 超长单元(>AST_HARD)按语句块/成员
#                           二次切, 贪心装箱到 ~AST_TARGET(400-800 中值)——输出 (text,kind,
#                           name) 三元组, 与 spanvec.split_spans 的 list[str] 同构(展平即得)。
#   ast_symbols(code,lang)  def/class/方法签名首行 + docstring/文档注释首行(可读可正则)。
#   ast_symbol_names(...)   裸符号名(文档序·去重)——k13 symbols 指纹增补源(补 go/rust/java)。
#   detect_lang(text,path)  扩展名 > shebang > 内容启发计分(纯函数·确定性)。
#   split_spans_ast(text)   spanvec.split_spans 的 AST 优先变体(接入 hunk a·逐段 AST 优先
#                           正则兜底·门关/不适用→None 回退 pristine)。
# 语言: python 精解; javascript/typescript/tsx/go/rust/java 尽力(grammar 装得上即支持)。
# 依赖: tree-sitter + grammar wheel(dev_u7/pylibs/ 专用目录·共享 venv 零改动·引擎启动脚本
#       PYTHONPATH 前置即用); 全部 lazy import + 缓存(含负缓存), grammar 缺席→回退正则。
# 门控: BRAINOS_AST(总闸·缺省0=关) + BRAINOS_AST_SPAN(spanvec 变体) + BRAINOS_AST_SYM
#       (k13 增补)——全部缺省关=逐字节零行为差。纯函数 ast_spans/ast_symbols/detect_lang
#       不读门(可单测); 门在消费侧 wrapper(split_spans_ast/symbols_augment)生效。
# fail-soft: 解析失败(has_error 严格档)/语言未知/过短/超长/grammar 缺席/任何异常→None→
#       调用方回退现有正则切片, 绝不阻塞绝不抛(消费侧 wrapper 再兜一层 try→整卡回退)。
# 切分全覆盖口径: spans 模式只捕"顶层单元"(含 wrapper), 非单元顶层节点整块入 mod 流——
#       单元+mod = 全文零重不漏(无嵌套重复); symbols 模式深走(类→方法限定名)。
# 合规: 日志零内容(只记计数/语言/时延); 工作区 /opt/braios/run/dev_u7(不碰共享树/venv)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
import threading

log = logging.getLogger("ast_supply")

# ── 门与参数(env可调·缺省=交付报告口径) ─────────────────────────────────────────
def master_on() -> bool:
    return os.environ.get("BRAINOS_AST", "0") == "1"           # 缺省关(评测门控纪律)


def span_on() -> bool:
    return master_on() and os.environ.get("BRAINOS_AST_SPAN", "0") == "1"


def sym_on() -> bool:
    return master_on() and os.environ.get("BRAINOS_AST_SYM", "0") == "1"


def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)


AST_TARGET   = _i("BRAINOS_AST_TARGET", "600")     # 分片目标字符(400-800 中值·装箱用)
AST_HARD     = _i("BRAINOS_AST_HARD", "900")       # 单元超此→语句块/成员二次切(=spanvec HARD_CAP 同档)
AST_MIN_UNIT = _i("BRAINOS_AST_MIN_CHARS", "120")  # 代码段短此→不值得 AST(直接不适用)
AST_MAXCHARS = _i("BRAINOS_AST_MAX_CHARS", "200000")  # 超此长度防御性放弃(解析耗时护栏)
AST_TOLERANT = os.environ.get("BRAINOS_AST_TOLERANT", "0") == "1"  # 1=ERROR 节点容忍(缺省0=严格失败即回退)
AST_MAX_SYM  = _i("BRAINOS_AST_MAX_SYM", "128")    # 符号行/名上限(k13 MAX_FP=64 有余量)
AST_MAX_SPANS = _i("BRAINOS_AST_MAX_SPANS", "12")  # 与 spanvec MAX_SPANS 同档
AST_TAIL_CLIP = _i("BRAINOS_AST_TAIL_CLIP", "1500")  # 与 spanvec TAIL_CLIP 同档

SUPPORTED = ("python", "javascript", "typescript", "tsx", "go", "rust", "java")

# ── grammar 注册(lazy·缓存含负项·进程内一次) ───────────────────────────────────
_LANG_MODS = {
    "python":     ("tree_sitter_python", "language"),
    "javascript": ("tree_sitter_javascript", "language"),
    "typescript": ("tree_sitter_typescript", "language_typescript"),
    "tsx":        ("tree_sitter_typescript", "language_tsx"),
    "go":         ("tree_sitter_golang", "language"),
    "rust":       ("tree_sitter_rust", "language"),
    "java":       ("tree_sitter_java", "language"),
}
_LANG_LOCK = threading.RLock()      # RLock: _parser_for 持锁调 _language(同线程重入)
_LANGS: dict[str, object] = {}      # lang -> Language | None(负缓存)
_PARSERS: dict[str, object] = {}    # lang -> Parser(共享实例·parse 串行锁)


def _language(lang: str):
    """加载并缓存 Language(失败→None 负缓存·绝不重复抛)。"""
    with _LANG_LOCK:
        if lang in _LANGS:
            return _LANGS[lang]
        out = None
        if lang in _LANG_MODS:
            try:
                import importlib
                mod = importlib.import_module(_LANG_MODS[lang][0])
                from tree_sitter import Language
                out = Language(getattr(mod, _LANG_MODS[lang][1])())
            except Exception as exc:  # noqa: BLE001 — grammar 缺席/ABI 不合→负缓存
                log.warning("ast_supply grammar load FAIL lang=%s err=%s", lang, type(exc).__name__)
                out = None
        _LANGS[lang] = out
        return out


def ast_ready(lang: str = "") -> bool:
    """tree-sitter + 指定 grammar 可用?(lang 空=任一语言可用)"""
    try:
        import tree_sitter  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    if lang:
        return _language(lang) is not None
    return any(_language(x) is not None for x in _LANG_MODS)


def _parser_for(lang: str):
    with _LANG_LOCK:
        p = _PARSERS.get(lang)
        if p is None:
            L = _language(lang)
            if L is None:
                return None
            from tree_sitter import Parser
            p = Parser(L)
            _PARSERS[lang] = p
        return p


_PARSE_LOCK = threading.Lock()  # Parser 实例非并发安全·串行化(ms 级·嵌入才是瓶颈)


def _parse(code: str, lang: str):
    """解析→tree|None(fail-soft)。bytes 切片按 token 边界·utf-8 安全。"""
    p = _parser_for(lang)
    if p is None:
        return None
    try:
        with _PARSE_LOCK:
            return p.parse(code.encode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("ast_supply parse FAIL lang=%s err=%s", lang, type(exc).__name__)
        return None


# ── 语言检测(扩展名 > shebang > 内容启发·纯函数·确定性) ─────────────────────────
_EXT_LANG = {
    "py": "python", "pyi": "python",
    "js": "javascript", "mjs": "javascript", "cjs": "javascript", "jsx": "javascript",
    "ts": "typescript", "mts": "typescript", "cts": "typescript",
    "tsx": "tsx", "go": "go", "rs": "rust", "java": "java",
}
_SHEBANG = (
    (re.compile(r"python[0-9.]*\b"), "python"),
    (re.compile(r"\bnode\b|\bts-node\b"), "javascript"),
    (re.compile(r"\bdeno\b"), "typescript"),
)
# 内容启发计分(每中一计 1 分; ts 组同时借 js 证据压制纯 js)。阈值 ≥2 分才判。
_RXH = {
    "python": (r"^\s*def\s+\w+\s*\(", r"^\s*class\s+\w+", r"^\s*from\s+[\w.]+\s+import\b",
               r"^\s*import\s+[\w.]+", r"\bself\.", r"\belif\b", r"\bNone\b",
               r"^\s*if\s+__name__\s*=="),
    "javascript": (r"\bfunction\s+\w*\s*\(", r"=>", r"\bconst\s+\w+\s*=[^=]", r"\bconsole\.",
                   r"\brequire\s*\(", r"\bexport\s+(?:default|function|const|class)\b",
                   r"\basync\s+function\b"),
    "typescript": (r"\binterface\s+\w+\s*\{", r":\s*(?:string|number|boolean|any|void)\b",
                   r"\bimplements\s+\w+", r"\btype\s+\w+\s*=", r"<[A-Z]\w*(?:,\s*\w+)?>",
                   r"\benum\s+\w+\s*\{"),
    "go": (r"^package\s+\w+", r"^\s*func\s", r":=", r"^import\s*\(", r"\bfmt\.",
           r"^type\s+\w+\s+struct\b"),
    "rust": (r"\bfn\s+\w+\s*\(", r"\blet\s+mut\s", r"\bimpl\s+\w+", r"\bpub\s+(?:fn|struct|enum)\b",
             r"\buse\s+[\w:]+;", r"::\w+", r"\bmatch\s+\w+\s*\{"),
    "java": (r"\bpublic\s+(?:class|static|void|int|final|private)\b",
             r"\bprivate\s+\w+\s+\w+\s*\(", r"System\.out\.print", r"@Override\b",
             r"\bthrows\s+\w+", r"\bnew\s+\w+\s*\(", r"\bnew\s+\w+\s*<",
             r"\b(?:private|protected)\s+(?:final\s+)?\w+\s+\w+\s*[;=]"),
}
_RXH_C: dict[str, list[re.Pattern]] = {}


def _heur_scores(code: str) -> dict[str, int]:
    """启发计分=命中模式种数(每模式至多计1·防 console.log×N 刷分)。"""
    if not _RXH_C:
        for lg, pats in _RXH.items():
            _RXH_C[lg] = [re.compile(p, re.M) for p in pats]
    out: dict[str, int] = {}
    for lg, rxs in _RXH_C.items():
        out[lg] = sum(1 for rx in rxs if rx.search(code))
    if out.get("typescript", 0) >= 2:  # ts 借 js 证据(子集语言·防 js 抢答·上限2)
        out["typescript"] += min(out.get("javascript", 0), 2)
    return out


def detect_lang(text: str, path: str = "") -> str:
    """语言检测: 扩展名 > shebang > 内容启发(≥2分·最高者·平局按 SUPPORTED 序)。"" =未知。"""
    t = str(text or "")
    if path:
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        lg = _EXT_LANG.get(ext)
        if lg and _language(lg) is not None:
            return lg
    head = t.lstrip()[:160]
    if head.startswith("#!"):
        for rx, lg in _SHEBANG:
            if rx.search(head.splitlines()[0]) and _language(lg) is not None:
                return lg
    if len(t) < 20:
        return ""
    sc = _heur_scores(t)
    best, bs = "", 0
    for lg in SUPPORTED:  # SUPPORTED 序=平局确定性
        s = sc.get(lg, 0)
        if s >= 2 and s > bs and _language(lg) is not None:
            best, bs = lg, s
    return best


# ── fence 识别(轨迹卡代码块多带 ```lang 围栏) ───────────────────────────────────
_RX_FENCE_OPEN = re.compile(r"^\s*```+\s*([\w+#.-]*)\s*$")
_RX_FENCE_CLOSE = re.compile(r"^\s*```+\s*$")
_FENCE_LANG = {"python": "python", "py": "python", "javascript": "javascript", "js": "javascript",
               "node": "javascript", "typescript": "typescript", "ts": "typescript",
               "tsx": "tsx", "go": "go", "golang": "go", "rust": "rust", "rs": "rust",
               "java": "java"}


def _strip_fence(seg: str) -> tuple[str, str]:
    """```lang ... ``` 围栏剥离→(lang_hint|"", inner)。非围栏→("", 原文)。"""
    lines = seg.split("\n")
    if len(lines) >= 2:
        m = _RX_FENCE_OPEN.match(lines[0])
        if m:
            hint = _FENCE_LANG.get(m.group(1).lower(), "")
            end = len(lines) - 1
            while end > 0 and not lines[end].strip():
                end -= 1
            if end > 0 and _RX_FENCE_CLOSE.match(lines[end]):
                return hint, "\n".join(lines[1:end])
    return "", seg


# ── 单元捕获(每语言的节点种类与取名规则) ────────────────────────────────────────
_UNIT = {
    "python": {"function_definition": "func", "class_definition": "class"},
    "javascript": {"function_declaration": "func", "generator_function_declaration": "func",
                   "class_declaration": "class", "method_definition": "method",
                   "lexical_declaration": "func", "variable_declaration": "func"},
    "typescript": {"function_declaration": "func", "generator_function_declaration": "func",
                   "class_declaration": "class", "method_definition": "method",
                   "abstract_class_declaration": "class", "interface_declaration": "type",
                   "lexical_declaration": "func", "variable_declaration": "func"},
    "go": {"function_declaration": "func", "method_declaration": "method",
           "type_declaration": "type"},
    "rust": {"function_item": "func", "struct_item": "type", "enum_item": "type",
             "trait_item": "type", "impl_item": "impl", "mod_item": "impl",
             "type_item": "type", "union_item": "type"},
    "java": {"class_declaration": "class", "interface_declaration": "type",
             "enum_declaration": "type", "record_declaration": "type",
             "method_declaration": "method", "constructor_declaration": "method"},
}
_UNIT["tsx"] = _UNIT["typescript"]
_WRAPPER = ("decorated_definition", "export_statement")
_NAME_KINDS = {"identifier", "type_identifier", "field_identifier", "property_identifier"}
_BODY_KINDS = {"block", "statement_block", "class_body", "field_declaration_list",
               "declaration_list", "interface_body"}
_COMMENT_KINDS = {"comment", "line_comment", "block_comment"}
_RX_JS_FN_VALUE = re.compile(r"(arrow_function|function_expression|function|class)\Z")
_RX_DOCSTRIP = re.compile(r"^[rbfu]{0,2}[\"']{1,3}|[\"']{1,3}$")


def _named(node) -> list:
    return [c for c in node.children if c.is_named]


def _decl_name(node) -> str:
    """声明节点→裸名(py def f / go method M / js const f = …→f)。尽力空串。"""
    for c in _named(node):
        if c.type == "variable_declarator":  # js/ts const f = fn
            for d in _named(c):
                if d.type in _NAME_KINDS:
                    return d.text.decode("utf-8", "replace")
            return ""
        if c.type in ("parameters", "formal_parameters", "modifiers", "type_parameters",
                      "type_annotation", "decorator", "visibility_modifier", "class_heritage"):
            continue  # go receiver / java 修饰符跳过
        if c.type in _NAME_KINDS:
            return c.text.decode("utf-8", "replace")
        if c.type in ("type_definition", "type_alias", "type_spec"):  # go type X struct{...}
            return _decl_name(c)
    return ""


def _is_fn_value(decl_node) -> bool:
    """js/ts 变量声明是否函数/类赋值(const f = () => …)。"""
    for c in _named(decl_node):
        if c.type == "variable_declarator":
            v = c.child_by_field_name("value")
            if v is not None and _RX_JS_FN_VALUE.search(v.type):
                return True
    return False


def _wrapper_inner(node, utab):
    """wrapper(decorated/export)→内层声明节点|None。"""
    if node.type in utab:
        return node
    for d in _named(node):
        r = _wrapper_inner(d, utab)
        if r is not None:
            return r
    return None


def _body_of(node):
    """声明节点→body 节点|None(最后1个 _BODY_KINDS named 子·wrapper 递归一层)。"""
    for c in reversed(_named(node)):
        if c.type in _BODY_KINDS:
            return c
        if c.type in _WRAPPER:
            b = _body_of(c)
            if b is not None:
                return b
    return None


def _qualify(node, kind: str, lang: str, prefix: str) -> str:
    """单元名: py 方法限定 Cls.m; go 方法 T.M(receiver); 其余裸名。"""
    n = _decl_name(node)
    if lang == "go" and node.type == "method_declaration":
        recv = ""
        for c in _named(node):
            if c.type == "parameters":
                for d in _named(c):
                    if d.type in _NAME_KINDS:
                        recv = d.text.decode("utf-8", "replace")
                        break
                break
        if recv and n:
            return f"{recv}.{n}"
    if prefix and n and kind == "method":
        return f"{prefix}.{n}"
    return n


def _mk_unit(node, kind: str, lang: str, prefix: str = "") -> dict | None:
    utab = _UNIT.get(lang, {})
    if node.type in _WRAPPER:
        inner = _wrapper_inner(node, utab)
        k2 = utab.get(inner.type, "") if inner is not None else ""
        if not k2 or (inner.type in ("lexical_declaration", "variable_declaration")
                      and not _is_fn_value(inner)):
            return None
        # 名/体取内层·span 取 wrapper(装饰器/export 随行)
        return {"node": node, "kind": k2, "name": _decl_name(inner),
                "start": node.start_byte, "end": node.end_byte, "body": _body_of(inner)}
    if not kind:
        return None  # 非单元→调用方入 mod 流
    if node.type in ("lexical_declaration", "variable_declaration") \
            and not _is_fn_value(node):
        return None  # 普通变量赋值不当函数单元(入 mod 流)
    return {"node": node, "kind": kind, "name": _qualify(node, kind, lang, prefix),
            "start": node.start_byte, "end": node.end_byte, "body": _body_of(node)}


def _first_line(s: str) -> str:
    return s.split("\n", 1)[0].strip()


def _py_docstring(node, src: bytes) -> str:
    """python def/class 的 docstring 首行(无→"")。"""
    body = _body_of(node)
    if body is None:
        return ""
    for c in _named(body):
        if c.type == "comment":
            continue
        if c.type == "expression_statement":
            for d in _named(c):
                if d.type == "string":
                    txt = src[d.start_byte:d.end_byte].decode("utf-8", "replace")
                    txt = _RX_DOCSTRIP.sub("", txt.strip())
                    return _first_line(txt)
            return ""  # 首条语句非字符串=无 docstring
        return ""
    return ""


def _prev_comment(node, src: bytes) -> str:
    """紧邻前置注释首行(go/rust/java 文档注释·尽力)。"""
    p = node.prev_named_sibling if hasattr(node, "prev_named_sibling") else None
    if p is not None and p.type in _COMMENT_KINDS:
        t = src[p.start_byte:p.end_byte].decode("utf-8", "replace")
        line = _first_line(t).lstrip("/#* ").strip()
        return line.rstrip("*/ ").strip()
    return ""


# ── AST 单元切分核心(spans: 顶层全覆盖零重复 / symbols: 深走采集) ────────────────
def _top_seq(root, lang: str) -> list[dict]:
    """顶层序列: 单元(kind 非空)与 mod 原文块(kind='mod')按文档序混合——全覆盖零重不漏。"""
    utab = _UNIT.get(lang, {})
    seq: list[dict] = []
    for c in _named(root):
        u = _mk_unit(c, utab.get(c.type, ""), lang)
        if u is not None:
            seq.append(u)
        else:
            seq.append({"node": c, "kind": "mod", "name": "",
                        "start": c.start_byte, "end": c.end_byte, "body": None})
    return seq


def _deep_units(node, lang: str, utab: dict, prefix: str, out: list, depth: int) -> None:
    """深走采集全部声明单元(含容器内方法限定名·供 symbols)。捕获单元仍下钻一层容器。"""
    if depth > 24 or len(out) >= AST_MAX_SYM * 2:
        return
    for c in _named(node):
        u = _mk_unit(c, utab.get(c.type, ""), lang, prefix)
        if u is not None:
            out.append(u)
            if u["kind"] in ("class", "impl", "type") and depth < 8:  # 容器内方法(限定名)
                b = u["body"]
                if b is not None:
                    _deep_units(b, lang, utab, u["name"] or prefix, out, depth + 1)
            continue
        if c.type in _WRAPPER:
            _deep_units(c, lang, utab, prefix, out, depth + 1)
            continue
        _deep_units(c, lang, utab, prefix, out, depth + 1)


def _soften(u: dict, src: bytes, hard: int, depth: int = 0) -> list[tuple[str, str, str]]:
    """超长单元→按语句块/成员二次切(签名头随行·终止保证: 深度≤2 后整块兜底)。"""
    txt = src[u["start"]:u["end"]].decode("utf-8", "replace")
    ktag = u["kind"] if depth == 0 else f"{u['kind']}_part"
    if len(txt) <= hard or depth > 2:
        return [(txt, ktag, u["name"])]
    body = u["body"]
    kids = [c for c in _named(body)] if body is not None else []
    if not kids:
        return [(txt, ktag, u["name"])]
    header = ""
    if body is not None:
        header = src[u["start"]:body.start_byte].decode("utf-8", "replace").rstrip()
    # 声明行=签名头末个非空行(装饰器/export 前缀不随行·`def f(..):`/`public class A` 随行)
    hline = ""
    for ln in reversed(header.split("\n")):
        if ln.strip():
            hline = ln.strip()
            break
    tail = src[body.end_byte:u["end"]].decode("utf-8", "replace") if body is not None else ""
    ptag = f"{u['kind']}_part"
    pieces: list[tuple[str, str, str]] = []
    n = 0
    for k in kids:
        ktxt = src[k.start_byte:k.end_byte].decode("utf-8", "replace")
        if len(ktxt) > hard:  # 单语句仍超长→递归一层(嵌套块)
            pieces.extend(_soften({"node": k, "kind": u["kind"], "name": u["name"],
                                   "start": k.start_byte, "end": k.end_byte,
                                   "body": _body_of(k)}, src, hard, depth + 1))
            continue
        n += 1
        piece = (hline + "\n" + ktxt) if hline else ktxt
        pieces.append((piece, ptag, f"{u['name']}#{n}"))
    if tail.strip():
        pieces.append(((hline + "\n" + tail) if hline else tail, ptag, f"{u['name']}#tail"))
    return pieces or [(txt, ktag, u["name"])]


def _box(pieces: list[tuple[str, str, str]], target: int) -> list[tuple[str, str, str]]:
    """贪心装箱(同 spanvec 0.6 规则): 片不劈·小片并入·kind/name=首片(多片=mixed)。"""
    out: list[tuple[str, str, str]] = []
    cur: list[tuple[str, str, str]] = []
    cur_len = 0
    for p in pieces:
        L = len(p[0])
        if cur and cur_len + L > target and cur_len >= target * 0.6:
            out.append(_merge(cur))
            cur, cur_len = [], 0
        cur.append(p)
        cur_len += L + 1
    if cur:
        out.append(_merge(cur))
    return out or pieces


def _merge(cur: list[tuple[str, str, str]]) -> tuple[str, str, str]:
    if len(cur) == 1:
        return cur[0]
    return ("\n".join(p[0] for p in cur), "mixed", cur[0][2])


def ast_spans(code: str, lang: str | None = None, path: str = "") \
        -> list[tuple[str, str, str]] | None:
    """代码→AST 边界 (text,kind,name) 三元组列表(纯函数·确定性·不读门)。
    函数/类/方法=完整单元; >AST_HARD 单元按语句块/成员二次切; 装箱 ~AST_TARGET;
    顶层非单元节点整块入 mod 流(单元+mod=全文零重不漏)。
    不适用/失败→None(调用方回退正则): 语言未知/过短/超长/grammar 缺席/解析失败
    (严格档 has_error)/异常。"""
    code = str(code or "")
    if not code.strip() or len(code) < AST_MIN_UNIT or len(code) > AST_MAXCHARS:
        return None
    lg = lang or detect_lang(code, path)
    if not lg or lg not in _LANG_MODS or _language(lg) is None:
        return None
    tree = _parse(code, lg)
    if tree is None or tree.root_node is None:
        return None
    if tree.root_node.has_error and not AST_TOLERANT:
        return None  # 严格档: 解析失败→回退正则(fail-soft 铁律)
    try:
        src = code.encode("utf-8")
        pieces: list[tuple[str, str, str]] = []
        for u in _top_seq(tree.root_node, lg):
            if u["kind"] == "mod":
                raw = src[u["start"]:u["end"]].decode("utf-8", "replace")
                if raw.strip():
                    pieces.append((raw, "mod", ""))
                continue
            txt = src[u["start"]:u["end"]].decode("utf-8", "replace")
            if len(txt) > AST_HARD:
                pieces.extend(_soften(u, src, AST_HARD))
            else:
                pieces.append((txt, u["kind"], u["name"]))
        spans = _box(pieces, AST_TARGET)
        if len(spans) > AST_MAX_SPANS:  # 超上限并尾(同 spanvec 语义)
            head = spans[:AST_MAX_SPANS - 1]
            tail = "\n".join(p[0] for p in spans[AST_MAX_SPANS - 1:])[:AST_TAIL_CLIP]
            spans = head + [(tail, "tail", spans[-1][2])]
        if spans and len(spans[-1][0]) > AST_TAIL_CLIP:
            spans[-1] = (spans[-1][0][:AST_TAIL_CLIP], spans[-1][1], spans[-1][2])
        return spans or None
    except Exception as exc:  # noqa: BLE001 — 任何意外→回退(绝不阻塞)
        log.warning("ast_supply ast_spans FAIL lang=%s err=%s", lg, type(exc).__name__)
        return None


# ── 符号抽取(k13 symbols 增补源) ────────────────────────────────────────────────
def _parse_deep(code: str, lg: str) -> list[dict] | None:
    """解析+深走采集单元。None=解析失败(严格档)·[]=无单元。"""
    tree = _parse(code, lg)
    if tree is None or tree.root_node is None:
        return None
    if tree.root_node.has_error and not AST_TOLERANT:
        return None
    try:
        utab = _UNIT.get(lg, {})
        out: list[dict] = []
        _deep_units(tree.root_node, lg, utab, "", out, 0)
        return out
    except Exception:  # noqa: BLE001
        return None


def _lang_ok(code: str, lang: str | None, path: str) -> str | None:
    """公共守卫: 长度/语言/grammar→lang|None。"""
    code = str(code or "")
    if not code.strip() or len(code) < AST_MIN_UNIT or len(code) > AST_MAXCHARS:
        return None
    lg = lang or detect_lang(code, path)
    if not lg or lg not in _LANG_MODS or _language(lg) is None:
        return None
    return lg


def ast_symbols(code: str, lang: str | None = None, path: str = "") -> list[str] | None:
    """def/class/方法签名首行 + docstring/文档注释首行(文档序·去重·cap=AST_MAX_SYM)。
    喂 k13 symbols 指纹的增补源(py/js 行可直接被既有 _RX_DEF/_RX_CLS/_RX_FUNC 命中;
    go/rust/java 方法签名+裸名走 ast_symbol_names)。失败→None(回退纯正则)。"""
    lg = _lang_ok(code, lang, path)
    if lg is None:
        return None
    units = _parse_deep(str(code), lg)
    if units is None:
        return None
    try:
        src = str(code).encode("utf-8")
        utab = _UNIT.get(lg, {})
        out: list[str] = []
        seen: set[str] = set()
        for u in units:
            u_node = u["node"]
            inner = _wrapper_inner(u_node, utab) if u_node.type in _WRAPPER else None
            if inner is not None:
                u_node = inner  # 装饰器定义→签名/docstring 取内层 def(@dec 行不是签名)
            sig = _first_line(src[u_node.start_byte:u_node.end_byte].decode("utf-8", "replace"))
            if sig and sig not in seen:
                seen.add(sig)
                out.append(sig[:200])
            doc = _py_docstring(u_node, src) if lg == "python" else _prev_comment(u_node, src)
            if doc and doc not in seen:
                seen.add(doc)
                out.append(doc[:200])
            if len(out) >= AST_MAX_SYM:
                break
        return out
    except Exception as exc:  # noqa: BLE001
        log.warning("ast_supply ast_symbols FAIL lang=%s err=%s", lg, type(exc).__name__)
        return None


def ast_symbol_names(code: str, lang: str | None = None, path: str = "") \
        -> list[str] | None:
    """裸符号名+限定名(文档序·去重·cap=AST_MAX_SYM)——k13 symbols 直接增补源(全语言)。"""
    lg = _lang_ok(code, lang, path)
    if lg is None:
        return None
    units = _parse_deep(str(code), lg)
    if units is None:
        return None
    out: list[str] = []
    seen: set[str] = set()
    for u in units:
        n = (u.get("name") or "").strip()
        if not n:
            continue
        for part in (n, n.rsplit(".", 1)[-1]):  # 限定名+裸名(k13 小写归一后命中面更大)
            if part and part not in seen:
                seen.add(part)
                out.append(part)
                if len(out) >= AST_MAX_SYM:
                    return out
    return out


# ── 接入 wrapper ①: spanvec.split_spans 的 AST 优先变体(hunk a) ────────────────
# 段界正则(镜像 spanvec._RX_BOUND 四类·\fence 感知扫描器专用·spanvec 本件零改动):
_RX_BOUND_MIRROR = re.compile(
    r"\n\n+"
    r"|\[tool_result\]|\[/tool_result\]"
    r"|\[tool_use\]|\[/tool_use\]"
    r"|^diff --git "
    r"|^Exit code:?\s*\d+\s*$", re.M)


def ast_segments(text: str) -> list[str]:
    """fence 感知分段: ``` 围栏块整体成段(代码内部空行不切段·AST 可解析),
    围栏外文字按 spanvec 四类边界切段(镜像 _RX_BOUND)。纯函数·确定性。
    接入 hunk a 以本函数替代 spanvec._segments 作 split_spans_ast 的分段器。"""
    t = str(text or "")
    lines = t.split("\n")
    out: list[str] = []
    buf: list[str] = []

    def _flush() -> None:
        if not buf:
            return
        chunk = "\n".join(buf)
        last = 0
        for m in _RX_BOUND_MIRROR.finditer(chunk):
            if m.start() > last:
                out.append(chunk[last:m.start()])
                last = m.start()
        if last < len(chunk):
            out.append(chunk[last:])
        buf.clear()

    i = 0
    while i < len(lines):
        if _RX_FENCE_OPEN.match(lines[i]):
            _flush()
            block = [lines[i]]
            i += 1
            while i < len(lines):
                block.append(lines[i])
                if _RX_FENCE_CLOSE.match(lines[i]):
                    i += 1
                    break
                i += 1
            out.append("\n".join(block))  # 围栏整体一段(含开合行)
            continue
        buf.append(lines[i])
        i += 1
    _flush()
    return [s for s in (x.strip() for x in out) if s]


def split_spans_ast(text: str, base_segments=None, base_hard_split=None) -> list[str] | None:
    """spanvec.split_spans 的 AST 优先变体(hunk a 消费口)。
    门关/ast_ready 假/空文本/任何异常→None(调用方回退 pristine split_spans)。
    逐段: 围栏剥离→检语言→AST 分片→围栏回填; 段级不适用→该段正则段内切
    (base_segments/base_hard_split 缺省用本模块 ast_segments+行兜底)。"""
    if not span_on():
        return None
    try:
        t = str(text or "").strip()
        if not t:
            return None
        if len(t) <= _i("BRAINOS_SV_SPLIT_MIN", "1200"):
            return None  # 短卡 pristine 自会整卡单片·无 AST 必要
        if not ast_ready():
            return None
        segs = base_segments(t) if base_segments else ast_segments(t)
        out: list[str] = []
        for seg in segs:
            spans = _seg_spans_ast(seg)
            if spans is None:  # 段级回退: 正则段内切
                if base_segments and base_hard_split:
                    for s2 in base_segments(seg):
                        out.extend(base_hard_split(s2, _i("BRAINOS_SV_HARD_CAP", "900"),
                                                   _i("BRAINOS_SV_TARGET", "500")))
                elif base_segments:
                    out.extend(base_segments(seg))
                else:
                    for s2 in ast_segments(seg):
                        out.extend(_plain_split(s2, _i("BRAINOS_SV_HARD_CAP", "900"),
                                                _i("BRAINOS_SV_TARGET", "500")))
                continue
            out.extend(spans)
        if not out:
            return None
        if len(out) > AST_MAX_SPANS:  # 与 pristine 同款并尾截尾
            head = out[:AST_MAX_SPANS - 1]
            tail = "\n".join(out[AST_MAX_SPANS - 1:])[:AST_TAIL_CLIP]
            out = head + [tail]
        if len(out[-1]) > AST_TAIL_CLIP:
            out[-1] = out[-1][:AST_TAIL_CLIP]
        return out
    except Exception as exc:  # noqa: BLE001 — 整卡回退 pristine(绝不阻塞)
        log.warning("ast_supply split_spans_ast FAIL err=%s", type(exc).__name__)
        return None


def _plain_split(seg: str, hard: int, target: int) -> list[str]:
    """非代码段正则兜底切(镜像 spanvec._hard_split 行/定长策略·确定性)。"""
    if len(seg) <= hard:
        return [seg]
    out: list[str] = []
    for lb in seg.split("\n"):
        if len(lb) <= hard:
            out.append(lb)
        else:
            for i in range(0, len(lb), target):
                out.append(lb[i:i + target])
    merged: list[str] = []
    for b in out:
        if merged and len(merged[-1]) + len(b) + 1 <= target:
            merged[-1] = merged[-1] + "\n" + b
        else:
            merged.append(b)
    return merged or [seg[:target]]


def _seg_spans_ast(seg: str) -> list[str] | None:
    """段→AST 分片(list[str]); 围栏回填(每片自带语言上下文·嵌入语义完整); 不适用→None。"""
    hint, inner = _strip_fence(seg)
    if not inner.strip() or len(inner) < AST_MIN_UNIT:
        return None
    lg = hint or detect_lang(inner)
    if not lg:
        return None
    triples = ast_spans(inner, lg)
    if not triples:
        return None
    if hint:
        return [f"```{hint}\n{p[0]}\n```" for p in triples]
    return [p[0] for p in triples]


# ── 接入 wrapper ②: k13 fingerprint 的 symbols 增补(hunk b) ────────────────────
def symbols_augment(text: str, base_symbols: set[str] | None = None) -> set[str] | None:
    """k13.fingerprint symbols 增补(hunk b 消费口): 门关/不适用/异常→None(零行为);
    否则返回 base_symbols ∪ AST 符号名(小写·含新增·不改入参)。卡文本多段代码→
    全文本+逐围栏段两路并集(确定性)。"""
    if not sym_on():
        return None
    try:
        t = str(text or "")
        if not t.strip() or len(t) > AST_MAXCHARS:
            return None
        if not ast_ready():
            return None
        names: set[str] = set()
        lg = detect_lang(t)
        if lg:
            got = ast_symbol_names(t, lg)
            if got:
                names |= {n.lower() for n in got}
        for seg in ast_segments(t):  # fence 感知: 围栏块整段(代码内空行不破坏)
            hint, inner = _strip_fence(seg)
            if not inner.strip() or len(inner) < AST_MIN_UNIT:
                continue
            lg2 = hint or detect_lang(inner)
            if not lg2:
                continue
            got = ast_symbol_names(inner, lg2)
            if got:
                names |= {n.lower() for n in got}
        if not names:
            return None
        out = set(base_symbols or set())
        out |= names
        return out
    except Exception as exc:  # noqa: BLE001
        log.warning("ast_supply symbols_augment FAIL err=%s", type(exc).__name__)
        return None
