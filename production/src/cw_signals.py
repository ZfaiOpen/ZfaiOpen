# cw_signals.py — M10 编程方言检测增敏信号源(K补位·20261025)
# ═══════════════════════════════════════════════════════════════════════════════
# 依据(消融实测20261025): Q1 全集357条纯描述 query 现版 is_coding 判入仅 58.0%
#   (漏150例: score=0 全盲63 + score=1 差1分87); 命中编程管线的队列 R@10 0.198 vs
#   未命中 0.087 —— 检测命中=增益翻倍, 盲区=白扔的分。
#
# 本模块: 纯query侧增量信号(fail-soft·确定性·零LLM·不碰租户数据), 供
#   coding_window.coding_score_v2 在现版五类标记+指纹之上加权求和。
#   每类信号独立 env 门 BRAINOS_CW_SIG_<NAME>(缺省1=开·"0"=关), 权重 env
#   BRAINOS_CW_W_<NAME>(缺省=拟合值, 见 M10 权重表)。总开关 BRAINOS_CW_V2 在
#   coding_window 侧: 缺省0=完全不引入本模块(与 integ 现版字节级等价)。
#
# 五类新信号(全部词面规则·命中记1·加权):
#   EXC  裸异常类名: [A-Z]\w*(?:Error|Exception)\b + 显式词表(Python/Java/JS常见
#        异常类)。E报告盲区主因——query 只写 "ValueError" 不带 "Traceback"/冒号时
#        现版 errsig 路径径(需 cls:msg 形态)不命中。漏判样本实测: 裸异常名9例。
#   TEST 测试框架词: pytest/unittest/doctest/fixture/parametrize/assert/test_*形/
#        unit test/test suite/regression test。
#   SWE  SWE动词短语: fix(ed|es)/implement/add support/refactor/regression/breaks/
#        fails to/crashes/workaround/deprecate/breaking change/pull request 等。
#        漏判150例中75例含此类词(现版完全不看)。
#   ENT  池内已知库/entity 名: query 词命中 k13 communities 质心 p:<tok> 反查集
#        (严格过滤: len≥4+停用词表), 或 env BRAINOS_CW_ENT_FILE 指定 JSON 词表,
#        或静态种子表(自 pool 53 repo slug 派生=数据可导出, 非手拍)。fail-soft:
#        k13 缺席/空库→退静态表→再退空(零信号)。
#   PROB 问题陈述形态: 长句(≥120字符) ∧ 行为描述标记(should/expected/supposed
#        to/but it/instead of/actually/when I/steps to reproduce/currently/
#        behavior)≥1 —— SWE-bench problem_statement 的典型形态。
#
# 权重来源(M10拟合, 非手拍): 357正(Q1) + 40负(smoke文本10+日常30) 上零依赖
#   逻辑回归 + 整数网格 {0,1,2}×TH{1,2,3} 网格扫描, FP=0 硬约束下最大判入,
#   产出 PR 曲线(out/pr_curve.json)。推荐缺省见 DEFAULT_W。
#
# 纪律: 本模块任何异常→bonus()=0(退化为现版打分); 零外部调用; 单文件零状态
#   (entity 缓存带锁+TTL); 回滚=BRAINOS_CW_V2=0 或摘 import。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import json
import os
import re
import threading
import time

# ── env 门/权重读取(每次调用读env·与 coding_window.TH() 同风格·测试可热调) ────
def _on(name: str, dft: str = "1") -> bool:
    try:
        return str(os.environ.get(name, dft)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return True


def _w(name: str, dft: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(dft)) or dft))
    except (TypeError, ValueError):
        return int(dft)


def W_EXC():  return _w("BRAINOS_CW_W_EXC", DEFAULT_W["exc"])
def W_TEST(): return _w("BRAINOS_CW_W_TEST", DEFAULT_W["test"])
def W_SWE():  return _w("BRAINOS_CW_W_SWE", DEFAULT_W["swe"])
def W_ENT():  return _w("BRAINOS_CW_W_ENT", DEFAULT_W["ent"])
def W_PROB(): return _w("BRAINOS_CW_W_PROB", DEFAULT_W["prob"])

# 权重缺省=M10拟合推荐(357正Q1 / 55负=smoke10+daily30+adv15对抗近邻·FP=0硬约束·
# 网格最优, 见 out/weight_fit.json 与 out/pr_curve.json):
#   exc=1 test=1 swe=1 ent=1 prob=2 —— TH=2(缺省不变)下: 任一新信号救回现版score=1
#   漏判(87例), 两信号组合救回score=0全盲(63例); prob加权2因"长句+行为描述"形态
#   单独即可定性(负面对抗集零命中·logreg系数最大)且单独碰撞日常英语的概率被
#   ≥120字符长度门槛压制。判入 335/357=93.8% (现版207/357=58.0%)·FP=0/55。
DEFAULT_W = {"exc": 1, "test": 1, "swe": 1, "ent": 1, "prob": 2}


# ── ① EXC 裸异常类名(无冒号/无Traceback也算) ─────────────────────────────────
_RX_CAMEL_ERR = re.compile(r"\b[A-Z][A-Za-z0-9_]*(?:Error|Exception)\b")
_EXC_EXPLICIT = (
    # Python 内建/常用
    "ValueError", "TypeError", "KeyError", "AttributeError", "IndexError",
    "NameError", "RuntimeError", "OSError", "IOError", "ImportError",
    "ModuleNotFoundError", "StopIteration", "ZeroDivisionError", "OverflowError",
    "AssertionError", "NotImplementedError", "RecursionError", "MemoryError",
    "SystemError", "SystemExit", "TimeoutError", "ConnectionError",
    "PermissionError", "FileNotFoundError", "FileExistsError", "InterruptedError",
    "UnicodeDecodeError", "UnicodeEncodeError", "UnicodeError", "ArithmeticError",
    "FloatingPointError", "StopAsyncIteration", "EOFError", "LookupError",
    "UnboundLocalError", "Warning", "DeprecationWarning", "PendingDeprecationWarning",
    # DB/API 常见
    "DatabaseError", "IntegrityError", "OperationalError", "ProgrammingError",
    "DataError", "InterfaceError", "InternalError", "PoolError",
    # Java/JS 常见
    "NullPointerException", "ArrayIndexOutOfBoundsException",
    "StringIndexOutOfBoundsException", "ClassCastException",
    "IllegalArgumentException", "IllegalStateException", "InterruptedException",
    "CloneNotSupportedException", "SQLException", "IOException",
    "ReferenceError", "SyntaxError", "RangeError", "EvalError", "URIError",
    "AggregateError", "PromiseRejectionEvent", "StackOverflowError",
    "OutOfMemoryError", "SegmentationFault",
)
_RX_EXC_EXPLICIT = re.compile(
    r"\b(?:" + "|".join(re.escape(e) for e in _EXC_EXPLICIT) + r")\b")


def sig_exc(q: str) -> int:
    return 1 if (_RX_CAMEL_ERR.search(q) or _RX_EXC_EXPLICIT.search(q)) else 0


# ── ② TEST 测试框架关键词 ────────────────────────────────────────────────────
_RX_TEST = re.compile(
    r"(?:\b(?:pytest|unittest|doctest|nose2?|tox|nox|mock|fixture|parametrize|"
    r"assertions?)\b"
    r"|\btest_[a-z_][a-z0-9_]*"
    r"|\b[a-z0-9_]+_test\b"
    r"|\bdef\s+test_"
    r"|\bunit\s+tests?\b"
    r"|\btest\s+suites?\b"
    r"|\btest\s+cases?\b"
    r"|\bregression\s+tests?\b"
    r"|\bFAIL_TO_PASS\b"
    r"|\bPASS_TO_PASS\b"
    r"|\bconftest\b"
    r"|\bci\s+fails?\b"
    r"|\bfailing\s+tests?\b"
    r"|\btests?\s+fail(?:s|ed|ing)?\b)", re.I)


def sig_test(q: str) -> int:
    return 1 if _RX_TEST.search(q) else 0


# ── ③ SWE 动词短语(问题/任务陈述动词面) ──────────────────────────────────────
_RX_SWE = re.compile(
    r"(?:\bfix(?:es|ed|ing)?\b"
    r"|\bimplement(?:s|ed|ing|ation)?\b"
    r"|\badd(?:ing)?\s+(?:support|missing|the\s+missing|a\s+new)\b"
    r"|\bsupport\s+for\b"
    r"|\brefactor(?:ing|ed)?\b"
    r"|\bregress(?:ion|ed|ing|es)?\b"
    r"|\bbreak(?:s|ing)\b"
    r"|\bbroken\b"
    r"|\bfail(?:s|ed|ing)?\s+to\b"
    r"|\bcrash(?:es|ed|ing)?\b"
    r"|\bworkaround\b"
    r"|\bdeprecat(?:e|ed|es|ing|ion)\b"
    r"|\bbackwards?\s+compatib|\bbackward\s+compatib"
    r"|\bbreaking\s+change"
    r"|\bbug\s+(?:report|fix)\b"
    r"|\bfeature\s+request\b"
    r"|\bpull\s+request\b"
    r"|\bmerge\s+conflict\b"
    r"|\bcode\s+review\b"
    r"|\bfollow.?up\s+(?:of|to)\b"
    r"|\bpatch(?:ed|ing)?\b"
    r"|\broute\s+cause\b|\broot\s+cause\b"
    r"|\bstack\s+trace\b"
    r"|\bstep(?:s)?\s+to\s+reproduce\b"
    r"|\breproduc(?:e|ed|ible|ing)\b"
    r"|\bupgrade[d]?\s+to\s+v?\d"
    r"|\bversion\s+\d+(?:\.\d+)+\s+(?:to\s+)?\d*)", re.I)


def sig_swe(q: str) -> int:
    return 1 if _RX_SWE.search(q) else 0


# ── ④ ENT 池内已知库/entity 名(k13 社区质心 p:token 反查·严格过滤) ─────────────
# 停用词表: 通用英语功能词 + 通用软件泛词(社区token里混入的目录噪声如 the/code/model)
_ENT_STOP = frozenset("""
the and that this with from have has was were will would could should been being
into over under about after before more most other some such only same than then
them they there these those what when where which while who whom whose why how
your yours our ours their theirs does done doing also just like made make makes
taking given using used uses need needs want wants good best new old all any can
get got set put run runs not but for are you your was had did its it's don't
code data model models file files line lines test tests name names value values
item items list dict cache index config content context text error errors
logger image images page pages site sites user users group groups class classes
node nodes core main base common util utils helper helpers library lib package
packages module modules bundle assets static public private src bin tmp temp
opt etc var usr home root dev null true false none nil undefined instance
diff merge patch remote origin master branch tags issue issues
project default service server client request response format template themes
learn three ruby requests framework frameworks immutable carbon fluent hugo
julius
""".split())
# 注: test/tests 已被停用表排除——TEST 信号以更强的 test_*框架词判定, 避免双计。
# 尾行=与日常英语高碰撞的池来源token(learn/three/ruby/carbon/fluent/hugo=常见词或
# 人名·requests/framework/immutable/core/bundle=泛软件词), 双来源(k13/静态)统一过滤。

_ENT_LOCK = threading.Lock()
_ENT_CACHE: dict[str, tuple[float, frozenset[str]]] = {}
_ENT_TTL_S = 300.0
_RX_WORD = re.compile(r"(?<![A-Za-z0-9_])([a-z][a-z0-9_\-]{3,})(?![A-Za-z0-9_])")
_RX_PLAIN_TOK = re.compile(r"^[a-z][a-z0-9_]{3,}$")
_RX_CODE_SHAPE = re.compile(r"[0-9_]")  # k13 token准入形状: 含数字/下划线(es2022/l10n/django_debug)
# 依据: k13 p:token 反查实测(198个全检)混入大量路径/目录/人名噪声(reference/alex/
# source/latest/admin/basic...)——纯英文形token不可直接信; 代码形状token(es2022,
# l10n, mpl_toolkits类)零日常碰撞, 准入; 库名本体由静态种子表(池repo派生)承担。


def _ent_from_file() -> frozenset[str]:
    p = os.environ.get("BRAINOS_CW_ENT_FILE", "").strip()
    if not p:
        return frozenset()
    try:
        raw = json.load(open(p, encoding="utf-8"))
        if isinstance(raw, dict):
            raw = raw.get("tokens") or []
        toks = {str(t).lower() for t in raw if _RX_PLAIN_TOK.match(str(t).lower())}
        return frozenset(t for t in toks if t not in _ENT_STOP)
    except Exception:  # noqa: BLE001
        return frozenset()


def _ent_from_static() -> frozenset[str]:
    """静态种子表: pool 53 repo slug 派生(org+name 拆词·停用词过滤·数据可导出)。"""
    try:
        import json as _json
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "cw_ent_seed.json")
        raw = _json.load(open(p, encoding="utf-8"))
        return frozenset(t for t in raw if t not in _ENT_STOP)
    except Exception:  # noqa: BLE001
        return frozenset()


def _ent_from_k13() -> frozenset[str]:
    """k13 communities p:<tok> 反查(全租户并集·一次SQL·缓存TTL·fail-soft→空)。
    准入=plain-token ∧ 停用词外 ∧ 代码形状(含数字/下划线)——拒路径/人名/目录噪声。"""
    try:
        import knife13_repo as k13  # noqa: PLC0415
        g = getattr(k13, "get", None)
        if g is None:
            return frozenset()
        inst = g()
        conn = getattr(inst, "_conn", None)
        if conn is None:
            return frozenset()
        rows = conn.execute("SELECT comm_id FROM communities").fetchall()
        toks: set[str] = set()
        for (cid,) in rows:
            c = str(cid or "")
            if c.startswith("p:"):
                t = c[2:].lower()
                if _RX_PLAIN_TOK.match(t) and _RX_CODE_SHAPE.search(t):
                    toks.add(t)
        return frozenset(t for t in toks if t not in _ENT_STOP)
    except Exception:  # noqa: BLE001
        return frozenset()


def known_entities() -> frozenset[str]:
    """entity 词表 = 静态种子(池repo派生) ∪ k13代码形状token ∪ env文件覆盖。
    三源并集·缓存TTL 300s·全源空→空集(信号退场·fail-soft)。"""
    key = "ent"
    now = time.monotonic()
    with _ENT_LOCK:
        hit = _ENT_CACHE.get(key)
        if hit and now - hit[0] < _ENT_TTL_S:
            return hit[1]
    ent = set(_ent_from_static())
    ent |= _ent_from_k13()
    ent |= _ent_from_file()
    ent = frozenset(t for t in ent if t not in _ENT_STOP)
    with _ENT_LOCK:
        _ENT_CACHE[key] = (now, ent)
    return ent


def ent_dump_file() -> str:
    """把当前生效词表落盘(权重拟合/评测取证用·幂等路径)。"""
    p = "/opt/braios/run/dev_k/m10/out/ent_effective.json"
    try:
        json.dump(sorted(known_entities()), open(p, "w", encoding="utf-8"), indent=1)
    except Exception:  # noqa: BLE001
        pass
    return p


def sig_ent(q: str) -> int:
    toks = {m.group(1) for m in _RX_WORD.finditer(str(q or "").lower())}
    if not toks:
        return 0
    ent = known_entities()
    return 1 if (toks & ent) else 0


# ── ⑤ PROB 问题陈述形态(长句+行为描述标记) ───────────────────────────────────
_PROB_MARKS = re.compile(
    r"\b(?:should|expected|supposed\s+to|but\s+(?:it|the|this|instead)|"
    r"instead\s+of|actually|when\s+(?:I|we|the|a|an|you)|"
    r"steps\s+to\s+reproduce|currently|behavior|behaviour|expectedly)\b", re.I)
_PROB_MIN_CHARS = 120


def sig_prob(q: str) -> int:
    s = str(q or "")
    return 1 if (len(s) >= _PROB_MIN_CHARS and _PROB_MARKS.search(s)) else 0


# ── 汇总: 加权增量分(coding_window.coding_score_v2 调用·fail-soft=0) ──────────
def bonus_parts(query: str) -> dict[str, int]:
    """各信号命中与加权分(取证/拟合视图·不抛异常)。"""
    q = str(query or "")
    if not q.strip():
        return {}
    parts: dict[str, int] = {}
    try:
        if _on("BRAINOS_CW_SIG_EXC"):
            parts["exc"] = sig_exc(q) * W_EXC()
    except Exception:  # noqa: BLE001
        pass
    try:
        if _on("BRAINOS_CW_SIG_TEST"):
            parts["test"] = sig_test(q) * W_TEST()
    except Exception:  # noqa: BLE001
        pass
    try:
        if _on("BRAINOS_CW_SIG_SWE"):
            parts["swe"] = sig_swe(q) * W_SWE()
    except Exception:  # noqa: BLE001
        pass
    try:
        if _on("BRAINOS_CW_SIG_ENT"):
            parts["ent"] = sig_ent(q) * W_ENT()
    except Exception:  # noqa: BLE001
        pass
    try:
        if _on("BRAINOS_CW_SIG_PROB"):
            parts["prob"] = sig_prob(q) * W_PROB()
    except Exception:  # noqa: BLE001
        pass
    return parts


def bonus(query: str) -> int:
    """v2增量分=Σ(命中×权重); 任何异常=0(退化为现版打分·fail-soft契约)。"""
    try:
        return sum(bonus_parts(query).values())
    except Exception:  # noqa: BLE001
        return 0
