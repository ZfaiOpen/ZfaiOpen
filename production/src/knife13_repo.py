# knife13_repo.py — 部署线第13刀·M1代码指纹层(ADD相指纹倒排+repo社区聚类 → SEARCH相重叠度硬加权·20261025轨道B)
# ═════════════════════════════════════════════════════════════════════════════════
# 血洞: 编程赛道query携带强代码信号(文件路径/traceback错误签名/失败测试名/构建命令),
#       纯语义嵌入对"numpy/core/src/multiarray/arrays.c"这类词面几乎零区分度——
#       "RAG for code is losing to grep"(代码检索共识2025-26·Aider repo-map 70.3%)。
# 既有件复用(对齐家规·零重复造轮):
#   knife11_forget.py = 边车骨架范本(DB路径解析/observe fail-soft/模块级单例/purge)。
#   knife3_time.py    = 纯结构化overlay纪律范本(零LLM·sqlite WAL·幂等INSERT OR IGNORE)。
#   evidence_rerank   = SEARCH相代码语境重排先例(本刀=其词面精确信号补强·erank后挂接)。
# 做法(零LLM零外部API·纯正则+sqlite·对冻结核心/organ零修改·纯overlay可整件摘除):
#   ADD相    observe(tenant, cards): 逐卡 fingerprint(text)→{paths,symbols,errsig,errcls,
#            tests,cmds,refs} 落 sqlite 倒排 fp(tenant,card_id,fp_type,fp_value);
#            卡文镜像 texts(card_id+content_hash·search相id漂移解引用·k11同病预防)。
#   社区聚类 (防numpy/pandas同域异库误伤的核心): 租户内对"路径词干+稀有符号"建社区质心
#            (文档频率<10%的符号权重高·df>50%通用词权重≈0·rarity分段线性), 卡片入最近
#            社区(cos≥ASSIGN_MIN, 否则按路径前缀种子建新社区"p:numpy"); 批后合并
#            (质心余弦≥MERGE_COS=0.3·自适应社区数·上限MAX_COMM=64)。
#   SEARCH相 search_boost(tenant, query, cand_ids, cand_texts=None)→{cid: float}:
#            query抽同款指纹→与候选卡倒排值交集→按类型加权: 路径3.0>errsig2.5>测试名
#            1.5>符号1.0>命令0.5(每类命中数设cap·errcls类名弱命中1.0); 跨社区软降权
#            ×0.4(绝不硬删)。cand_texts给定时自动做id漂移解引用(剥[fact-time]面后
#            content_hash回查·返回键=调用方原键)。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与aml_service._evidence_id同源同值
#            (knife1/2/3/11同法)。幂等: 同卡重放零增量(texts INSERT OR IGNORE rowcount=0
#            →df/质心不二次累计)。
# 挂接(3处·回滚=摘3处+删本件): ①aml_service import区 ②_knives_process加k13.observe
#   (k11块后) ③search相erank后a1r前加权块。可选第4处=startup k3.backfill同位回填。
# 持久化   knife13_repo.db (sqlite WAL + synchronous=NORMAL + busy_timeout=5000·
#           knife1/2/3/7/11同款纪律·重启不丢)。
# 合规: 日志零内容 — 只记计数/类型/id哈希; 不落query/卡文本。env总闸BRAINOS_K13(缺省0=关)。
# ═════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("knife13")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KNIFE13_DB", str(APP_DIR.parent / "data" / "knife13_repo.db")))


# ── 总闸与权重(env可调·缺省值=交付报告权重表) ─────────────────────────────────
def _master_on() -> bool:
    return os.environ.get("BRAINOS_K13", "0") == "1"          # 缺省关(评测门控纪律)

def _f(name: str, dft: str) -> float:
    try:
        return float(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return float(dft)

def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)

W_PATH  = _f("BRAINOS_K13_W_PATH", "3.0")    # 路径命中(per hit·cap=CAP_PATH)
W_ERR   = _f("BRAINOS_K13_W_ERRSIG", "2.5")  # 错误签名全sig命中(Exc类名+归一消息)
W_ERRC  = _f("BRAINOS_K13_W_ERRCLS", "1.0")  # 仅Exc类名弱命中
W_TEST  = _f("BRAINOS_K13_W_TEST", "1.5")    # 测试名命中
W_SYM   = _f("BRAINOS_K13_W_SYM", "1.0")     # 符号命中(per hit·cap=CAP_SYM)
W_CMD   = _f("BRAINOS_K13_W_CMD", "0.5")     # 构建命令命中
CAP_PATH = _i("BRAINOS_K13_CAP_PATH", "2")   # 路径命中计次上限(→路径总分max 6.0)
CAP_SYM  = _i("BRAINOS_K13_CAP_SYM", "3")    # 符号命中计次上限(→符号总分max 3.0)
CAP_ERR  = _i("BRAINOS_K13_CAP_ERR", "1")    # errsig计次上限
CAP_TEST = _i("BRAINOS_K13_CAP_TEST", "1")
CAP_CMD  = _i("BRAINOS_K13_CAP_CMD", "1")
XCOMM   = _f("BRAINOS_K13_XCOMM", "0.4")     # 跨社区软降权因子(绝不硬删)
MERGE_COS  = _f("BRAINOS_K13_MERGE_COS", "0.3")   # 社区合并阈值(质心余弦)
ASSIGN_MIN = _f("BRAINOS_K13_ASSIGN_MIN", "0.12") # 入社区最低余弦(低于→前缀种子/新社区)
QUERY_MIN  = _f("BRAINOS_K13_QUERY_MIN", "0.10")  # query入社区最低余弦
DF_RARE = _f("BRAINOS_K13_DF_RARE", "0.10")  # df/N<此值→rarity=1(稀有·权重高)
DF_ZERO = _f("BRAINOS_K13_DF_ZERO", "0.50")  # df/N≥此值→rarity=0(通用词·权重≈0)
MAX_COMM = _i("BRAINOS_K13_MAX_COMM", "64")  # 租户社区数上限(超→最小社区并最近邻)
MERGE_SEED_MIN = _f("BRAINOS_K13_MERGE_SEED_MIN", "0.75")  # 异前缀种子社区合并高门槛
                                                           # (p:numpy vs p:pandas这类
                                                           # 显式repo身份·防共享词干误并)
MAX_FP   = _i("BRAINOS_K13_MAX_FP", "64")    # 单卡单类型指纹值上限(防大diff爆库)

# ── 指纹抽取正则(零LLM·确定性) ───────────────────────────────────────────────
# 路径: ①带目录的路径(≥1斜杠+字母扩展名) ②裸文件名(白名单扩展) ③无扩展名特殊文件名。
# 起始lookbehind防版本号/URL主机误切(?<![\w@.:-]·允许"/"后起=URL内路径可截取)。
_RX_PATH = re.compile(r"(?<![\w@.:-])(?:[A-Za-z0-9_\-.]+/){1,8}[A-Za-z0-9_\-.]+\.[A-Za-z]{1,8}\b")
_RX_BARE = re.compile(
    r"(?<![\w@.:/-])[A-Za-z0-9_\-]+\.(?:py|js|mjs|ts|tsx|jsx|go|rs|c|h|cpp|cc|hpp|hh|java|rb|sh"
    r"|bash|yaml|yml|toml|json|cfg|ini|proto|sql|md|rst|lock)\b")
_RX_NOEXT = re.compile(r"(?<![\w@.:/-])(Makefile|Dockerfile|Rakefile|Gemfile|Cargo\.toml|go\.mod)\b")
# 符号: def/class(含缩进方法)/function/const/import模块(JS const+py import·各取标识符)
_RX_DEF = re.compile(r"(?m)^[ \t]*(?:async\s+)?def\s+([A-Za-z_]\w*)")
_RX_CLS = re.compile(r"(?m)^[ \t]*(?:abstract\s+)?class\s+([A-Za-z_]\w*)")
_RX_FUNC = re.compile(r"\bfunction\s+([A-Za-z_$][\w$]*)")
_RX_CONST = re.compile(r"\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=[^=]")
_RX_IMP = re.compile(r"(?m)^[ \t]*import\s+([A-Za-z_][\w.]*)")
_RX_FROM = re.compile(r"(?m)^[ \t]*from\s+([A-Za-z_][\w.]*)\s+import\b")
# 错误签名: Exception类名(后缀词表) + 消息前80字符归一化; assert失败文本
_RX_EXC = re.compile(r"(?<![\w.])([A-Za-z_]\w*(?:Error|Exception|Warning|Interrupt|Exit|Failure"
                     r"|Fault|Timeout|Iteration|NotFound|Mismatch))\b\s*[:：]?\s*([^\n]{0,120})")
_RX_ASSERT = re.compile(r"(?im)^[ \t]*(?:assertionerror|assertion failed|assert)\s*[:：]?\s*(.{4,120})$")
# 测试名: test_*/it_*/*_test (jest/go惯例)
_RX_TEST = re.compile(r"\b(test_[A-Za-z_]\w{1,60}|it_[A-Za-z_]\w{1,60}|[A-Za-z_]\w{1,60}_test)\b")
# 构建命令: 行首(可选$提示符)工具+实参(截120字符)
_RX_CMD = re.compile(r"(?m)(?:^|\n)[ \t]*\$?[ \t]*(pytest|python[23]?\s+-m\s+pytest|npm|yarn|cargo"
                     r"|go|make|cmake|pip3?|python[23]?)\s+([^\n]{0,120})")
# 引用: Fixes/Closes/Resolves/References/see + #N; GitHub URL /issues/N /pull/N
_RX_REF = re.compile(r"\b(?:[Ff]ix(?:e[sd])?|[Cc]lose[sd]?|[Rr]esolve[sd]?|"
                     r"[Rr]ef(?:erence[sd])?|see|[Ii]ssue)\s*#?(\d{1,7})\b")
_RX_REF_URL = re.compile(r"github\.com/[^\s\"']*?/(?:issues|pull)/(\d{1,7})", re.I)
_RX_REF_BARE = re.compile(r"(?<![\w#])#(\d{1,7})\b")   # 仅query侧用(召回优先·卡侧关键词门控保精确)

# 路径分词停用组件(通用目录名·无区分度·不入指纹/社区/df)
_PATH_STOP = frozenset((
    "src", "lib", "libs", "test", "tests", "core", "internal", "include", "doc", "docs",
    "example", "examples", "scripts", "script", "main", "master", "blob", "tree", "raw",
    "app", "apps", "pkg", "bin", "build", "dist", "github.com", "www", "com", "http",
    "https", "v1", "v2", "user", "users", "home", "repo", "trunk", "head"))
_CMD_STOP_TOOL = ("python", "python2", "python3")  # python裸调用不算构建命令(pytest -m除外)


def _norm_msg(s: str) -> str:
    """错误消息归一: 小写+压空白+剥首尾标点+截80字符。"""
    return re.sub(r"\s+", " ", str(s or "").strip().strip(":：.,;'\"")).lower()[:80]


def _path_tokens(path: str) -> set[str]:
    """路径→指纹token集{全路径, 文件词干, 特定目录组件}(小写·停用词剔除·len≥3)。
    git diff前缀a//b//相对./剥除(全路径token与真实路径对齐)。"""
    p = path.strip().lower().rstrip("/")
    while p[:2] in ("a/", "b/", "./"):
        p = p[2:]
    out = {p} if "/" in p else set()
    parts = [x for x in p.split("/") if x]
    stem = parts[-1].rsplit(".", 1)[0] if parts else ""
    if stem and len(stem) >= 3 and stem not in _PATH_STOP:
        out.add(stem)
    for c in parts[:-1]:
        if len(c) >= 3 and c not in _PATH_STOP and "." not in c:
            out.add(c)
    return out


def fingerprint(text: str) -> dict[str, list[str]]:
    """卡文本→代码指纹{paths,symbols,errsig,errcls,tests,cmds,refs}(纯函数·零LLM·
    脏输入→空列表fail-soft绝不抛)。全部值小写归一; paths=全路径+词干+特定目录组件。"""
    t = str(text or "")
    if not t or len(t) > 400_000:  # 超长 defensive 截断(单卡指纹有上限·防爆库)
        t = t[:200_000] if t else ""
        if not t:
            return {"paths": [], "symbols": [], "errsig": [], "errcls": [],
                    "tests": [], "cmds": [], "refs": []}
    paths: set[str] = set()
    for m in _RX_PATH.finditer(t):
        paths |= _path_tokens(m.group(0))
    for m in _RX_BARE.finditer(t):
        paths |= _path_tokens(m.group(0))
    for m in _RX_NOEXT.finditer(t):
        paths |= _path_tokens(m.group(0).lower())
    symbols: set[str] = set()
    for rx in (_RX_DEF, _RX_CLS, _RX_FUNC, _RX_CONST):
        for m in rx.finditer(t):
            symbols.add(m.group(1).lower())
    for m in _RX_IMP.finditer(t):
        symbols.add(m.group(1).lower())
    for m in _RX_FROM.finditer(t):
        symbols.add(m.group(1).lower())
    symbols -= {""}
    # ── U7 hunk b: AST 符号增补(BRAINOS_AST=1 且 BRAINOS_AST_SYM=1 才激活·缺省关=
    #    零行为; go/rust/java 方法签名+js/ts 类方法=正则盲区, tree-sitter 补采;
    #    AST 不可用/解析失败→None→纯正则原样·fail-soft 绝不阻塞) ──
    if os.environ.get("BRAINOS_AST", "0") == "1" and \
            os.environ.get("BRAINOS_AST_SYM", "0") == "1":
        try:
            import ast_supply
            _aug = ast_supply.symbols_augment(t, symbols)
            if _aug is not None:
                symbols = _aug
        except Exception:  # noqa: BLE001 — 任何意外→纯正则原样
            pass
    errsig: set[str] = set()
    errcls: set[str] = set()
    for m in _RX_EXC.finditer(t):
        cls = m.group(1)
        errcls.add(cls.lower())
        msg = _norm_msg(m.group(2))
        if msg:
            errsig.add(f"{cls.lower()}:{msg}")
    for m in _RX_ASSERT.finditer(t):
        msg = _norm_msg(m.group(1))
        if msg:
            errsig.add(f"assert:{msg}")
            errcls.add("assert")
    tests: set[str] = {m.group(1).lower() for m in _RX_TEST.finditer(t)}
    cmds: set[str] = set()
    for m in _RX_CMD.finditer(t):
        tool = re.sub(r"\s+", " ", m.group(1)).strip().lower()
        args = re.sub(r"\s+", " ", m.group(2)).strip().lower()
        if tool.split()[0] in _CMD_STOP_TOOL:
            continue  # "python foo.py"非幂命令; "python -m pytest"已由pytest分支收
        cmds.add(f"{tool}|{args}"[:160])
    refs: set[str] = set()
    for m in _RX_REF.finditer(t):
        refs.add(f"#{m.group(1)}")
    for m in _RX_REF_URL.finditer(t):
        refs.add(f"#{m.group(1)}")
    _cap = lambda s: sorted(s)[:MAX_FP]  # noqa: E731
    return {"paths": _cap(paths), "symbols": _cap(symbols), "errsig": _cap(errsig),
            "errcls": _cap(errcls), "tests": _cap(tests), "cmds": _cap(cmds), "refs": _cap(refs)}


def query_refs(text: str) -> list[str]:
    """query侧引用号抽取(召回优先): 关键词门控 + GitHub URL + 裸#N。citeref消费。"""
    t = str(text or "")
    out: set[str] = set()
    for m in _RX_REF.finditer(t):
        out.add(f"#{m.group(1)}")
    for m in _RX_REF_URL.finditer(t):
        out.add(f"#{m.group(1)}")
    for m in _RX_REF_BARE.finditer(t):
        out.add(f"#{m.group(1)}")
    return sorted(out)


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16]·k1/2/3/11同法)。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


# organ卡面"[fact-time ...]"前缀(W2-1双时间轴)→窗id与镜像id漂移·剥除后content_hash回查
_FACE_PREFIX_RE = re.compile(r"^\[fact-time [^\]]*\]\s*")


def content_hash_of(content: str) -> str:
    """窗卡content→镜像回查键(剥fact-time面·小写归一空白的原文sha1[:16])。"""
    core = _FACE_PREFIX_RE.sub("", str(content or "").strip(), count=1).strip()
    return hashlib.sha1(core.encode("utf-8")).hexdigest()[:16]


def _cos(a: dict[str, float], b: dict[str, float]) -> float:
    """稀疏dict余弦(空向量=0)。"""
    if not a or not b:
        return 0.0
    small, big = (a, b) if len(a) <= len(b) else (b, a)
    dot = 0.0
    for k, v in small.items():
        w = big.get(k)
        if w:
            dot += v * w
    if dot <= 0.0:
        return 0.0
    na = sum(v * v for v in a.values()) ** 0.5
    nb = sum(v * v for v in b.values()) ** 0.5
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (na * nb)


# ── 持久化(sqlite WAL·knife1/2/3/7/11同款纪律) ───────────────────────────────
class Knife13:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            # 卡文镜像(search相id漂移解引用·k11 card_texts存文先例);
            # [M1.5·20261025] text列=指纹直召回通道全文(observe时写入·旧库ALTER补列)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS texts(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, content_hash TEXT NOT NULL,
                sess TEXT NOT NULL, dt TEXT NOT NULL, text TEXT NOT NULL DEFAULT '',
                obs_ms INTEGER NOT NULL,
                PRIMARY KEY(tenant, card_id))""")
            try:
                self._conn.execute("ALTER TABLE texts ADD COLUMN text TEXT NOT NULL DEFAULT ''")
            except Exception:  # noqa: BLE001 — 列已存在(新库CREATE已含/旧库已ALTER)
                pass
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_k13_hash ON texts(tenant, content_hash)")
            # 指纹倒排(PK=正排·idx=倒排)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS fp(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, fp_type TEXT NOT NULL,
                fp_value TEXT NOT NULL, PRIMARY KEY(tenant, card_id, fp_type, fp_value))""")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_k13_inv ON fp(tenant, fp_type, fp_value)")
            # token文档频率(社区rarity权重源)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS dfc(
                tenant TEXT NOT NULL, token TEXT NOT NULL, n INTEGER NOT NULL,
                PRIMARY KEY(tenant, token))""")
            # 租户计数(k='cards'→N·rarity分母)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS counters(
                tenant TEXT NOT NULL, k TEXT NOT NULL, v INTEGER NOT NULL,
                PRIMARY KEY(tenant, k))""")
            # 社区质心(JSON稀疏{token:加权和}+成员数) + 卡→社区归属
            self._conn.execute("""CREATE TABLE IF NOT EXISTS communities(
                tenant TEXT NOT NULL, comm_id TEXT NOT NULL, centroid TEXT NOT NULL,
                n INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
                PRIMARY KEY(tenant, comm_id))""")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS card_comm(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, comm_id TEXT NOT NULL,
                PRIMARY KEY(tenant, card_id))""")
            self._conn.commit()
        except Exception as exc:  # noqa: BLE001 — DB损坏/只读→本刀整体no-op(服务不死)
            self._conn = None
            log.error("knife13 init FAILED db=%s err=%s", self.db_path, type(exc).__name__)
        self.n_obs = 0        # 观察卡数(累计)
        self.n_new = 0        # 新卡(首见)数
        self.n_fp = 0         # 新指纹行数
        self.n_boost = 0      # search相加权查询次数
        self.n_hit = 0        # boost非零卡次
        self.n_recall = 0     # 直召回返回卡次(M1.5)
        self.n_err = 0

    # ── 内部小工具(全部容忍_conn=None) ──
    def _q(self, sql: str, args: tuple = ()) -> list[tuple]:
        with self._lock:
            return list(self._conn.execute(sql, args).fetchall())

    def _x(self, sql: str, args: tuple = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
            return cur.rowcount

    # ── rarity: token文档频率→权重因子(稀有=1·通用→0) ──
    def _rarity(self, tenant: str, tokens: list[str]) -> dict[str, float]:
        n_cards = 0
        for (v,) in self._q("SELECT v FROM counters WHERE tenant=? AND k='cards'", (tenant,)):
            n_cards = int(v)
        if n_cards < 4:
            return {t: 1.0 for t in tokens}
        out: dict[str, float] = {}
        for i in range(0, len(tokens), 400):
            chunk = tokens[i:i + 400]
            sql = ("SELECT token, n FROM dfc WHERE tenant=? AND token IN ("
                   + ",".join("?" * len(chunk)) + ")")
            df = {str(r[0]): int(r[1]) for r in self._q(sql, (tenant, *chunk))}
            for t in chunk:
                ratio = df.get(t, 0) / float(n_cards)
                if ratio < DF_RARE:
                    out[t] = 1.0
                elif ratio >= DF_ZERO:
                    out[t] = 0.0
                else:
                    out[t] = 1.0 - (ratio - DF_RARE) / (DF_ZERO - DF_RARE)
        return out

    def _card_vec(self, tenant: str, fp: dict[str, list[str]]) -> dict[str, float]:
        """指纹→社区向量: 路径token(词干+目录)×2.0 + 符号/测试名×1.0, 各乘rarity。"""
        toks = [t for t in fp.get("paths", []) if "/" not in t]  # 全路径串不入向量
        syms = list(fp.get("symbols", [])) + list(fp.get("tests", []))
        rar = self._rarity(tenant, toks + syms)
        vec: dict[str, float] = {}
        for t in toks:
            w = 2.0 * rar.get(t, 1.0)
            if w > 0:
                vec[t] = w
        for s in syms:
            w = 1.0 * rar.get(s, 1.0)
            if w > 0:
                vec[s] = max(vec.get(s, 0.0), w)
        return vec

    def _df_bump(self, tenant: str, tokens: list[str], n: int = 1) -> None:
        with self._lock:
            for t in tokens:
                self._conn.execute(
                    "INSERT INTO dfc VALUES(?,?,?) ON CONFLICT(tenant,token)"
                    " DO UPDATE SET n=n+?", (tenant, t, n, n))
            self._conn.execute(
                "INSERT INTO counters VALUES(?,'cards',?) ON CONFLICT(tenant,k)"
                " DO UPDATE SET v=v+?", (tenant, n, n))
            self._conn.commit()

    # ── ADD相: 指纹抽取+倒排+社区归属 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]] | None) -> int:
        """逐卡观察: 镜像+指纹倒排+df+社区。fail-soft: 单卡失败只计数不抛(绝不影响add
        成功语义)。幂等: 同卡重放零增量(rowcount=0→df/质心不二次累计)。返回新卡数。"""
        if not cards or not _master_on() or self._conn is None:
            return 0
        return self._observe_batch(str(tenant), cards)

    def _observe_batch(self, tenant: str, cards: list[dict[str, Any]]) -> int:
        n_new = 0
        for c in cards:
            try:
                if self._observe_card(tenant, c):
                    n_new += 1
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("knife13 observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        if n_new:
            try:
                self._merge_pass(tenant)
            except Exception as exc:  # noqa: BLE001
                self.n_err += 1
                log.error("knife13 merge FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        self.n_new += n_new
        return n_new

    def _observe_card(self, tenant: str, c: dict[str, Any]) -> bool:
        raw = c.get("text")
        if not isinstance(raw, str) or not raw:  # 脏text(非str/空)→跳过(禁str(123)="123"脏行)
            return False
        text = raw
        sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
        cid = card_id_of(sess, dt, text)
        now_ms = int(time.time() * 1000)
        chash = hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO texts(tenant,card_id,content_hash,sess,dt,text,obs_ms)"
                " VALUES(?,?,?,?,?,?,?)",
                (tenant, cid, chash, sess, dt, text, now_ms))
            self._conn.commit()
        if cur.rowcount == 0:
            return False  # 重放卡: 零增量直通
        fp = fingerprint(text)
        with self._lock:
            for ftype in ("paths", "symbols", "errsig", "errcls", "tests", "cmds", "refs"):
                for v in fp.get(ftype, ()):
                    self._conn.execute("INSERT OR IGNORE INTO fp VALUES(?,?,?,?)",
                                       (tenant, cid, ftype, v))
            self._conn.commit()
        self.n_fp += sum(len(fp.get(k, ())) for k in
                         ("paths", "symbols", "errsig", "errcls", "tests", "cmds", "refs"))
        # df/计数在新卡路径上bump一次(幂等)
        df_tokens = [t for t in fp.get("paths", []) if "/" not in t] \
            + list(fp.get("symbols", [])) + list(fp.get("tests", []))
        if df_tokens:
            self._df_bump(tenant, df_tokens)
        else:
            self._count_bump(tenant)
        # 社区归属
        vec = self._card_vec(tenant, fp)
        if vec:
            self._assign_community(tenant, cid, vec, fp)
        return True

    def _count_bump(self, tenant: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO counters VALUES(?,'cards',1) ON CONFLICT(tenant,k)"
                " DO UPDATE SET v=v+1", (tenant,))
            self._conn.commit()

    def _seed_of(self, fp: dict[str, list[str]]) -> str:
        """路径前缀种子: 第一个带目录全路径的首个特定组件(无路径→'')。"""
        for p in fp.get("paths", []):
            if "/" in p:
                for c in p.split("/"):
                    if len(c) >= 3 and c not in _PATH_STOP and "." not in c:
                        return c
        return ""

    def _assign_community(self, tenant: str, cid: str, vec: dict[str, float],
                          fp: dict[str, list[str]]) -> None:
        """归属规则(路径前缀种子粘性优先): ①卡有seed s且"p:s"已存在→直入(初始按路径
        前缀聚类·repo身份是强信号); ②否则余弦argmax≥ASSIGN_MIN→入; ③有seed→建"p:s";
        ④无seed→新建"c{n}"。合并阶段再自适应(异前缀种子社区受MERGE_SEED_MIN高门槛保护)。"""
        rows = self._q("SELECT comm_id, centroid, n FROM communities WHERE tenant=?", (tenant,))
        cents = {str(r[0]): (json.loads(str(r[1]) or "{}"), int(r[2])) for r in rows}
        best_id, best_cos = "", 0.0
        for comm_id in sorted(cents):
            c = _cos(vec, cents[comm_id][0])
            if c > best_cos:
                best_id, best_cos = comm_id, c
        seed = self._seed_of(fp)
        if seed:
            # 路径前缀种子=repo身份强信号: 除非与既有社区近重复(cos≥MERGE_SEED_MIN),
            # 否则一律入/建自己的种子社区(numpy/pandas式异库不被共享词干吸并)。
            if best_id and best_cos >= MERGE_SEED_MIN:
                target = best_id
            else:
                target = f"p:{seed}"
        elif best_cos >= ASSIGN_MIN:
            target = best_id
        else:
            target = f"c{len(cents) + 1}"
        with self._lock:
            row = self._conn.execute(
                "SELECT centroid, n FROM communities WHERE tenant=? AND comm_id=?",
                (tenant, target)).fetchone()
            if row:
                cent = json.loads(str(row[0]) or "{}")
                n = int(row[1]) + 1
                for k, v in vec.items():
                    cent[k] = round(float(cent.get(k, 0.0)) + float(v), 6)
                self._conn.execute(
                    "UPDATE communities SET centroid=?, n=?, updated_ms=?"
                    " WHERE tenant=? AND comm_id=?",
                    (json.dumps(cent, sort_keys=True), n, int(time.time() * 1000), tenant, target))
            else:
                self._conn.execute(
                    "INSERT INTO communities VALUES(?,?,?,?,?)",
                    (tenant, target, json.dumps(vec, sort_keys=True), 1, int(time.time() * 1000)))
            self._conn.execute("INSERT OR REPLACE INTO card_comm VALUES(?,?,?)",
                               (tenant, cid, target))
            self._conn.commit()

    def _merge_pass(self, tenant: str) -> None:
        """社区自适应合并: 质心余弦≥MERGE_COS迭代合并(小并大); 超MAX_COMM强并最小→最近邻。"""
        rows = self._q("SELECT comm_id, centroid, n FROM communities WHERE tenant=?", (tenant,))
        cents = {str(r[0]): (json.loads(str(r[1]) or "{}"), int(r[2])) for r in rows}
        for _ in range(64):
            if len(cents) <= 1:
                return
            best: tuple[float, str, str] | None = None
            ids = sorted(cents)
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    c = _cos(cents[ids[i]][0], cents[ids[j]][0])
                    thr = MERGE_COS
                    if (ids[i].startswith("p:") and ids[j].startswith("p:")
                            and ids[i] != ids[j]):
                        thr = MERGE_SEED_MIN  # 异repo前缀种子社区: 高门槛防共享词干误并
                    if c >= thr and (best is None or c > best[0]):
                        best = (c, ids[i], ids[j])
            if best is None:
                break
            _c, a, b = best
            if cents[a][1] < cents[b][1]:
                a, b = b, a  # 小并大
            cent = dict(cents[a][0])
            for k, v in cents[b][0].items():
                cent[k] = round(float(cent.get(k, 0.0)) + float(v), 6)
            cents[a] = (cent, cents[a][1] + cents[b][1])
            del cents[b]
            self._merge_commit(tenant, a, b, cent, cents[a][1])
        # 上限强并
        while len(cents) > MAX_COMM:
            smallest = min(sorted(cents), key=lambda k: cents[k][1])
            rest = [k for k in sorted(cents) if k != smallest]
            nearest = max(rest, key=lambda k: (_cos(cents[smallest][0], cents[k][0]), k))
            cent = dict(cents[nearest][0])
            for k, v in cents[smallest][0].items():
                cent[k] = round(float(cent.get(k, 0.0)) + float(v), 6)
            cents[nearest] = (cent, cents[nearest][1] + cents[smallest][1])
            del cents[smallest]
            self._merge_commit(tenant, nearest, smallest, cent, cents[nearest][1])

    def _merge_commit(self, tenant: str, keep: str, gone: str,
                      cent: dict[str, float], n: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE communities SET centroid=?, n=?, updated_ms=? WHERE tenant=? AND comm_id=?",
                (json.dumps(cent, sort_keys=True), n, int(time.time() * 1000), tenant, keep))
            self._conn.execute("DELETE FROM communities WHERE tenant=? AND comm_id=?",
                               (tenant, gone))
            self._conn.execute("UPDATE card_comm SET comm_id=? WHERE tenant=? AND comm_id=?",
                               (keep, tenant, gone))
            self._conn.commit()

    # ── 启动相: 存量全量回填(幂等·可选挂接) ──
    def backfill(self, organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
        """organ存量卡回填(k3.backfill同法: conv冒号前=tenant)。幂等·返回(卡数,新卡,ms)。"""
        t0 = time.time()
        if self._conn is None:
            return (0, 0, 0.0)
        by_t: dict[str, list[dict[str, Any]]] = {}
        for c in organ_cards or []:
            try:
                if not isinstance(c, dict) or not str(c.get("text") or ""):
                    continue
                tn = str(c.get("conv") or "").split(":", 1)[0] or "u_default"
                by_t.setdefault(tn, []).append(c)
            except Exception:  # noqa: BLE001
                continue
        n_c = 0
        for tn, cs in sorted(by_t.items()):
            n_c += self._observe_batch(tn, cs)  # 直走批内路径(不吃总闸·恢复既有索引)
        return (sum(len(v) for v in by_t.values()), n_c, (time.time() - t0) * 1000)

    # ── SEARCH相 ──
    def resolve_keys(self, tenant: str, cand_ids: list[str],
                     cand_texts: dict[str, str] | None = None) -> dict[str, str]:
        """调用方窗卡键→镜像card_id(id直中; 否则剥fact-time面content_hash回查; 均不中=
        原键透传)。fail-soft: 任何异常→恒等映射。"""
        ident = {str(c): str(c) for c in cand_ids if c}
        try:
            if self._conn is None:
                return ident
            tn = str(tenant)
            out: dict[str, str] = {}
            hashes: dict[str, list[str]] = {}
            for cid in ident:
                if self._q("SELECT 1 FROM texts WHERE tenant=? AND card_id=? LIMIT 1",
                           (tn, cid)):
                    out[cid] = cid
                elif cand_texts:
                    h = content_hash_of(cand_texts.get(cid, ""))
                    if h:
                        hashes.setdefault(h, []).append(cid)
            for h, cids in hashes.items():
                rows = self._q("SELECT card_id FROM texts WHERE tenant=? AND content_hash=?"
                               " LIMIT 1", (tn, h))
                if rows:
                    for cid in cids:
                        out[cid] = str(rows[0][0])
            for cid in ident:
                out.setdefault(cid, cid)
            return out
        except Exception:  # noqa: BLE001
            return ident

    def _fps_of(self, tenant: str, card_ids: list[str]) -> dict[str, dict[str, list[str]]]:
        """镜像id→{fp_type:[values]}(分块IN)。"""
        out: dict[str, dict[str, list[str]]] = {}
        ids = sorted({c for c in card_ids if c})
        for i in range(0, len(ids), 300):
            chunk = ids[i:i + 300]
            sql = ("SELECT card_id, fp_type, fp_value FROM fp WHERE tenant=? AND card_id IN ("
                   + ",".join("?" * len(chunk)) + ")")
            for cid, ftype, val in self._q(sql, (tenant, *chunk)):
                out.setdefault(str(cid), {}).setdefault(str(ftype), []).append(str(val))
        return out

    def communities_of(self, tenant: str, card_ids: list[str]) -> dict[str, str]:
        """镜像id→comm_id(缺省无归属=空dict项省略)。citeref联合校验消费。"""
        out: dict[str, str] = {}
        ids = sorted({str(c) for c in card_ids if c})
        try:
            for i in range(0, len(ids), 300):
                chunk = ids[i:i + 300]
                sql = ("SELECT card_id, comm_id FROM card_comm WHERE tenant=? AND card_id IN ("
                       + ",".join("?" * len(chunk)) + ")")
                for cid, comm in self._q(sql, (str(tenant), *chunk)):
                    out[str(cid)] = str(comm)
        except Exception:  # noqa: BLE001
            return {}
        return out

    def communities_of_sess(self, tenant: str, sess: str, cap: int = 48) -> dict[str, str]:
        """[R1·2026-10-28] 源会话→{card_id: comm_id}: mirror texts 按 sess 反查卡id
        →communities_of。英雄卡社区门(deep_expdistill.comm_filter)消费——英雄卡是
        蒸馏新卡不在k13索引, 但其源会话的原文卡在add相已被k13.observe镜像, sess即
        源任务身份(记分板 sess=instance_id·生产 sess=会话id 同构可用)。
        fail-soft: 库坏/空sess/无镜像→{}(调用方按"无社区=放行"兼容)。"""
        try:
            if self._conn is None or not str(sess or "").strip():
                return {}
            ids = [str(r[0]) for r in self._q(
                "SELECT card_id FROM texts WHERE tenant=? AND sess=? LIMIT ?",
                (str(tenant), str(sess), max(1, int(cap))))]
            return self.communities_of(str(tenant), ids) if ids else {}
        except Exception:  # noqa: BLE001 — fail-soft: 无社区=放行
            return {}

    def query_community(self, tenant: str, query: str) -> str:
        """query→最近社区质心(cos≥QUERY_MIN)·无→''(退化路径)。
        快路径: query词面含种子token(如"proja")且社区"p:proja"存在→直入
        (裸库名无路径形态·强repo身份信号·citeref联合校验依赖此路)。"""
        try:
            if self._conn is None:
                return ""
            tn = str(tenant)
            rows = self._q("SELECT comm_id, centroid FROM communities WHERE tenant=?", (tn,))
            if not rows:
                return ""
            q = str(query or "").lower()
            q_words = set(re.findall(r"(?<![a-z0-9_])[a-z_][a-z0-9_\-]{2,}", q))
            for comm_id, _cent in sorted(rows, key=lambda r: str(r[0])):
                name = str(comm_id)
                if name.startswith("p:") and name[2:] in q_words:
                    return name
            fp = fingerprint(q)
            vec = self._card_vec(tn, fp)
            if not vec:
                return ""
            best_id, best_cos = "", 0.0
            for comm_id, cent in sorted(rows, key=lambda r: str(r[0])):
                c = _cos(vec, json.loads(str(cent) or "{}"))
                if c > best_cos:
                    best_id, best_cos = str(comm_id), c
            return best_id if best_cos >= QUERY_MIN else ""
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _cmd_match(a: str, b: str) -> bool:
        """命令匹配: 全等 或 同工具且实参token交≥1。"""
        if a == b:
            return True
        try:
            ta, aa = a.split("|", 1)
            tb, ab = b.split("|", 1)
            if ta != tb:
                return False
            sa, sb = set(aa.split()), set(ab.split())
            return bool(sa & sb)
        except ValueError:
            return False

    @staticmethod
    def _err_match(q_sigs: set[str], c_sigs: set[str]) -> int:
        """errsig前缀匹配数: q与卡sig互为前缀(短者≥10字符·"cls:msg"形态)即命中
        (归一化前80字符设计的本意——query与卡消息尾部差异不断开匹配)。"""
        n = 0
        for q in q_sigs:
            for c in c_sigs:
                s, l = (q, c) if len(q) <= len(c) else (c, q)
                if len(s) >= 10 and l.startswith(s):
                    n += 1
                    break
        return n

    def search_boost(self, tenant: str, query: str, cand_ids: list[str],
                     cand_texts: dict[str, str] | None = None) -> dict[str, float]:
        """query指纹×候选卡倒排→{调用方卡键: 加权分}。权重: 路径{W_PATH}>errsig{W_ERR}>
        测试{W_TEST}>符号{W_SYM}>命令{W_CMD}(每类cap); errcls弱命中{W_ERRC}; 跨社区软降权
        ×{XCOMM}(绝不硬删)。零命中/门关/异常→{}(fail-soft)。"""
        try:
            if not _master_on() or self._conn is None or not cand_ids or not query:
                return {}
            tn = str(tenant)
            resolved = self.resolve_keys(tn, cand_ids, cand_texts)
            mirror_ids = sorted({v for v in resolved.values() if v})
            if not mirror_ids:
                return {}
            qfp = fingerprint(str(query))
            q_paths = set(qfp.get("paths", ()))
            q_sym = set(qfp.get("symbols", ()))
            q_err = set(qfp.get("errsig", ()))
            q_errc = set(qfp.get("errcls", ()))
            q_test = set(qfp.get("tests", ()))
            q_cmd = set(qfp.get("cmds", ()))
            if not (q_paths or q_sym or q_err or q_errc or q_test or q_cmd):
                return {}  # 纯描述query退化路径: 零指纹→零加权(不崩)
            fps = self._fps_of(tn, mirror_ids)
            if not fps:
                return {}
            q_comm = self.query_community(tn, str(query))
            comms = self.communities_of(tn, mirror_ids) if q_comm else {}
            scores: dict[str, float] = {}
            for mid in mirror_ids:
                cfp = fps.get(mid, {})
                n_path = len(q_paths & set(cfp.get("paths", ())))
                n_sym = len(q_sym & set(cfp.get("symbols", ())))
                n_err = self._err_match(q_err, set(cfp.get("errsig", ())))
                n_errc = len((q_errc & set(cfp.get("errcls", ()))) - {"assert"}) \
                    if not n_err else 0
                n_test = len(q_test & set(cfp.get("tests", ())))
                n_cmd = sum(1 for qc in q_cmd
                            if any(self._cmd_match(qc, cc) for cc in cfp.get("cmds", ())))
                s = (W_PATH * min(n_path, CAP_PATH) + W_ERR * min(n_err, CAP_ERR)
                     + W_ERRC * min(n_errc, 1) + W_TEST * min(n_test, CAP_TEST)
                     + W_SYM * min(n_sym, CAP_SYM) + W_CMD * min(n_cmd, CAP_CMD))
                if s <= 0.0:
                    continue
                if q_comm:
                    c_comm = comms.get(mid)
                    if c_comm and c_comm != q_comm:
                        s *= XCOMM  # 跨社区软降权(绝不硬删)
                scores[mid] = round(s, 4)
            if not scores:
                return {}
            self.n_boost += 1
            self.n_hit += len(scores)
            return {cid: scores[mid] for cid, mid in resolved.items() if mid in scores}
        except Exception as exc:  # noqa: BLE001 — 加权失败=原序直通(服务不死)
            self.n_err += 1
            log.error("knife13 search_boost FAILED err=%s", type(exc).__name__)
            return {}

    # ── SEARCH相·直召回通道(M1.5·治"gold不在organ候选池=boost无效"结构病) ──
    def recall_by_fingerprint(self, tenant: str, query: str,
                              cap: int = 40) -> list[dict[str, Any]]:
        """query指纹→fp倒排直查卡→按boost同款权重排序→返回完整卡列表
        [{id,content,dt,sess,score}](cap默认40)。零指纹query→[](不硬塞·fail-soft)。
        排序权重=search_boost同款(路径>errsig>测试>符号>命令·errcls弱命中·跨社区
        ×XCOMM软降权)。texts.text列为空(旧库行)的卡不参与直召回。"""
        try:
            if not _master_on() or self._conn is None or not query:
                return []
            qfp = fingerprint(str(query))
            q_paths = set(qfp.get("paths", ()))
            q_sym = set(qfp.get("symbols", ()))
            q_err = set(qfp.get("errsig", ()))
            q_errc = set(qfp.get("errcls", ()))
            q_test = set(qfp.get("tests", ()))
            q_cmd = set(qfp.get("cmds", ()))
            if not (q_paths or q_sym or q_err or q_errc or q_test or q_cmd):
                return []  # 纯描述query: 零指纹→零直召回(不硬塞)
            tn = str(tenant)
            # 倒排直查: 每类型精确命中的card_id集(errsig另做前缀重算·见下)
            per_type_cands: dict[str, set[str]] = {}
            for ftype, qvals in (("paths", q_paths), ("symbols", q_sym), ("errsig", q_err),
                                 ("errcls", q_errc), ("tests", q_test), ("cmds", q_cmd)):
                vals = sorted(qvals)
                ids: set[str] = set()
                for i in range(0, len(vals), 300):
                    chunk = vals[i:i + 300]
                    sql = ("SELECT card_id FROM fp WHERE tenant=? AND fp_type=? AND fp_value IN ("
                           + ",".join("?" * len(chunk)) + ")")
                    ids |= {str(r[0]) for r in self._q(sql, (tn, ftype, *chunk))}
                if ids:
                    per_type_cands[ftype] = ids
            if not per_type_cands:
                return []
            all_ids = set().union(*per_type_cands.values())
            # 候选卡指纹重取→与search_boost完全同款匹配语义计分(errsig前缀匹配/
            # errcls弱命中/命令工具+实参交·cap同) — 候选池由精确IN+errcls类名保证覆盖
            fps_all = self._fps_of(tn, sorted(all_ids))
            scores: dict[str, float] = {}
            for mid, cfp in fps_all.items():
                n_path = len(q_paths & set(cfp.get("paths", ())))
                n_sym = len(q_sym & set(cfp.get("symbols", ())))
                n_err = self._err_match(q_err, set(cfp.get("errsig", ())))
                n_errc = len((q_errc & set(cfp.get("errcls", ()))) - {"assert"}) \
                    if not n_err else 0
                n_test = len(q_test & set(cfp.get("tests", ())))
                n_cmd = sum(1 for qc in q_cmd
                            if any(self._cmd_match(qc, cc) for cc in cfp.get("cmds", ())))
                s = (W_PATH * min(n_path, CAP_PATH) + W_ERR * min(n_err, CAP_ERR)
                     + W_ERRC * min(n_errc, 1) + W_TEST * min(n_test, CAP_TEST)
                     + W_SYM * min(n_sym, CAP_SYM) + W_CMD * min(n_cmd, CAP_CMD))
                if s > 0.0:
                    scores[mid] = s
            if not scores:
                return []
            # 跨社区软降权(与search_boost同口径)
            q_comm = self.query_community(tn, str(query))
            comms = self.communities_of(tn, sorted(scores)) if q_comm else {}
            if comms:
                for mid in list(scores):
                    mc = comms.get(mid)
                    if mc and mc != q_comm:
                        scores[mid] *= XCOMM
            # 取全文行(text非空才可直召回)→排序→cap
            rows: dict[str, tuple[str, str, str]] = {}
            ids_sorted = sorted(scores)
            for i in range(0, len(ids_sorted), 300):
                chunk = ids_sorted[i:i + 300]
                sql = ("SELECT card_id, text, dt, sess FROM texts WHERE tenant=? AND card_id IN ("
                       + ",".join("?" * len(chunk)) + ") AND text != ''")
                for cid, text, dt, sess in self._q(sql, (tn, *chunk)):
                    rows[str(cid)] = (str(text), str(dt or ""), str(sess or ""))
            out: list[dict[str, Any]] = []
            for mid, s in sorted(scores.items(), key=lambda kv: (-kv[1], kv[0])):
                if mid not in rows or len(out) >= max(1, int(cap)):
                    continue
                text, dt, sess = rows[mid]
                out.append({"id": mid, "content": text, "dt": dt, "sess": sess,
                            "created_at": dt, "score": round(float(s), 4)})
            self.n_recall += len(out)
            return out
        except Exception as exc:  # noqa: BLE001 — 直召回失败=空(服务不死)
            self.n_err += 1
            log.error("knife13 recall FAILED err=%s", type(exc).__name__)
            return []
    def stats(self) -> dict[str, Any]:
        try:
            if self._conn is None:
                return {"db": str(self.db_path), "dead": True, "master": _master_on()}
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "texts": g("SELECT COUNT(*) FROM texts"),
                "fp_rows": g("SELECT COUNT(*) FROM fp"),
                "df_tokens": g("SELECT COUNT(*) FROM dfc"),
                "communities": g("SELECT COUNT(*) FROM communities"),
                "assigned_cards": g("SELECT COUNT(*) FROM card_comm"),
                "obs_cards_total": self.n_obs, "new_total": self.n_new,
                "fp_total": self.n_fp, "boost_total": self.n_boost,
                "hit_total": self.n_hit, "recall_total": self.n_recall,
                "err_total": self.n_err,
                "master": _master_on()}
        except Exception:  # noqa: BLE001
            return {"db": str(self.db_path), "dead": True, "master": _master_on()}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部k13状态行。"""
        if self._conn is None:
            return 0
        n = 0
        for tbl in ("texts", "fp", "dfc", "counters", "communities", "card_comm"):
            n += max(0, self._x(f"DELETE FROM {tbl} WHERE tenant=?", (str(tenant),)))  # noqa: S608
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ────────────────────────────
_K13: Knife13 | None = None
_K13_LOCK = threading.Lock()


def get() -> Knife13:
    global _K13
    with _K13_LOCK:
        if _K13 is None:
            _K13 = Knife13(DB_PATH)
            log.info("knife13 init db=%s master=%s", _K13.db_path, _master_on())
        return _K13


def observe(tenant: str, cards: list[dict[str, Any]] | None) -> int:
    return get().observe(tenant, cards)


def backfill(organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    return get().backfill(organ_cards)


def search_boost(tenant: str, query: str, cand_ids: list[str],
                 cand_texts: dict[str, str] | None = None) -> dict[str, float]:
    return get().search_boost(tenant, query, cand_ids, cand_texts)


def recall_by_fingerprint(tenant: str, query: str, cap: int = 40) -> list[dict[str, Any]]:
    """[M1.5] 指纹直召回: query指纹→倒排直查→完整卡[{id,content,dt,sess,score}]。"""
    return get().recall_by_fingerprint(tenant, query, cap)


def query_community(tenant: str, query: str) -> str:
    return get().query_community(tenant, query)


def communities_of(tenant: str, card_ids: list[str]) -> dict[str, str]:
    return get().communities_of(tenant, card_ids)


def communities_of_sess(tenant: str, sess: str, cap: int = 48) -> dict[str, str]:
    """[R1·2026-10-28] 源会话→社区映射(英雄卡社区门消费·fail-soft={})。"""
    return get().communities_of_sess(tenant, sess, cap)


def resolve_keys(tenant: str, cand_ids: list[str],
                 cand_texts: dict[str, str] | None = None) -> dict[str, str]:
    return get().resolve_keys(tenant, cand_ids, cand_texts)
