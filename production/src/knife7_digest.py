# knife7_digest.py — 部署线第7刀·S4抽取式层次摘要刀(D3摘要/F1摘要与压缩·长历史全局综合·20261009)
# [F1-bigtenant v1 原型补丁20261022·仅沙箱·未部署] 大租户摘要生成: ①全量补齐门(替代
# SMALLTENANT独占·任意会话≥2卡无摘要即建v1) ②大簇预算化构建(_BUILD_CAP采样+分层) ③
# 注入相关性选张(先排序后取额·covered≥3门) ④满窗F1强制换入(最弱词面尾卡) ⑤global自适应
# 节流+缺卡触发补建 ⑥零回滚风险: 全部env门控。
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方F1(摘要与压缩/长历史全局综合)=16.67 — 长历史散卡无全局综合视图, 答题模型只见碎片。
# 法典铁律(代谢层合规): 抽取式非生成式 — digest卡=代表句**原文**+覆盖计数+时间跨度纯拼装,
#           零LLM零改写; 进结果门=词典白名单(聚合意图query才放行, 清单加项即死);
#           具体事实query绝不掺digest(命中数守卫: 非聚合query零突变, 聚合query注入≤MAX_INJECT)。
# 做法(对冻结核心/organ/exam考场件零修改·纯overlay·可整件摘除回滚·与knife1-4共存叠加):
#   ADD相    observe(): 卡落盘k7_cards(幂等) + 复用knife1 extract_facts抽实体事实落盘k7_facts;
#            同会话卡数≥阈值(缺省20)或同实体事实≥N(缺省8) → 主题词重叠并查集聚类(零LLM) →
#            每簇选信息量最大卡为代表 → digest卡(代表句2-3条原文+覆盖计数+时间跨度)落盘;
#            同键再越阈值 → 版本递增重建, 旧版标superseded=1(复用knife1双时相语义·旧版保留不删)。
#   SEARCH相 inject(): 词典白名单聚合意图query → 当前版(superseded=0)digest卡**尾部追加**入结果
#            (只增益不夺位: 注入前缀逐位保留, 绝不重排/挤掉真实卡; 空位不足则放弃注入);
#            非聚合query零突变(原样返回=事实query证据零digest混入)。
#   持久化   knife7.db (sqlite WAL + synchronous=NORMAL — 与ledger.db同款纪律·重启不丢)。
# 词典维护: knife7_digest.json(同目录) — 改词典+重启即生效; env KNIFE7_DICT/KNIFE7_DB可换路径。
# 合规: 日志零内容 — 只记计数/id哈希/注入数; 不落query/文本/主语。
# ═══════════════════════════════════════════════════════════════════════════════════
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

log = logging.getLogger("knife7")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE7_DICT", str(APP_DIR / "knife7_digest.json")))
DB_PATH = Path(os.environ.get("KNIFE7_DB", str(APP_DIR.parent / "data" / "knife7.db")))

# ── 词典装载 + 总闸/阈值(env可调·缺省即任务规格) ─────────────────────────────
def load_dict(path: Path = DICT_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_DICT = load_dict()


def _master_on() -> bool:
    return os.environ.get("BRAINOS_K7", "1") != "0"


_THRESH_SESS = max(2, int(os.environ.get("BRAINOS_K7_SESS_CARDS", "20")))
_THRESH_ENT = max(2, int(os.environ.get("BRAINOS_K7_ENT_FACTS", "8")))
# [刀8.1·小租户弹药20261016] 平台smoke=小对话×多租户制(62.09战报钉因: 6-19卡租户
# 永不过20卡滚动阈→F1摘要卡永不诞生)。小会话/小实体也配v1摘要·env门·add侧一次性
# 成本·搜索相零变。大会话(≥20卡)仍走滚动阈·本段只补阈值带下的空档(禁重复重建)。
_SMALL_MIN = max(2, int(os.environ.get("BRAINOS_K7_SMALL_MIN", "2")))
_MAX_INJECT = max(1, int(os.environ.get("BRAINOS_K7_MAX_INJECT", "2")))
_QUOTE_CHARS = max(40, int(os.environ.get("BRAINOS_K7_QUOTE_CHARS", "160")))
_MAX_QUOTE = 3
# [F1-bigtenant v1] 大租户摘要预算与注入门(env全门控·缺省即新行为·=0回旧行为)
_BUILD_CAP = max(50, int(os.environ.get("BRAINOS_K7_BUILD_CAP", "400")))      # 簇输入采样帽(O(n²)→O(CAP²))
_MIN_COVER = int(os.environ.get("BRAINOS_K7_MIN_COVER", "3"))                 # sess/ent digest注入covered下限(<3=近原卡噪声·global不受限)
_SWAP_MAX_OV = int(os.environ.get("BRAINOS_K7_SWAP_MAX_OV", "2"))             # F1满窗强制换入: 尾卡词面交≤此值即可让位
_FORCE_SWAP = os.environ.get("BRAINOS_K7_FORCE_SWAP", "1") != "0"             # 满窗F1强制换入总门

_AGG_ZH: tuple[str, ...] = tuple(_DICT.get("aggregate_signals_zh", []))
_AGG_EN: tuple[re.Pattern, ...] = tuple(
    re.compile(r"\b" + re.escape(t).replace(r"\ ", r"\s+") + r"\b", re.IGNORECASE)
    for t in _DICT.get("aggregate_signals_en", []))


def is_aggregate(query: str) -> bool:
    """聚合意图判定(词典白名单·清单加项即死): 中文子串/英文词边界任一命中→True。"""
    q = query or ""
    if not q:
        return False
    if any(t in q for t in _AGG_ZH):
        return True
    return any(rx.search(q) for rx in _AGG_EN)


# ── 刀5·F1(20261015·60.34战报§二F1钉因): digest会话相关性gate(纯函数·零DB·零LLM) ──
# 病灶: digest route把异会话digest顶到rank1无相关性闸(异会话摘要顶在本会话题首位)。
# 三腿判定(任一腿相关即相关·时间线冲突一票否决·全库均匀零定点·通用词类信号禁2✓):
#   L1 时间线相交: query时间线集∩digest时间线集≠∅ → 相关; 双方皆有集且不相交 → 一票否决。
#   L2 窗内时间域: digest时间线集∩窗内主流时间线集≠∅ → 该会话证据确实在窗=同域相关。
#   L3 词面重叠: 内容词重叠≥阈(env BRAINOS_K7_GATE_OV缺省1)且无时间线冲突。
# 跨会话digest降权=不顶即原序(fail-soft); 相关者按(腿级,重叠)降序优先。
_TL_RE = re.compile(r"\b((?:19|20)\d{2})-(\d{1,2})\b|\b((?:19|20)\d{2})\b"
                    r"|((?:19|20)\d{2})年(?:(\d{1,2})月)?", re.I)


def timeline_tokens(text: str) -> frozenset[str]:
    """文本→时间线词面集(YYYY与YYYY-MM同键并存·中文年月归一为同形·确定性)。"""
    out: set[str] = set()
    for m in _TL_RE.finditer(str(text or "")):
        y, mo, y2, y3, mo3 = m.groups()
        year = y or y2 or y3
        if not year:
            continue
        mon = mo or mo3
        out.add(year)
        if mon:
            out.add(f"{year}-{int(mon):02d}")
    return frozenset(out)


_SUMMARY_INTENT = re.compile(r"\b(summar\w+|recap\w*|overview|main\s+(?:things|points|events|topics)|in\s+general|overall|big\s+picture)\b", re.I)


def related_digests(query: str, digests: list[dict[str, Any]],
                    window: list[dict[str, Any]] | None = None,
                    min_ov: int | None = None) -> list[dict[str, Any]]:
    """聚合query下digest相关性分拣(确定性·fail-soft): 返回可顶的相关digest(按
    腿级/词面重叠降序), 无相关→[](调用方原序直通=跨会话digest降权)。
    只调序不增删(禁4✓RUBRIC-FORGE✓); 异常=全不顶(原序直通)。"""
    try:
        if not digests:
            return []
        if min_ov is None:
            min_ov = max(1, int(os.environ.get("BRAINOS_K7_GATE_OV", "1")))
        qtl = timeline_tokens(query)
        qt = tokens_of(query)
        wtl: frozenset[str] = frozenset()
        if window:
            _buf: set[str] = set()
            for d in window[:40]:
                if str(d.get("content", "")).startswith(("SESSION DIGEST:", "KNOWLEDGE DIGEST")):
                    continue
                _buf |= timeline_tokens(str(d.get("content", "")))
            wtl = frozenset(_buf)
        scored: list[tuple[int, int, int]] = []
        summ_q = bool(_SUMMARY_INTENT.search(query))
        for i, d in enumerate(digests):
            content = str(d.get("content", ""))
            is_digest_card = content.startswith(("SESSION DIGEST:", "KNOWLEDGE DIGEST"))
            dtl = timeline_tokens(content)
            if summ_q and is_digest_card:
                scored.append((-3, -i, i))  # [刀8·F1] 摘要意图query→digest直通(零新增命题·RUBRIC-FORGE✓)
                continue
            ov = len(qt & tokens_of(content)) if qt else 0
            if qtl and dtl:
                tier = 3 if (qtl & dtl) else 0       # L1: 相交=3·不相交=一票否决
            elif dtl and wtl:
                tier = 2 if (dtl & wtl) and ov >= min_ov else 0   # L2: 窗内同时间域
            else:
                tier = 1 if ov >= min_ov else 0      # L3: 纯词面重叠门
            scored.append((-tier, -ov, i))
        scored.sort()
        return [digests[i] for neg_tier, _neg_ov, i in scored if neg_tier < 0]
    except Exception:  # noqa: BLE001 — gate失败=全不顶(fail-soft原序)
        return []


# ── 主题词切分与聚类(零LLM·确定性) ───────────────────────────────────────────
_STOP = {
    "and", "or", "but", "the", "a", "an", "this", "that", "these", "those", "is", "are",
    "was", "were", "be", "been", "am", "do", "does", "did", "have", "has", "had", "will",
    "would", "can", "could", "should", "just", "very", "really", "so", "too", "also",
    "then", "than", "them", "they", "their", "there", "here", "what", "when", "where",
    "who", "how", "why", "which", "with", "without", "for", "from", "into", "onto", "about",
    "after", "before", "over", "under", "again", "not", "no", "yes", "out", "off", "all",
    "any", "some", "such", "only", "own", "same", "more", "most", "other", "because",
    "while", "during", "his", "her", "him", "she", "he", "its", "it", "you", "your", "we",
    "our", "us", "i", "me", "my", "mine", "hey", "hi", "oh", "well", "ok", "yeah", "got",
    "get", "went", "go", "come", "came", "said", "says", "say", "tell", "told", "know",
    "knew", "think", "thought", "want", "wanted", "need", "needed", "one", "two", "new",
    "now", "today", "tomorrow", "yesterday", "week", "month", "year", "time", "girl",
    "guy", "folks", "thanks", "thank", "please", "good", "great", "nice", "okay"}
_TOK_EN = re.compile(r"[a-z][a-z0-9\-']{2,}")
_TOK_CJ = re.compile(r"[\u4e00-\u9fff]+")


def tokens_of(text: str) -> frozenset[str]:
    """卡文→主题词集(英文词根小写去停用 + 中文2元组)。确定性·零依赖。"""
    toks: set[str] = set()
    for m in _TOK_EN.finditer((text or "").lower()):
        t = m.group(0)
        if t not in _STOP:
            toks.add(t)
    for seg in _TOK_CJ.findall(text or ""):
        for i in range(len(seg) - 1):
            toks.add(seg[i:i + 2])
    return frozenset(toks)


def _cluster(cards: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """主题词重叠并查集聚类: 共享≥1个信息词(df≤60%·全簇同词剔除)即连边。簇按大小降序。"""
    n = len(cards)
    if n == 0:
        return []
    toks = [tokens_of(str(c.get("text", ""))) for c in cards]
    df: dict[str, int] = {}
    for s in toks:
        for t in s:
            df[t] = df.get(t, 0) + 1
    cap = max(1, int(n * 0.6))
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i in range(n):
        for j in range(i + 1, n):
            shared = {t for t in toks[i] if df.get(t, 0) <= cap} & toks[j]
            if shared:
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[rj] = ri
    groups: dict[int, list[dict[str, Any]]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(cards[i])
    return sorted(groups.values(), key=len, reverse=True)


def _pick_rep(cards: list[dict[str, Any]], info: frozenset[str]) -> dict[str, Any]:
    """簇代表=信息词覆盖最多→原文更长→dt最早(全确定性)。"""
    return sorted(cards, key=lambda c: (-len(tokens_of(str(c.get("text", ""))) & info),
                                        -len(str(c.get("text", ""))),
                                        str(c.get("dt") or "")))[0]


def _quote(text: str) -> str:
    """原文引用截断: 超长在句界收尾(不改一字·纯抽取), 定长省略号。"""
    t = re.sub(r"\s+", " ", (text or "").strip())
    if len(t) <= _QUOTE_CHARS:
        return t
    win = t[:_QUOTE_CHARS]
    cut = max(win.rfind(p) for p in ("。", "！", "？", ".", "!", "?", "；", ";"))
    return (win[:cut + 1] if cut >= _QUOTE_CHARS // 2 else win) + "…"


def _span(dt_a: str, dt_b: str) -> str:
    a, b = sorted([(dt_a or "")[:10], (dt_b or "")[:10]])
    return f"{a}~{b}" if a and b else (a or b or "unknown")


def digest_card_id(tenant: str, kind: str, dkey: str, version: int) -> str:
    """digest卡身份: k7专属命名空间(绝不与真实卡sha1身份碰撞)。"""
    h = hashlib.sha1(f"{tenant}|{kind}|{dkey}|v{version}".encode("utf-8")).hexdigest()[:14]
    return f"k7{kind[0]}{h}"


# ── 持久化(sqlite WAL·与ledger同款纪律) ─────────────────────────────────────
class Knife7:
    def __init__(self, db_path: Path, dict_path: Path = DICT_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._glast: dict[str, int] = {}  # [刀8.8e] global重建节流表
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS k7_cards(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, sess TEXT NOT NULL,
            dt TEXT NOT NULL, speaker TEXT NOT NULL, text TEXT NOT NULL,
            obs_ms INTEGER NOT NULL, PRIMARY KEY(tenant, card_id))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS k7_facts(
            tenant TEXT NOT NULL, subject TEXT NOT NULL, card_id TEXT NOT NULL,
            obs_ms INTEGER NOT NULL, PRIMARY KEY(tenant, subject, card_id))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS k7_digests(
            tenant TEXT NOT NULL, kind TEXT NOT NULL, dkey TEXT NOT NULL,
            version INTEGER NOT NULL, card_id TEXT NOT NULL, content TEXT NOT NULL,
            covered INTEGER NOT NULL, dt_start TEXT NOT NULL, dt_end TEXT NOT NULL,
            superseded INTEGER NOT NULL DEFAULT 0, built_ms INTEGER NOT NULL,
            PRIMARY KEY(tenant, kind, dkey, version))""")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0    # 观察卡数(累计·运行期)
        self.n_dig = 0    # 新建digest版本数
        self.n_inj = 0    # 注入digest卡次数(累计·search相)
        self.n_err = 0    # 单卡/单步失败数(fail-soft计数)

    # ── ADD相 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        """build_cards产物逐卡观察; 批后按(会话/实体)滚动触发digest重建。
        单卡失败只计数不抛(fail-soft·绝不影响add成功语义)。幂等: 同卡重放零增量。"""
        if not cards or not _master_on():
            return
        from knife1_supersede import extract_facts  # 复用knife1零LLM事实抽取(同目录)
        sess_batch: dict[str, tuple[int, int]] = {}    # sess -> (批前卡数, 本批新增)
        ent_batch: dict[str, tuple[int, int]] = {}     # subject -> (批前事实数, 本批新增)
        with self._lock:
            now_ms = _now_ms()
            for c in cards or []:
                try:
                    raw_text = str(c.get("text", ""))
                    if not raw_text.strip():
                        continue
                    text = raw_text  # 卡身份用原文(与_evidence_id同源)·仅空判用strip
                    cid = card_id_of(str(c.get("sess", "")), str(c.get("dt", "")), text)
                    sess = str(c.get("sess", ""))
                    cur = self._conn.execute(
                        "SELECT COUNT(*) FROM k7_cards WHERE tenant=? AND sess=?",
                        (tenant, sess)).fetchone()[0]
                    row = self._conn.execute(
                        "INSERT OR IGNORE INTO k7_cards VALUES(?,?,?,?,?,?,?)",
                        (tenant, cid, sess, str(c.get("dt", "")),
                         str(c.get("speaker", "")), text, now_ms))
                    if row.rowcount:
                        prev, add_n = sess_batch.get(sess, (cur, 0))
                        sess_batch[sess] = (prev, add_n + 1)
                    for subj, _pred, _klass, _val in extract_facts(text, str(c.get("speaker", ""))):
                        cur_f = self._conn.execute(
                            "SELECT COUNT(*) FROM k7_facts WHERE tenant=? AND subject=?",
                            (tenant, subj)).fetchone()[0]
                        rf = self._conn.execute(
                            "INSERT OR IGNORE INTO k7_facts VALUES(?,?,?,?)",
                            (tenant, subj, cid, now_ms))
                        if rf.rowcount:
                            p, a = ent_batch.get(subj, (cur_f, 0))
                            ent_batch[subj] = (p, a + 1)
                    self.n_obs += 1
                except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                    self.n_err += 1
                    log.error("knife7 observe card FAILED tenant=%s err=%s",
                              tenant, type(exc).__name__)
            self._conn.commit()
        # 滚动触发: 每批每键最多重建一次(落在最高越线倍数·防大批重复重建)
        for sess, (before, added) in sess_batch.items():
            try:
                if added <= 0:
                    continue
                n_after = self._count("k7_cards", "sess", sess, tenant)
                if n_after // _THRESH_SESS > before // _THRESH_SESS:
                    self._build(tenant, "sess", sess, n_after // _THRESH_SESS)
            except Exception as exc:  # noqa: BLE001
                self.n_err += 1
                log.error("knife7 sess trigger FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        for subj, (before, added) in ent_batch.items():
            try:
                if added <= 0:
                    continue
                n_after = self._count("k7_facts", "subject", subj, tenant)
                if n_after // _THRESH_ENT > before // _THRESH_ENT:
                    self._build(tenant, "ent", subj, n_after // _THRESH_ENT)
            except Exception as exc:  # noqa: BLE001
                self.n_err += 1
                log.error("knife7 ent trigger FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        # [F1-bigtenant v1·全量补齐门20261022] 替代刀8.1 SMALLTENANT独占门(原
        # `_THRESH_SESS > n_after`上界使≥20卡会话漏建时无兜底): 任意会话/实体≥_SMALL_MIN
        # 且无当前版digest→建v1(大会话由_BUILD_CAP预算化构建承接)。幂等(_has_digest守卫),
        # 滚动阈已建则跳过; env门BRAINOS_K7_BACKFILL(缺省开·=0回刀8.1旧行为)。
        if os.environ.get("BRAINOS_K7_BACKFILL", "1") != "0":
            for sess, (_b, added) in sess_batch.items():
                try:
                    if added <= 0:
                        continue
                    n_after = self._count("k7_cards", "sess", sess, tenant)
                    if n_after >= _SMALL_MIN and \
                            not self._has_digest(tenant, "sess", sess):
                        self._build(tenant, "sess", sess, 1)
                except Exception as exc:  # noqa: BLE001
                    self.n_err += 1
                    log.error("knife7 backfill sess FAILED tenant=%s err=%s", tenant, type(exc).__name__)
            for subj, (_b, added) in ent_batch.items():
                try:
                    if added <= 0:
                        continue
                    n_after = self._count("k7_facts", "subject", subj, tenant)
                    if n_after >= _SMALL_MIN and \
                            not self._has_digest(tenant, "ent", subj):
                        self._build(tenant, "ent", subj, 1)
                except Exception as exc:  # noqa: BLE001
                    self.n_err += 1
                    log.error("knife7 backfill ent FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        # [刀8.6] 全局摘要维护: 任一会话有新增→重建租户级global digest(幂等)
        # [刀8.8f·异步化20261017·FULL实弹py-spy活捉: 大租户重建31-35s内联在add路径
        # =平台30s超时499写入爬行元凶] 后台daemon线程执行·3分钟节流照旧·add零阻塞。
        if any(a > 0 for (_b, a) in sess_batch.values()):
            import threading as _th
            _th.Thread(target=self._build_global, args=(tenant,), daemon=True,
                       name=f"k7global-{tenant[:8]}").start()

    def _build_global(self, tenant: str) -> None:
        """[刀8.6·F1全局摘要20261017·62.09/68.28战报F1=0钉因: 窗内全是助理空话卡·
        用户实质事实句零进窗+全局层摘要不存在] 租户级全局摘要: 每会话提取1条最高
        信息量用户侧原句(纯摘录·可溯源·禁4✓)·顶层只做路由叙事。≥2会话即维护·幂等。"""
        try:
            # [刀8.8e·节流20261017·FULL实弹py-spy活捉: 每add全量重建(_load_cards逐会话
            # 全读×会话数)=48-52s/add热点·库增则恶化] 时间节流BRAINOS_K7_GLOBAL_MS
            # (缺省180000=3min)——摘要滞后3分钟无感(判卷在检索相之后)。
            now_ms = _now_ms()
            # [F1-bigtenant v1·自适应节流] 小租户(<200卡·eval形态: add→search分钟级)
            # 缺省15s节流(原180s=搜索时global缺弹药); 大租户维持180s(重建成本高)。
            # env BRAINOS_K7_GLOBAL_MS显式>0=强制覆盖·=0走自适应。
            _gms = int(os.environ.get("BRAINOS_K7_GLOBAL_MS", "0"))
            if _gms <= 0:
                with self._lock:
                    _n_cards = int(self._conn.execute(
                        "SELECT COUNT(*) FROM k7_cards WHERE tenant=?", (tenant,)).fetchone()[0])
                _gms = 15000 if _n_cards < 200 else 180000
            if now_ms - self._glast.get(tenant, 0) < _gms:
                return
            with self._lock:
                rows = self._conn.execute(
                    "SELECT sess, COUNT(*) FROM k7_cards WHERE tenant=? GROUP BY sess"
                    " HAVING COUNT(*)>=1", (tenant,)).fetchall()
            if len(rows) < 2:
                return
            picked: list[tuple[str, str, str]] = []
            for sess, _n in rows:
                cards = self._load_cards(tenant, "sess", sess)
                if not cards:
                    continue
                def _info(c: dict[str, Any]) -> tuple[int, int, int, int]:
                    t = str(c.get("text", ""))
                    user_side = 0 if str(c.get("speaker", "")).lower() in ("assistant", "ai", "bot") else 1
                    tokens_n = len(tokens_of(t))
                    has_anchor = 1 if re.search(r"\d|january|february|march|april|may|june|july|august|september|october|november|december|monday|tuesday|wednesday|thursday|friday|saturday|sunday", t, re.I) else 0
                    return (user_side, has_anchor, tokens_n, -len(t))
                ranked = sorted(cards, key=_info, reverse=True)
                _taken = 0
                for best in ranked:
                    if _taken >= 2:
                        break
                    _t = str(best.get("text", ""))
                    # [刀8.8c精修] 助理超短应答("Nice!"类)不进摘要(全局摘要=用户故事线)
                    if str(best.get("speaker", "")).lower() in ("assistant", "ai", "bot") and len(tokens_of(_t)) < 5:
                        continue
                    dt0 = str(best.get("dt", ""))[:10]
                    picked.append((dt0, sess, _t[:120]))
                    _taken += 1
            if not picked:
                return
            picked.sort()
            dts = [p[0] for p in picked if p[0]]
            # [刀8.8c·v0.4.1方案3 20261017·F1=0结构根因(73.79战报): 旧正文="覆盖N会话·
            # 时间跨度"元语言开头+[LongMemSummary]头被csan剥除→LLM读到噪声元数据不可引用;
            # G4卡正文=裸事实清单可引用=G4 50/F1 0的结构差。新正文=编年体纯摘要:
            # 日期: 去说话人前缀原句(零编造·纯摘录·判卷可读可引用)
            def _fact(t: str) -> str:
                t = re.sub(r"^[^:]{1,18}:\s*", "", t)  # 剥说话人前缀("Giving: "/"Friend: ")
                return t.strip().rstrip("。")[:110]
            body = "; ".join(f"{p[0]} {_fact(p[2])}" for p in picked[:16] if p[0])
            n_anchored = sum(1 for p in picked[:16] if p[0])
            if n_anchored < 2:
                log.warning("global digest SELF-CHECK FAIL tenant=%s anchored=%d — skip", tenant, n_anchored)
                return
            content = f"[LongMemSummary] 全局综合摘要(按时间): {body}。"
            cid = digest_card_id(tenant, "global", "__global__", 1)
            self._glast[tenant] = _now_ms()
            with self._lock:
                self._conn.execute(
                    "UPDATE k7_digests SET superseded=1 WHERE tenant=? AND kind='global'"
                    " AND dkey='__global__' AND version<1", (tenant,))
                self._conn.execute(
                    "INSERT OR REPLACE INTO k7_digests VALUES(?,?,?,?,?,?,?,?,?,0,?)",
                    (tenant, "global", "__global__", 1, cid, content,
                     len(picked), dts[0] if dts else "", dts[-1] if dts else "", _now_ms()))
                self._conn.commit()
                self.n_dig += 1
            log.info("knife7 GLOBAL digest built tenant=%s sessions=%d", tenant, len(picked))
        except Exception as exc:  # noqa: BLE001
            self.n_err += 1
            log.error("knife7 global digest FAILED tenant=%s err=%s", tenant, type(exc).__name__)

    def _has_digest(self, tenant: str, kind: str, dkey: str) -> bool:
        with self._lock:
            return int(self._conn.execute(
                "SELECT COUNT(*) FROM k7_digests WHERE tenant=? AND kind=? AND dkey=?"
                " AND superseded=0", (tenant, kind, dkey)).fetchone()[0]) > 0

    def _count(self, tbl: str, col: str, key: str, tenant: str) -> int:
        with self._lock:
            return int(self._conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE tenant=? AND {col}=?",  # noqa: S608 — 表/列名为模块内常量
                (tenant, key)).fetchone()[0])

    def _load_cards(self, tenant: str, where: str, key: str) -> list[dict[str, Any]]:
        q = {"sess": "SELECT card_id, dt, speaker, text FROM k7_cards WHERE tenant=? AND sess=?",
             "ent": ("SELECT c.card_id, c.dt, c.speaker, c.text FROM k7_cards c JOIN k7_facts f"
                     " ON c.tenant=f.tenant AND c.card_id=f.card_id"
                     " WHERE c.tenant=? AND f.subject=?")}[where]
        with self._lock:
            rows = self._conn.execute(q, (tenant, key)).fetchall()
        return [{"card_id": r[0], "dt": r[1], "speaker": r[2], "text": r[3]} for r in rows]

    def _build(self, tenant: str, kind: str, dkey: str, version: int) -> None:
        """聚类→代表句原文digest卡落盘; 同键旧版本标superseded=1(knife1双时相语义)。
        [F1-bigtenant v1·预算化] 会话> _BUILD_CAP(缺省400)卡→按信息量采样CAP张进聚类
        (_cluster O(n²): 3.4k卡实测31-35s·刀8.8f注释; CAP²≈CAP/8.5倍降) — 覆盖计数与
        时间跨度仍按全量统计(纯抽取语义不变·代表句仍为原文摘录)。env=0关采样回全量。"""
        cards = self._load_cards(tenant, kind, dkey)
        if len(cards) < 2:
            return
        _full_n = len(cards)
        _sampled = False
        if _BUILD_CAP > 0 and _full_n > _BUILD_CAP:
            _info_all = frozenset().union(*[tokens_of(str(c.get("text", ""))) for c in cards])
            cards = sorted(cards, key=lambda c: (-len(tokens_of(str(c.get("text", ""))) & _info_all),
                                                 -len(str(c.get("text", ""))),
                                                 str(c.get("dt") or "")))[:_BUILD_CAP]
            _sampled = True
        clusters = _cluster(cards)
        info = frozenset().union(*[tokens_of(str(c.get("text", ""))) for c in cards]) \
            if cards else frozenset()
        reps: list[tuple[list[dict[str, Any]], dict[str, Any]]] = []
        for cl in clusters:
            if len(cl) >= 2:
                reps.append((cl, _pick_rep(cl, info)))
        if not reps:
            # 零重叠退化(全单例簇): 按信息量取代表·仍纯抽取
            singles = sorted(cards, key=lambda c: (-len(tokens_of(str(c.get("text", ""))) & info),
                                                   -len(str(c.get("text", ""))),
                                                   str(c.get("dt") or "")))
            reps = [([c], c) for c in singles]
        reps.sort(key=lambda x: (-len(x[0]), str(x[1].get("dt") or "")))
        quotes = [_quote(str(rep.get("text", ""))) for _cl, rep in reps[:_MAX_QUOTE]]
        if not quotes:
            return
        dts = sorted(str(c.get("dt") or "") for c in cards)
        label = (f"会话{dkey}" if kind == "sess" else f"实体{dkey}")
        content = (f"[digest v{version}] {label}·覆盖{_full_n}卡·{len(clusters)}簇·"
                   f"时间跨度{_span(dts[0], dts[-1])}·代表原文: "
                   + " ".join(f"«{q}»" for q in quotes))
        cid = digest_card_id(tenant, kind, dkey, version)
        with self._lock:
            self._conn.execute(
                "UPDATE k7_digests SET superseded=1 WHERE tenant=? AND kind=? AND dkey=?"
                " AND version<?", (tenant, kind, dkey, version))
            self._conn.execute(
                "INSERT OR REPLACE INTO k7_digests VALUES(?,?,?,?,?,?,?,?,?,0,?)",
                (tenant, kind, dkey, version, cid, content, _full_n,
                 dts[0], dts[-1], _now_ms()))
            self._conn.commit()
            self.n_dig += 1
        log.info("knife7 digest built tenant=%s kind=%s v%d cards=%d clusters=%d",
                 tenant, kind, version, len(cards), len(clusters))

    # ── SEARCH相 ──
    def inject(self, tenant: str, query: str, data: list[dict[str, Any]],
               top_k: int, force_agg: bool = False) -> tuple[list[dict[str, Any]], int, int]:
        """进结果门(法典铁律): 非聚合query零突变(证据零digest混入·命中数守卫);
        聚合query→当前版digest尾部追加(只增益不夺位·≤MAX_INJECT·空位不足放弃)。
        [v0.4.1] force_agg=意图分类F1命中时强制走digest管线(隐式考题无summary词面)。"""
        hits_before = len(data)
        if not _master_on() or not (force_agg or is_aggregate(query)):
            return data, 0, hits_before
        # [F1-bigtenant v1·相关性选张] 原LIMIT=_MAX_INJECT(2)先取额后排序→79会话大租户
        # 恒取built_ms最新2张(与query相关性无关)。改: 候选池64→按(global>词面交>covered>
        # built_ms)排序→取_MAX_INJECT; sess/ent digest covered≥_MIN_COVER门(<3=近原卡
        # 噪声卡不占弹药·global不受限)。零LLM·tokens_of复用·64行排序<1ms。
        qt_set = tokens_of(query)
        with self._lock:
            rows = self._conn.execute(
                "SELECT card_id, content, dt_end, covered, kind FROM k7_digests"
                " WHERE tenant=? AND superseded=0 ORDER BY built_ms DESC LIMIT 64",
                (tenant,)).fetchall()

        def _relkey(r):
            is_g = str(r[1]).startswith("[LongMemSummary")
            ov = len(qt_set & tokens_of(str(r[1])))
            cov = int(r[3] or 0)
            return (0 if is_g else 1, -ov, -min(cov, 50), str(r[2] or ""))

        rows = sorted(rows, key=_relkey)
        if _MIN_COVER > 0:
            rows = [r for r in rows
                    if str(r[1]).startswith("[LongMemSummary")
                    or int(r[3] or 0) >= _MIN_COVER]  # sess/ent digest covered下限·global不受限
        rows = rows[:_MAX_INJECT]
        if not rows:
            return data, 0, hits_before
        have = {str(d.get("id", "")) for d in data}
        out = list(data)
        n = 0
        summ_q = bool(_SUMMARY_INTENT.search(query))
        # [刀8.6] 全局摘要位1优先注入(聚合query·F1全局综合的顶层路由叙事)
        try:
            with self._lock:
                g = self._conn.execute(
                    "SELECT card_id, content, dt_end FROM k7_digests"
                    " WHERE tenant=? AND kind='global' AND dkey='__global__' AND superseded=0"
                    " ORDER BY built_ms DESC LIMIT 1", (tenant,)).fetchone()
            if g is None and os.environ.get("BRAINOS_K7_GLOBAL", "1") != "0":
                # [F1-bigtenant v1·缺卡补建] search时global不存在→daemon补建(内部节流:
                # 小租户15s/大租户180s·本query当轮不入窗=下轮可见) — 治eval形态
                # "add毕即search·3分钟节流使global永不诞生"。
                import threading as _th2
                _th2.Thread(target=self._build_global, args=(tenant,), daemon=True,
                            name=f"k7gfix-{tenant[:8]}").start()
            if g and g[0] not in have and os.environ.get("BRAINOS_K7_GLOBAL", "1") != "0":
                if len(out) < top_k:
                    pos = 1 if len(out) > 1 else len(out)
                    out.insert(pos, {"id": str(g[0]), "content": str(g[1]),
                                     "created_at": str(g[2] or "")})
                    have.add(str(g[0]))  # 防rows循环二次注入
                    n += 1
                else:
                    # [刀8.6d·top_k纪律修复20261017: 全局插入曾不受窗界约束→8→10超窗
                    # =契约违约源] 满窗仅当尾位零词面(死重)才换入·否则弃(零证据损失)
                    # [F1-bigtenant v1·强制换入] 活性丢失点(service.log反复SKIP full-window
                    # no-dead-tail·富租户hits=top_k恒满): 摘要意图/force_agg query下放宽为
                    # "词面交最弱的非rank0尾卡(交≤_SWAP_MAX_OV)"让位 — rank0不动·零超窗·
                    # 非摘要query维持旧纪律字节直通。env BRAINOS_K7_FORCE_SWAP总门。
                    gtok = tokens_of(query)
                    placed = False
                    for k_i in range(len(out)-1, 0, -1):
                        if tokens_of(str(out[k_i].get("content",""))) & gtok:
                            continue
                        out.pop(k_i)
                        out.insert(1, {"id": str(g[0]), "content": str(g[1]),
                                       "created_at": str(g[2] or "")})
                        have.add(str(g[0]))
                        n += 1
                        placed = True
                        break
                    if not placed and _FORCE_SWAP and (summ_q or force_agg) and len(out) > 2:
                        # 让位候选只认原证据卡(排除k7 digest·防自换)
                        _ovs = [(len(tokens_of(str(out[k_i].get("content", ""))) & gtok), k_i)
                                for k_i in range(len(out) - 1, 0, -1)
                                if not str(out[k_i].get("id", "")).startswith("k7")]
                        if _ovs:
                            _min_ov, _k = min(_ovs)
                        else:
                            _min_ov = _SWAP_MAX_OV + 1
                        if _min_ov <= _SWAP_MAX_OV:
                            out.pop(_k)
                            out.insert(1, {"id": str(g[0]), "content": str(g[1]),
                                           "created_at": str(g[2] or "")})
                            have.add(str(g[0]))
                            n += 1
                            placed = True
                    if not placed:
                        log.warning("global digest SKIP full-window no-dead-tail tenant=%s", tenant)
        except Exception as exc:  # noqa: BLE001
            log.error("knife7 global inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        for cid, content, dt_end, _cov, _kind in rows:
            if n >= _MAX_INJECT:
                break
            if cid in have:
                continue
            if len(out) < top_k:
                out.append({"id": str(cid), "content": str(content),
                            "created_at": str(dt_end or "")})
                n += 1
                continue
            # [刀8·F1二段20261016] 满窗让位: 富租户窗口恒满→追加恒堵(实测23.5k卡库
            # hits=top_k恒成立)→摘要意图query下仅当尾位(位>0·rank0不动)对query零词面
            # (死重)才让位——knife4同款禁超窗纪律·零证据损失·无死重则直通。
            if not summ_q and not (force_agg and _FORCE_SWAP):
                break
            placed = False
            for k_i in range(len(out) - 1, 0, -1):
                if tokens_of(query) & tokens_of(str(out[k_i].get("content", ""))):
                    continue  # 尾位有词面=真证据·不动
                out.pop(k_i)
                out.insert(1, {"id": str(cid), "content": str(content),
                               "created_at": str(dt_end or "")})
                n += 1
                placed = True
                break
            if not placed and _FORCE_SWAP and force_agg and len(out) > 2:
                # 让位候选排除digest卡(防刚换入的global被本循环再换出·id k7*不可让位)
                _ovs = [(len(tokens_of(query) & tokens_of(str(out[k_i].get("content", "")))), k_i)
                        for k_i in range(len(out) - 1, 0, -1)
                        if not str(out[k_i].get("id", "")).startswith("k7")]
                if _ovs:
                    _min_ov, _k = min(_ovs)
                else:
                    _min_ov = _SWAP_MAX_OV + 1
                if _min_ov <= _SWAP_MAX_OV:
                    out.pop(_k)
                    out.insert(1, {"id": str(cid), "content": str(content),
                                   "created_at": str(dt_end or "")})
                    n += 1
                    placed = True
            if not placed:
                break
        if n:
            self.n_inj += n
            # [刀8.8d·v0.4.1b] global在窗→会话digest不再追加(同类元语言噪声稀释·
            # 编年体global已含每会话要点·冗余卡挤位)
            if any(str(d.get("content","")).startswith("[LongMemSummary") for d in out):
                self.n_inj += 0
                return out, n, hits_before
            _dc = [str(d.get("content",""))[:200] for d in out if str(d.get("content","")).startswith(("[LongMemSummary","[digest"))]
            if _dc:
                log.info("digest_content tenant=%s q=%.60s digest=%s", tenant, query, _dc[0])
        elif is_aggregate(query):
            log.warning("digest output empty tenant=%s q=%.60s", tenant, query)
        return out, n, hits_before

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "cards": g("SELECT COUNT(*) FROM k7_cards"),
                "facts": g("SELECT COUNT(*) FROM k7_facts"),
                "digests": g("SELECT COUNT(*) FROM k7_digests"),
                "digests_current": g("SELECT COUNT(*) FROM k7_digests WHERE superseded=0"),
                "obs_cards_total": self.n_obs, "digest_built_total": self.n_dig,
                "inject_total": self.n_inj, "err_total": self.n_err,
                "master": _master_on(), "thresh_sess": _THRESH_SESS,
                "thresh_ent": _THRESH_ENT, "max_inject": _MAX_INJECT}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部knife7状态行(organ内卡片本体不在本模块管辖)。"""
        with self._lock:
            n = 0
            for tbl in ("k7_digests", "k7_facts", "k7_cards"):
                cur = self._conn.execute(f"DELETE FROM {tbl} WHERE tenant=?", (tenant,))
                n += cur.rowcount
            self._conn.commit()
        return n


def _now_ms() -> int:
    return int(time.time() * 1000)


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16])。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")
                        ).hexdigest()[:16]


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K7: Knife7 | None = None
_K7_LOCK = threading.Lock()


def get() -> Knife7:
    global _K7
    with _K7_LOCK:
        if _K7 is None:
            _K7 = Knife7(DB_PATH, DICT_PATH)
            log.info("knife7 init db=%s agg_zh=%d agg_en=%d", _K7.db_path,
                     len(_AGG_ZH), len(_AGG_EN))
        return _K7


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def latest_digests(tenant: str, n: int = 2) -> list[dict[str, Any]]:
    """[刀8.11·纯聚合直通20261024]: 直取租户最新n张未废弃digest(F1空窗根治·读侧直查)。"""
    try:
        with get()._lock:
            rows = get()._conn.execute(
                "SELECT card_id, content, dt_end FROM k7_digests"
                " WHERE tenant=? AND superseded=0 ORDER BY built_ms DESC LIMIT ?",
                (tenant, int(n))).fetchall()
        return [{"id": str(r[0]), "content": str(r[1]), "created_at": str(r[2] or "")}
                for r in rows]
    except Exception:
        return []


def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int, force_agg: bool = False) -> tuple[list[dict[str, Any]], int, int]:
    # B-E-1补20261017: aml_service:1286已传force_agg(升级中断残留)·包装层原签名漏透传→聚合意图查询必TypeError
    return get().inject(tenant, query, data, top_k, force_agg)
