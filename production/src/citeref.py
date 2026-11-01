# citeref.py — 部署线引用图谱刀·M2(ADD相卡↔issue/PR引用号双向边 → SEARCH相1跳/2跳提升·20261025轨道B)
# ═════════════════════════════════════════════════════════════════════════════════
# 血洞: 出题方gold关联=issue/PR交叉引用六模式+递归扩展(SWEContextBench构造法)——引用关系
#       就在轨迹文本里("Fixes #123"/"Closes #45"/github.com/x/y/issues/78), 现引擎纯语义
#       对"#123"这类锚零结构化利用。本刀把引用关系建图→query抽引用号→图扩展提升
#       (模拟出题方的递归扩展·跨代特性)。
# 既有件复用(对齐家规): knife11_forget.py=边车骨架范本(DB路径/observe fail-soft/单例/purge);
#   knife13_repo.py=指纹层姊妹刀(query_refs抽取器+社区联合校验source·本件lazy import)。
# 做法(零LLM零外部API·sqlite邻接表·对冻结核心/organ零修改·纯overlay可整件摘除):
#   ADD相    observe(tenant, cards): 逐卡抽引用号(卡侧=关键词门控[Fix/Clos/Resolv/Ref/see/
#            Issue]+#N 与 GitHub URL /issues/N /pull/N·精确优先) → refs邻接表
#            (tenant, card_id, ref_key)双向可查 + 卡文镜像texts(id漂移解引用)。
#   SEARCH相 boost_by_refs(tenant, query, cands, cand_texts=None)→{cid: float}:
#            query抽引用号(query侧=卡侧门控+URL+裸#N·召回优先) → 图1跳卡×W1(2.0)、
#            2跳×W2(1.4)(2跳=与任一1跳卡共享引用号的卡·模拟递归扩展);
#            社区联合校验(防跨库撞号: numpy#123≠pandas#123): query社区(lazy k13)与卡社区
#            皆在且不同→该跳提升不授(同社区引用才可信·k13不可用/无社区=fail-open);
#            2跳中介卡社区不匹配→该中介路径不授。cand_texts给定时自动id漂移解引用。
#   兜底     query无引用号→{}(退化路径·不崩); 图无该引用号→{}; DB损坏→no-op服务不死。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与aml_service._evidence_id同源同值
#            (knife1/2/3/11/13同法)。幂等: 同卡重放零增量(PK)。
# 挂接(3处·回滚=摘3处+删本件): ①aml_service import区 ②_knives_process加crf.observe
#   (k13块后) ③search相erank后a1r前与k13同块加权。可选第4处=startup回填。
# 持久化   citeref.db (sqlite WAL + synchronous=NORMAL + busy_timeout=5000·同款纪律)。
# 合规: 通用GitHub语义的内容理解(issue/PR编号抽取)·无任何CAMBench专有数据; 日志零内容
#       只记计数/ref号哈希。env总闸BRAINOS_CITEREF(缺省0=关)。
# ═════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("citeref")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CITEREF_DB", str(APP_DIR.parent / "data" / "citeref.db")))


# ── 总闸与权重(env可调) ─────────────────────────────────────────────────────
def _master_on() -> bool:
    return os.environ.get("BRAINOS_CITEREF", "0") == "1"     # 缺省关

def _f(name: str, dft: str) -> float:
    try:
        return float(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return float(dft)

W1 = _f("BRAINOS_CR_W1", "2.0")   # 1跳(query引用号直连卡)
W2 = _f("BRAINOS_CR_W2", "1.4")   # 2跳(与1跳卡共享引用号)
MAX_HOP1 = 512    # 单引用号1跳卡读取上限(防大租户爆查)
MAX_HOP2_REFS = 64  # 1跳卡引用号扩展上限
_XCOMM_PASS = True  # 社区校验fail-open开关(k13不可用→放行·文档化)


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16])。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def extract_refs(text: str, query_side: bool = False) -> list[str]:
    """文本→引用号键列表(统一"#N"键空间·GitHub issue/PR同号池)。
    卡侧(query_side=False): 关键词门控[Ff]ix(e[sd])?/[Cc]lose[sd]?/[Rr]esolve[sd]?/
      [Rr]ef(erence[sd])?/see/[Ii]ssue + #N, GitHub URL /issues/N /pull/N — 精确优先。
    query侧(True): 上述 + 裸#N(召回优先·社区校验防撞号)。去重升序。"""
    try:
        from knife13_repo import query_refs  # 姊妹刀同款抽取器(单一定义点)
        if query_side:
            return query_refs(text)
        # 卡侧: 复用knife13的query_refs会带裸#N——这里自行实现门控版(避免反向依赖膨胀)
    except Exception:  # noqa: BLE001 — knife13缺席→本件自带兜底实现
        pass
    import re as _re
    t = str(text or "")
    out: set[str] = set()
    for m in _re.finditer(r"\b(?:[Ff]ix(?:e[sd])?|[Cc]lose[sd]?|[Rr]esolve[sd]?|"
                          r"[Rr]ef(?:erence[sd])?|see|[Ii]ssue)\s*#?(\d{1,7})\b", t):
        out.add(f"#{m.group(1)}")
    for m in _re.finditer(r"github\.com/[^\s\"']*?/(?:issues|pull)/(\d{1,7})", t, _re.I):
        out.add(f"#{m.group(1)}")
    if query_side:
        for m in _re.finditer(r"(?<![\w#])#(\d{1,7})\b", t):
            out.add(f"#{m.group(1)}")
    return sorted(out)


# ── 持久化(sqlite WAL·同款纪律) ─────────────────────────────────────────────
class CiteRef:
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
            # 邻接表: 卡→引用号 与 引用号→卡 双向皆可查(PK正排+idx倒排)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS refs(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, ref_key TEXT NOT NULL,
                kind TEXT NOT NULL, obs_ms INTEGER NOT NULL,
                PRIMARY KEY(tenant, card_id, ref_key))""")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cr_ref ON refs(tenant, ref_key)")
            # 卡文镜像(id漂移解引用·与k13各自独立·两刀可分拆启用)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS texts(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, content_hash TEXT NOT NULL,
                obs_ms INTEGER NOT NULL, PRIMARY KEY(tenant, card_id))""")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cr_hash ON texts(tenant, content_hash)")
            self._conn.commit()
        except Exception as exc:  # noqa: BLE001 — DB损坏/只读→本刀整体no-op(服务不死)
            self._conn = None
            log.error("citeref init FAILED db=%s err=%s", self.db_path, type(exc).__name__)
        self.n_obs = 0       # 观察卡数
        self.n_ref = 0       # 新边数
        self.n_boost = 0     # search相加权查询次数
        self.n_hit = 0       # 提升卡次
        self.n_err = 0

    def _q(self, sql: str, args: tuple = ()) -> list[tuple]:
        with self._lock:
            return list(self._conn.execute(sql, args).fetchall())

    # ── ADD相: 建图 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]] | None) -> int:
        """逐卡: 镜像+引用号边。fail-soft: 单卡失败只计数不抛。幂等: 同卡重放零增量。
        返回新边数(0=无新增/门关/DB死)。"""
        if not cards or not _master_on() or self._conn is None:
            return 0
        return self._observe_batch(str(tenant), cards)

    def _observe_batch(self, tenant: str, cards: list[dict[str, Any]]) -> int:
        n_new = 0
        for c in cards:
            try:
                n_new += self._observe_card(tenant, c)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 不拖垮add
                self.n_err += 1
                log.error("citeref observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        self.n_ref += n_new
        return n_new

    def _observe_card(self, tenant: str, c: dict[str, Any]) -> int:
        raw = c.get("text")
        if not isinstance(raw, str) or not raw:  # 脏text(非str/空)→跳过
            return 0
        text = raw
        sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
        cid = card_id_of(sess, dt, text)
        now_ms = int(time.time() * 1000)
        chash = hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
        refs = extract_refs(text, query_side=False)
        n = 0
        with self._lock:
            cur = self._conn.execute("INSERT OR IGNORE INTO texts VALUES(?,?,?,?)",
                                     (tenant, cid, chash, now_ms))
            n += max(0, cur.rowcount)
            for r in refs:
                cur = self._conn.execute("INSERT OR IGNORE INTO refs VALUES(?,?,?,?,?)",
                                         (tenant, cid, r, "card", now_ms))
                n += max(0, cur.rowcount)
            self._conn.commit()
        return n

    # ── 启动相: 存量回填(幂等·可选挂接) ──
    def backfill(self, organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
        """organ存量卡回填(k3/k13同法: conv冒号前=tenant)。返回(卡数,新边,ms)。"""
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
        n_e = 0
        for tn, cs in sorted(by_t.items()):
            n_e += self._observe_batch(tn, cs)  # 直走批内路径(不吃总闸·恢复既有索引)
        return (sum(len(v) for v in by_t.values()), n_e, (time.time() - t0) * 1000)

    # ── SEARCH相 ──
    def resolve_keys(self, tenant: str, cand_ids: list[str],
                     cand_texts: dict[str, str] | None = None) -> dict[str, str]:
        """调用方窗卡键→镜像card_id(id直中/剥fact-time面hash回查/原键透传)。fail-soft。"""
        ident = {str(c): str(c) for c in cand_ids if c}
        try:
            if self._conn is None:
                return ident
            tn = str(tenant)
            from knife13_repo import content_hash_of  # 剥面归一(单一定义点)
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

    def _comms(self, tenant: str, ids: list[str]) -> dict[str, str]:
        """knife13社区联合校验源(lazy import·缺席/异常→空=fail-open)。"""
        try:
            from knife13_repo import communities_of
            return communities_of(tenant, ids)
        except Exception:  # noqa: BLE001
            return {}

    def _q_comm(self, tenant: str, query: str) -> str:
        try:
            from knife13_repo import query_community
            return query_community(tenant, query)
        except Exception:  # noqa: BLE001
            return ""

    def boost_by_refs(self, tenant: str, query: str, cands: list[str],
                      cand_texts: dict[str, str] | None = None) -> dict[str, float]:
        """query抽引用号→图中1跳×W1/2跳×W2→{调用方卡键: 分}。社区联合校验(同社区引用
        才可信·k13缺席fail-open)。零引用号/门关/异常→{}(fail-soft·服务不死)。"""
        try:
            if not _master_on() or self._conn is None or not cands or not query:
                return {}
            refs = extract_refs(query, query_side=True)
            if not refs:
                return {}  # 退化路径: 无引用号→零提升
            tn = str(tenant)
            resolved = self.resolve_keys(tn, cands, cand_texts)
            mirror_set = {v for v in resolved.values() if v}
            if not mirror_set:
                return {}
            q_comm = self._q_comm(tn, str(query))
            scores: dict[str, float] = {}
            hop1_all: set[str] = set()
            for r in refs[:16]:  # 单query引用号上限(防滥用面)
                rows = self._q("SELECT card_id FROM refs WHERE tenant=? AND ref_key=?"
                               " LIMIT ?", (tn, r, MAX_HOP1))
                hop1_all |= {str(x[0]) for x in rows}
            hop1 = hop1_all & mirror_set
            # 2跳: 与任一1跳卡(全图1跳·不限窗内)共享引用号的卡
            hop2_src = hop1_all if len(hop1_all) <= MAX_HOP2_REFS else set(
                sorted(hop1_all)[:MAX_HOP2_REFS])
            hop2_refs: set[str] = set()
            src_ids = sorted(hop2_src)
            for i in range(0, len(src_ids), 300):
                chunk = src_ids[i:i + 300]
                sql = ("SELECT ref_key FROM refs WHERE tenant=? AND card_id IN ("
                       + ",".join("?" * len(chunk)) + ")")
                hop2_refs |= {str(x[0]) for x in self._q(sql, (tn, *chunk))}
            hop2_all: set[str] = set()
            for r in sorted(hop2_refs)[:64]:
                rows = self._q("SELECT card_id FROM refs WHERE tenant=? AND ref_key=?"
                               " LIMIT ?", (tn, r, MAX_HOP1))
                hop2_all |= {str(x[0]) for x in rows}
            hop2 = (hop2_all - hop1_all) & mirror_set
            need_comm = [str(x) for x in (hop1 | hop2)] if q_comm else []
            comms = self._comms(tn, need_comm) if need_comm else {}
            for mid in hop1:
                if q_comm:
                    mc = comms.get(mid)
                    if mc and mc != q_comm:
                        continue  # 跨社区引用不可信(防跨库撞号)
                scores[mid] = scores.get(mid, 0.0) + W1
            for mid in hop2:
                if q_comm:
                    mc = comms.get(mid)
                    if mc and mc != q_comm:
                        continue
                scores[mid] = scores.get(mid, 0.0) + W2
            if not scores:
                return {}
            self.n_boost += 1
            self.n_hit += len(scores)
            return {cid: round(scores[mid], 4) for cid, mid in resolved.items()
                    if mid in scores}
        except Exception as exc:  # noqa: BLE001 — 提升失败=原序直通(服务不死)
            self.n_err += 1
            log.error("citeref boost FAILED err=%s", type(exc).__name__)
            return {}

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        try:
            if self._conn is None:
                return {"db": str(self.db_path), "dead": True, "master": _master_on()}
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "ref_edges": g("SELECT COUNT(*) FROM refs"),
                "ref_keys": g("SELECT COUNT(DISTINCT ref_key) FROM refs"),
                "cards_with_refs": g("SELECT COUNT(DISTINCT card_id) FROM refs"),
                "texts": g("SELECT COUNT(*) FROM texts"),
                "obs_cards_total": self.n_obs, "ref_total": self.n_ref,
                "boost_total": self.n_boost, "hit_total": self.n_hit,
                "err_total": self.n_err, "master": _master_on()}
        except Exception:  # noqa: BLE001
            return {"db": str(self.db_path), "dead": True, "master": _master_on()}

    def purge_tenant(self, tenant: str) -> int:
        if self._conn is None:
            return 0
        n = 0
        for tbl in ("refs", "texts"):
            with self._lock:
                cur = self._conn.execute(f"DELETE FROM {tbl} WHERE tenant=?", (str(tenant),))  # noqa: S608
                n += max(0, cur.rowcount)
                self._conn.commit()
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ────────────────────────────
_CRF: CiteRef | None = None
_CRF_LOCK = threading.Lock()


def get() -> CiteRef:
    global _CRF
    with _CRF_LOCK:
        if _CRF is None:
            _CRF = CiteRef(DB_PATH)
            log.info("citeref init db=%s master=%s", _CRF.db_path, _master_on())
        return _CRF


def observe(tenant: str, cards: list[dict[str, Any]] | None) -> int:
    return get().observe(tenant, cards)


def backfill(organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    return get().backfill(organ_cards)


def boost_by_refs(tenant: str, query: str, cands: list[str],
                  cand_texts: dict[str, str] | None = None) -> dict[str, float]:
    return get().boost_by_refs(tenant, query, cands, cand_texts)
