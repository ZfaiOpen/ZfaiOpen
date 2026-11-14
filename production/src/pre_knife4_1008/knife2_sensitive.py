# knife2_sensitive.py — 部署线H2隐私闸: ADD相敏感标注 + SEARCH相最小披露放行门(零LLM·纯结构化·app层overlay·20261008)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方冒烟H2敏感信息最小披露=58.33% — 泛化query("tell me about Alice"/"what happened recently")
#       返回的卡片里带出敏感字面量(证件号/精确财务数字/健康状况/家庭矛盾/门牌全址)。
# 法典张力: H2要"泛化不漏敏感"; E3健康关怀=100分资产要"健康query照常返回健康卡"(禁回退)。
# 做法(对冻结核心/organ/exam考场件零修改·纯overlay·可整件摘除回滚):
#   ADD相    observe(): 词典+模式逐卡扫描(刀2词典knife2_sensitive.json·高置信类目) →
#            命中的卡标 sensitive: 类目落盘 knife2.db(持久·重启不丢)。
#   SEARCH相 filter_gate(): query显式问到某域(health/salary/family/id/address类目词命中)
#            → 该类敏感卡放行(E3守护: "How is X's health?"照常回健康卡);
#            query泛化/无关域 → 敏感卡滤除(整卡不返回)。卡级判定: 卡任一类目∈放行域即放行
#            (ANY-release·保E3混合卡资产), 泛化query放行域=空集 → 敏感卡100%滤除(H2主防线)。
# 选型留痕(滤除 vs 脱敏): 选滤除。①服务契约"只回证据原文"禁改写(memory_service投影注释·
#            疫苗9自检基于原文) — 脱敏=改写证据文本破契约; ②滤除保证严格更强(卡不出现=
#            字面量必不出现), 脱敏靠pattern全覆盖漏一即泄; ③滤除配超采补偿维持top_k填充
#            (knife1同款纪律·排除非降权)。
#   持久化   knife2.db (sqlite WAL + synchronous=NORMAL·与knife1/ledger同款纪律)。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与 aml_service._evidence_id 同源同值(knife1同法)。
# 词典维护: knife2_sensitive.json(同目录) — 改词典+重启即生效; env KNIFE2_DICT/KNIFE2_DB可换路径。
# 合规: 日志零内容 — 只记uid/放行域/卡id哈希/计数; 不落query文本/卡文本/敏感值。
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

log = logging.getLogger("knife2")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE2_DICT", str(APP_DIR / "knife2_sensitive.json")))
DB_PATH = Path(os.environ.get("KNIFE2_DB", str(APP_DIR.parent / "data" / "knife2.db")))


def load_dict(path: Path = DICT_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_DICT = load_dict()
# 类目 → 编译后模式列表
_CAT_RES: list[tuple[str, list[re.Pattern]]] = [
    (c["key"], [re.compile(rx, re.IGNORECASE) for rx in c.get("patterns", [])])
    for c in _DICT.get("categories", [])]
# 类目 → query放行意图模式
_RELEASE_RES: dict[str, list[re.Pattern]] = {
    cat: [re.compile(rx, re.IGNORECASE) for rx in rxs]
    for cat, rxs in (_DICT.get("release_intents", {}) or {}).items()}
_CAT_KEYS = {k for k, _ in _CAT_RES}


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16], knife1同法)。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def classify_text(text: str) -> set[str]:
    """卡文本 → 命中的敏感类目集(高置信词典·未命中=普通卡)。"""
    t = text or ""
    if not t:
        return set()
    hits: set[str] = set()
    for cat, pats in _CAT_RES:
        for rx in pats:
            if rx.search(t):
                hits.add(cat)
                break
    return hits


def release_domains(query: str) -> frozenset[str]:
    """query显式问到的敏感域(类目词命中→该域放行); 泛化/无关域=空集(敏感全滤)。"""
    q = query or ""
    if not q:
        return frozenset()
    out = set()
    for cat, pats in _RELEASE_RES.items():
        for rx in pats:
            if rx.search(q):
                out.add(cat)
                break
    return frozenset(out)


# ── 持久化(sqlite WAL·knife1同款纪律·重启不丢) ───────────────────────────────
class Knife2:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS sensitive_cards(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, category TEXT NOT NULL,
            obs_seq INTEGER NOT NULL, PRIMARY KEY(tenant, card_id, category))""")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0      # 观察卡数(累计·运行期)
        self.n_flag = 0     # 新标注(卡,类目)对数
        self.n_drop = 0     # search相滤除卡数
        self.n_err = 0      # 单卡观察失败数(fail-soft计数)

    # ── ADD相: 敏感标注落盘 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        """build_cards产物逐卡观察标注。单卡失败只计数不抛(fail-soft·绝不影响add成功语义)。"""
        for c in cards or []:
            try:
                self._observe_card(str(tenant), c)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("knife2 observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)

    def _observe_card(self, tenant: str, c: dict[str, Any]) -> None:
        text = str(c.get("text", ""))
        if not text:
            return
        cats = classify_text(text)
        if not cats:
            return
        cid = card_id_of(str(c.get("sess", "")), str(c.get("dt", "")), text)
        with self._lock:
            seq = time.time_ns()
            for cat in sorted(cats):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO sensitive_cards VALUES(?,?,?,?)",
                    (tenant, cid, cat, seq))
                if cur.rowcount:
                    self.n_flag += 1
            self._conn.commit()

    # ── SEARCH相: 放行门 ──
    def sensitive_cats(self, tenant: str, card_ids: list[str]) -> dict[str, set[str]]:
        """card_ids → {card_id: {类目集}}(仅标注过的卡)。分块IN查询。"""
        out: dict[str, set[str]] = {}
        ids = [c for c in card_ids if c]
        if not ids:
            return out
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = ("SELECT card_id, category FROM sensitive_cards WHERE tenant=? AND card_id IN ("
                     + ",".join("?" * len(chunk)) + ")")
                for cid, cat in self._conn.execute(q, (tenant, *chunk)):
                    out.setdefault(str(cid), set()).add(str(cat))
        return out

    def extra_fetch(self, tenant: str) -> int:
        """超采补偿量=该租户敏感卡数(封顶128) — 滤除后仍能按top_k填满(排除非降权)。"""
        try:
            with self._lock:
                n = int(self._conn.execute(
                    "SELECT COUNT(DISTINCT card_id) FROM sensitive_cards WHERE tenant=?",
                    (tenant,)).fetchone()[0])
            return min(n, 128)
        except Exception:  # noqa: BLE001
            return 0

    def filter_gate(self, tenant: str, query: str, data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        """H2放行门: 泛化query(放行域=空)→敏感卡全滤; 显式域query→任一类目∈放行域的卡放行(ANY-release·保E3)。
        返回(过滤后data, 滤除数)。门决策逐条落[privacy-gate]日志(零内容·只记域/哈希/计数)。"""
        if not data:
            return data, 0
        cats_by_id = self.sensitive_cats(tenant, [str(d.get("id", "")) for d in data])
        if not cats_by_id:
            return data, 0  # 本页无敏感卡 → 零成本直通
        release = release_domains(query)
        kept: list[dict[str, Any]] = []
        dropped: list[str] = []
        for d in data:
            cid = str(d.get("id", ""))
            cats = cats_by_id.get(cid)
            if cats and not (cats & release):
                dropped.append(cid[:8])
                continue
            kept.append(d)
        self.n_drop += len(dropped)
        # 门决策审计行: uid/放行域/滤除哈希前8/保留数 — 零query文本零卡文本
        log.info("[privacy-gate] tenant=%s release=%s dropped=%s kept=%d",
                 tenant, ",".join(sorted(release)) or "-",
                 (",".join(dropped) if dropped else "-"), len(kept))
        return kept, len(dropped)

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "sensitive_rows": g("SELECT COUNT(*) FROM sensitive_cards"),
                "sensitive_cards": g("SELECT COUNT(DISTINCT card_id) FROM sensitive_cards"),
                "by_category": {k: g(f"SELECT COUNT(*) FROM sensitive_cards WHERE category='{k}'")
                                for k in sorted(_CAT_KEYS)},
                "obs_cards_total": self.n_obs, "flag_total": self.n_flag,
                "drop_total": self.n_drop, "obs_err_total": self.n_err}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部标注行(organ内卡片本体不在本模块管辖)。"""
        with self._lock:
            cur = self._conn.execute("DELETE FROM sensitive_cards WHERE tenant=?", (tenant,))
            n = cur.rowcount
            self._conn.commit()
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K2: Knife2 | None = None
_K2_LOCK = threading.Lock()


def get() -> Knife2:
    global _K2
    with _K2_LOCK:
        if _K2 is None:
            _K2 = Knife2(DB_PATH)
            log.info("knife2 init db=%s cats=%d release_domains=%d",
                     _K2.db_path, len(_CAT_RES), len(_RELEASE_RES))
        return _K2


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def filter_gate(tenant: str, query: str, data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    return get().filter_gate(tenant, query, data)


def extra_fetch(tenant: str) -> int:
    return get().extra_fetch(tenant)
