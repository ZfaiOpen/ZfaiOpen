# knife10_order.py — wave2·第10刀(k10)·C2事件顺序注记刀(零LLM·纯排序呈现·app层overlay·20261009)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞(SPEC_wave2 P3): 官方C2事件顺序=40。顺序query("in what order/sequence/先后")的证据窗
# 按普通相关度浮动——多事件会话的证据卡在top10内呈乱序, 答题模型读不到事件先后; 且官方语料
# 毒dt普遍(缺timestamp/同毫秒/乱序时间戳), 任何按卡dt(墙钟)排序的方案在毒dt下自身即乱序。
# 机制(ADD侧零业务新增·时序唯一实现=k3.cards.obs_seq·五禁令④):
#   1 触发(让位协议·SPEC§k10): k3_tier=="order"才动作(k3=C2档owner·k10零自建词典=清单加项
#     即死护栏同k7); k3_tier其他档(when/duration/none)零动作让位; k1 history意图让位
#     (k1时间线dt升序=C3资产·禁打架); 检测/词典/排他全部复用k3与k1既有产出。
#   2 到达序主键=k3.cards.obs_seq(add相逐卡time_ns落盘·到达即 ledger时序·全程墙钟免疫);
#     次键=本刀order_idx.dia_idx(同obs_seq tie组内会话内位次: add相取卡dia="{rid}:{i}"消息序,
#     backfill相按organ到达迭代取会话内位次) — 毒dt/无dt卡不参与排序依据。
#   3 SEARCH相 order_annotate(): 主证据会话块(=已知卡中原始位次最高卡所属会话· queried story
#     以相关度居首)按(obs_seq,dia_idx)升序重排·整块前插(k3 _front_interval_pair同法·其余
#     相对序不变) + 块内序号注记 metadata.event_order={seq,of,label:"第r",basis:"ledger_arrival"}
#     (metadata附注·content零改写·与k3/k8同一赌注通道)。
#   4 逐退守卫(双档·env BRAINOS_K10_GUARD缺省0): =0散布生效档(缺省·床三跑实证得分零回退:
#     放行top10换血——块内事件卡自窗深处上浮·被换下的噪声卡仍在窗内仅位次变化——官方C2事件
#     散布top10外形态的主收益路径·C2-03床题rank2→1实证); =1严格档(重排致旧top10非块卡跌出
#     新top10→降级注记零重排·逐题字节恒等保证·未见题保守回退档)。
# 链位: /search k7.inject后·截断前(链尾·重排不被后续刀扰动; k4/k6在k3_tier!=none时已让位·
#   k8在k3_tier==order时直通让位=C2域归k10)。
# env: BRAINOS_K10(总闸·缺省关"0"·=1启用·部署候令) / KNIFE10_DB / BRAINOS_K10_BLOCK(8)
# 回滚 = 还原pre_k10_1009/aml_service.py + 摘除knife10_order.py (k10_order.db可留档·秒级)
# 合规: 日志零内容(只记uid/计数)·fail-soft(异常由aml_service调用方兜底=退回k7后原窗)
# ═══════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

log = logging.getLogger("knife10")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KNIFE10_DB", str(APP_DIR.parent / "data" / "k10_order.db")))

_stats: Counter = Counter()


def _master() -> bool:
    """总闸: BRAINOS_K10缺省关(briefing红线·部署候令显式=1启用)。"""
    return os.environ.get("BRAINOS_K10", "0") == "1"


def _block_cap() -> int:
    try:
        return max(2, int(os.environ.get("BRAINOS_K10_BLOCK", "8")))
    except ValueError:
        return 8


def stats() -> dict[str, int]:
    return dict(_stats)


# ── 本刀索引库(次键底册·sqlite WAL·knife1/2/3同款纪律·重启不丢) ───────────────────
class Knife10:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS order_idx(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, sess TEXT NOT NULL,
            dia_idx INTEGER NOT NULL, PRIMARY KEY(tenant, card_id))""")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_k10_sess ON order_idx(tenant, sess)")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0
        self.n_err = 0

    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        """ADD相: 卡dia="{rid}:{i}"消息序落盘(次键·到达序主键在k3.cards.obs_seq)。
        单卡失败只计数不抛(fail-soft·绝不影响add成功语义)。"""
        for pos, c in enumerate(cards or []):
            try:
                text = str(c.get("text") or "")
                sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
                if not text or not sess:
                    continue
                cid = _k3m_card_id(sess, dt, text)
                dia_idx = _dia_idx_of(c, pos)
                with self._lock:
                    self._conn.execute("INSERT OR IGNORE INTO order_idx VALUES(?,?,?,?)",
                                       (str(tenant), cid, sess, dia_idx))
                    self._conn.commit()
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("knife10 observe card FAILED tenant=%s err=%s",
                          str(tenant)[:16], type(exc).__name__)

    def backfill(self, organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
        """启动相: organ存量卡全量回填次键(修前已入库卡亦覆盖·幂等OR IGNORE)。
        organ._cards迭代序=到达序(账本 insertion order)→会话内位次=真到达位次。
        返回(扫过卡数, 新行数, 耗时ms)。"""
        t0 = time.time()
        n_c = n_i = 0
        sess_seq: dict[str, int] = {}
        with self._lock:
            for c in organ_cards or []:
                try:
                    text = str(c.get("text") or "")
                    conv = str(c.get("conv") or "")
                    sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
                    if not text or not sess or not dt or not conv:
                        continue
                    tenant = conv.split(":", 1)[0] or "u_default"
                    cid = _k3m_card_id(sess, dt, text)
                    di = sess_seq.get(f"{tenant}|{sess}", 0) + 1
                    sess_seq[f"{tenant}|{sess}"] = di
                    cur = self._conn.execute(
                        "INSERT OR IGNORE INTO order_idx VALUES(?,?,?,?)",
                        (tenant, cid, sess, di))
                    n_i += cur.rowcount
                    n_c += 1
                except Exception:  # noqa: BLE001 — 单卡脏数据跳过(回填fail-soft)
                    continue
            self._conn.commit()
        return n_c, n_i, (time.time() - t0) * 1000

    def dia_of(self, tenant: str, card_ids: list[str]) -> dict[str, int]:
        """card_ids → {cid: dia_idx}(次键查询·分块IN)。缺席卡不在返回dict中。"""
        out: dict[str, int] = {}
        ids = [c for c in card_ids if c]
        if not ids:
            return out
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = ("SELECT card_id, dia_idx FROM order_idx WHERE tenant=? AND card_id IN ("
                     + ",".join("?" * len(chunk)) + ")")
                for cid, di in self._conn.execute(q, (str(tenant), *chunk)):
                    out[str(cid)] = int(di)
        return out

    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {"db": str(self.db_path), "idx_rows": g("SELECT COUNT(*) FROM order_idx"),
                    "sessions": g("SELECT COUNT(DISTINCT sess) FROM order_idx"),
                    "obs_total": self.n_obs, "obs_err_total": self.n_err}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部次键行(organ内卡片本体不在本模块管辖)。"""
        with self._lock:
            n = self._conn.execute("DELETE FROM order_idx WHERE tenant=?",
                                   (str(tenant),)).rowcount
            self._conn.commit()
        return n


def _dia_idx_of(c: dict[str, Any], pos: int) -> int:
    """dia="{rid}:{i}"→消息序i; 解析不出→批内位次pos(同为到达单调 tie-break)。"""
    dia = str(c.get("dia") or "")
    tail = dia.rsplit(":", 1)[-1] if ":" in dia else ""
    try:
        return int(tail)
    except ValueError:
        return pos


def _k3m_card_id(sess: str, dt: str, text: str) -> str:
    """卡身份唯一实现=k3.card_id_of(sha1(sess|dt|text)[:16]·与_evidence_id同源同值)。"""
    import knife3_time as _k3m
    return _k3m.card_id_of(sess, dt, text)


# ── k3.cards ro直查(到达序主键·唯一实现·k8 obs_of同件同法) ────────────────────────
_ro_lock = threading.Lock()
_ro: dict[str, sqlite3.Connection] = {}


def _k3_db_path() -> Path | None:
    try:
        import knife3_time as k3  # 时序唯一owner
        return Path(k3.get().db_path)
    except Exception as exc:  # noqa: BLE001
        _stats["k3_absent"] += 1
        return None


def _ro_conn(path: Path) -> sqlite3.Connection | None:
    try:
        if not path.exists():
            return None
        with _ro_lock:
            conn = _ro.get("k3")
            if conn is not None:
                try:
                    if Path(conn.execute("PRAGMA database_list").fetchone()[2]) == path.resolve():
                        return conn
                except Exception:  # noqa: BLE001
                    pass
                conn.close()
                _ro.pop("k3", None)
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=3000")
            _ro["k3"] = conn
            return conn
    except Exception as exc:  # noqa: BLE001
        _stats["ro_err"] += 1
        log.warning("knife10 ro connect FAILED err=%s", type(exc).__name__)
        return None


def arrival_of(tenant: str, cids: list[str]) -> tuple[dict[str, int], dict[str, str]]:
    """k3.cards → ({cid: obs_seq}, {cid: sess})(到达序主键+会话归属·ro只读)。"""
    seq: dict[str, int] = {}
    sess: dict[str, str] = {}
    p = _k3_db_path()
    if p is None:
        return seq, sess
    conn = _ro_conn(p)
    if conn is None:
        return seq, sess
    ids = [c for c in cids if c]
    try:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            q = ("SELECT card_id, sess, obs_seq FROM cards WHERE tenant=? AND card_id IN ("
                 + ",".join("?" * len(chunk)) + ")")
            for cid, se, os_ in conn.execute(q, (tenant, *chunk)):
                seq[str(cid)] = int(os_)
                sess[str(cid)] = str(se)
    except Exception as exc:  # noqa: BLE001
        _stats["arrival_err"] += 1
        log.warning("knife10 arrival_of FAILED err=%s", type(exc).__name__)
    return seq, sess


# ── SEARCH相主入口(aml_service经此挂接·k7.inject后截断前) ─────────────────────────
def order_annotate(tenant: str, query: str, data: list[dict[str, Any]],
                   k1_intent: str = "current", k3_tier: str = "none") -> tuple[list[dict[str, Any]], int]:
    """返回(处理后data, 块内卡数)。零命中/非order档/history意图=原样直通(零成本)。
    全路径fail-soft: 异常由aml_service调用方兜底(=退回k7后原窗)。"""
    if not _master() or not data:
        return data, 0
    if k3_tier != "order":
        _stats["yield_tier"] += 1   # 非order档零动作(k3档owner让位协议)
        return data, 0
    if k1_intent == "history":
        _stats["yield_history"] += 1  # C3时间线资产禁打架
        return data, 0
    try:
        return _apply(tenant, data)
    except Exception as exc:  # noqa: BLE001
        _stats["apply_err"] += 1
        log.warning("knife10 apply exception err=%s", type(exc).__name__)
        return data, 0


def _apply(tenant: str, data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    ids = [str(d.get("id", "")) for d in data]
    seq_of, sess_of = arrival_of(tenant, ids)
    known = [(pos, cid) for pos, cid in enumerate(ids) if cid in seq_of]
    if len(known) < 2:
        _stats["known_lt2"] += 1
        return data, 0
    dia_of = get().dia_of(tenant, ids)
    INF = 1 << 62
    # 事件序键=(obs_seq主键, dia_idx次键, 原始位次稳定) — dt零参与=墙钟免疫
    key_of = {cid: (seq_of[cid], dia_of.get(cid, INF), pos) for pos, cid in known}
    dom_sess = sess_of[known[0][1]]  # 主证据会话=已知卡中原始位次最高者所属(queried story)
    dom_pos = [(key_of[cid], pos) for pos, cid in known if sess_of.get(cid) == dom_sess]
    dom_pos.sort()  # 按事件序升序(位置去重: 同id重复卡按pos唯一)
    block_pos = [pos for _, pos in dom_pos[:_block_cap()]]
    if len(block_pos) < 2:
        _stats["block_lt2"] += 1
        return data, 0
    block_pos_set = set(block_pos)
    block = [data[pos] for pos in block_pos]  # 已按事件序(k10的呈现序=块序)
    n_dom = len(dom_pos)
    _annotate(block, n_dom)
    block_ids = {ids[pos] for pos in block_pos_set}
    rest = [d for pos, d in enumerate(data) if pos not in block_pos_set]
    # 逐退守卫(零扰动档): 重排致旧top10非块卡跌出新top10 → 注记零重排(床分恒等)。
    # BRAINOS_K10_GUARD=0(散布形态生效档): 放行top10换血——块内事件卡自窗深处上浮,
    # 被换下的是纯噪声卡(仍在窗内·仅top10位次变化)——官方C2散布形态的主收益路径。
    top_n = min(10, len(data))
    old_top = ids[:top_n]
    new_top = [str(d.get("id", "")) for d in (block + rest)[:top_n]]
    new_top_set = set(new_top)
    if (os.environ.get("BRAINOS_K10_GUARD", "0") == "1"
            and any(cid not in block_ids and cid not in new_top_set for cid in old_top)):
        _stats["guard_annotate_only"] += 1
        return data, len(block)
    _stats["fired"] += 1
    log.info("knife10 order tenant=%s sess=%s.. block=%d dom=%d", str(tenant)[:16],
             str(dom_sess)[:8], len(block), n_dom)
    data[:] = block + rest  # 块整段前插(事件序)·其余相对序不变(k3前插同法)
    return data, len(block)


def _annotate(block: list[dict[str, Any]], n_dom: int) -> None:
    """序号注记: metadata.event_order={seq,of,label,basis}(零改写content·k3/k8同一赌注通道)。"""
    for r, d in enumerate(block, 1):
        md = d.setdefault("metadata", {})
        md["event_order"] = {"seq": r, "of": n_dom, "label": f"第{r}",
                             "basis": "ledger_arrival"}


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────────
_K10: Knife10 | None = None
_K10_LOCK = threading.Lock()


def get() -> Knife10:
    global _K10
    with _K10_LOCK:
        if _K10 is None:
            _K10 = Knife10(DB_PATH)
            log.info("knife10 init db=%s", _K10.db_path)
        return _K10


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def backfill(organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    return get().backfill(organ_cards)


def purge_runtime() -> None:
    """自检清场: 关闭ro连接与单例(不删任何db)。"""
    with _ro_lock:
        for conn in _ro.values():
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        _ro.clear()
    global _K10
    with _K10_LOCK:
        if _K10 is not None:
            try:
                _K10._conn.close()
            except Exception:  # noqa: BLE001
                pass
            _K10 = None
