# knife8_d2pair.py — wave2·第8刀(k8_d2)·D2余量刀·矛盾显影(零LLM·纯排序呈现·app层overlay·20261009)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞(SPEC_wave2.md §3.1): k1在add侧已把矛盾对落盘knife1.db——conflicts表(preference类
# likes/hates/favorite值不同=共存对)+superseded表(supersede类旧值/新值对)。但search侧对
# conflict对"不排除也不利用"(双卡随分沉浮), superseded对在current态被整体排除、history态
# dt升序但不配对不注记。官方D2=40: 判卷要点="两值都可见且新旧分明"——已标未用。
# 机制(SPEC §3.2·ADD侧零新增·矛盾检测唯一owner=k1·本刀只做search侧呈现):
#   1 比较意图词典(knife8_d2_terms.json·与k1/k3/k5/k7词典零交集=selftest S1守门)
#     零命中→字节直通(死护栏·构造保证)
#   2 命中→ro只读k1.get().db_path两表(不建第二套矛盾检测·五禁令①②)
#   3 对内时序唯一实现=k3.cards.obs_seq(k3.get().db_path ro直查·cid即eid·五禁令④);
#     对呈现顺序=旧前新后; k3缺席卡→对级dt回退+metadata.chrono_src落痕(诚实披露)
#   4 补对: 对中一卡在窗外(含被k1现态排除的superseded卡)→svc.organ._cards按eid反查补进
#     (k4 _attach同款直读法+k4 _read_guard同锁)·对整块前插(k3 _front_interval_pair同法)
#   5 差异注记: 每卡附metadata.conflict={pred,other_value,this_is,note:"changed from X to Y"}
#     (注记通道=metadata·与k3同一赌注·本刀排序收益不依赖该赌注)
#   6 让位协议: k5聚合标记命中→整体直通(C3/B1时间线资产·D2-06; k5同款"聚合在·无论intent
#     都让位"先例); k3_tier==order→直通(k10域); k3_tier==duration→带metadata.time.interval
#     的卡所属对跳过(防双前插·五禁令③)
# 链位: /search k6.boost后·k7.inject前(SPEC §二:566)·无startup件(惰性ro连接+进程内懒单例)
# env: BRAINOS_K8D2(总闸·缺省关"0"·=1启用·部署候令·briefing对SPEC BRAINOS_K8缺省1的覆盖)
#      / KNIFE8_D2_DICT(词典路径) / BRAINOS_K8_MAX_PAIRS(2)
# 回滚 = 还原pre_k8d2_1009/aml_service.py + 摘除knife8_d2pair.py/knife8_d2_terms.json
#        (本刀零DB零状态·秒级)
# 合规: 日志零内容(只记计数/id短哈希)·fail-soft(任何异常由aml_service调用方兜底=退回k6后原窗)
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
from collections import Counter
from pathlib import Path
from typing import Any

log = logging.getLogger("knife8")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE8_D2_DICT", str(APP_DIR / "knife8_d2_terms.json")))

_dict_mt = 0.0
_CMP_RES: list[re.Pattern] = []

_stats: Counter = Counter()


def _master() -> bool:
    """总闸: BRAINOS_K8D2缺省关(briefing红线·部署候令显式=1启用)。"""
    return os.environ.get("BRAINOS_K8D2", "0") == "1"


def stats() -> dict[str, int]:
    return dict(_stats)


def _load_dict() -> None:
    """词典热装载(mtime·k5同款)。文件缺失/损坏→保持已装载态(fail-soft)。"""
    global _dict_mt, _CMP_RES
    try:
        mt = DICT_PATH.stat().st_mtime
        if mt == _dict_mt and _CMP_RES:
            return
        with open(DICT_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        res = [re.compile(rx, re.IGNORECASE)
               for rx in (raw.get("compare_signals_en", [])
                          + raw.get("compare_signals_zh", [])) if rx.strip()]
        if not res:
            return  # 空词典拒载(防误清空→永远直通也算安全, 但宁可保持旧表)
        _CMP_RES = res
        _dict_mt = mt
    except Exception as exc:  # noqa: BLE001
        _stats["dict_err"] += 1
        log.warning("knife8 dict load FAILED err=%s", type(exc).__name__)


def detect(query: str) -> bool:
    """比较意图检测。零命中=False→调用方字节直通。"""
    _load_dict()
    return any(r.search(query or "") for r in _CMP_RES)


# ── ro连接(惰性单例·表名schema锁定: knife1.conflicts/superseded·knife3.cards) ─────────
_ro_lock = threading.Lock()
_ro: dict[str, sqlite3.Connection] = {}


def _ro_conn(key: str, path: Path) -> sqlite3.Connection | None:
    try:
        p = str(path)
        with _ro_lock:
            conn = _ro.get(key)
            if conn is not None:
                try:
                    if Path(conn.execute("PRAGMA database_list").fetchone()[2]) == path.resolve():
                        return conn
                except Exception:  # noqa: BLE001 — 路径核对失败→重建
                    pass
                conn.close()
                _ro.pop(key, None)
            if not path.exists():
                return None
            conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=3000")
            _ro[key] = conn
            return conn
    except Exception as exc:  # noqa: BLE001
        _stats["ro_err"] += 1
        log.warning("knife8 ro connect FAILED key=%s err=%s", key, type(exc).__name__)
        return None


def _k1_db_path() -> Path | None:
    """k1库路径唯一来源=k1单例自身(零第二套路径解析·与k1所写db严格同件)。"""
    try:
        import knife1_supersede as k1  # 矛盾检测唯一owner·只读其db
        return Path(k1.get().db_path)
    except Exception as exc:  # noqa: BLE001
        _stats["k1_absent"] += 1
        log.warning("knife8 k1 db path FAILED err=%s", type(exc).__name__)
        return None


def _k3_db_path() -> Path | None:
    try:
        import knife3_time as k3  # 时序唯一owner
        return Path(k3.get().db_path)
    except Exception as exc:  # noqa: BLE001
        _stats["k3_absent"] += 1
        return None


def fetch_pairs(tenant: str) -> list[dict[str, Any]]:
    """该租户已标矛盾对(conflicts共存对+superseded覆盖对)·ro只读·返回统一对结构。"""
    p = _k1_db_path()
    if p is None:
        return []
    conn = _ro_conn("k1", p)
    if conn is None:
        return []
    out: list[dict[str, Any]] = []
    try:
        for subj, pred, ca, cb, va, vb, seq in conn.execute(
                "SELECT subject,pred,card_a,card_b,value_a,value_b,obs_seq FROM conflicts "
                "WHERE tenant=?", (tenant,)):
            out.append({"klass": "conflict", "subject": subj, "pred": pred,
                        "a": ca, "b": cb, "va": va, "vb": vb, "row_seq": int(seq)})
        for cid, pred, sby, ov, nv, seq in conn.execute(
                "SELECT card_id,pred,superseded_by,old_value,new_value,obs_seq FROM superseded "
                "WHERE tenant=?", (tenant,)):
            out.append({"klass": "supersede", "subject": "", "pred": pred,
                        "a": cid, "b": sby, "va": ov, "vb": nv, "row_seq": int(seq)})
    except Exception as exc:  # noqa: BLE001
        _stats["pairs_err"] += 1
        log.warning("knife8 fetch_pairs FAILED err=%s", type(exc).__name__)
        return []
    return out


def obs_of(tenant: str, cids: list[str]) -> dict[str, int]:
    """k3.cards.obs_seq查询(到达序·时序唯一实现)。缺席卡不在返回dict中。"""
    p = _k3_db_path()
    if p is None:
        return {}
    conn = _ro_conn("k3", p)
    if conn is None:
        return {}
    out: dict[str, int] = {}
    ids = [c for c in cids if c]
    try:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            q = ("SELECT card_id,obs_seq FROM cards WHERE tenant=? AND card_id IN ("
                 + ",".join("?" * len(chunk)) + ")")
            for cid, seq in conn.execute(q, (tenant, *chunk)):
                out[str(cid)] = int(seq)
    except Exception as exc:  # noqa: BLE001
        _stats["obs_err"] += 1
        log.warning("knife8 obs_of FAILED err=%s", type(exc).__name__)
    return out


def organ_fetch(svc: Any, tenant: str, want: set[str]) -> dict[str, dict[str, Any]]:
    """organ._cards按eid反查补对卡(k4 _attach同款直读法+k4 _read_guard同锁·只读)。"""
    got: dict[str, dict[str, Any]] = {}
    if not want:
        return got
    try:
        guard = None
        try:
            import knife4_streaming as k4
            if getattr(k4, "_svc", None) is not None:
                guard = k4._read_guard
        except Exception:  # noqa: BLE001 — k4缺席=裸读+异常兜底
            guard = None

        def _scan() -> None:
            pre = f"{tenant}:"
            for kk, c in list(svc.organ._cards.items()):
                if not str(kk).startswith(pre):
                    continue
                eid = hashlib.sha1(
                    f'{c.get("sess", "")}|{c.get("dt", "")}|{c.get("text", "")}'.encode("utf-8")
                ).hexdigest()[:16]
                if eid in want:
                    got[eid] = {"id": eid, "content": str(c.get("text", "")),
                                "created_at": str(c.get("dt", ""))}
                    if len(got) == len(want):
                        return

        if guard is not None:
            with guard():
                _scan()
        else:
            _scan()
    except Exception as exc:  # noqa: BLE001
        _stats["organ_err"] += 1
        log.warning("knife8 organ_fetch FAILED err=%s", type(exc).__name__)
    return got


def _agg_hit(query: str) -> bool:
    """k5聚合标记命中判定(唯一实现=k5._agg_res·本函数零自建词典·五禁令④)。"""
    try:
        import knife5_multihop as k5
        try:
            k5._load_dict()  # 幂等(mtime)·k5每次search本就调用·仅为早退路径兜底
        except Exception:  # noqa: BLE001
            pass
        return any(r.search(query) for r in getattr(k5, "_agg_res", []) or [])
    except Exception:  # noqa: BLE001
        return False


def _has_interval(d: dict[str, Any]) -> bool:
    md = d.get("metadata") or {}
    return bool((md.get("time") or {}).get("interval"))


def d2pair(svc: Any, tenant: str, query: str, data: list[dict[str, Any]],
           k1_intent: str = "current", k3_tier: str = "none") -> tuple[list[dict[str, Any]], int]:
    """主入口(k6.boost后·k7.inject前调用)。返回(data, 成对数)。
    全路径fail-soft: 异常由aml_service调用方兜底(=退回k6后原窗)。"""
    if not _master() or not data:
        return data, 0
    if k3_tier == "order":
        _stats["yield_order"] += 1
        return data, 0  # C2顺序域归k10·直通
    if not detect(query):
        _stats["dict_miss"] += 1
        return data, 0  # 零命中字节直通(死护栏)
    if _agg_hit(query):
        _stats["yield_agg"] += 1
        return data, 0  # 聚合/时间线资产让位(D2-06·k5同款先例)
    try:
        return _apply(svc, tenant, query, data, k3_tier)
    except Exception as exc:  # noqa: BLE001
        _stats["apply_err"] += 1
        log.warning("knife8 apply exception err=%s", type(exc).__name__)
        return data, 0


def _apply(svc: Any, tenant: str, query: str, data: list[dict[str, Any]],
           k3_tier: str = "none") -> tuple[list[dict[str, Any]], int]:
    pairs = fetch_pairs(tenant)
    if not pairs:
        _stats["no_pairs"] += 1
        return data, 0
    q = (query or "").lower()
    pos_of = {str(d.get("id", "")): i for i, d in enumerate(data)}
    by_id = {str(d.get("id", "")): d for d in data}
    max_pairs = max(1, int(os.environ.get("BRAINOS_K8_MAX_PAIRS", "2")))

    # ── 候选筛选: 至少一卡在窗·duration间隔卡重叠跳过(防双前插) ────────────────
    cand: list[dict[str, Any]] = []
    for p in pairs:
        ina, inb = p["a"] in by_id, p["b"] in by_id
        if not ina and not inb:
            continue  # 对零卡在窗=对不可见·零召回扩容(不补)
        ent_a, ent_b = by_id.get(p["a"]), by_id.get(p["b"])
        if k3_tier == "duration" and (
                (ent_a is not None and _has_interval(ent_a))
                or (ent_b is not None and _has_interval(ent_b))):
            _stats["skip_duration_overlap"] += 1
            continue  # k3已前插间隔对·重叠(同卡)让位
        hit = any(v and v in q for v in (p["va"], p["vb"]))
        cand.append({**p, "ina": ina, "inb": inb,
                     "score": (0 if hit else 1, 0 if (ina and inb) else 1, -p["row_seq"])})
    if not cand:
        return data, 0
    cand.sort(key=lambda x: x["score"])
    cand = cand[:max_pairs]

    # ── 对内时序: k3.cards.obs_seq唯一实现·对级缺席→dt回退(落痕) ──────────────
    cids = sorted({c for p in cand for c in (p["a"], p["b"])})
    seqs = obs_of(tenant, cids)
    need_fetch: set[str] = set()
    for p in cand:
        for side in ("a", "b"):
            if not p[f"in{side}"]:
                need_fetch.add(p[side])
    fetched = organ_fetch(svc, tenant, need_fetch) if need_fetch else {}

    block: list[dict[str, Any]] = []
    used: set[str] = set()
    n_pair = 0
    for p in cand:
        if p["a"] in used or p["b"] in used:
            continue  # 链式对共享卡互斥(rock→metal→punk中间卡不得双入·防同卡重复返回)
        ea = by_id.get(p["a"]) or fetched.get(p["a"])
        eb = by_id.get(p["b"]) or fetched.get(p["b"])
        if ea is None or eb is None:
            _stats["half_missing"] += 1
            continue  # 补对失败=只降级(对不呈现)·绝不造残卡
        sa, sb = seqs.get(p["a"]), seqs.get(p["b"])
        if sa is not None and sb is not None:
            old_side, new_side = ("a", "b") if sa <= sb else ("b", "a")
            fallback = False
        else:  # k3缺席(如k3.observe历史失败)→对级dt回退·诚实落痕
            da = str(ea.get("created_at") or "")
            db_ = str(eb.get("created_at") or "")
            old_side, new_side = ("a", "b") if da <= db_ else ("b", "a")
            fallback = True
            _stats["chrono_dt_fallback"] += 1
        old_e, new_e = (ea, eb) if old_side == "a" else (eb, ea)
        old_v, new_v = (p["va"], p["vb"]) if old_side == "a" else (p["vb"], p["va"])
        note = f"changed from {old_v} to {new_v}"
        for e, role, other in ((old_e, "old", new_v), (new_e, "new", old_v)):
            md = e.setdefault("metadata", {})
            md["conflict"] = {
                "pred": p["pred"], "this_is": role, "other_value": other,
                "note": note, "pair_class": p["klass"],
                **({"chrono_src": "dt_fallback"} if fallback else {})}
        block.extend([old_e, new_e])
        used.add(p["a"])
        used.add(p["b"])
        n_pair += 1
        log.info("knife8 pair tenant=%s pred=%s klass=%s old=%s.. new=%s..",
                 tenant[:16], p["pred"], p["klass"], str(p[old_side])[:8], str(p[new_side])[:8])
    if not n_pair:
        return data, 0
    rest = [d for i, d in enumerate(data) if str(d.get("id", "")) not in used]
    _stats["fired"] += 1
    return block + rest, n_pair  # 对整块前插·其余相对序不变(k3前插同法)


# ── 模块级出口(aml_service经此挂接) ─────────────────────────────────────────────
def purge_runtime() -> None:
    """自检清场: 关闭ro连接(不删任何db)。"""
    with _ro_lock:
        for conn in _ro.values():
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        _ro.clear()
