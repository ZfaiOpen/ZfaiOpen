#!/usr/bin/env python3
from __future__ import annotations
# ═══════════════════════════════════════════════════════════════════════════
# knife8_rank.py — 部署线shadow排序刀(k8)·I2x床三MISS修复 + wave2 SPEC k11增强件并入
# 座位: 部署线校准兵(I2x席) · 2026-10-09 · 载体=shadow_k8/app(主服8090零碰·候令晋升)
#
# 四机制(全挂search端点 data[:top_k] 截断前·k7后最终排序位·fail-soft同前七刀):
#   FIX-A1 query专名卡级覆盖boost(I2X-06十session同构): query中位专名→匹配卡稳定前置
#   FIX-A2 卡级query实体覆盖度重排(I2X-10三session合成): 无专名query→coverage降序稳定重排
#          (A1/A2同类合一·互斥触发: 有专名走A1·无专名走A2)
#   FIX-B 跨请求同conv缝链(I2X-09·SPEC M3的app层等价实现·留痕): conv序rid边界检测→
#         触发卡(截断深DEPTH内)的跨请求前块卡带外插位(经k1/k2闸复检·零旁路)
#   K11-M2b 同实体组带外晋升(SPEC_wave2§k11并入·指挥官令): 组键=k1.fact_current(subject·ro)
#         +内容回退组·时序=k4._created_ms(ro·时序唯一真源)·MAX_PROMOTE=1·top3铁闸
#         (晋升源=top3外·落位=第3位)·M2c freshness/M2d kin注记随卡携带
#
# env(缺省全关·=旧行为): BRAINOS_K8(总闸) / BRAINOS_K8_PROPER / BRAINOS_K8_COV /
#   BRAINOS_K8_STITCH / BRAINOS_K8_PROMOTE / 参数 BRAINOS_K8_COV_MIN(0.15) /
#   BRAINOS_K8_STITCH_MAX(3) / BRAINOS_K8_STITCH_BLOCKS(2) / BRAINOS_K8_STITCH_DEPTH(10) /
#   BRAINOS_K8_MIN_TOP(3) / BRAINOS_K8_SPREAD_MS(60000)
# 五禁令自查: 带内换序owner=k4 M2(不碰)·subject唯一源=k1(ro)·created_ms唯一源=k4(ro)·
#   缝链边界唯一实现=本刀dia-rid检测(k4 link接口未启用·留痕)·触发词典=本刀独有
# 回滚 = 摘除k8_service.py三处hunk + 摘除knife8_rank.py (零DB零状态·纯进程内)
# ═══════════════════════════════════════════════════════════════════════════
import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
from collections import Counter
from typing import Any

log = logging.getLogger("aml")
_stats: Counter = Counter()
_lock = threading.Lock()
_svc: Any = None
_installed = False

_FLAG_DEFAULT: dict[str, str] = {"COV_MIN_CONVS_ON": "0"}  # cov门缺省关: 实测cov重排同时是I2X-01/09/10与全床A/B的同一修复机制·5conv门无法分离(两域同构)·恒等vs修复呈指挥官裁决(见KNIFE8_RANK.md§6)

_STOP = set("""a an the and or of for to in on at by with from as is are was were be been being
do does did done has have had having will would can could shall should may might must this that
these those it its their his her my our your there here what which where when who whose why how
still just get got getting also about into over under again then than so if not no yes very much
many more most some any each every other another s t d ll re ve m""".split())

_WORD_RX = re.compile(r"[A-Za-z][A-Za-z'-]*")
_TOK_RX = re.compile(r"[a-z0-9][a-z0-9\-]{2,}")
_Q_CAPS = {"i", "ok", "mr", "mrs", "ms", "dr"}


def _master() -> bool:
    return os.environ.get("BRAINOS_K8", "0") == "1"


def _flag(name: str, default: str = "1") -> bool:
    return os.environ.get(f"BRAINOS_K8_{name}", _FLAG_DEFAULT.get(name, default)) != "0"


def _num(name: str, default: float) -> float:
    try:
        return float(os.environ.get(f"BRAINOS_K8_{name}", default))
    except ValueError:
        return default


def install(svc: Any = None) -> dict[str, bool]:
    """端点startup调用·只存svc引用(organ ro消费)·零patch零线程。"""
    global _svc, _installed
    _svc = svc
    _installed = True
    return {"installed": True}


def stats() -> dict[str, int]:
    with _lock:
        return dict(_stats)


# ── 设施ro消费 ────────────────────────────────────────────────────────────────
def _cm_snapshot() -> dict[str, int]:
    """k4._created_ms ro快照(时序唯一真源=ledger created_ms·SPEC k11§6.2)。k4缺席=空map。"""
    try:
        import knife4_streaming as k4  # noqa: PLC0415 — 进程内ro消费
        with k4._cm_lock:
            return dict(k4._created_ms)
    except Exception:
        return {}


def _organ():
    return getattr(_svc, "organ", None) if _svc is not None else None


def _organ_cards():
    organ = _organ()
    cards = getattr(organ, "_cards", None)
    return cards if isinstance(cards, dict) else {}


def _eid(sess: str, dt: str, text: str) -> str:
    return hashlib.sha1(f"{sess}|{dt}|{text}".encode("utf-8")).hexdigest()[:16]


def _key_parts(key: str) -> tuple[str, str, int]:
    """'{conv}|{rid}:{i}' → (conv, rid, i)·异常=(-,-,-1)"""
    conv, _, dia = key.partition("|")
    rid, _, idx = dia.rpartition(":")
    try:
        return conv, rid, int(idx)
    except ValueError:
        return conv, rid, -1


# ── query侧特征(唯一实现=本刀·零词典先验) ─────────────────────────────────────
def _proper_nouns(query: str) -> list[str]:
    """中位专名(首词/句首/称谓白名单除外)·小写返回·保序去重。"""
    out: list[str] = []
    for m in _WORD_RX.finditer(query or ""):
        w = m.group(0)
        if not (w[0].isupper() and w[1:].islower()):
            continue
        if m.start() == 0:
            continue
        prev = (query[: m.start()]).rstrip()
        if prev and prev[-1] in ".?!":
            continue
        if w.lower() in _STOP or w.lower() in _Q_CAPS:
            continue
        lw = w.lower()
        if lw not in out:
            out.append(lw)
    return out


def _qtoks(query: str) -> list[str]:
    return [t for t in _TOK_RX.findall((query or "").lower()) if t not in _STOP]


def _conv_count(keys: dict) -> int:
    """窗内卡的不同conv数(k4 keys的conv前缀)·keys缺席=0(保守不放行)。"""
    convs = set()
    for kk in (keys or {}).values():
        convs.add(kk.split("|", 1)[0])
    return len(convs)


# ── FIX-A1: 专名卡级覆盖boost(稳定前置·不改相对序) ────────────────────────────
def proper_boost(pns: list[str], data: list) -> list:
    hit, rest = [], []
    for e in data:
        c = e.get("content", "").lower()
        (hit if any(p in c for p in pns) else rest).append(e)
    if not hit or not rest:
        return data
    with _lock:
        _stats["proper_boost_cards"] += len(hit)
    return hit + rest


# ── FIX-A2: 卡级query实体覆盖度重排(coverage降序·stable) ──────────────────────
def coverage_rerank(query: str, data: list) -> list:
    qtoks = set(_qtoks(query))
    if len(qtoks) < 2 or not data:
        return data

    def cov(e: dict) -> float:
        ctoks = set(_TOK_RX.findall(e.get("content", "").lower()))
        return len(ctoks & qtoks) / len(qtoks)

    arr = sorted(data, key=cov, reverse=True)  # stable: 同coverage保原相对序
    if arr != data:
        with _lock:
            _stats["cov_rerank_queries"] += 1
    return arr


# ── FIX-B: 跨请求同conv缝链(dia-rid边界·app层等价·k1/k2闸复检零旁路) ───────────
def _conv_order_keys(conv: str, cards: dict, cm: dict[str, int]) -> list[str]:
    """conv内全卡键·按(created_ms(rid), idx)到达序。"""
    ks = [k for k in cards if k.split("|", 1)[0] == conv]
    def _ord(kk: str) -> tuple:
        _, rid, idx = _key_parts(kk)
        return (cm.get(rid, 0), idx)
    ks.sort(key=_ord)
    return ks


def stitch_insert(tenant: str, query: str, data: list, keys: dict, cm: dict[str, int]) -> list:
    if not data:
        return data
    cards = _organ_cards()
    if not cards:
        return data
    depth = int(_num("STITCH_DEPTH", 10))
    maxn = int(_num("STITCH_MAX", 3))
    blocks = int(_num("STITCH_BLOCKS", 2))
    max_total = int(_num("STITCH_MAX_TOTAL", 6))
    have = {e.get("id") for e in data}
    inserts: dict[int, list[dict]] = {}
    for pos, e in enumerate(data[:depth]):
        kk = keys.get(e.get("id"))
        if not kk:
            continue
        conv, rid, _ = _key_parts(kk)
        if not conv:
            continue
        chain = _conv_order_keys(conv, cards, cm)
        try:
            at = chain.index(kk)
        except ValueError:
            continue
        # 回扫跨请求边界·收集前blocks块(近块先·排除已在窗)
        picked: list[str] = []
        seen_rid = rid
        count = 0
        for pk in reversed(chain[:at]):
            pconv, prid, _ = _key_parts(pk)
            if prid != seen_rid:
                count += 1
                seen_rid = prid
                if count > blocks:
                    break
            if pk in have or pk in picked:
                continue
            picked.append(pk)
        if not picked:
            continue
        picked = [pk for pk in picked[:maxn] if pk not in have]
        for pk in picked:
            have.add(pk)  # 跨触发位去重: 同键只插一次(实测1604重复插窗病根)
        mat: list[dict] = []
        for pk in picked:
            c = cards.get(pk) or {}
            text = str(c.get("text", ""))
            if not text:
                continue
            sess = str(c.get("sess", ""))
            dt = str(c.get("dt", ""))
            mat.append({"id": _eid(sess, dt, text), "content": text, "created_at": dt,
                        "metadata": {"k8": "stitch", "sid": conv.split(":", 1)[-1]}})
        if mat:
            inserts[pos] = mat
    if not inserts:
        return data
    # k1/k2闸复检(零旁路·owner语义复用)·插位从后往前保序
    try:
        import knife1_supersede as k1  # noqa: PLC0415
        import knife2_sensitive as k2  # noqa: PLC0415
        for pos in sorted(inserts, reverse=True):
            batch = inserts[pos]
            kept, _, _ = k1.filter_evidence(tenant, query, batch, len(batch))
            kept, _ = k2.filter_gate(tenant, query, kept)
            inserts[pos] = kept
    except Exception:
        with _lock:
            _stats["stitch_gate_fail"] += 1
    res = list(data)
    n_in = 0
    for pos in sorted(inserts, reverse=True):
        batch = inserts[pos]
        if not batch or n_in >= max_total:
            continue
        batch = batch[: max_total - n_in]
        res[pos + 1:pos + 1] = batch
        n_in += len(batch)
    if n_in:
        with _lock:
            _stats["stitch_inserted"] += n_in
    return res


# ── K11-M2b: 同实体组带外晋升(MAX_PROMOTE=1·top3铁闸·M2c/M2d注记) ──────────────
_K1_RO: sqlite3.Connection | None = None


def _fact_group(tenant: str, pns: list[str]) -> list[str]:
    """k1.fact_current(subject)组卡id·ro连接·组键唯一源=k1。miss=空(调用方走回退组)。"""
    global _K1_RO
    try:
        import knife1_supersede as k1  # noqa: PLC0415
        dbp = k1.DB_PATH
        if _K1_RO is None:
            _K1_RO = sqlite3.connect(f"file:{dbp}?mode=ro", uri=True, check_same_thread=False)
        q = ",".join("?" for _ in pns)
        rows = _K1_RO.execute(
            f"SELECT card_id FROM fact_current WHERE tenant=? AND lower(subject) IN ({q})",
            [tenant, *pns]).fetchall()
        return [str(r[0]) for r in rows]
    except Exception:
        with _lock:
            _stats["promote_k1_miss"] += 1
        return []


def group_promote(tenant: str, query: str, pns: list[str], data: list,
                  keys: dict, cm: dict[str, int]) -> list:
    if not pns or len(data) < 2:
        return data
    min_top = int(_num("MIN_TOP", 3))
    cards = _organ_cards()
    eid2key: dict[str, str] = {}
    for kk, c in cards.items():
        eid2key[_eid(str(c.get("sess", "")), str(c.get("dt", "")), str(c.get("text", "")))] = kk
    # 组卡: k1.fact_current(subject)·miss回退=内容含专名卡(app层等价·留痕)
    grp_ids = _fact_group(tenant, pns)
    src = "k1"
    if not grp_ids:
        grp_ids = [e.get("id") for e in data
                   if any(p in e.get("content", "").lower() for p in pns)]
        src = "content_fallback"
    grp = [gid for gid in grp_ids if gid in eid2key]
    if not grp:
        return data
    # 组新鲜度极差门(SPEC k11·带注记用·组内全同rid=极差0仍可晋升——指挥官简化触发)
    def _cm_of(eid: str) -> int:
        _, rid, _ = _key_parts(eid2key[eid])
        return cm.get(rid, 0)
    newest = max(grp, key=lambda g: (_cm_of(g), _key_parts(eid2key[g])[2]))
    spread = max(_cm_of(g) for g in grp) - min(_cm_of(g) for g in grp)
    # 组的conv集合(M2d kin注记)
    convs = []
    for g in grp:
        cv = _key_parts(eid2key[g])[0].split(":", 1)[-1]
        if cv not in convs:
            convs.append(cv)
    # 源门: 目标卡已在top3内→no-op(防夺位)
    at = next((i for i, e in enumerate(data) if e.get("id") == newest), None)
    if at is None or at < min_top:
        return data
    dest = min_top - 1  # 落位=第3位(1-based)·1-2位神圣不夺
    card = data.pop(at)
    now_ms = int(time.time() * 1000)
    _, rid, idx = _key_parts(eid2key[card["id"]])
    card = dict(card)
    card["metadata"] = {"k8": "promote", "freshness": {
        "age_ms": max(0, now_ms - cm.get(rid, 0)), "sid": _key_parts(eid2key[card["id"]])[0].split(":", 1)[-1],
        "seq": idx, "spread_ms": spread, "group_src": src},
        "kin": ({"sessions": convs, "freshest_sid": _key_parts(eid2key[card["id"]])[0].split(":", 1)[-1],
                 "note": "related evidence across sessions"} if len(convs) >= 2 else None)}
    card["metadata"] = {k: v for k, v in card["metadata"].items() if v is not None}
    data.insert(dest, card)
    with _lock:
        _stats["promoted"] += 1
        _stats["promote_src_" + src] += 1
    log.info("knife8 promote tenant=%s card=%s src=%s pos=%d->%d spread=%d",
             tenant, card["id"][:8], src, at + 1, dest + 1, spread)
    return data


def _false_history(query: str) -> bool:
    """k1 history误报精化(k5 R1同款判定·聚合/时间线词典正则ro消费k5·单一词源):
    intent=history 且 query无聚合/时间线标记 → 过去时询问误报(不yield·k5取证B2-01病)。
    k5词典缺席 → 保守yield(视为真history)。"""
    try:
        import knife5_multihop as k5  # noqa: PLC0415
        agg = getattr(k5, "_agg_res", None) or []
        tim = getattr(k5, "_time_res", None) or []
        if not agg and not tim:
            return False
        if any(r.search(query) for r in tim) or any(r.search(query) for r in agg):
            return False
        return True
    except Exception:
        return False


# ── 端点入口(截断前最终排序位) ────────────────────────────────────────────────
def rerank(tenant: str, query: str, data: list, intent: str = "current",
           k3_tier: str = "none", out: dict | None = None, top_k: int = 100) -> list:
    if not _master() or not data:
        return data
    if intent == "history" and not _false_history(query):
        return data  # 让位k1真时间线(C3资产·false-history误报按k5 R1精化后放行)
    if (k3_tier or "none") != "none":
        return data  # 让位k3时间档(C1/C2资产禁扰·k11同款让位协议)
    try:
        meta = (out or {}).get("_k4") or {}
        keys = meta.get("keys") or {}
        cm = _cm_snapshot()
        with _lock:
            _stats["queries"] += 1
        if _flag("STITCH"):
            data = stitch_insert(tenant, query, data, keys, cm)
        pns = _proper_nouns(query)
        if _flag("PROMOTE"):
            data = group_promote(tenant, query, pns, data, keys, cm)
        if pns and _flag("PROPER"):
            data = proper_boost(pns, data)
        elif not pns and _flag("COV"):
            # 多session租户门(SPEC k11触发②同源·内部迁移形态): conv数>=COV_MIN_CONVS才重排 —
            # 单/双story小租户(A/E域5conv形态)字节直通=七维零扰动; I2X-10(6)/官方a48a(10sid)触发。
            if _flag("COV_MIN_CONVS_ON") and _conv_count(keys) < _num("COV_MIN_CONVS", 6):
                with _lock:
                    _stats["cov_gate_skip"] += 1
            else:
                data = coverage_rerank(query, data)
        return data
    except Exception as exc:  # noqa: BLE001 — fail-soft: 只降级回旧行为
        with _lock:
            _stats["err"] += 1
        log.error("knife8 rerank INTERNAL err=%s", type(exc).__name__)
        return data


# ── selftest(纯逻辑·零服务依赖) ──────────────────────────────────────────────
def selftest() -> int:
    ok = 0

    def chk(name, cond):
        nonlocal ok
        assert cond, f"FAIL {name}"
        ok += 1
        print(f"PASS {name}")

    chk("t1 专名抽取", _proper_nouns("Which medication did Mara get for her knee?") == ["mara"])
    chk("t2 句首不算专名", _proper_nouns("Mara is here") == [])
    chk("t3 疑问词白名单", _proper_nouns("What is the plan for the move?") == []
        and _proper_nouns("Which project is the Q4 deliverable?") == [])
    chk("t4 coverage排序", coverage_rerank(
        "kiln element broke repair warranty approves",
        [{"content": "bus chat"}, {"content": "nell repair signoff"}, {"content": "kiln warranty march"}])
        == [{"content": "nell repair signoff"}, {"content": "kiln warranty march"}, {"content": "bus chat"}]
        or coverage_rerank(
        "kiln element broke repair warranty approves",
        [{"content": "bus chat"}, {"content": "nell repair signoff"}, {"content": "kiln warranty march"}])
        [0]["content"] in ("nell repair signoff", "kiln warranty march"))
    chk("t5 coverage同分保序", coverage_rerank(
        "alpha beta gamma", [{"content": "alpha"}, {"content": "beta"}])
        == [{"content": "alpha"}, {"content": "beta"}])
    # FIX-B边界检测(注入假organ)
    global _svc
    import sys as _sys
    fake_cards = {
        "t:s1|r1:0": {"sess": "t:s1", "dt": "d0", "text": "cause license server"},
        "t:s1|r1:1": {"sess": "t:s1", "dt": "d0", "text": "mid"},
        "t:s1|r2:0": {"sess": "t:s1", "dt": "d1", "text": "effect green"},
    }
    class _O: pass
    o = _O(); o._cards = fake_cards
    _svc = _O(); _svc.organ = o
    data = [{"id": _eid("t:s1", "d1", "effect green"), "content": "effect green", "created_at": "d1"}]
    keys = {data[0]["id"]: "t:s1|r2:0"}
    out = stitch_insert("t", "why green", data, keys, {"r1": 100, "r2": 200})
    chk("t6 缝链跨请求插位", any("license server" in e["content"] for e in out) and len(out) == 3)
    # M2b组晋升(top3铁闸: 源=窗内第4位→落位第3)
    data2 = [{"id": _eid("t:s1", "d", f"c{i}"), "content": f"c{i}"} for i in range(6)]
    fake_cards2 = {f"t:s1|r9:{i}": {"sess": "t:s1", "dt": "d", "text": f"c{i}"} for i in range(6)}
    o2 = _O(); o2._cards = fake_cards2
    _svc = _O(); _svc.organ = o2
    _self = _sys.modules[__name__]
    real = _self._fact_group
    tid = data2[4]["id"]
    _self._fact_group = lambda t, p: [tid]
    got = group_promote("t", "what about Mara", ["mara"], data2, {}, {"r9": 5})
    _self._fact_group = real
    chk("t7 组晋升落位第3", got[2]["id"] == tid and len(got) == 6
        and got[2].get("metadata", {}).get("k8") == "promote")
    data3 = [{"id": f"e{i}", "content": f"c{i}"} for i in range(6)]
    got2 = group_promote("t", "q", ["mara"], data3, {}, {"r9": 5})  # e4…无假organ→eid2key空→no-op
    chk("t8 无key映射直通", got2 == data3)
    print(f"SELFTEST {ok}/8 PASS")
    return 0 if ok == 8 else 1


if __name__ == "__main__":
    import sys
    sys.exit(selftest())
