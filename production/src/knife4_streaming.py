# /home/zfai/zo_deploy/app/knife4_streaming.py — 部署线第4刀·I1流式延迟刀(纯overlay·冻结核心/organ/exam零修改)
# ═══════════════════════════════════════════════════════════════════════════════
# 刀口(取证=~/exam/pipeline/SPEC_I1_streaming.md §二/§四·官方低分格最大死因=队列谋杀:
#  15路search同毫秒涌入→全局RLock串行→队尾68.7s·超30s的12查全落低分域):
#   L1 全局RLock串行车队(memory_service.py:85) → M1b RW读写分离(overlay层·写优先·读共享)
#   L2 每conv重复encode同query(hippocampal_recall.py:712·66-132次/search) → M1a encode-memo
#   L3 小租户双趟全库重扫(memory_service.py:141-147) → M1b 单趟直达(语义逐位等价)
# 机制:
#   M1a encode-memo  — organ._qmodel.encode 实例级包装: exact-string单飞memo(缺省TTL 500ms;
#                      ST encode对同串确定性→零正确性风险·TTL只控驻留)·Event单飞防15路同查
#                      重复encode·返回copy()防跨调用方共享可变视图·批量调用原样直通(卡embed不受扰)
#   M1b 单趟+RW      — SVC.search包装: 小租户(n_t<k且全库>k*8)时原逻辑首趟必被二趟整体替换,
#                      直达topk=total单趟=逐位等价; 其余路径逐字复刻原逻辑(含脏兜底finalize/
#                      二趟回退/租户前缀过滤/四字段投影)。读路径走RW.reader共享, 写路径(SVC.add
#                      包装)走RW.writer独占·写优先防饿死; 原RLock在orig内部保留(关门路径零变化;
#                      锁序恒 rw→svc 无环)。并发读安全性: organ检索相共享态全只读(last_*属性
#                      GIL原子·FSRS retrieve_with_meta纯dict get·BM25 get_scores纯计算·写相
#                      (ingest/finalize改_idx/_cards/_emb+重建BM25)被writer独占排除)
#   M2  带内recency  — organ._compose_window包装捕获ranked(key→organ终分·同线程tls) +
#                      ledger created_ms(不受墙钟污染的真实入库时序): search端点k3后data[:top_k]
#                      前调recency_reorder——相邻分差<5%(相对·env可调)带内按入库时序新者在前,
#                      只换序不改集(窗口集合恒等·官方已定谳格零集合风险); k1 history时间线意图
#                      (dt升序=C3资产)直接让位; organ邻接扩展卡无ranked分→不参与band(保序)
# env(缺省=开·=0一键关门回旧行为): BRAINOS_K4(总闸) / BRAINOS_K4_ENCODE_MEMO /
#   BRAINOS_K4_ONEPASS / BRAINOS_K4_RW / BRAINOS_K4_RECENCY / BRAINOS_K4_MEMO_TTL_MS(500)
#   / BRAINOS_K4_RECENCY_EPS(0.05)
# 回滚 = 还原 pre_knife4_1008/aml_service.py + 摘除本文件/knife4_probe.py/knife4_selftest.py
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import contextlib
import hashlib
import os
import sqlite3
import threading
import time
from bisect import bisect_right
from collections import Counter
from typing import Any

_MEMO_TTL_S = float(os.environ.get("BRAINOS_K4_MEMO_TTL_MS", "500")) / 1000.0
_EPS = float(os.environ.get("BRAINOS_K4_RECENCY_EPS", "0.05"))
_MEMO_CAP = 256
# RW缺省关(2026-10-09红蓝实测留痕): 15路同拍洪峰 RW并行读=7.9s vs RLock读汇聚=3.43s —
# GIL纯CPU负载下并行读互踩+77%争抢税; 车队已由M1a+M1b消(临界段7.4s→0.23s/查), 读汇聚即最优。
# RW机制保留可用(BRAINOS_K4_RW=1): 写多读少/释放GIL负载形态下再战(自检C1-C3护栏在)。
_FLAG_DEFAULT = {"RW": "0"}

_stats: Counter = Counter()


def _master() -> bool:
    return os.environ.get("BRAINOS_K4", "1") != "0"


def _flag(name: str) -> bool:
    return os.environ.get(f"BRAINOS_K4_{name}", _FLAG_DEFAULT.get(name, "1")) != "0"


def stats() -> dict[str, int]:
    return dict(_stats)


# ── RW读写锁(写优先: 读者共享/写者独占/等待写者阻塞新读者防饿死) ──────────────
class _RWLock:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    @contextlib.contextmanager
    def reader(self):
        with self._cond:
            while self._writer or self._waiting_writers:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextlib.contextmanager
    def writer(self):
        with self._cond:
            self._waiting_writers += 1
            while self._writer or self._readers:
                self._cond.wait()
            self._waiting_writers -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._cond:
                self._writer = False
                self._cond.notify_all()


# ── M1a encode-memo(exact-string单飞·确定性编码故TTL只控驻留) ─────────────────
class _EncodeMemo:
    def __init__(self, ttl_s: float) -> None:
        self._ttl = ttl_s
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, Any]] = {}
        self._inflight: dict[str, threading.Event] = {}

    def wrap(self, orig_encode):
        def encode(sentences, *a, **kw):
            if not (isinstance(sentences, list) and len(sentences) == 1
                    and isinstance(sentences[0], str)):
                return orig_encode(sentences, *a, **kw)  # 批量/非串: 原样(卡增量embed走此路)
            q = sentences[0]
            now = time.monotonic()
            with self._lock:
                hit = self._cache.get(q)
                if hit is not None and hit[0] > now:
                    _stats["memo_hit"] += 1
                    return hit[1].copy()
                ev = self._inflight.get(q)
                owner = ev is None
                if owner:
                    ev = threading.Event()
                    self._inflight[q] = ev
            if not owner:
                _stats["memo_singleflight_wait"] += 1
                ev.wait(timeout=10.0)
                with self._lock:
                    hit = self._cache.get(q)
                if hit is not None and hit[0] > time.monotonic():
                    return hit[1].copy()
                return orig_encode(sentences, *a, **kw)  # 等待超时: 自编(fail-soft)
            try:
                vec = orig_encode(sentences, *a, **kw)
                with self._lock:
                    if len(self._cache) >= _MEMO_CAP:
                        self._cache.clear()  # 驻留上限: 到顶即清(确定性编码·清零无正确性影响)
                    self._cache[q] = (time.monotonic() + self._ttl, vec)
                _stats["memo_encode"] += 1
                return vec.copy()
            finally:
                with self._lock:
                    self._inflight.pop(q, None)
                ev.set()

        return encode


# ── 模块态(install后生效) ────────────────────────────────────────────────────
_svc: Any = None
_rw = _RWLock()
_orig_search: Any = None
_orig_add: Any = None
_created_ms: dict[str, int] = {}
_cm_lock = threading.Lock()
_tls = threading.local()
_installed = False


def install(svc: Any, ledger: Any = None, ledger_db: Any = None) -> dict[str, bool]:
    """启动期一次性安装(aml _on_startup内·_warmup之后_qmodel已载); 幂等; 调用方fail-soft。"""
    global _svc, _orig_search, _orig_add, _installed
    if _installed:
        return {"installed": True}
    if not _master():
        return {}  # 总闸关门: 零挂件(自检F2契约)
    _svc = svc
    flags: dict[str, bool] = {"memo": False, "search": False, "add": False,
                              "compose": False, "ledger": False}
    # M1a: encode memo(冻结organ零修改·实例级方法包装)
    if _flag("ENCODE_MEMO"):
        qm = getattr(svc.organ, "_qmodel", None)
        if qm is not None and not getattr(qm, "_k4_memo", False):
            qm.encode = _EncodeMemo(_MEMO_TTL_S).wrap(qm.encode)
            qm._k4_memo = True
            flags["memo"] = True
    # M2取数: compose快照(organ.search每调一次·与k4_search同线程经tls交接)
    if _flag("RECENCY") and not getattr(svc.organ, "_k4_compose", False):
        organ = svc.organ
        orig_compose = organ._compose_window

        def _compose_wrap(ranked, spk_keys, topk, __o=orig_compose):
            _tls.ranked = list(ranked)
            return __o(ranked, spk_keys, topk)

        organ._compose_window = _compose_wrap
        organ._k4_compose = True
        flags["compose"] = True
    # M1b/M2: search包装(单趟+RW读+stash)
    if (_flag("ONEPASS") or _flag("RW") or _flag("RECENCY")) and not getattr(svc, "_k4_search", False):
        _orig_search = svc.search
        svc.search = _k4_search
        svc._k4_search = True
        flags["search"] = True
    # RW写侧: add包装(写独占; 原RLock在orig内部保留=关门路径零变化)
    if _flag("RW") and not getattr(svc, "_k4_add", False):
        _orig_add = svc.add
        svc.add = _k4_add
        svc._k4_add = True
        flags["add"] = True
    # M2: ledger created_ms载入(只读连接一次性SELECT)+put包装增量维护
    if _flag("RECENCY") and ledger is not None and ledger_db is not None:
        try:
            conn = sqlite3.connect(f"file:{ledger_db}?mode=ro", uri=True)
            try:
                for rid, cm in conn.execute("SELECT request_id, created_ms FROM ledger"):
                    _created_ms[str(rid)] = int(cm)
            finally:
                conn.close()
            orig_put = ledger.put

            def _put(rid, user_id, response_json, cards_json, __o=orig_put):
                first = __o(rid, user_id, response_json, cards_json)
                if first:
                    with _cm_lock:
                        _created_ms[str(rid)] = int(time.time() * 1000)
                return first

            ledger.put = _put
            flags["ledger"] = True
        except Exception:
            _stats["ledger_load_fail"] += 1
    _installed = True
    return flags


# ── 读守卫: RW开=读共享; 关/写侧未装=SVC.lock(旧行为) ────────────────────────
@contextlib.contextmanager
def _read_guard():
    if _flag("RW") and getattr(_svc, "_k4_add", False):
        with _rw.reader():
            yield
    else:
        with _svc.lock:
            yield


def _k4_add(items, finalize, tenant: str = "default"):
    if not _master() or not _flag("RW"):
        return _orig_add(items, finalize, tenant)
    with _rw.writer():
        return _orig_add(items, finalize, tenant)


# ── M1b: search包装(单趟租户取窗·语义与memory_service.search逐位等价) ─────────
# perf_c1配套(20261011): n_t租户计数按ver缓存 — 原式每search全库29k次startswith扫描
# (~3-5ms)。输出与原式逐位等价: organ键恒为"{tenant}:{conv}|{dia}"(tenant消毒禁冒号,
# add/replay/forget全路径加前缀)⇒startswith(t+":")⟺首段split(":")==t; ver=卡数变化
# (add/forget)才重建(一次全扫=原一次同价·仅写相后首查支付)。
# BUGFIX-perfc1(20261011首版回窗竞态归因): 首版误以pre("t:"带尾冒号)查_tcnt(键=t无冒号)
# ⇒n_t恒0⇒fk=total单趟⇒organ冷耗时667ms vs 基线两趟1377ms⇒k5预算门(>60%×1500=900ms
# 降级)竞态翻面(基线degrade/首版fire)→二跳卡注入差3卡。本版改传tenant(无冒号)=与基线
# n_t逐位同值⇒fk/两趟行为/冷态organ耗时全同基线。
_tcnt: dict[str, int] = {}
_tcnt_ver = [-1]


def _tenant_count(svc: Any, tenant: str) -> int:
    cards = svc.organ._cards
    ver = len(cards)
    if _tcnt_ver[0] != ver:
        _tcnt.clear()
        for kk in cards:
            t = kk.split(":", 1)[0]
            _tcnt[t] = _tcnt.get(t, 0) + 1
        _tcnt_ver[0] = ver
    return _tcnt.get(tenant, 0)


def _k4_search(query: str, conv_scope: str | None, top_k: int, tenant: str = "default"):
    if not _master():
        return _orig_search(query, conv_scope, top_k, tenant)
    try:
        return _search_impl(query, conv_scope, top_k, tenant)
    except Exception:
        _stats["search_fallback"] += 1  # fail-soft: k4内部异常→原路径(含原锁)兜底重试
        return _orig_search(query, conv_scope, top_k, tenant)


def _search_impl(query: str, conv_scope: str | None, top_k: int, tenant: str):
    svc = _svc
    k = max(1, int(top_k))
    if conv_scope is not None or not _flag("ONEPASS"):
        with _read_guard():
            out = _orig_search(query, conv_scope, top_k, tenant)
        return _attach(out)
    if os.environ.get("BRAINOS_MS_TENANT_FETCH", "0") == "1":
        # [M1.5共存·轨道B 20261025] 租户直取门开时k4让位: 下方ONEPASS复刻体是legacy
        # 二趟语义的逐位复刻·不含memory_service.search的M1.5租户直取分支→K4=1时
        # MS_TENANT_FETCH=1被静默短路(k4_m15_composition_test实证calls=0·记分板
        # phaseC≡phaseB2佐证)。让位=委托_orig_search(内含M1.5分支·自带fail-soft
        # 回退legacy); k4其余挂件不受影响——M1a encode-memo包的是organ._qmodel.encode
        # (M1.5 _tenant_fetch经同一实例encode照吃memo·依旧单次encode), M2 recency/
        # _attach按evidence_id反查与生产者无关, RW读守卫原样保留。
        # 门关(缺省0)=本分支不可达=ONEPASS逐字节原行为。
        _stats["search_m15_yield"] += 1
        with _read_guard():
            out = _orig_search(query, conv_scope, top_k, tenant)
        return _attach(out)
    with _read_guard():
        total = len(svc.organ._cards)
        pre = f"{tenant}:"
        n_t = _tenant_count(svc, tenant)
        fetch = min(total, k * 8)
        # 小租户必败首趟消去: n_t<k ⇒ 原首趟租户命中<k且fetch<total ⇒ 二趟topk=total整体替换
        # ⇒ 直达单趟=输出逐位等价; 其余情形逐字复刻原逻辑(含二趟回退)
        fk = total if (n_t < k and fetch < total) else fetch
        if svc._total and svc._dirty:
            # [刀8.8h·v0.4.1终结修20261017·FULL实弹py-spy活捉] search内联finalize_ingest
            # (113k卡BM25重建分钟级)=add/search全堵真凶。改置防抖pending标志——后台
            # _finalize_worker接手(请求路径零重建)·可见性延迟≤防抖窗(T-A既有已接受语义)。
            svc._finalize_pending = True
        _oms_acc = 0.0  # perf_c1: organ虚拟实耗累加(两趟=pass1+pass2·与基线冷态同时序)
        cards, score = svc.organ.search_ex(query, None, fk)
        _oms_acc += getattr(_tls, "organ_ms", None) or 0.0
        _tls.organ_ms = None
        cards = [c for c in cards if str(c.get("conv", "")).split(":", 1)[0] == tenant]
        if len(cards) < k and fk < total:
            cards, score = svc.organ.search_ex(query, None, total)
            _oms_acc += getattr(_tls, "organ_ms", None) or 0.0
            _tls.organ_ms = None
            cards = [c for c in cards if str(c.get("conv", "")).split(":", 1)[0] == tenant]
        cards = cards[:k]
        _stats["search_onepass" if fk == total else "search_std"] += 1
    out = {"evidence": [
        {"session": c.get("sess", ""), "dt": c.get("dt", ""),
         "speaker": c.get("speaker", ""), "text": c.get("text", "")} for c in cards],
        "evidence_score": float(score)}
    # perf_c1: memo命中的organ虚拟实耗(端点侧k5预算门冻结用·基线无缓存时同时序)
    if _oms_acc:
        out["_pc1_organ_ms"] = float(_oms_acc)
    return _attach(out)


# ── M2取数: evidence四字段→organ key/ranked分 反查(evidence_id同aml _evidence_id式) ──
# perf_c1配套(20261011): 原 Attach 每search全库29k卡逐条sha1+早停扫描(~30-50ms)。
# 改为ver缓存倒排(eid→升序出现位表) + 早停语义精确模拟: B=各want eid首次出现位的最大值
# (全集中完位点); keys[e]=该eid ≤B 的最大出现位(原"break前最后一次覆写")。eid重复
# (同sess|dt|text重复卡)与scores覆写路径逐位等价; want含缺失eid(不可能路径·防御)=回退原全扫。
_eid_occ: dict[str, list[int]] = {}
_eid_keys: list[str] = []
_eid_ver = [-1]


def _eid_rebuild(cards: dict) -> None:
    occ: dict[str, list[int]] = {}
    keylist: list[str] = []
    for i, (kk, c) in enumerate(cards.items()):
        eid = hashlib.sha1(
            f'{c.get("sess", "")}|{c.get("dt", "")}|{c.get("text", "")}'.encode("utf-8")
        ).hexdigest()[:16]
        occ.setdefault(eid, []).append(i)
        keylist.append(kk)
    _eid_occ.clear()
    _eid_occ.update(occ)
    _eid_keys.clear()
    _eid_keys.extend(keylist)
    _eid_ver[0] = len(cards)


def _eid_lookup(want: set, sc: dict) -> tuple[dict, dict]:
    cards = _svc.organ._cards
    if _eid_ver[0] != len(cards):
        _eid_rebuild(cards)
    keys: dict[str, str] = {}
    scores: dict[str, float] = {}
    for e in want:
        if not _eid_occ.get(e):  # 防御: 缺失=回退原全扫路径(逐位同原实现)
            for kk, c in cards.items():
                eid = hashlib.sha1(
                    f'{c.get("sess", "")}|{c.get("dt", "")}|{c.get("text", "")}'.encode("utf-8")
                ).hexdigest()[:16]
                if eid in want:
                    keys[eid] = kk
                    s = sc.get(kk)
                    if s is not None:
                        scores[eid] = float(s)
                    if len(keys) == len(want):
                        break
            return keys, scores
    b = max(_eid_occ[e][0] for e in want)  # 全集中完位点(原break位置)
    for e in want:
        o = _eid_occ[e]
        kk = _eid_keys[o[bisect_right(o, b) - 1]]  # ≤B最大出现位=break前最后覆写
        keys[e] = kk
        s = sc.get(kk)
        if s is not None:
            scores[e] = float(s)
    return keys, scores


def _attach(out: dict) -> dict:
    ranked = getattr(_tls, "ranked", None)
    rmap = getattr(_tls, "ranked_map", None)
    _tls.ranked = None
    _tls.ranked_map = None
    if not _master() or not _flag("RECENCY") or not ranked or not out.get("evidence"):
        return out
    try:
        want = {hashlib.sha1(
            f'{e.get("session", "")}|{e.get("dt", "")}|{e.get("text", "")}'.encode("utf-8")
        ).hexdigest()[:16] for e in out["evidence"]}
        sc = rmap if rmap is not None else dict(ranked)  # perf_c1: memo命中时复用预建dict
        with _read_guard():
            keys, scores = _eid_lookup(want, sc)
        out["_k4"] = {"scores": scores, "keys": keys}
        _stats["attach_ok"] += 1
    except Exception:
        _stats["attach_fail"] += 1  # fail-soft: 无stash=M2直通(检索行为零影响)
    return out


# ── M2: 带内recency决胜(端点k3后·data[:top_k]前·只换序不改集) ─────────────────
def _in_band(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False
    da = abs(a - b)
    if da == 0.0:
        return True
    return da / max(abs(a), abs(b), 1e-12) < _EPS


def recency_reorder(tenant: str, query: str, data: list, intent: str,
                    out: dict | None = None, k3_tier: str = "none") -> list:
    """相邻分差<eps(相对5%)带内按ledger created_ms新者在前·只换序不改集。
    让位协议: intent=history(k1时间线dt升序=C3资产)或 k3_tier!="none"(k1/k3时间域排序资产:
    duration双日期卡前插/when浮前排/C2位次)→原样返回。无分卡不参与band(原稳态序)。"""
    if not _master() or not _flag("RECENCY") or intent == "history" or k3_tier != "none":
        return data
    if out is None or len(data) < 2:
        return data
    meta = out.get("_k4") or {}
    scores: dict = meta.get("scores") or {}
    keys: dict = meta.get("keys") or {}
    if len(scores) < 2:
        return data
    with _cm_lock:
        cm_snap = dict(_created_ms)

    def sort_key(e: dict):
        eid = e.get("id")
        s = scores.get(eid)
        if s is None:
            return (0, 0, -1)  # 无分: 恒键→stable sort保原序
        kk = keys.get(eid) or ""
        dia = kk.split("|", 1)[1] if "|" in kk else ""
        rid, _, idx = dia.rpartition(":")
        try:
            idx_i = int(idx)
        except ValueError:
            idx_i = -1
        return (1, cm_snap.get(rid, 0), idx_i)

    arr = list(data)
    res: list = []
    moved = 0
    i, n = 0, len(arr)
    while i < n:
        j = i + 1
        while j < n and _in_band(scores.get(arr[j - 1].get("id")),
                                 scores.get(arr[j].get("id"))):
            j += 1
        seg = arr[i:j]
        if j - i > 1:
            srt = sorted(seg, key=sort_key, reverse=True)  # reverse仍稳定(同键保原相对序)
            if srt != seg:
                moved += 1
            seg = srt
        res.extend(seg)
        i = j
    if moved:
        _stats["recency_moved"] += moved
    return res
