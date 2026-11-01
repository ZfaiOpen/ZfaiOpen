# perf_cache.py — C1席搜索性能刀(20261011·app层精确memo overlay·冻结核心/organ/exam文件零修改)
# ═════════════════════════════════════════════════════════════════════════════
# 语义保证: 三件全是"同输入⇒同输出"纯记忆化(同query+同organ状态⇒逐字节同窗口),
#   零排序键/阈值/过滤逻辑改动(性能动刀铁律: 只许快不许变)。
#   ① organ.search_ex 精确参数memo — key=(query, conv_scope, topk, ver, gen):
#     ver=len(organ._cards)(add/forget即失效), gen=finalize代数(包装finalize_ingest自增;
#     治"add→防抖finalize→新卡emb向量补齐→同ver同query窗口变"的时序病)。
#     连带缓存k4 compose的ranked stash(_tls.ranked/ranked_map)并在命中时复放 —
#     保knife4 recency/_attach消费语义逐位(_attach见knife4内eid索引配套改造)。
#     脏态(svc._dirty/_finalize_pending)直接旁路不读不写=基线路径逐位。
#   ② rank_bm25.BM25Okapi.get_scores per-instance memo — 索引重建(_rebuild_conv新建
#     BM25Okapi实例)即自然失效; 返回同一ndarray只读(organ仅min/max/argsort/索引)。
#     加速search_ex memo miss后的重复扫描与k5潜在二跳。
#   ③ SVC.recents 租户级memo(原单条目跨租户互踩) — 同原实现计算(dt降序四字段投影),
#     ver校验失效, 返回前捷新list(kfill只读)。
# 并发: organ读相全在SVC.lock/_read_guard内(k4 RW缺省关)⇒memo字典无并发写; OrderedDict仅
#   在锁内触碰。内存: search_ex≤CAP条(cards引用+ranked快照, 满扫条目~230KB·上限128)。
# env: BRAINOS_PERF_CACHE(缺省开·=0关门回基线逐位)。
# 回滚 = aml_service.py摘"import perf_cache"与install两行+还原knife4三hunk+摘本文件。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import collections
import os
import time
from typing import Any

CAP_SEARCH = int(os.environ.get("BRAINOS_PERF_CAP_SEARCH", "192"))
CAP_RECENTS = 64
CAP_BM25 = 4  # 每BM25Okapi实例缓存的query分数组数(w/c双通道交替)

_stats: dict[str, int] = {}


def _master() -> bool:
    return os.environ.get("BRAINOS_PERF_CACHE", "1") != "0"


def install(svc: Any) -> dict[str, bool]:
    """启动期一次性安装(aml _on_startup·k4之后); 幂等; 调用方fail-soft。"""
    flags = {"search_ex": False, "bm25": False, "recents": False, "gen": False}
    if not _master() or getattr(svc, "_pc1_installed", False):
        return flags
    organ = svc.organ

    # ── finalize代数: 每次finalize_ingest自增(search_ex memo key分量) ──
    orig_fin = organ.finalize_ingest
    gen = [0]

    def _fin(force: bool = False):
        gen[0] += 1
        return orig_fin(force)

    organ.finalize_ingest = _fin
    flags["gen"] = True

    # ── ① organ.search_ex 精确memo(k4 ranked stash复放) ──
    # organ_ms: memoize该(query,topk)的organ实耗 — 暖态命中时经knife4 _tls交给
    # _search_impl写out["_pc1_organ_ms"], 端点侧据此把k5预算门虚拟t0回推 — 预算决策
    # ="若无缓存基线会怎么判"(基线冷态organ 700-1400ms跨k5预算线·纯时序竞态·冻结之)。
    orig_sex = organ.search_ex
    cache: collections.OrderedDict = collections.OrderedDict()

    def _set_oms(v):
        try:
            from knife4_streaming import _tls as _k4tls
            _k4tls.organ_ms = v
        except Exception:
            pass

    def search_ex(query, conv_scope=None, topk=10):
        if getattr(svc, "_dirty", False) or getattr(svc, "_finalize_pending", False):
            _set_oms(None)  # 脏态旁路=基线路径(无虚拟organ耗时)
            return orig_sex(query, conv_scope=conv_scope, topk=topk)
        key = (query, conv_scope, topk, len(organ._cards), gen[0])
        hit = cache.get(key)
        if hit is not None:
            cache.move_to_end(key)
            _stats["sex_hit"] = _stats.get("sex_hit", 0) + 1
            k4t = hit[2]
            if k4t is not None:  # 复放k4 ranked stash(recency/_attach消费)
                try:
                    from knife4_streaming import _tls as _k4tls
                    _k4tls.ranked = k4t[0]
                    _k4tls.ranked_map = k4t[1]
                except Exception:
                    pass
            _set_oms(hit[3])
            return hit[0], hit[1]
        _t_o = time.perf_counter()
        cards, score = orig_sex(query, conv_scope=conv_scope, topk=topk)
        oms = (time.perf_counter() - _t_o) * 1000.0
        rk = rmap = None
        try:
            from knife4_streaming import _tls as _k4tls
            rk = getattr(_k4tls, "ranked", None)
            if rk is not None:
                rmap = dict(rk)  # 预建sc字典(_attach复用·免每请求重建)
        except Exception:
            rk = rmap = None
        if len(cache) >= CAP_SEARCH:
            cache.popitem(last=False)
        cache[key] = (cards, float(score),
                      None if rk is None else (rk, rmap), oms)
        _set_oms(oms)
        _stats["sex_fill"] = _stats.get("sex_fill", 0) + 1
        return cards, score

    organ.search_ex = search_ex
    flags["search_ex"] = True

    # ── ② BM25Okapi.get_scores per-instance memo ──
    try:
        import rank_bm25

        orig_gs = rank_bm25.BM25Okapi.get_scores

        def gs(self, query):
            try:
                q = tuple(query)
                m = self.__dict__.get("_pc1_gs")
                if m is not None:
                    arr = m.get(q)
                    if arr is not None:
                        _stats["bm25_hit"] = _stats.get("bm25_hit", 0) + 1
                        return arr
                arr = orig_gs(self, query)
                m = self.__dict__.get("_pc1_gs")
                if m is None:
                    m = {}
                    self.__dict__["_pc1_gs"] = m
                if len(m) >= CAP_BM25:
                    m.pop(next(iter(m)))
                m[q] = arr
                _stats["bm25_fill"] = _stats.get("bm25_fill", 0) + 1
                return arr
            except Exception:
                return orig_gs(self, query)

        rank_bm25.BM25Okapi.get_scores = gs
        flags["bm25"] = True
    except Exception:
        pass

    # ── ③ SVC.recents 租户级memo ──
    orig_rec = svc.recents
    rc: collections.OrderedDict = collections.OrderedDict()

    def recents(tenant: str, n: int):
        try:
            ver = len(organ._cards)
            ent = rc.get(tenant)
            if ent is None or ent[0] != ver:
                cards = [c for c in organ._cards.values()
                         if str(c.get("conv", "")).split(":", 1)[0] == tenant]
                cards.sort(key=lambda c: str(c.get("dt", "")), reverse=True)
                rows_all = [{"session": c.get("sess", ""), "dt": c.get("dt", ""),
                             "speaker": c.get("speaker", ""), "text": c.get("text", "")}
                            for c in cards]
                if len(rc) >= CAP_RECENTS:
                    rc.popitem(last=False)
                ent = (ver, rows_all)
                rc[tenant] = ent
                _stats["rec_fill"] = _stats.get("rec_fill", 0) + 1
            else:
                _stats["rec_hit"] = _stats.get("rec_hit", 0) + 1
            rows = ent[1]
            return list(rows[:max(0, int(n))])
        except Exception:
            return orig_rec(tenant, n)

    svc.recents = recents
    flags["recents"] = True
    svc._pc1_installed = True
    return flags


def stats() -> dict[str, int]:
    return dict(_stats)
