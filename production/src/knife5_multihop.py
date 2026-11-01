# /home/zfai/zo_deploy/app/knife5_multihop.py — 部署线第5刀·S3多跳扩查刀(纯overlay·零LLM)
# ═══════════════════════════════════════════════════════════════════════════════
# 刀口(官方B2因果链=0·最大剩余血洞; 影子根因取证20261009):
#   ①k1历史意图误报: "Why did X..."式过去时**因果/事实询问**命中 \bdid\b / has+Ved
#     →k1按history把整窗dt升序重排→真正证据卡(会话后段)被埋到rank 31+(影子B2-01实测:
#     证据卡rank 31/34/36·窗召回100%但top10=0——"不缺召回缺分辨"的排序病)
#   ②关系链二跳: "A的哥哥是X→X住在Y"链式事实, 单跳query词面只命中桥卡(是X卡),
#     Y卡与query零词面交集→单跳必漏(Y卡只在含X时可达)
# 机制(三件套·全部零LLM零题目先验·词典可增量knife5_relations.json):
#   R1 意图精化(false-history矫正) — k1.detect_intent=history 且 query含**聚合/时间线标记**
#      (list/every/all/trace/timeline/in order/哪些/轨迹...)→真历史→S3整体让位(零触碰);
#      history但无聚合标记=过去时询问误报→修复分支: 桥卡+二跳新卡前置(其余原dt序保持)。
#      k1代码零修改(误报由S3在其后修复·让位协议对表PK4判例: 在飞排序资产必须订阅让位)。
#   R2 实体桥接扩查 — query抽专名(大写启发式+的-前缀·中英)→从一跳窗(含speaker字段)找桥卡
#      (专名命中speaker/text; 无专名→词面重叠卡)→桥卡内容抽扩查词(去停用/去query词·频序
#      ·封顶12)→二跳 query=原query+扩查词(原query保留:语义三通道不丢锚)。
#   R3 合并去重+邻插 — 二跳卡按卡id(eid=sha1(session|dt|text)[:16]与_evidence_id同源)去重;
#      修复分支=桥卡+二跳新卡前置; 常规分支=二跳新卡邻插到首桥卡之后(封顶INSERT_MAX防
#      top10挤位·余量窗尾追加); out["_k4"]scores/keys并集(k4 recency对合并窗继续有效)。
# 触发门(与预算): 关系链词命中 **或** 一跳top1分<阈(默认0=关·env可调) 才走二跳;
#   总预算BRAINOS_K5_BUDGET_MS(缺省8000)·已耗时>60%预算→降级单跳(fail-soft);
#   真历史/时序域(C1/C2)整体让位→A/C/D/E/G/H/I零结构扰动。
# 顺序: 管线位=k1过滤后、k2门前挂载(修复分支必须作用于k1 dt重排后的窗·eid对齐防错位),
#   k2/k3/k4全部在合并窗上继续生效(二跳卡同过H2/C1/M2门=零旁路); 刀线位=第5刀(knife4后装)。
# env: BRAINOS_K5(总闸·缺省1) / BRAINOS_K5_BUDGET_MS(8000) / BRAINOS_K5_MIN_TOP(0=分门关)
#   / BRAINOS_K5_HOP2_K(64) / BRAINOS_K5_TOP_M(3) / BRAINOS_K5_INSERT_MAX(3)
#   / BRAINOS_K5_FRONT_MAX(6) / BRAINOS_K5_MAX_EXPAND(12)
# 回滚 = 还原 pre_knife5_1008/aml_service.py + 摘除 knife5_multihop.py/knife5_relations.json/
#   knife5_probe.py/knife5_selftest.py (无DB·S3零状态)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import logging  # noqa: E402

log = logging.getLogger("aml")

AppDir = Path(__file__).resolve().parent
DICT_PATH = AppDir / "knife5_relations.json"

_BUDGET_MS = float(os.environ.get("BRAINOS_K5_BUDGET_MS", "8000"))
_MIN_TOP = float(os.environ.get("BRAINOS_K5_MIN_TOP", "0"))   # 0=分数门关(词典门独立生效)
_HOP2_K = int(os.environ.get("BRAINOS_K5_HOP2_K", "64"))
_TOP_M = int(os.environ.get("BRAINOS_K5_TOP_M", "3"))
_INSERT_MAX = int(os.environ.get("BRAINOS_K5_INSERT_MAX", "3"))
_FRONT_MAX = int(os.environ.get("BRAINOS_K5_FRONT_MAX", "6"))
_MAX_EXPAND = int(os.environ.get("BRAINOS_K5_MAX_EXPAND", "12"))

_stats: Counter = Counter()
_lock = threading.Lock()
_dict_mt = 0.0
_DICT: dict[str, Any] = {}
_trip_res: list[re.Pattern] = []
_agg_res: list[re.Pattern] = []
_time_res: list[re.Pattern] = []
_stop_en: set[str] = set()
_stop_zh: set[str] = set()

_EN_CAP = re.compile(r"\b([A-Z][a-zA-Z]{2,}(?:'s)?)\b")
_ZH_DE = re.compile(r"([\u4e00-\u9fa5]{2,4})的")
_EN_WORD = re.compile(r"[a-zA-Z][a-zA-Z']{2,}")
_ZH_RUN = re.compile(r"[\u4e00-\u9fa5]{2,4}")
_CAP_STOP = {
    "the", "this", "that", "these", "those", "there", "then", "they", "his", "her",
    "what", "which", "who", "whom", "whose", "when", "where", "why", "how", "did",
    "does", "was", "were", "is", "are", "list", "trace", "summarize", "tell", "give",
    "after", "before", "monday", "tuesday", "wednesday", "thursday", "friday",
    "saturday", "sunday", "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}


def _master() -> bool:
    return os.environ.get("BRAINOS_K5", "1") != "0"


def stats() -> dict[str, int]:
    return dict(_stats)


def _load_dict() -> None:
    """词典加载(mtime增量感知·改JSON即生效·无需重启; 损坏fail-soft保留旧表)。"""
    global _dict_mt, _DICT, _trip_res, _agg_res, _time_res, _stop_en, _stop_zh
    try:
        mt = DICT_PATH.stat().st_mtime
    except OSError:
        return
    with _lock:
        if mt == _dict_mt and _DICT:
            return
        try:
            raw = json.loads(DICT_PATH.read_text(encoding="utf-8"))
        except Exception:
            _stats["dict_load_fail"] += 1
            return
        trip = [re.compile(re.escape(w) if re.search(r"[\u4e00-\u9fa5]", w)
                           else r"(?<![a-z])" + re.escape(w.strip()) + r"(?![a-z])",
                           re.IGNORECASE)
                for w in raw.get("trip", {}).get("en", []) + raw.get("trip", {}).get("zh", [])
                if w.strip()]
        agg = [re.compile(re.escape(w) if re.search(r"[\u4e00-\u9fa5]", w)
                          else r"(?<![a-z])" + re.escape(w.strip()) + r"(?![a-z])",
                          re.IGNORECASE)
               for w in (raw.get("yield_history_agg", {}).get("en", [])
                         + raw.get("yield_history_agg", {}).get("zh", [])) if w.strip()]
        tim = [re.compile(re.escape(w) if re.search(r"[\u4e00-\u9fa5]", w)
                          else r"(?<![a-z])" + re.escape(w.strip()) + r"(?![a-z])",
                          re.IGNORECASE)
               for w in (raw.get("yield_time", {}).get("en", [])
                         + raw.get("yield_time", {}).get("zh", [])) if w.strip()]
        _trip_res, _agg_res, _time_res = trip, agg, tim
        _stop_en = {w.lower() for w in raw.get("stop", {}).get("en", [])}
        _stop_zh = set(raw.get("stop", {}).get("zh", []))
        _DICT = raw
        _dict_mt = mt
        _stats["dict_loaded"] += 1


def _eid(session: str, dt: str, text: str) -> str:
    return hashlib.sha1(f"{session}|{dt}|{text}".encode("utf-8")).hexdigest()[:16]


def _query_entities(query: str) -> list[str]:
    """query专名抽取(大写启发式+的-前缀·零LLM·零词典先验)。"""
    ents: list[str] = []
    for m in _EN_CAP.finditer(query):
        w = m.group(1).removesuffix("'s").removesuffix("'S")
        if w.lower() not in _CAP_STOP and w.lower() not in _stop_en and w not in ents:
            ents.append(w)
    for m in _ZH_DE.finditer(query):
        w = m.group(1)
        if w not in _stop_zh and w not in ents and not any(r.search(w) for r in _trip_res):
            ents.append(w)
    return ents


def _tokens(text: str) -> list[str]:
    toks = [t.lower() for t in _EN_WORD.findall(text or "") if t.lower() not in _stop_en]
    toks += [r for r in _ZH_RUN.findall(text or "") if r not in _stop_zh]
    return toks


def _expansion_terms(bridge_texts: list[str], query: str) -> list[str]:
    """桥卡内容扩查词: 去停用/去query词·频次序·封顶MAX_EXPAND(零LLM)。"""
    qtok = set(_tokens(query))
    freq: Counter = Counter()
    for tx in bridge_texts:
        seen: set[str] = set()
        for t in _tokens(tx):
            if t in qtok or t in seen:
                continue
            seen.add(t)
            freq[t] += 1
    return [t for t, _ in freq.most_common(_MAX_EXPAND)]


def _bridge_indices(evidence: list[dict], ents: list[str], query: str) -> list[int]:
    """桥卡选择(返回evidence下标): 专名命中speaker/text优先(保持窗序), 无则词面重叠卡。"""
    qtok = set(_tokens(query))
    if ents:
        low = [e.lower() for e in ents]
        hit = [i for i, e in enumerate(evidence)
               if (str(e.get("speaker", "")).strip().lower() in low)
               or any(nm in str(e.get("text", "")).lower() for nm in low)]
        if hit:
            return hit[:_TOP_M] if len(hit) > _TOP_M else hit
    scored = []
    for i, e in enumerate(evidence):
        ov = len(qtok & set(_tokens(str(e.get("text", "")))))
        if ov:
            scored.append((-ov, i))
    scored.sort()
    return [i for _, i in scored[:_TOP_M]]


def _bridge_positions(evidence: list[dict], data: list[dict], bridge_idx: list[int]) -> list[int]:
    """桥卡在data(可能已被k1 dt重排)中的现位置: 按eid对齐(索引空间解耦·防重排错位)。"""
    want = {_eid(str(evidence[i].get("session", "")), str(evidence[i].get("dt", "")),
                 str(evidence[i].get("text", ""))) for i in bridge_idx}
    return [p for p, d in enumerate(data) if d.get("id") in want]


def install(svc: Any = None) -> dict[str, bool]:
    """启动期一次性装载(kml _on_startup内·幂等·调用方fail-soft)。S3无状态零monkey-patch。"""
    _load_dict()
    if not _master():
        return {}
    return {"dict": bool(_DICT), "trip": len(_trip_res), "stateless": True}


def multihop_expand(svc: Any, tenant: str, query: str, data: list[dict],
                    out: dict | None, t0: float) -> tuple[list[dict], dict]:
    """S3主入口(端点在k1过滤后调用)。返回(可能合并扩窗的data, out)。
    全路径fail-soft: 任何异常由调用方兜底(k5失败=退一跳原窗)。"""
    if not _master():
        return data, out or {}
    if not data or out is None:
        _stats["gate_skip"] += 1
        return data, out or {}
    _load_dict()
    if not _DICT:
        _stats["gate_skip"] += 1
        return data, out
    try:
        res = _expand(svc, tenant, query, data, out, t0)
        log.info("knife5 decide lex=%s top1=%s reason=stats=%s",
                 any(r.search(query) for r in _trip_res),
                 (res[1].get("_k4") or {}).get("scores", {}).get(data[0].get("id")) if data else None,
                 dict(_stats))
        return res
    except Exception as exc:
        _stats["expand_fail"] += 1
        log.warning("knife5 expand exception err=%s", type(exc).__name__)
        return data, out


def _expand(svc: Any, tenant: str, query: str, data: list[dict],
            out: dict, t0: float) -> tuple[list[dict], dict]:
    # ── 让位协议(优先级最高): 真历史/聚合(C3·B1资产)与时序域(C1/C2归k3)零触碰 ──
    agg = any(r.search(query) for r in _agg_res)
    tim = any(r.search(query) for r in _time_res)
    if tim:
        _stats["yield_time"] += 1
        return data, out
    if agg:
        _stats["yield_agg"] += 1  # 聚合标记在·无论intent都让位(list every/timeline/哪些...)
        return data, out

    from knife1_supersede import detect_intent  # 复用k1意图件(无逻辑重复·单一实现)

    raw_intent, _ = detect_intent(query)
    false_history = raw_intent == "history"  # 走到此=无聚合标记→history必为过去时询问误报

    # ── 触发门: 关系链词 或 一跳top1分<阈 ────────────────────────────────────
    evidence: list[dict] = out.get("evidence") or []
    lex_hit = any(r.search(query) for r in _trip_res)
    top1_score: float | None = None
    k4meta = out.get("_k4") or {}
    scores: dict = k4meta.get("scores") or {}
    if data and scores:
        top1_score = scores.get(data[0].get("id"))
    score_hit = _MIN_TOP > 0 and top1_score is not None and top1_score < _MIN_TOP
    if not lex_hit and not score_hit:
        _stats["gate_skip"] += 1
        return data, out

    # ── 预算硬门: 已耗时>60%预算→降级单跳(t0为端点墙钟time.time()·同钟对表) ──
    elapsed_ms = (time.time() - t0) * 1000.0
    if elapsed_ms > _BUDGET_MS * 0.6:
        _stats["degrade_budget"] += 1
        return data, out

    ents = _query_entities(query)
    bridges = _bridge_indices(evidence, ents, query)
    if not bridges:
        _stats["no_bridge"] += 1
        return data, out
    bridge_pos = _bridge_positions(evidence, data, bridges)
    if not bridge_pos:
        _stats["no_bridge"] += 1
        return data, out
    bridge_texts = [str(evidence[i].get("text", "")) for i in bridges]
    terms = _expansion_terms(bridge_texts, query)
    if not terms:
        _stats["no_terms"] += 1
        return data, out

    # ── 第二跳(原query保留+扩查词·过k4包装search=M1a memo/M1b onepass受益) ────
    hop2_q = f"{query} {' '.join(terms)}"
    t_h = time.time()
    out2 = svc.search(hop2_q, None, min(max(1, _HOP2_K), 256), tenant)
    _stats["hop2_ms"] += int((time.time() - t_h) * 1000)
    ev2: list[dict] = out2.get("evidence") or []
    if not ev2:
        _stats["hop2_empty"] += 1
        return data, out

    # ── 合并去重(卡id同源eid) ────────────────────────────────────────────────
    seen_text = {str(e.get("text", "")) for e in evidence}
    new_ev = [e for e in ev2 if str(e.get("text", "")) not in seen_text]
    if not new_ev and not false_history:
        # 常规分支无新卡=无可并入(真noop); 修复分支即使无新卡也须前置桥卡(排序修复·B2-01判例:
        # 扩查词命中说明目标卡本就在窗内·只是被k1误报dt序埋底——修复动作=前置不是并入)
        _stats["merge_noop"] += 1
        return data, out
    for e in new_ev:
        seen_text.add(str(e.get("text", "")))

    proj = [{"id": _eid(str(e.get("session", "")), str(e.get("dt", "")), str(e.get("text", ""))),
             "content": e.get("text", ""), "created_at": e.get("dt", "")}
            for e in new_ev if e.get("text")]
    merged = list(data)
    if false_history:
        # 修复分支: 桥卡+二跳新卡前置(其余保持k1 dt序·减去桥卡·eid对齐防重排错位)
        bset = set(bridge_pos)
        front = [data[p] for p in bridge_pos]
        front += proj[:max(0, _FRONT_MAX - len(front))]
        rest = [d for p, d in enumerate(data) if p not in bset]
        merged = front + rest
        _stats["falsehist_fix"] += 1
    else:
        # 常规分支: 二跳新卡邻插首桥卡之后(封顶INSERT_MAX·余量窗尾·防top10挤位)
        pos = min(bridge_pos)
        if pos <= 8:
            merged = data[:pos + 1] + proj[:_INSERT_MAX] + data[pos + 1:] + proj[_INSERT_MAX:]
        else:
            merged = data + proj
        _stats["adj_insert"] += 1

    # out并集: evidence/k4 scores·keys(k4 recency与后续探测对合并窗继续有效)
    out = dict(out)
    out["evidence"] = evidence + new_ev
    m2 = out2.get("_k4") or {}
    if m2 or k4meta:
        out["_k4"] = {"scores": {**scores, **(m2.get("scores") or {})},
                      "keys": {**k4meta.get("keys", {}), **(m2.get("keys") or {})}}
    total_ms = (time.time() - t0) * 1000.0
    if total_ms > _BUDGET_MS:
        _stats["budget_breach"] += 1
    _stats["fired"] += 1
    return merged, out
