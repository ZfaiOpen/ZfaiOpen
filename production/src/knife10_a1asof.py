"""knife10_a1asof.py — 刀10·A1时变点查(20261023·第三波·部署层零冻结核心改动)

病灶: "Who was the owner of GMMTV in Aug, 2013?"类 时变归属点查 — 证据卡在库,
但跨引擎实例的嵌入索引重建顺序差异使该卡时进时出窗口(实测六轮A1=41.5~66.2摆动),
嵌入排序对"短问句×JSON语料卡"天然弱(格式偏置+时变歧义)。

刀: query=单点形态 ∧ 显式日期(in/on/as of <月 年>) ∧ 归属角色词(owner/ceo/president/
chairman/founder/creator/parent company/subsidiary) → 对租户原文卡(SVC.recents只读
+当前窗)做确定性词面扫描: 卡须同含 query实体token与角色词, 卡内日期≤query日期取最近
(as-of语义), 最佳卡不在窗→插位1(原文直入·零改写), 已在窗→提位1。确定性=免疫实例漂移。

门: BRAINOS_A1_ASOF(缺省0=关·字节级直通)。回滚=摘钩子+删本件。fail-soft全链路。"""
from __future__ import annotations
import os, re, logging
from typing import Any

log = logging.getLogger("aml")
_ROLE_RE = re.compile(r"\b(owners?|ceo|chief\s+executive|presidents?|chairman|chairmen|founders?|creators?|parent\s+company|subsidiar\w+|acquired\s+by|owned\s+by)\b", re.I)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b|\b(?:january|february|march|april|may|june|july|august|september|october|november|december),?\s+\d{4}\b", re.I)
_QDATE_RE = re.compile(r"\bin\s+((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*),?\s+(\d{4})\b|\bon\s+(\d{4}-\d{2}-\d{2})\b|\bas\s+of\s+((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*),?\s+(\d{4})\b", re.I)
_MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
        "january":1,"february":2,"march":3,"april":4,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12}
_STOP = frozenset({"who", "was", "the", "of", "in", "on", "at", "is", "are", "a", "an", "what", "which", "did", "do", "does", "to", "by", "after", "before"})

def _mk(year, mon, day):
    return (int(year), _MON.get(str(mon).lower()[:3], 1), int(day))

def _qdate(query: str):
    m = _QDATE_RE.search(str(query or ""))
    if not m:
        return None
    if m.group(3):
        y, mo, d = m.group(3).split("-")
        return _mk(y, mo, d)
    mon = m.group(1) or m.group(4)
    yr = m.group(2) or m.group(5)
    return _mk(yr, _MON.get(str(mon).lower()[:3], 1), 1)

def _card_date(content: str):
    best = None
    for tok in _DATE_RE.finditer(str(content or "")):
        t = tok.group(0).lower()
        try:
            m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
            if m:
                d = _mk(m.group(1), m.group(2), m.group(3))
            else:
                m = re.search(r"([a-z]{3,})\.?\s+(\d{1,2}),?\s+(\d{4})", t)
                if m and m.group(1)[:3] in _MON:
                    d = _mk(m.group(3), m.group(1), m.group(2))
                else:
                    m = re.search(r"([a-z]{3,}),?\s+(\d{4})", t)
                    if not m or m.group(1)[:3] not in _MON:
                        continue
                    d = _mk(m.group(2), m.group(1), 1)
        except (ValueError, KeyError):
            continue
        if best is None or d > best:
            best = d
    return best

def inject(SVC, tenant: str, query: str, data: list[dict[str, Any]], top_k: int) -> tuple[list[dict[str, Any]], int]:
    """触发=单点+显式日期+角色词; 命中→确定性插位/提位。quota门(BRAINOS_A1_ASOF·缺省0)。"""
    if os.environ.get("BRAINOS_A1_ASOF", "0") != "1" or not data:
        return data, 0
    try:
        q = str(query or "")
        qd = _qdate(q)
        if qd is None or not _ROLE_RE.search(q):
            return data, 0
        # 单点形态护栏(复用aml_service判定信号: 词数≤14·无多跳词·单点句式由调用方不查——
        # 此处自守: 词数≤16)
        if len(q.split()) > 16:
            return data, 0
        et = {t.lower() for t in re.findall(r"[A-Za-z][A-Za-z0-9&\.]{1,}", q)} - _STOP
        et = {t for t in et if len(t) > 2}
        if not et:
            return data, 0
        inwin_ids = {str(d.get("content", "")) for d in data}
        cands: list[tuple[tuple, int, dict[str, Any]]] = []
        try:
            pool = SVC.recents(tenant, int(os.environ.get("BRAINOS_A1_ASOF_SCAN", "400")))
        except Exception:
            pool = []
        seen = set()
        for e in pool + [{"text": str(d.get("content", "")), "dt": d.get("created_at", ""), "_inwin": True} for d in data]:
            c = str(e.get("text", "") or e.get("content", ""))
            if not c or c in seen:
                continue
            seen.add(c)
            if not _ROLE_RE.search(c):
                continue
            lt = {t.lower() for t in re.findall(r"[A-Za-z][A-Za-z0-9&\.]{1,}", c[:400])}
            if not (et & lt):
                continue
            cd = _card_date(c)
            if cd is None or cd > qd:
                continue
            cands.append((cd, -len(c), {"content": c, "created_at": str(e.get("dt", ""))}))
        if not cands:
            return data, 0
        cands.sort(reverse=True)  # as-of: ≤qd中最近日期优先·同日短卡优先
        best = cands[0][2]
        if best["content"] in inwin_ids:
            # 已在窗→提位1(确定性)
            src = next(d for d in data if str(d.get("content", "")) == best["content"])
            out = [src] + [d for d in data if d is not src]
            return out, 1
        out = list(data)
        out.insert(min(1, len(out)), {"id": "k10" + str(abs(hash(best['content'])) % 10**12), "content": best["content"], "created_at": best.get("created_at", "")})
        if len(out) > top_k:
            out = out[:top_k]
        log.info("knife10 a1asof uid=%s date=%s cand=%d injected=1", tenant, qd, len(cands))
        return out, 1
    except Exception as exc:
        log.error("knife10 a1asof FAILED err=%s", type(exc).__name__)
        return data, 0
