# w2_timeline.py — W2-1时间线卡(梯队一第一刀·确定性版·零LLM·20261014)
# ═════════════════════════════════════════════════════════════════════════════
# 战役依据(A2错题归因实证): temporal 259错题 83% 系"证据只有Yesterday没有绝对日期",
# 根因=ingestion丢session日期(源数据288个session_N_date_time字段→1540条证据仅52含年份;
# VPS床 gold进窗率 4.4%, temporal 0/11)。配套 dt 透传修复见 card_construct.dt_of。
#
# 三职责:
#   ①时间线卡: 同session消息按原时间排序的事件链摘要卡(确定性·零LLM) — 一等公民入
#     organ索引(BM25/语义三通道)·任意实体/时间query可召回·随窗携带绝对日期词面。
#     与cc派生边车卡(仅query词面强匹配才尾注)不同: 时间线卡面向"检索层"补日期通道。
#   ②双时间轴: 卡头 ISO日期+人读格式("2023-05-08 | Monday, 8 May 2023")=事实时间轴;
#     行内保留消息原文(相对词"yesterday"原样)=说的时间轴 — 两轴分列并存。
#     (created_at字段随dt透传修复一并回归事实时间, 冻结核"[session|dt|speaker]"索引头
#      自动进日期通道, 本件零改动冻结核。)
#   ③按天分窗: 跨天session插日期分窗头; 行超上限按"事实提示句优先+每日首条必保+
#     均匀补采"确定性采样(排序不变)。
#
# 纪律: 确定性(同输入同卡流·禁now()/随机=幂等重放安全)·零LLM·零新依赖·零题目硬编码
#       (通用句法启发)·fail-soft(单会话失败只丢该卡绝不影响add语义)。
# 幂等: dia="W2TL:{sess}"稳定键 — session跨多add批次时逐批重建覆写(organ键=conv|dia
#       覆写语义实测), 末批=全会话全量; ledger重放按行序覆写=终态一致。
# 合规: 时间线卡正文含会话原句 → 必须同过k2敏感标注(aml_serviceadd相将tl_cards并入
#       k2.observe调用), 泛化query隐私闸对时间线卡与原卡同口径滤除·零旁路。
# 回滚: env BRAINOS_W2_TIMELINE=0关门(不产卡·organ索引回纯原文流) + 摘除aml_service
#       两处hunk + 删本件。测试副本专用件, 生产8090零触碰。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any

log = logging.getLogger("w2_timeline")

_MONTHS = ("January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December")
# 事实提示句家族(与card_construct原子刀同族·通用句法·零题目先验)
_FACT_HINTS = (" is ", " was ", " are ", " were ", " has ", " have ", " had ",
               " lives", " lived", " works", " worked", " born", " moved",
               " married", " graduated", " died", " killed", " lost ", " got ",
               " started ", " joined ", " bought ", " sold ", " went to ",
               " came from ", " will ", " traveling", " visiting", " planned")
_WS_RE = re.compile(r"\s+")


def _cap(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(f"BRAINOS_W2_{name}", str(default))))
    except (TypeError, ValueError):
        return default


def ON() -> bool:
    """总闸: env BRAINOS_W2_TIMELINE(缺省开·=0关门回纯原文卡流)。"""
    return os.environ.get("BRAINOS_W2_TIMELINE", "1") != "0"


def _day_header(day: str) -> str:
    """ISO日→"2023-05-08 | Monday, 8 May 2023"(双格式日期头·BM25数字+月名+星期全覆盖)。"""
    try:
        d = datetime.strptime(day, "%Y-%m-%d")
        return f"{day} | {d.strftime('%A')}, {d.day} {_MONTHS[d.month - 1]} {d.year}"
    except ValueError:
        return day


def _line(dt: str, speaker: str, text: str, max_chars: int) -> str:
    t = _WS_RE.sub(" ", str(text or "")).strip()
    if len(t) > max_chars:
        t = t[:max_chars].rstrip() + "…"
    spk = _WS_RE.sub(" ", str(speaker or "")).strip() or "?"
    return f"[{dt}] {spk}: {t}" if dt else f"{spk}: {t}"


def _select(rows: list[tuple[int, str, bool, str]], cap: int) -> list[tuple[int, str, bool, str]]:
    """确定性采样(保时序): 每日首条必保(骨架) → 事实提示句按序补 → 均匀补采至cap。"""
    if len(rows) <= cap:
        return rows
    keep: list[int] = []
    seen_days: set[str] = set()
    for i, r in enumerate(rows):           # ①每日首条=时间线骨架
        if r[1] not in seen_days:
            seen_days.add(r[1])
            keep.append(i)
    for i, r in enumerate(rows):           # ②事实提示句(关键事实聚合·确定性)
        if len(keep) >= cap:
            break
        if i not in keep and r[2]:
            keep.append(i)
    ks = set(keep)
    rest = [i for i in range(len(rows)) if i not in ks]
    if len(keep) < cap and rest:           # ③均匀补采(全覆盖·时序均匀)
        need = cap - len(keep)
        step = max(1, len(rest) // need)
        keep.extend(rest[::step][:need])
    keep = sorted(set(keep))[:cap]
    return [rows[i] for i in keep]


def session_cards(cards: list[dict[str, Any]], rid: str = "") -> list[dict[str, Any]]:
    """build_cards产物→每session一张时间线卡(确定性·fail-soft)。
    只消费原文消息卡(dia=="{rid}:{i}"·dt来自透传修复后的真实时间); cc派生卡
    (注记/原子·dia含额外段)不进时间线(语义重复)。无rid(无法精确排除派生卡)或
    无有效dt的session不产卡(诚实·organ跳空dt卡)。rid=该请求request_id。"""
    if not cards or not rid:
        return []  # 无rid=无法精确排除派生卡→诚实弃权(fail-closed·生产接线恒传rid)
    orig = {f"{rid}:{i}" for i in range(len(cards) + 1)}
    max_lines = _cap("TL_MAX_LINES", 40)
    max_line_chars = _cap("TL_LINE_CHARS", 160)
    by_sess: dict[str, list[dict[str, Any]]] = {}
    for c in cards:
        if not isinstance(c, dict):
            continue
        if not str(c.get("text", "")):
            continue
        if rid and str(c.get("dia", "")) not in orig:
            continue  # 派生卡排除(原文卡=精确键集)
        by_sess.setdefault(str(c.get("sess", "")), []).append(c)
    out: list[dict[str, Any]] = []
    for sess, cs in by_sess.items():
        try:
            if not sess:
                continue
            rows: list[tuple[int, str, bool, str]] = []  # (序, day, is_fact, line)
            dts: list[str] = []
            for k, c in enumerate(cs):  # cs保持build_cards原序=消息序(会话内保序)
                dt = str(c.get("dt", ""))
                dt_ok = bool(dt) and dt != "unknown"
                if dt_ok:
                    dts.append(dt)
                day = dt[:10] if dt_ok else ""
                low = " " + txt_low(c.get("text")) + " "
                rows.append((k, day or f"~{k:04d}", any(h in low for h in _FACT_HINTS),
                             _line(dt if dt_ok else "", str(c.get("speaker", "")),
                                   c.get("text", ""), max_line_chars)))
            rows.sort(key=lambda r: (r[1].startswith("~"), r[1], r[0]))  # dt序·无dt排尾保原序
            picked = _select(rows, max_lines)
            if not picked:
                continue
            span = f"{min(dts)[:10]}..{max(dts)[:10]}" if dts else "span unknown"
            parts = [f"[SESSION TIMELINE] session={sess} | {span} | "
                     f"events {len(picked)}/{len(rows)}"]
            cur_day = None
            for _i, day, _f, line in picked:
                if day != cur_day:
                    cur_day = day
                    parts.append(f"== {_day_header(day) if not day.startswith('~') else 'undated'} ==")
                parts.append(line)
            text = "\n".join(parts)
            if not dts:
                continue  # 全会话无有效时间→不产卡(诚实·organ空dt卡会被跳过)
            out.append({
                "conv": sess,
                "sess": sess,
                "dia": f"W2TL:{sess}",
                "speaker": "timeline",
                "text": text,
                "dt": min(dts),
            })
        except Exception as exc:  # noqa: BLE001 — 单会话失败只丢该卡
            log.warning("timeline card failed sess=%s err=%s", sess, type(exc).__name__)
    return out


def txt_low(v: Any) -> str:
    return str(v or "").lower()


# ── W2-1②扩展: 事实时间面(fact-time face·原文卡面可检索) ────────────────────
# 任务书"日期文本入卡面或元数据入检索面"的卡面实现: 原文卡text前置单行
# "[fact-time 2023-05-08 | Monday, 8 May 2023] "(ISO+人读双格式) — 事实时间轴入
# BM25/语义/呈现三通道; 说的时间轴=原文相对词原样保留 → 双轴分列。
# 只对dt透传成功的原文卡生效(unknown零伪造); 派生卡不动(其语义已由cc刀承载)。
_MONTH_NUM = {m: i + 1 for i, m in enumerate(_MONTHS)}


def face_prefix(dt: str) -> str:
    """dt(ISO)→"[fact-time 2023-05-08 | Monday, 8 May 2023] "; 无效dt→""(零伪造)。"""
    if not dt or dt == "unknown" or len(dt) < 10:
        return ""
    day = dt[:10]
    try:
        d = datetime.strptime(day, "%Y-%m-%d")
        return f"[fact-time {day} | {d.strftime('%A')}, {d.day} {_MONTHS[d.month - 1]} {d.year}] "
    except ValueError:
        return ""


def face_cards(cards: list[dict[str, Any]], rid: str) -> list[dict[str, Any]]:
    """原文卡(精确键集)text前置fact-time面→新列表(不改动入参); 其余卡原样引用。
    确定性(同dt同text同输出=ledger重放幂等)·fail-soft。无rid→原样返回。"""
    if not rid or not cards:
        return cards
    orig = {f"{rid}:{i}" for i in range(len(cards) + 1)}
    out: list[dict[str, Any]] = []
    for c in cards:
        try:
            if isinstance(c, dict) and str(c.get("dia", "")) in orig:
                p = face_prefix(str(c.get("dt", "")))
                if p:
                    out.append(dict(c, text=p + str(c.get("text", ""))))
                    continue
        except Exception:  # noqa: BLE001
            pass
        out.append(c)
    return out


# ── W2-1③扩展: 搜索相时间线注入器(query日期→session确定性路由) ──────────────
# 病灶实测(arm B): 时间线卡已入索引但BM25长文稀释+题面↔会话词面缺口→正确session的
# 时间线卡常不在窗。本器: query显式日期信号→确定性解析(day>month>year)→按会话日期
# 分窗路由对应session时间线卡(未在窗才注·插位1·rank0不动·≤2张) — 纯确定性零LLM,
# 时序题"问哪天→窗里必有那天的会话事件链"。注入后同过k2隐私闸(id同源同值=零旁路)。
_MONTH_RE = "|".join(_MONTHS)
_Q_ISO_RE = re.compile(r"\b((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})\b")
_Q_MDY_RE = re.compile(rf"\b({_MONTH_RE})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*((?:19|20)\d{{2}})?\b", re.I)
_Q_DMY_RE = re.compile(rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTH_RE})\.?,?\s*((?:19|20)\d{{2}})?\b", re.I)
_Q_MY_RE = re.compile(rf"\b({_MONTH_RE})\.?,?\s*((?:19|20)\d{{2}})\b", re.I)
_Q_YR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
_NUM2 = re.compile(r"\b(\d{1,2})\b")


def query_days(query: str) -> tuple[int, list[str]]:
    """query→(最强信号kind, 值列表) kind: 0=day(YYYY-MM-DD) 1=month(YYYY-MM或MM)
    2=year(YYYY); 无信号→(3, [])。确定性·只认显式日期词(零猜测)。"""
    q = str(query or "")
    days: list[str] = []
    for m in _Q_ISO_RE.finditer(q):
        y, mo, d = m.groups()
        days.append(f"{y}-{int(mo):02d}-{int(d):02d}")
    for pat, year_last in ((_Q_MDY_RE, True), (_Q_DMY_RE, False)):
        for m in pat.finditer(q):
            g = m.groups()
            mon_s, d_s, y_s = (g[0], g[1], g[2]) if year_last else (g[1], g[0], g[2])
            mo = _MONTH_NUM.get(str(mon_s).capitalize())
            if not mo or not (1 <= int(d_s) <= 31):
                continue
            days.append(f"{int(y_s)}-{mo:02d}-{int(d_s):02d}" if y_s else f"{mo:02d}-{int(d_s):02d}")
    months: list[str] = []
    for m in _Q_MY_RE.finditer(q):
        mon_s, y_s = m.groups()
        mo = _MONTH_NUM.get(str(mon_s).capitalize())
        if mo:
            months.append(f"{int(y_s)}-{mo:02d}")
    years: list[str] = [m.group(1) for m in _Q_YR_RE.finditer(q)]
    if days:
        kind = 0 if all(len(d) == 10 for d in days) else 1
        return kind, list(dict.fromkeys(days))[:3]
    if months:
        return 1, list(dict.fromkeys(months))[:3]
    # 裸月名(无年无日·"visit in May?"): 首字母须大写(防情态动词may误报·确定性)
    bare = [str(m.group(1)).capitalize() for m in re.finditer(rf"\b({_MONTH_RE})\b", q)
            if str(m.group(1))[:1].isupper()]
    bms = [_MONTH_NUM[b] for b in bare if b in _MONTH_NUM]
    if bms:
        return 1, [f"{v:02d}" for v in dict.fromkeys(bms)][:3]
    if years:
        return 2, list(dict.fromkeys(years))[:2]
    return 3, []


def _tl_days(text: str) -> list[str]:
    """时间线卡text→会话覆盖的日期窗(== 分窗头解析·确定性)。"""
    out = []
    for line in str(text or "").splitlines():
        if line.startswith("== ") and not line.startswith("== undated"):
            d = line[3:].split(" ", 1)[0]
            if len(d) == 10 and d[4] == "-" and d not in out:
                out.append(d)
    return out


def timeline_inject(svc: Any, tenant: str, query: str, data: list[dict[str, Any]],
                    top_k: int) -> tuple[list[dict[str, Any]], int]:
    """search相: query日期信号→对应session时间线卡确定性入窗(未在窗才注·≤2张·插位1)。
    只读svc.organ._cards(扫描W2TL卡·与aml启动heal同款只读内部访问); 无信号/无匹配/
    异常→原窗直通(零回归面)。返回(新data, 注入数)。"""
    try:
        kind, vals = query_days(query)
        if kind >= 3 or not vals or not data:
            return data, 0
        organ = getattr(svc, "organ", None)
        cards_map = getattr(organ, "_cards", None)
        if not cards_map:
            return data, 0
        tprefix = f"{tenant}:"
        cands: list[tuple[str, str, str, list[str]]] = []  # (sess, dt, text, dayset)
        for key, c in cards_map.items():
            try:
                dia = str(c.get("dia", ""))
                conv = str(c.get("conv", ""))
                if not dia.startswith("W2TL:") or not conv.startswith(tprefix):
                    continue
                if conv[len(tprefix):] != dia[5:]:
                    continue  # 租户错配防御(键=conv|dia自洽才取)
                cands.append((dia[5:], str(c.get("dt", "")), str(c.get("text", "")),
                              _tl_days(c.get("text", ""))))
            except Exception:  # noqa: BLE001
                continue
        if not cands:
            return data, 0

        def _hit(dayset: list[str]) -> bool:
            if kind == 0:
                return any(v in dayset for v in vals)
            if kind == 1:
                for v in vals:
                    for d in dayset:
                        if len(d) != 10:
                            continue
                        if len(v) == 7 and d[:7] == v:
                            return True
                        if len(v) == 5 and d[5:7] == v[:2]:
                            return True
                        if len(v) == 2 and d[5:7] == v:
                            return True
                return False
            return any(d[:4] in vals for d in dayset)

        picked = [c for c in cands if _hit(c[3])]
        if not picked:
            return data, 0
        # 歧义消解(确定性·零LLM): 月/年信号命中多session时, 按query实词与时间线文本
        # 的词面重叠降序→sess id升序取前2 — "问哪天的事→窗里是那天最相关会话的事件链"。
        import card_construct as _cc
        _qt = {t for t in re.findall(r"[a-z0-9]+", str(query).lower())
               if len(t) > 2 and t not in ("the", "and", "was", "were", "did", "does",
                                           "what", "when", "where", "who", "how", "her",
                                           "his", "their", "with", "that", "this", "have",
                                           "had", "has", "did", "she", "him", "for", "from")}
        if _qt:
            def _score(c):
                txt = set(re.findall(r"[a-z0-9]+", str(c[2]).lower()))
                return (-len(_qt & txt), c[0])
            picked.sort(key=_score)
        else:
            picked.sort(key=lambda c: c[0])
        picked = picked[:_cap("TL_INJECT_MAX", 2)]
        have = {str(d.get("content", "")) for d in data}
        out = list(data)
        n = 0
        for sess, dt, text, _ds in picked:
            if text in have:
                continue
            out.insert(min(1, len(out)), {"id": _cc._cid_of(sess, dt, text),
                                          "content": text, "created_at": dt})
            have.add(text)
            n += 1
            if n >= 2:
                break
        return out, n
    except Exception:  # noqa: BLE001 — fail-soft: 注入失败=原窗直通
        return data, 0
