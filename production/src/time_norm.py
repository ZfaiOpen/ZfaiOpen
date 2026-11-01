#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
time_norm.py — 英文相对时间归一器（LoCoMo 梯队一 W2-3 独立模块版）

职责：
  把会话消息里的英文相对时间表达（yesterday / last Friday / three years ago /
  in two weeks / the other day / that birthday / for four months now ...）
  按锚点（消息时间戳）解析为绝对日期，产出「原始表达 + 解析值」双写所需的全部信息。

置信度分档（W2-3 规约，三档）：
  1.0  EXPLICIT  文本自带显式绝对日期（"May 8, 2023" / "2023-05-08" / "August 2022"），
                 归一化只是格式标准化，语义零风险。
  0.9  ANCHOR    相对锚点的确定性日历偏移（today/yesterday/last week/next month/
                 two days ago/in three weeks/last year ...），只要消息时间戳可信即可复现。
  0.6  INFER     需要日历/序数推断的表达（last Friday 的星期回推、last weekend、
                 季节、the other day、a few days ago 的模糊区间、"for four months now"
                 起点反推、缺年份月份的年份推断、节假日最近邻、just≈消息当天）。
  0.0  VAGUE     无绝对日期可解析（soon/recently/last time/that birthday 类事件指代），
                 只产标注（annotate_only），不产日期值。

双写原则（低置信只标注不替换）：
  confidence >= REPLACE_MIN_CONF(0.9)  -> action="replace"    （可安全写入归一化值）
  0 < confidence < 0.9                 -> action="annotate"   （双写标注，值仅供参考）
  confidence == 0（不可解析）          -> action="annotate_only"（只记原始表达，无值）

纯 python3 标准库实现（calendar/datetime/re/dataclasses），零第三方依赖，可直插 organ。
集成规格见模块尾部 docstring「INTEGRATION」与 W2-3 战报。
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Union

__all__ = [
    "Resolution", "normalize", "normalize_message", "render_dual",
    "parse_anchor", "TIER_EXPLICIT", "TIER_ANCHOR", "TIER_INFER", "TIER_VAGUE",
    "REPLACE_MIN_CONF",
]

# ----------------------------------------------------------------------------- 常量
TIER_EXPLICIT = 1.0   # 显式绝对日期
TIER_ANCHOR = 0.9     # 消息锚点确定性偏移
TIER_INFER = 0.6      # 序数/日历推断
TIER_VAGUE = 0.0      # 不可解析（仅标注）
REPLACE_MIN_CONF = 0.9  # >= 此阈值才允许替换；低于只标注（双写原则）

Action = str  # "replace" | "annotate" | "annotate_only"

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_RE = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"

_WEEKDAYS = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4,
    "saturday": 5, "sunday": 6,
    "mon": 0, "tue": 1, "tues": 1, "wed": 2, "thu": 3, "thur": 3, "thurs": 3,
    "fri": 4, "sat": 5, "sun": 6,
}
_WEEKDAY_RE = r"(?:mon(?:day)?|t(?:ues|ue)?day|tues?|wed(?:nesday)?|thu(?:r(?:s(?:day)?)?)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)"

_NUMWORDS = {
    "a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11,
    "twelve": 12, "couple": 2, "few": 3, "several": 3,
}
_NUM_RE = r"(?:a|an|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|couple|few|several|\d{1,3})"
# 精确数字（不含 few/several 模糊量词——它们走区间规则）
_NUM_EXACT_RE = r"(?:a|an|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|couple|\d{1,3})"

_UNIT_RE = r"(?:days?|weeks?|months?|years?|hours?|hrs?|minutes?|mins?)"
_UNIT_CANON = {"day": "day", "days": "day", "week": "week", "weeks": "week",
               "month": "month", "months": "month", "year": "year", "years": "year",
               "hour": "hour", "hours": "hour", "hrs": "hour", "hr": "hour",
               "minute": "minute", "minutes": "minute", "min": "minute", "mins": "minute"}

_SEASONS = {
    "spring": (3, 5),   # 3-5 月
    "summer": (6, 8),
    "fall": (9, 11), "autumn": (9, 11),
    "winter": (12, 2),
}
_SEASON_RE = r"(?:spring|summer|fall|autumn|winter)"

# 固定日期节假日（月, 日）
_FIXED_HOLIDAYS = {
    "christmas": (12, 25), "xmas": (12, 25),
    "halloween": (10, 31),
    "new year": (1, 1), "new years": (1, 1), "new year's": (1, 1),
    "valentine": (2, 14), "valentines": (2, 14), "valentine's": (2, 14),
    "independence day": (7, 4), "july 4th": (7, 4), "4th of july": (7, 4),
}
# 无固定日期事件指代（只标注，不解析）
_EVENT_NOUNS = r"(?:birthday|wedding|anniversary|graduation|funeral|engagement|baby shower|housewarming)"


def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm（MM 未知年份复用标准算法）。"""
    y = year
    a = y % 19
    b, c = divmod(y, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(y, month, day)


def _thanksgiving(year: int) -> date:
    """美国感恩节 = 11 月第 4 个周四。"""
    d = date(year, 11, 1)
    offset = (3 - d.weekday()) % 7  # 周四 weekday=3
    return d + timedelta(days=offset + 21)


def _clamp_day(y: int, m: int, d: int) -> date:
    return date(y, m, min(d, calendar.monthrange(y, m)[1]))


def _add_months(d: date, n: int) -> date:
    total = (d.month - 1) + n
    y = d.year + total // 12
    m = total % 12 + 1
    return _clamp_day(y, m, d.day)


def _week_start(d: date) -> date:
    """ISO 周（周一起）。"""
    return d - timedelta(days=d.weekday())


def _month_name_to_num(s: str) -> Optional[int]:
    s = s.lower().rstrip(".")
    if s in _MONTHS:
        return _MONTHS[s]
    for k, v in _MONTHS.items():
        if k.startswith(s) and len(s) >= 3:
            return v
    return None


def parse_anchor(anchor: Union[str, datetime, date]) -> datetime:
    """锚点：接受 ISO 字符串 / datetime / date，统一为 aware datetime（UTC）。
    不可解析的字符串回退为今天（不抛异常，保证 ingestion 全量可跑）。"""
    if isinstance(anchor, datetime):
        dt = anchor
    elif isinstance(anchor, date):
        dt = datetime(anchor.year, anchor.month, anchor.day)
    else:
        s = str(anchor).strip()
        s_norm = s[:-1] + "+00:00" if s.endswith("Z") else s
        try:
            dt = datetime.fromisoformat(s_norm)
        except ValueError:
            try:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
            except ValueError:
                dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ----------------------------------------------------------------------------- 结果结构
@dataclass
class Resolution:
    raw: str                       # 原始表达（卡面「原始表达」列）
    start: int                     # 字符 span
    end: int
    rule: str                      # 命中规则名
    confidence: float              # 1.0 / 0.9 / 0.6 / 0.0
    action: Action                 # replace / annotate / annotate_only
    value: Optional[dict] = None   # {"iso":..., "granularity":..., "range":[...], "duration_days":..., "direction":...}
    anchor: Optional[str] = None   # 使用的锚点 ISO
    note: str = ""                 # 低置信原因 / 推断说明

    def __post_init__(self):
        if self.action == "auto":  # 内部占位
            if self.confidence >= REPLACE_MIN_CONF and self.value:
                self.action = "replace"
            elif self.value:
                self.action = "annotate"
            else:
                self.action = "annotate_only"

    def to_dict(self) -> dict:
        return asdict(self)


def _mk(raw, start, end, rule, conf, value, note="", anchor=None) -> Resolution:
    return Resolution(raw=raw, start=start, end=end, rule=rule, confidence=conf,
                      value=value, note=note, anchor=anchor, action="auto")


def _val_point(d: date, gran: str, direction: str = "past",
               note_gran: Optional[str] = None) -> dict:
    """点值 + 粒度桶（gran: day/week/month/year/season/hour）。"""
    v = {"iso": d.isoformat(), "granularity": gran, "direction": direction}
    if gran == "week":
        ws = _week_start(d)
        v["range"] = [ws.isoformat(), (ws + timedelta(days=6)).isoformat()]
    elif gran == "month":
        v["range"] = [d.replace(day=1).isoformat(),
                      d.replace(day=calendar.monthrange(d.year, d.month)[1]).isoformat()]
    elif gran == "year":
        v["range"] = [date(d.year, 1, 1).isoformat(), date(d.year, 12, 31).isoformat()]
    elif gran == "season":
        pass  # range 由调用方填
    if note_gran:
        v["granularity_note"] = note_gran
    return v


def _val_range(a: date, b: date, gran: str, direction: str = "past") -> dict:
    return {"iso": a.isoformat(), "granularity": gran, "direction": direction,
            "range": [min(a, b).isoformat(), max(a, b).isoformat()]}


def _dur_days(d: date, anchor_d: date) -> int:
    return abs((anchor_d - d).days)


# ----------------------------------------------------------------------------- 规则引擎
# 每条规则: (优先级序号, 规则名, 置信度档, 编译正则, handler(match, anchor_dt))
# handler 返回 (value|None, note) 或 (value|None, note, conf_override)——第三值可降档
# （如 "about a year ago" 的模糊量词把 0.9 降到 0.6）。
# 优先级序号小的先登记；重叠时按 (序号, span 长度更长者优先, 起点) 去重。

def _num_of(s: str) -> int:
    s = s.lower()
    if s.isdigit():
        return int(s)
    return _NUMWORDS.get(s, 1)


def _unit_of(s: str) -> str:
    return _UNIT_CANON.get(s.lower(), "day")


def _shift(anchor_d: date, n: int, unit: str) -> date:
    if unit == "day":
        return anchor_d + timedelta(days=n)
    if unit == "week":
        return anchor_d + timedelta(weeks=n)
    if unit == "month":
        return _add_months(anchor_d, n)
    if unit == "year":
        return _clamp_day(anchor_d.year + n, anchor_d.month, anchor_d.day)
    # hour/minute：日期粒度即锚点当天
    return anchor_d


def _gran_of(unit: str) -> str:
    return unit if unit in ("day", "week", "month", "year") else "hour"


RULES = []  # (order, name, conf, regex, handler)


def _rule(order, name, conf, pattern):
    def deco(fn):
        RULES.append((order, name, conf, re.compile(pattern, re.I), fn))
        return fn
    return deco


# ---- 0. 显式绝对日期（conf 1.0）
@_rule(0, "explicit_mdy", TIER_EXPLICIT,
       r"\b(" + _MONTH_RE + r"\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+\d{4})?)\b")
def _r_explicit_mdy(m, ad):
    return _parse_explicit(m.group(1), ad)


@_rule(1, "explicit_dmy", TIER_EXPLICIT,
       r"\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:of\s+)?" + _MONTH_RE + r"\.?(?:,?\s+\d{4})?)\b")
def _r_explicit_dmy(m, ad):
    return _parse_explicit(m.group(1), ad)


@_rule(2, "explicit_my", TIER_EXPLICIT,
       r"\b(" + _MONTH_RE + r"\.?,?\s+\d{4})\b")
def _r_explicit_my(m, ad):
    return _parse_explicit(m.group(1), ad)


@_rule(3, "explicit_iso", TIER_EXPLICIT, r"\b(\d{4}-\d{2}-\d{2})\b")
def _r_explicit_iso(m, ad):
    y, mo, dd = map(int, m.group(1).split("-"))
    return _val_point(date(y, mo, dd), "day", "point"), "explicit ISO date"


@_rule(4, "explicit_month_only", TIER_INFER,
       r"\b(in\s+)?(" + _MONTH_RE + r")\.?\b(?!\s*,?\s*\d{4})(?!\s+\d{1,2}\b)")
def _r_explicit_month_only(m, ad):
    """裸月份（无年份）→ 就近年份推断。例：'in August'（conf 0.6）。
    词性排除：小写 may=情态动词、march=动词/名词（游行）不算月份；
    句首大写 May（"May I..."）也不算。"""
    raw = m.group(2)
    low = raw.lower()
    if low in ("may", "march") and raw[0].islower():
        return None, f"'{raw}' is a common word here (modal/verb), not a month"
    if low == "may":
        pre = m.string[:m.start()]
        if m.start() == 0 or pre.endswith((". ", "! ", "? ", "\n")):
            return None, "sentence-initial 'May': ambiguous with modal 'may'; annotate only"
    mo = _month_name_to_num(low)
    if mo is None:
        return None, "unrecognized month"
    cands = []
    for dy in (0, -1, 1):
        cands.append(date(ad.year + dy, mo, 1))
    best = min(cands, key=lambda x: abs((x - ad.date()).days))
    return (_val_point(best, "month", "inferred"),
            f"no year in text; inferred {best.year} (nearest to anchor); low-confidence")


def _parse_explicit(s: str, ad: datetime):
    """'May 8, 2023' / '8 May 2023' / '22 August' / 'August 2023' → (value, note)。"""
    s2 = s.strip().rstrip(",").replace(".", "")
    s2 = re.sub(r"\bof\s+", "", s2, flags=re.I)
    s2 = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", s2, flags=re.I)
    year = None
    ym = re.search(r"\b(\d{4})\b", s2)
    if ym:
        year = int(ym.group(1))
        s2 = s2[:ym.start()] + s2[ym.end():]
    toks = s2.split()
    toks = [t.strip(",") for t in toks if t.strip(",")]
    month = None
    day = None
    for t in toks:
        tl = t.lower()
        if tl in _MONTHS or (len(tl) >= 3 and any(k.startswith(tl) and len(k) > len(tl) or k == tl for k in _MONTHS)):
            month = _month_name_to_num(tl)
        elif t.isdigit():
            day = int(t)
    if month is None:
        return None, "unparseable explicit date"
    if year is not None and day is not None:
        return _val_point(date(year, month, min(day, calendar.monthrange(year, month)[1])), "day", "point"), "explicit date"
    if year is not None:
        return _val_point(date(year, month, 1), "month", "point"), "explicit month-year"
    if day is not None:
        # 无年份的“月+日”：就近年份
        best = min([date(ad.year + dy, month, min(day, calendar.monthrange(ad.year + dy, month)[1]))
                    for dy in (0, -1, 1)], key=lambda x: abs((x - ad.date()).days))
        return (_val_point(best, "day", "inferred"),
                f"no year; inferred {best.year}; low-confidence")
    return None, "month token only"


# ---- 9. 模糊区间（先于精确 ago 规则登记，few/several 走区间）
@_rule(9, "ago_few_range", TIER_INFER,
       r"\b(?:a\s+)?(?:few|several)\s+(" + _UNIT_RE + r")\s+ago\b")
def _r_ago_few(m, ad):
    unit = _unit_of(m.group(1))
    k = {"day": 3, "week": 3, "month": 3, "year": 4, "hour": 3, "minute": 3}[unit]
    lo, hi = _shift(ad.date(), -(k + 2), unit), _shift(ad.date(), -(k - 1), unit)
    v = _val_range(lo, hi, _gran_of(unit), "past")
    v["duration_days"] = _dur_days(lo, ad.date())
    return v, "fuzzy 'a few' → range, low-confidence"


@_rule(19, "in_few_range", TIER_INFER,
       r"\bin\s+(?:a\s+)?(?:few|several)\s+(" + _UNIT_RE + r")\b")
def _r_in_few(m, ad):
    unit = _unit_of(m.group(1))
    k = {"day": 3, "week": 3, "month": 3, "year": 4, "hour": 3, "minute": 3}[unit]
    lo, hi = _shift(ad.date(), (k - 1), unit), _shift(ad.date(), (k + 2), unit)
    v = _val_range(lo, hi, _gran_of(unit), "future")
    v["duration_days"] = _dur_days(lo, ad.date())
    return v, "fuzzy 'in a few' → range, low-confidence"


# ---- 10. X ago（确定性偏移，conf 0.9；模糊量词降 0.6）
@_rule(10, "ago_exact", TIER_ANCHOR,
       r"\b(?:(about|around|almost|nearly|over|some)\s+)?(?P<num>" + _NUM_EXACT_RE + r")\s+(?P<unit>" + _UNIT_RE + r")\s+ago\b")
def _r_ago(m, ad):
    fuzzy = bool(m.group(1))
    n, unit = _num_of(m.group("num")), _unit_of(m.group("unit"))
    conf = TIER_INFER if (fuzzy or m.group("num").lower() in ("few", "several")) else TIER_ANCHOR
    d = _shift(ad.date(), -n, unit)
    v = _val_point(d, _gran_of(unit), "past")
    v["duration_days"] = _dur_days(d, ad.date())
    note = "anchored to message timestamp" if conf == TIER_ANCHOR else f"fuzzy quantifier '{m.group(1) or m.group('num')}'"
    return v, note, conf


@_rule(11, "ago_coupleof", TIER_ANCHOR,
       r"\bcouple\s+of\s+(" + _UNIT_RE + r")\s+ago\b")
def _r_ago_coupleof(m, ad):
    unit = _unit_of(m.group(1))
    d = _shift(ad.date(), -2, unit)
    v = _val_point(d, _gran_of(unit), "past")
    v["duration_days"] = _dur_days(d, ad.date())
    return v, "anchored to message timestamp"


@_rule(13, "long_time_ago", TIER_VAGUE,
       r"\b(a\s+)?long\s+time\s+ago\b|\bages\s+ago\b")
def _r_long(m, ad):
    return None, "vague: >1 year implied but unresolvable; annotate only"


@_rule(14, "while_ago", TIER_INFER,
       r"\ba\s+while\s+ago\b")
def _r_while(m, ad):
    lo, hi = ad.date() - timedelta(days=60), ad.date() - timedelta(days=7)
    return _val_range(lo, hi, "day", "past"), "fuzzy 'a while ago' → 7-60d range, low-confidence"


@_rule(15, "the_other_day", TIER_INFER,
       r"\bthe\s+other\s+(day|night)\b")
def _r_the_other_day(m, ad):
    return _val_range(ad.date() - timedelta(days=4), ad.date() - timedelta(days=2), "day", "past"), \
        "'the other day' → 2-4 days before anchor, low-confidence"


# ---- 20. 未来：in X units / X from now
@_rule(20, "in_num_unit", TIER_ANCHOR,
       r"\bin\s+(" + _NUM_RE + r")\s+(" + _UNIT_RE + r")\b(?!\s+(?:ago|from))")
def _r_in_num(m, ad):
    n, unit = _num_of(m.group(1)), _unit_of(m.group(2))
    conf = TIER_INFER if m.group(1).lower() in ("few", "several", "couple") else TIER_ANCHOR
    d = _shift(ad.date(), n, unit)
    v = _val_point(d, _gran_of(unit), "future")
    v["duration_days"] = _dur_days(d, ad.date())
    return v, "anchored to message timestamp (future)", conf


@_rule(21, "unit_from_now", TIER_ANCHOR,
       r"\b(" + _NUM_RE + r")\s+(" + _UNIT_RE + r")\s+from\s+(?:now|today|here)\b")
def _r_from_now(m, ad):
    n, unit = _num_of(m.group(1)), _unit_of(m.group(2))
    d = _shift(ad.date(), n, unit)
    v = _val_point(d, _gran_of(unit), "future")
    v["duration_days"] = _dur_days(d, ad.date())
    return v, "anchored to message timestamp (future)"


# ---- 30. 持续至今（起点反推，conf 0.6）：for/about X units now、it's been X units since
@_rule(30, "duration_so_far", TIER_INFER,
       r"\b(?:for\s+)?(?:(about|around|almost|nearly|over)\s+)?(" + _NUM_RE + r")\s+(" + _UNIT_RE + r")\s*(?:now|so\s+far|straight)\b")
def _r_dur_now(m, ad):
    n, unit = _num_of(m.group(2)), _unit_of(m.group(3))
    d = _shift(ad.date(), -n, unit)
    v = _val_point(d, _gran_of(unit), "past")
    v["duration_days"] = _dur_days(d, ad.date())
    return v, f"duration-until-now '{m.group(0)}': start-point back-calculated; low-confidence"


@_rule(31, "been_duration_since", TIER_INFER,
       r"\bit'?s\s+been\s+(?:a\s+)?(" + _NUM_RE + r")\s+(" + _UNIT_RE + r")\s+since\b")
def _r_been_since(m, ad):
    n, unit = _num_of(m.group(1)), _unit_of(m.group(2))
    d = _shift(ad.date(), -n, unit)
    v = _val_point(d, _gran_of(unit), "past")
    v["duration_days"] = _dur_days(d, ad.date())
    return v, "elapsed-since; start-point back-calculated; low-confidence"


# ---- 40. 去指度日（conf 0.9）
@_rule(40, "deictic_day", TIER_ANCHOR,
       r"\b(?P<w>today|tonight|yesterday|tomorrow|tomorrow\s+night)\b")
def _r_deictic(m, ad):
    w = m.group("w").lower()
    base = ad.date()
    if w == "today":
        return _val_point(base, "day", "point"), "anchored to message timestamp"
    if w == "tonight":
        v = _val_point(base, "day", "point"); v["granularity_note"] = "evening of the anchor day"
        return v, "anchored to message timestamp (evening)"
    if w == "yesterday":
        return _val_point(base - timedelta(days=1), "day", "past"), "anchored to message timestamp"
    d = base + timedelta(days=1)
    v = _val_point(d, "day", "future")
    if "night" in w:
        v["granularity_note"] = "evening of the next day"
    return v, "anchored to message timestamp (future)"


@_rule(41, "parts_of_day", TIER_ANCHOR,
       r"\b(this|last|tomorrow)\s+(morning|afternoon|evening|night)\b")
def _r_parts(m, ad):
    w, part = m.group(1).lower(), m.group(2).lower()
    d = ad.date() + (timedelta(days=1) if w == "tomorrow" else timedelta(days=-1) if w == "last" else timedelta(0))
    v = _val_point(d, "day", "future" if w == "tomorrow" else "past" if w == "last" else "point")
    qual = {"tomorrow": "next", "last": "previous"}.get(w, "")
    v["granularity_note"] = f"{part} of the {qual} day".replace("  ", " ").strip()
    return v, "anchored to message timestamp"


# ---- 42. just/right now：降为 vague（语料实测 371 命中里绝大多数是语气词
#      "just want to say/kidding"；即便时间义"刚刚"，消息自带时间戳、解析无信息增益）。
@_rule(42, "just_now", TIER_VAGUE,
       r"\b(just\s+now|right\s+now|just)\b")
def _r_just(m, ad):
    return None, "'just/right now': mostly discourse particle; message timestamp already carries the date; annotate only"


# ---- 50. last/next/this + 日历单位（conf 0.9）
_CALENDAR_UNIT = r"(week|month|year|weekend)"


@_rule(50, "last_unit", TIER_ANCHOR,
       r"\blast\s+" + _CALENDAR_UNIT + r"\b")
def _r_last_unit(m, ad):
    u = m.group(1).lower()
    d = ad.date()
    if u == "week":
        ws = _week_start(d) - timedelta(days=7)
        return _val_range(ws, ws + timedelta(days=6), "week", "past"), "previous ISO week (anchored)"
    if u == "month":
        pm = _add_months(d.replace(day=1), -1)
        return _val_point(pm, "month", "past"), "previous calendar month (anchored)"
    if u == "year":
        return _val_point(date(d.year - 1, d.month, d.day), "year", "past"), "previous calendar year (anchored)"
    # weekend
    sat = d - timedelta(days=(d.weekday() - 5) % 7)      # 最近一个周六（可为今天）
    sun = sat + timedelta(days=1)
    if sun > d:  # 锚点是周六 → 上上周
        sat, sun = sat - timedelta(days=7), sun - timedelta(days=7)
    return _val_range(sat, sun, "week", "past"), "most recent completed weekend; low-confidence"


@_rule(51, "next_unit", TIER_ANCHOR,
       r"\bnext\s+" + _CALENDAR_UNIT + r"\b")
def _r_next_unit(m, ad):
    u = m.group(1).lower()
    d = ad.date()
    if u == "week":
        ws = _week_start(d) + timedelta(days=7)
        return _val_range(ws, ws + timedelta(days=6), "week", "future"), "next ISO week (anchored)"
    if u == "month":
        nm = _add_months(d.replace(day=1), 1)
        return _val_point(nm, "month", "future"), "next calendar month (anchored)"
    if u == "year":
        return _val_point(date(d.year + 1, d.month, d.day), "year", "future"), "next calendar year (anchored)"
    sat = d + timedelta(days=(5 - d.weekday()) % 7 or 7)
    return _val_range(sat, sat + timedelta(days=1), "week", "future"), "next upcoming weekend; low-confidence"


@_rule(52, "this_unit", TIER_ANCHOR,
       r"\bthis\s+" + _CALENDAR_UNIT + r"\b")
def _r_this_unit(m, ad):
    u = m.group(1).lower()
    d = ad.date()
    if u == "week":
        ws = _week_start(d)
        return _val_range(ws, ws + timedelta(days=6), "week", "point"), "current ISO week (anchored)"
    if u == "month":
        return _val_point(d.replace(day=1), "month", "point"), "current calendar month (anchored)"
    if u == "year":
        return _val_point(date(d.year, 6, 30), "year", "point"), "current calendar year (anchored)"
    sat = d - timedelta(days=(d.weekday() - 5) % 7)
    return _val_range(sat, sat + timedelta(days=1), "week", "point"), "weekend of the current ISO week; low-confidence"


# ---- 60. last/next/this + 星期（conf 0.6：需要星期序数推算）
@_rule(60, "weekday_rel", TIER_INFER,
       r"\b(?P<rel>last|next|this)\s+(?P<wd>" + _WEEKDAY_RE + r")\b")
def _r_weekday(m, ad):
    rel, wd = m.group("rel").lower(), m.group("wd").lower()
    target = _WEEKDAYS[wd]
    d = ad.date()
    cur = d.weekday()
    if rel == "last":
        delta = (cur - target) % 7 or 7
        return _val_point(d - timedelta(days=delta), "day", "past"), f"most recent {wd.title()} before anchor; low-confidence"
    if rel == "next":
        delta = (target - cur) % 7 or 7
        return _val_point(d + timedelta(days=delta), "day", "future"), f"next {wd.title()} after anchor; low-confidence"
    delta = target - cur
    return _val_point(d + timedelta(days=delta), "day", "point"), f"{wd.title()} of the current ISO week; low-confidence"


# ---- 65. last/next/this + 季节（conf 0.6）
def _season_windows(se: str, d: date):
    """生成季节窗口 (year, start, end)，按年份降序（y+1, y, y-1）。
    北半球约定；冬季跨年：「y 年冬」= 去年 12/1 ~ 今年 2 月末。"""
    m1, m2 = _SEASONS[se]
    y = d.year
    for yy in (y + 1, y, y - 1):
        if m1 > m2:  # winter
            start = date(yy - 1, 12, 1)
            end = date(yy, 2, calendar.monthrange(yy, 2)[1])
        else:
            start = date(yy, m1, 1)
            end = _add_months(start, 3) - timedelta(days=1)
        yield yy, start, end


@_rule(65, "season_rel", TIER_INFER,
       r"\b(?P<rel>last|next|this)\s+(?P<se>" + _SEASON_RE + r")\b")
def _r_season(m, ad):
    rel, se = m.group("rel").lower(), m.group("se").lower()
    se = {"autumn": "fall"}.get(se, se)
    d = ad.date()
    wins = list(_season_windows(se, d))
    containing = next((w for w in wins if w[1] <= d <= w[2]), None)
    if rel == "this":
        tgt = containing or min(wins, key=lambda w: min(abs((w[1] - d).days), abs((w[2] - d).days)))
    elif rel == "next":
        after = [w for w in wins if w[1] > d]
        tgt = after[0] if after else wins[0]
    else:  # last：窗口过半→指进行中的本季；未过半→指上一个该季
        if containing:
            mid = containing[1] + (containing[2] - containing[1]) / 2
            prior = [w for w in wins if w[2] < containing[1]]
            tgt = containing if d > mid else (prior[0] if prior else containing)
        else:
            before = [w for w in wins if w[2] < d]
            tgt = before[0] if before else wins[-1]
    _, start, end = tgt
    v = {"iso": start.isoformat(), "granularity": "season", "season": se,
         "direction": {"last": "past", "next": "future"}.get(rel, "point"),
         "range": [start.isoformat(), end.isoformat()]}
    return v, f"{se} window (northern hemisphere, midpoint rule); low-confidence"


# ---- 70. last/next + 年度时点（last summer 无 rel 前缀形态亦覆盖）
@_rule(70, "bare_season", TIER_INFER,
       r"\b(?<!\w)(spring|summer|fall|autumn|winter)(?!\w)")
def _r_bare_season(m, ad):
    return None, "bare season word without last/next qualifier: too ambiguous, annotate only"


# ---- 80. 节假日（固定日历 + 计算型，conf 0.6）
@_rule(80, "holiday", TIER_INFER,
       r"\b(?:(last|next|this)\s+)?(christmas|xmas|halloween|new\s+years?(?:'s)?(?:\s+day)?|valentines?(?:'s)?(?:\s+day)?|thanksgiving|easter|independence\s+day|july\s+4th|4th\s+of\s+july)\b")
def _r_holiday(m, ad):
    rel = (m.group(1) or "").lower()
    name = re.sub(r"\s+day$", "", m.group(2).lower().replace("'", "").replace("xmas", "christmas"))
    name = re.sub(r"\s+", " ", name).strip()
    d = ad.date()
    if name in _FIXED_HOLIDAYS:
        mo, dy = _FIXED_HOLIDAYS[name]
    elif name == "thanksgiving":
        dy, mo = _thanksgiving(d.year).day, 11
    elif name == "easter":
        e = _easter(d.year)
        dy, mo = e.day, e.month
    else:
        return None, f"unhandled holiday {name}"
    def occ(yr):
        if name == "thanksgiving":
            return _thanksgiving(yr)
        if name == "easter":
            return _easter(yr)
        return date(yr, mo, dy)
    this_yr = occ(d.year)
    if rel == "next":
        tgt = this_yr if this_yr >= d else occ(d.year + 1)
    elif rel == "last":
        tgt = this_yr if this_yr < d else occ(d.year - 1)
    else:
        # 裸节假日：回忆语态默认「最近一个已过去的」（英语回溯偏好）；不足 14 天
        # 逼近未来时取未来（如 12 月中旬说 christmas 指即将到来的圣诞）。
        past = this_yr if this_yr < d else occ(d.year - 1)
        future = this_yr if this_yr >= d else occ(d.year + 1)
        tgt = future if (future - d).days <= 14 else past
    direction = "past" if tgt < d else "future" if tgt > d else "point"
    return _val_point(tgt, "day", direction), f"holiday '{name}' ({'qualified ' if rel else 'recent-'}occurrence); low-confidence"


# ---- 85. 事件指代（anaphoric，conf 0.0，只标注）
@_rule(85, "event_ref", TIER_VAGUE,
       r"\b(?:my|your|his|her|their|the|that|this|a|our)\s+(?:\w+\s+){0,2}?" + _EVENT_NOUNS + r"s?\b")
def _r_event(m, ad):
    return None, "anaphoric event reference (no date in text); resolve via discourse/coreference or explicit date"


# ---- 90. 模糊时间词（conf 0.0，只标注）
@_rule(90, "vague_marker", TIER_VAGUE,
       r"\b(soon|recently|lately|shortly|in\s+the\s+near\s+future|these\s+days|last\s+time|next\s+time|this\s+time|sometime|one\s+day)\b")
def _r_vague(m, ad):
    w = m.group(1).lower()
    if w in ("last time", "next time", "this time"):
        return None, "occasion reference, not a calendar date; annotate only"
    if w in ("soon", "shortly", "in the near future", "one day"):
        return None, f"vague future marker '{w}'; annotate only"
    return None, f"vague past marker '{w}'; annotate only"


# ---- 归一化主入口
def normalize(text: str, anchor: Union[str, datetime, date, None] = None) -> list:
    """
    解析 text 中全部相对时间表达。anchor 缺省 = 当前时间（UTC）。
    返回 Resolution 列表（按出现位置排序；重叠时保留高优先级/长 span）。
    """
    if not text:
        return []
    ad = parse_anchor(anchor if anchor is not None else datetime.now(timezone.utc))
    anchor_iso = ad.isoformat()
    found = []
    for order, name, conf, rx, fn in RULES:
        for m in rx.finditer(text):
            try:
                res = fn(m, ad)
                value, note = res[0], res[1]
                conf_eff = res[2] if len(res) > 2 else conf
            except Exception as exc:  # 单规则失败不拖垮全量
                value, note, conf_eff = None, f"rule error: {exc}", conf
            found.append({
                "order": order, "rule": name, "conf": conf_eff,
                "start": m.start(), "end": m.end(), "raw": m.group(0),
                "value": value, "note": note or "",
            })
    # 去重叠：order 优先，其次 span 更长，其次起点更早
    found.sort(key=lambda f: (f["order"], -(f["end"] - f["start"]), f["start"]))
    picked = []
    for f in found:
        if any(not (f["end"] <= p["start"] or f["start"] >= p["end"]) for p in picked):
            continue
        picked.append(f)
    picked.sort(key=lambda f: f["start"])
    out = []
    for f in picked:
        out.append(_mk(raw=f["raw"], start=f["start"], end=f["end"], rule=f["rule"],
                       conf=f["conf"], value=f["value"], note=f["note"], anchor=anchor_iso))
    return out


# ----------------------------------------------------------------------------- 双写渲染
def render_dual(text: str, anchor: Union[str, datetime, date]) -> dict:
    """
    双写渲染：original 原文不动；normalized 在 replace 级命中处追加「(→ISO)」标注，
    annotate 级追加「(orig→ISO?|low-conf)」仅供参考；annotate_only 不改动文本。
    返回 {"original", "normalized", "annotations", "stats"}，即卡面双列的生成原料。
    """
    rs = normalize(text, anchor)
    out = []
    last = 0
    for r in rs:
        out.append(text[last:r.start])
        if r.action == "replace":
            iso = r.value["iso"]
            tag = f"{r.raw} (->{iso})"
        elif r.action == "annotate":
            iso = r.value["iso"] if r.value else "?"
            tag = f"{r.raw} (->{iso}?|low-conf)"
        else:
            tag = r.raw
        out.append(tag)
        last = r.end
    out.append(text[last:])
    annotations = []
    for r in rs:
        a = {"expr": r.raw, "span": [r.start, r.end], "rule": r.rule,
             "confidence": r.confidence, "action": r.action, "note": r.note}
        if r.value:
            a["resolved"] = r.value
        annotations.append(a)
    stats = {
        "n_expr": len(rs),
        "n_replace": sum(1 for r in rs if r.action == "replace"),
        "n_annotate": sum(1 for r in rs if r.action == "annotate"),
        "n_annotate_only": sum(1 for r in rs if r.action == "annotate_only"),
    }
    return {"original": text, "normalized": "".join(out), "annotations": annotations, "stats": stats}


def normalize_message(text: str, ts: str, dia_id: Optional[str] = None) -> dict:
    """
    消息级入口（organ ingestion 直接调用）。
    返回卡面 time_norm 块：锚点、逐表达双写、统计。绝不改动原文。
    """
    res = render_dual(text, ts)
    res["anchor"] = parse_anchor(ts).isoformat()
    if dia_id:
        res["dia_id"] = dia_id
    return res


# ============================================================================ INTEGRATION
# 集成规格（供 W2-1 卡面采纳；此处为唯一真源，战报引用本节）
#
# 接入点：organ ingestion 的「消息→记忆卡」生成处（W2-1 负责面），在文本入库前调用：
#     from time_norm import normalize_message
#     tn = normalize_message(msg_text, msg_ts, dia_id)
# 卡面字段：每卡新增顶层 `time_norm` 块（不改动任何现有字段，零破坏）：
#     {
#       "time_norm": {
#         "anchor": "2022-08-22T10:57:00Z",
#         "dia_id": "D19:6",
#         "exprs": [
#           {"expr": "last week", "span": [42, 51], "rule": "last_unit",
#            "resolved": {"iso": "2022-08-15", "granularity": "week",
#                         "range": ["2022-08-15", "2022-08-21"], "direction": "past"},
#            "confidence": 0.9, "action": "replace"},
#           {"expr": "recently", "confidence": 0.0, "action": "annotate_only",
#            "note": "vague past marker 'recently'; annotate only"}
#         ],
#         "stats": {"n_expr": 2, "n_replace": 1, "n_annotate": 0, "n_annotate_only": 1}
#       }
#     }
# 双写格式：检索索引同时收 `text`（原文）与 `text_norm`（render_dual()["normalized"]，
# 形如 "... super nervous last week (->2022-08-15) when I shared ..."）；卡面展示两列
# （原文列 | 解析列），原文永不替换。conf>=0.9 才出现在 text_norm 的 (->ISO) 无问号标注；
# conf=0.6 带 "?|low-conf" 后缀仅供参考；conf=0 只进 exprs 数组不进 text_norm。
# 阈值：REPLACE_MIN_CONF = 0.9（本模块常量，勿在调用侧放宽）。
# 检索/判卷耦合：temporal 问句改写时用 resolved.iso 回填 "yesterday/last week" 类查询词；
# 判卷宽容层（W2-2）可将作答中的相对词经同一模块解析后与 gold ISO 比较（A5 孪生 4 题
# 相对时间分歧即此口径）。低置信值只允许用于召回扩展，不得直接判定对错。

if __name__ == "__main__":
    import json as _json
    import sys
    demo = [
        ("I lost my job as a banker yesterday.", "2023-01-19T19:06:00Z"),
        ("Was super nervous last week when I shared my book with my writers group.", "2022-08-22T10:57:00Z"),
        ("I started volunteering here about a year ago.", "2023-08-03T18:20:00Z"),
        ("I've been playing for about four months now.", "2023-12-06T17:34:00Z"),
        ("My mother also passed away last year.", "2023-01-23T16:06:00Z"),
        ("Something exciting happened last Friday.", "2022-04-17T18:44:00Z"),
        ("We're going to Tokyo next month, and I'll see you in two weeks.", "2023-03-01T10:00:00Z"),
        ("She retired a long time ago, and my 18th birthday was ten years ago.", "2023-06-27T10:37:00Z"),
    ]
    for t, ts in demo:
        print(_json.dumps(normalize_message(t, ts), ensure_ascii=False, indent=1))
    print("OK", file=sys.stderr)
