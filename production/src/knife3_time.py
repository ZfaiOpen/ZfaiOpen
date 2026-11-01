# knife3_time.py — 部署线C1时间锚定: ADD相相对/绝对时间归一 + SEARCH相时间query检索增强(零LLM·纯结构化·app层overlay·20261008)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方冒烟C1日期/相对时间/间隔=13.33%(全场第二血洞) — 卡文里的相对时间表达
#       ("three days ago/last Friday/两天后/上周")与绝对日期未被结构化利用; 间隔类query
#       ("How long between X and Y")的两张日期卡检索排名不稳(calib床C1-02锚9.30在top10外)。
# 法典: "时间粒度对表"(问天答天/问日期答日期); BRAIOS的add正确携带平台timestamp(锚可靠)。
# 做法(对冻结核心/organ/exam考场件零修改·纯overlay·可整件摘除回滚):
#   ADD相    observe(): 轻量解析器(相对词表+英文日历算术·零新增依赖)以消息timestamp为锚,
#            把卡文中相对/绝对时间表达归一成绝对日期 → 卡上双存(归一化ISO日期+原文措辞)
#            +可选事件区间(start/end) 落盘 knife3_time.db(sqlite WAL+synchronous=NORMAL,
#            knife1/2同款纪律·重启不丢)。全卡(sess/dt)入cards表=会话时间位次底册。
#   启动相   backfill(): 全量回填organ存量卡(修前已入库卡亦被索引覆盖·幂等INSERT OR IGNORE)。
#   SEARCH相 time_annotate(): 时间query检测(duration/when/order三档·中英) →
#            ①命中卡带归一化日期注记(metadata附注·不改写content原文)
#            ②when档: 窗内命中卡浮前排(钉住原top3·防回退)
#            ③duration档(间隔query): 同会话双日期卡必回(前插保位)+两卡日期差注记
#              (粒度对表: 问天给天数/问月给月数/问日期给日期)
#            ④order档: 同会话卡按事件时间位次标注(C2顺序红利·只注记不重排)
# 选型留痕: 冻结核recall_nlp.date_anchor(BRAINOS_DATE_ANCHOR缺省关·DA4回归实证锚入卡形态
#           净丢5.76分)与本刀互斥互补 — 本刀只附metadata零改写文本(不进embed/词面索引,
#           零DA4回归形态); 解析器为部署线自写轻量版(禁dateutil等重型依赖·L0-13纯Rust+bash
#           对部署件例外同knife1/2·python stdlib only)。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与 aml_service._evidence_id 同源同值(knife1/2同法)。
# 词典维护: knife3_time_terms.json(同目录) — 改词典+重启即生效; env KNIFE3_DICT/KNIFE3_DB可换路径。
# 合规: 日志零内容 — 只记uid/档位/计数; 不落query文本/卡文本。
# ═══════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import calendar as _cal
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger("knife3")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE3_DICT", str(APP_DIR / "knife3_time_terms.json")))
DB_PATH = Path(os.environ.get("KNIFE3_DB", str(APP_DIR.parent / "data" / "knife3_time.db")))


def load_dict(path: Path = DICT_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_DICT = load_dict()
_MONTHS: dict[str, int] = {k.lower(): v for k, v in _DICT.get("months_en", {}).items()}
_WEEKDAYS: dict[str, int] = {k.lower(): v for k, v in _DICT.get("weekdays_en", {}).items()}
_NUM_EN: dict[str, int] = {k.lower(): v for k, v in _DICT.get("numerals_en", {}).items()}
_NUM_CN: dict[str, int] = {k: v for k, v in _DICT.get("numerals_cn", {}).items()}
_WD_CN: dict[str, int] = {k: v for k, v in _DICT.get("weekday_cn", {}).items()}
_BOUNDS = _DICT.get("bounds", {})
_PAST_DAYS = int(_BOUNDS.get("past_days", 1827))
_FUTURE_DAYS = int(_BOUNDS.get("future_days", 366))


def _alt(keys: list[str]) -> str:
    """词典键→正则交替片段(长词优先防前缀截断)。"""
    return "(?:" + "|".join(sorted((re.escape(k) for k in keys), key=len, reverse=True)) + ")"


def _sig_res(entries: list) -> list[re.Pattern]:
    return [re.compile(rx, re.IGNORECASE) for rx in entries]


_MONTH_A = _alt(list(_MONTHS))
# "may"兼情态动词("may 3 people"≠日期) — 裸"may D"须跟标点/串尾且非", YYYY"(强年份式另收)
_MONTH_NO_MAY = _alt([k for k in _MONTHS if k != "may"])
_MAY_BARE_TAIL = r"(?=\s*[—–,.:;!?;。，：；！？]|$)(?!\s*,\s*\d{4})"
_WD_A = _alt(list(_WEEKDAYS))
_NUM_A = _alt(list(_NUM_EN))
_QSIG: dict[str, list[re.Pattern]] = {
    k: _sig_res(v) for k, v in (_DICT.get("query_signals", {}) or {}).items()}

# ── 时间表达语法(词表驱动·中英双语·高置信) ────────────────────────────────────
_RX_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_RX_MDY = re.compile(r"\b(" + _MONTH_NO_MAY + r")\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*,\s*(\d{4}))?\b", re.I)
_RX_MAY_Y = re.compile(r"\bmay\.?\s+(\d{1,2})(?:st|nd|rd|th)?\s*,\s*(\d{4})\b", re.I)
_RX_MAY_BARE = re.compile(r"\bmay\.?\s+(\d{1,2})(?:st|nd|rd|th)?" + _MAY_BARE_TAIL, re.I)
_RX_DMY = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?(" + _MONTH_NO_MAY + r")\.?(?:\s*,?\s*(\d{4}))?\b", re.I)
_RX_DMY_MAY_Y = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?may\.?,?\s*(\d{4})\b", re.I)
_RX_DMY_MAY_BARE = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?may\.?" + _MAY_BARE_TAIL, re.I)
_RX_MY = re.compile(r"\b(" + _MONTH_A + r")\.?,?\s+(\d{4})\b", re.I)
_RX_AGO = re.compile(r"\b(" + _NUM_A + r"|\d+)\s+(day|week|month|year)s?\s+ago\b", re.I)
_RX_IN = re.compile(r"\bin\s+(" + _NUM_A + r"|\d+)\s+(day|week|month|year)s?\b", re.I)
_RX_LATER = re.compile(r"\b(" + _NUM_A + r"|\d+)\s+(day|week|month|year)s?\s+(?:from\s+now|later)\b", re.I)
_RX_EN_DAY = re.compile(r"\b(yesterday|tonight|today|tomorrow)\b", re.I)
_RX_EN_WD = re.compile(r"\b(last|past|next|this|coming)\s+(" + _WD_A + r")\b", re.I)
_RX_EN_WM = re.compile(r"\b(last|past|next|this)\s+(week|month|year|weekend)\b", re.I)
_RX_CN_REL = re.compile(r"([一二两三四五六七八九十\d]+)\s*(天|周|星期|个?月|年)\s*(?:之)?([前后])")
_RX_CN_DAY = re.compile(r"(昨天|昨晚|今天|今日|今晚|明天|后天)")
_RX_CN_WD = re.compile(r"(上个?|下个?|这个?)\s*(?:周|星期)\s*([一二三四五六日天])?")
_RX_CN_WM = re.compile(r"(上个?|下个?)(?:周|星期)(?![一二三四五六日天])|(上个?|下个?)月|(去年|明年|今年|后年|前年)")


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16], knife1/2同法)。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def _num_of(raw: str) -> int | None:
    raw = (raw or "").strip().lower()
    if raw.isdigit():
        return int(raw)
    if raw in _NUM_EN:
        return _NUM_EN[raw]
    if raw in _NUM_CN:
        return _NUM_CN[raw]
    return None


def _parse_base(dt: str) -> date | None:
    """卡消息timestamp(ISO·毫秒epoch容错)→锚日期; 解析不出=None(该卡零锚·fail-soft)。"""
    s = str(dt or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt) + 2], fmt).date()
        except ValueError:
            continue
    try:
        v = float(s)
        if v > 1e11:  # 毫秒
            v /= 1000.0
        return datetime.fromtimestamp(v).date()
    except (TypeError, ValueError):
        return None


def _add_months(d: date, n: int) -> date:
    """日历月算术(月末钳位: Mar31-1mo=Feb28·非30天近似)。"""
    y, m = d.year + (d.month - 1 + n) // 12, (d.month - 1 + n) % 12 + 1
    return date(y, m, min(d.day, _cal.monthrange(y, m)[1]))


def _shift_years(d: date, n: int) -> date:
    """日历年算术(Feb29钳位: 2024-02-29-1y=2023-02-28·不抛ValueError)。"""
    y = d.year + n
    try:
        return d.replace(year=y)
    except ValueError:
        return d.replace(year=y, day=28)


def _yearless(d: date, base: date) -> date:
    """无年表达定年: 先取base年, 落到base+92d未来外→回退一年(近期过去语义)。"""
    cand = d.replace(year=base.year)
    if cand > base + timedelta(days=92):
        cand = cand.replace(year=base.year - 1)
    return cand


def _mk(expr: str, g: str, gstart: date, gend: date | None = None) -> dict[str, Any]:
    return {"expr": expr, "granularity": g, "gstart": gstart, "gend": gend}


def _in_gate(d: date, base: date) -> bool:
    """防毒闸(对表冻结核DA3): 锚须落在[base-1827d, base+366d], 出界静默弃锚。"""
    return (base - timedelta(days=_PAST_DAYS)) <= d <= (base + timedelta(days=_FUTURE_DAYS))


def parse_anchors(text: str, dt: str) -> list[dict[str, Any]]:
    """卡文本+消息timestamp锚 → 归一化时间锚列表(双存: 原文措辞expr+绝对日期gstart/gend)。
    解析不出/出界/脏输入 → [](fail-soft·绝不抛)。"""
    base = _parse_base(dt)
    t = str(text or "")
    if base is None or not t:
        return []
    out: list[dict[str, Any]] = []

    def push(a: dict[str, Any] | None) -> None:
        if a and _in_gate(a["gstart"], base):
            out.append(a)

    for m in _RX_ISO.finditer(t):  # 绝对: 2024-01-01
        try:
            push(_mk(m.group(0), "day", date(int(m.group(1)), int(m.group(2)), int(m.group(3)))))
        except ValueError:
            pass
    for m in _RX_MDY.finditer(t):  # 绝对: March 3, 2025 / March 3 / Aug 19
        mo = _MONTHS[m.group(1).lower()]
        try:
            d = date(int(m.group(3) or 0) or base.year, mo, int(m.group(2)))
        except ValueError:
            continue
        push(_mk(m.group(0), "day", d if m.group(3) else _yearless(d, base)))
    for m in _RX_MAY_Y.finditer(t):  # 绝对(强年份): May 15, 2025
        try:
            push(_mk(m.group(0), "day", date(int(m.group(2)), 5, int(m.group(1)))))
        except ValueError:
            continue
    for m in _RX_MAY_BARE.finditer(t):  # 绝对裸月日(守卫后): may 5.
        push(_mk(m.group(0), "day", _yearless(date(base.year, 5, int(m.group(1))), base)))
    for m in _RX_DMY.finditer(t):  # 绝对: 3 March 2025
        mo = _MONTHS[m.group(2).lower()]
        try:
            d = date(int(m.group(3) or 0) or base.year, mo, int(m.group(1)))
        except ValueError:
            continue
        push(_mk(m.group(0), "day", d if m.group(3) else _yearless(d, base)))
    for m in _RX_DMY_MAY_Y.finditer(t):  # 绝对(强年份): 15 May 2025
        try:
            push(_mk(m.group(0), "day", date(int(m.group(2)), 5, int(m.group(1)))))
        except ValueError:
            continue
    for m in _RX_DMY_MAY_BARE.finditer(t):  # 绝对裸月日(守卫后): 5 May.
        push(_mk(m.group(0), "day", _yearless(date(base.year, 5, int(m.group(1))), base)))
    for m in _RX_MY.finditer(t):  # 绝对月粒度: March 2025(DMY全日期已含→前置"12 "跳过防双锚)
        if re.fullmatch(r"\d{1,2} ", t[max(0, m.start() - 3):m.start()]):
            continue
        mo = _MONTHS[m.group(1).lower()]
        y = int(m.group(2))
        gs, ge = date(y, mo, 1), date(y, mo, _cal.monthrange(y, mo)[1])
        push(_mk(m.group(0), "month", gs, ge))
    for m in _RX_AGO.finditer(t):  # 相对过去: three days ago
        n = _num_of(m.group(1))
        if n is None:
            continue
        u = m.group(2).lower()
        gs = (base - timedelta(days=n) if u == "day" else base - timedelta(weeks=n)
              if u == "week" else _add_months(base, -n) if u == "month" else _shift_years(base, -n))
        push(_mk(m.group(0), "day" if u == "day" else u, gs))
    for rx in (_RX_IN, _RX_LATER):  # 相对将来: in three days / two weeks later
        for m in rx.finditer(t):
            n = _num_of(m.group(1))
            if n is None:
                continue
            u = m.group(2).lower()
            gs = (base + timedelta(days=n) if u == "day" else base + timedelta(weeks=n)
                  if u == "week" else _add_months(base, n) if u == "month" else _shift_years(base, n))
            push(_mk(m.group(0), "day" if u == "day" else u, gs))
    for m in _RX_EN_DAY.finditer(t):  # yesterday/today/tomorrow
        off = {"yesterday": -1, "tonight": 0, "today": 0, "tomorrow": 1}[m.group(1).lower()]
        push(_mk(m.group(0), "day", base + timedelta(days=off)))
    for m in _RX_EN_WD.finditer(t):  # last Friday / next Monday
        mod, tgt = m.group(1).lower(), _WEEKDAYS[m.group(2).lower()]
        wd = base.weekday()
        if mod in ("last", "past"):
            off = -((wd - tgt) % 7 or 7)        # 上个X(严格之前·向过去)
        elif mod in ("next", "coming"):
            off = (tgt - wd) % 7 or 7           # 将来的X(严格将来)
        else:
            off = tgt - wd                      # this X=本周内该日(可负: 周三说this Monday→本周一)
        push(_mk(m.group(0), "day", base + timedelta(days=off)))
    for m in _RX_EN_WM.finditer(t):  # last week/month/year/weekend
        mod, u = m.group(1).lower(), m.group(2).lower()
        wd = base.weekday()
        if u == "week":
            mon = base - timedelta(days=wd)
            gs = mon - timedelta(days=7) if mod in ("last", "past") else (
                mon + timedelta(days=7) if mod == "next" else mon)
            push(_mk(m.group(0), "week", gs, gs + timedelta(days=6)))
        elif u == "weekend":
            sat = base + timedelta(days=(5 - wd) % 7 if mod == "this" else
                                   ((5 - wd) % 7 or 7) if mod == "next" else -((wd + 1) % 7 + 1))
            push(_mk(m.group(0), "week", sat, sat + timedelta(days=1)))
        elif u == "month":
            gs = _add_months(base.replace(day=1), -1 if mod in ("last", "past") else (1 if mod == "next" else 0))
            push(_mk(m.group(0), "month", gs, date(gs.year, gs.month, _cal.monthrange(gs.year, gs.month)[1])))
        else:
            y = base.year + (-1 if mod in ("last", "past") else (1 if mod == "next" else 0))
            push(_mk(m.group(0), "year", date(y, 1, 1), date(y, 12, 31)))
    for m in _RX_CN_REL.finditer(t):  # 中文相对: 三天前/两周后/2个月前
        n = _num_of(m.group(1))
        if n is None:
            continue
        u, dr = m.group(2), (1 if m.group(3) == "后" else -1)
        if u == "天":
            gs, g = base + timedelta(days=dr * n), "day"
        elif u in ("周", "星期"):
            gs, g = base + timedelta(weeks=dr * n), "week"
        elif u.endswith("月"):
            gs, g = _add_months(base, dr * n), "month"
        else:
            gs, g = _shift_years(base, dr * n), "year"
        push(_mk(m.group(0), g, gs))
    for m in _RX_CN_DAY.finditer(t):  # 昨天/今天/明天/后天
        w = m.group(1)
        push(_mk(w, "day", base + timedelta(days={"昨天": -1, "昨晚": -1, "今天": 0, "今日": 0,
                                                  "今晚": 0, "明天": 1, "后天": 2}[w])))
    for m in _RX_CN_WD.finditer(t):  # 上周五/下周三/这周一
        tgt = _WD_CN.get(m.group(2))
        if tgt is None:
            continue
        wd = base.weekday()
        if m.group(1).startswith("上"):
            off = -((wd - tgt) % 7 or 7)   # 上个X(严格之前·向过去)
        elif m.group(1).startswith("下"):
            off = (tgt - wd) % 7 or 7      # 下个X(严格将来)
        else:
            off = tgt - wd                      # 本周X(周一基准可负: 周三说这周一→本周一)
        push(_mk(m.group(0), "day", base + timedelta(days=off)))
    for m in _RX_CN_WM.finditer(t):  # 上周/下个月/去年(周分支负向断言已排除上周X)
        g = m.group(0)
        if "月" in g:  # 上个月/下个月
            gs = _add_months(base.replace(day=1), -1 if g.startswith("上") else 1)
            push(_mk(g, "month", gs, date(gs.year, gs.month, _cal.monthrange(gs.year, gs.month)[1])))
        elif g.startswith(("上", "下")):  # 上周/下周
            mon = base - timedelta(days=base.weekday())
            gs = mon - timedelta(days=7) if g.startswith("上") else mon + timedelta(days=7)
            push(_mk(g, "week", gs, gs + timedelta(days=6)))
        else:  # 去年/明年/今年/后年/前年
            off = {"去年": -1, "前年": -2, "今年": 0, "明年": 1, "后年": 2}[g]
            push(_mk(g, "year", date(base.year + off, 1, 1), date(base.year + off, 12, 31)))
    # 同日去重 + 按日期排序(多锚按日序)
    seen: set[date] = set()
    uniq = []
    for a in sorted(out, key=lambda x: x["gstart"]):
        if a["gstart"] in seen:
            continue
        seen.add(a["gstart"])
        a["gstart"] = a["gstart"].isoformat()
        a["gend"] = a["gend"].isoformat() if a["gend"] else None
        uniq.append(a)
    return uniq


def detect_tier(query: str) -> str:
    """query时间档位: duration(间隔)>when(时点)>order(顺序)>none。零命中=none(零干预直通)。"""
    q = str(query or "")
    if not q:
        return "none"
    for tier in ("duration", "when", "order"):
        for rx in _QSIG.get(tier, ()):
            if rx.search(q):
                return tier
    return "none"


def _months_days(d1: date, d2: date) -> tuple[int, int]:
    """(整月数, 余天): 粒度对表用(问月给月数·附天数)。"""
    if d2 < d1:
        d1, d2 = d2, d1
    m = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    if d2.day < d1.day:
        m -= 1
    rest = (d2 - _add_months(d1, m)).days
    return m, rest


# ── 持久化(sqlite WAL·knife1/2同款纪律·重启不丢) ─────────────────────────────
class Knife3:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS cards(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, sess TEXT NOT NULL,
            dt TEXT NOT NULL, obs_seq INTEGER NOT NULL, PRIMARY KEY(tenant, card_id))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS time_anchors(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, expr TEXT NOT NULL,
            gstart TEXT NOT NULL, gend TEXT, granularity TEXT NOT NULL,
            obs_seq INTEGER NOT NULL, PRIMARY KEY(tenant, card_id, expr))""")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_k3_anchor_date ON time_anchors(tenant, gstart)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_k3_cards_sess ON cards(tenant, sess)")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0      # 观察卡数(累计·运行期)
        self.n_anchor = 0   # 新锚行数
        self.n_err = 0      # 单卡观察失败数(fail-soft计数)

    # ── ADD相: 时间归一落盘 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        """build_cards产物逐卡观察归一。单卡失败只计数不抛(fail-soft·绝不影响add成功语义)。"""
        for c in cards or []:
            try:
                self._observe_card(str(tenant), c)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("knife3 observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)

    def _observe_card(self, tenant: str, c: dict[str, Any]) -> None:
        text = str(c.get("text") or "")  # or"": None/空/脏值→空串直返(禁str(None)="None"脏行)
        sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
        if not text:
            return
        cid = card_id_of(sess, dt, text)
        anchors = parse_anchors(text, dt)
        with self._lock:
            seq = time.time_ns()
            self._conn.execute("INSERT OR IGNORE INTO cards VALUES(?,?,?,?,?)",
                               (tenant, cid, sess, dt, seq))
            for a in anchors:
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO time_anchors VALUES(?,?,?,?,?,?,?)",
                    (tenant, cid, a["expr"], a["gstart"], a["gend"], a["granularity"], seq))
                self.n_anchor += cur.rowcount
            self._conn.commit()

    # ── 启动相: 存量全量回填(幂等) ──
    def backfill(self, organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
        """organ存量卡全量回填索引(修前已入库卡亦覆盖)。幂等OR IGNORE·单事务·毫秒级。
        返回(扫过卡数, 新锚行数, 耗时ms)。"""
        t0 = time.time()
        n_c = n_a = 0
        with self._lock:
            seq = time.time_ns()
            for c in organ_cards:
                try:
                    text = str(c.get("text") or "")
                    conv = str(c.get("conv") or "")
                    sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
                    if not text or not sess or not dt:
                        continue
                    tenant = conv.split(":", 1)[0] or "u_default"
                    cid = card_id_of(sess, dt, text)
                    self._conn.execute("INSERT OR IGNORE INTO cards VALUES(?,?,?,?,?)",
                                       (tenant, cid, sess, dt, seq))
                    n_c += 1
                    for a in parse_anchors(text, dt):
                        cur = self._conn.execute(
                            "INSERT OR IGNORE INTO time_anchors VALUES(?,?,?,?,?,?,?)",
                            (tenant, cid, a["expr"], a["gstart"], a["gend"], a["granularity"], seq))
                        n_a += cur.rowcount
                except Exception:  # noqa: BLE001 — 单卡脏数据跳过(回填fail-soft)
                    continue
            self._conn.commit()
        return n_c, n_a, (time.time() - t0) * 1000

    # ── SEARCH相: 索引查询 ──
    def _chunks(self, ids: list[str], size: int = 500):
        for i in range(0, len(ids), size):
            yield ids[i:i + size]

    def lookup(self, tenant: str, card_ids: list[str]) -> tuple[dict[str, list[dict]], dict[str, str]]:
        """card_ids → (锚行{cid:[锚]}, 会话{cid:sess})(分块IN查询)。"""
        anchors: dict[str, list[dict]] = {}
        sess_of: dict[str, str] = {}
        ids = [c for c in card_ids if c]
        if not ids:
            return anchors, sess_of
        with self._lock:
            for chunk in self._chunks(ids):
                q = ("SELECT card_id, gstart, gend, granularity, expr FROM time_anchors "
                     "WHERE tenant=? AND card_id IN (" + ",".join("?" * len(chunk)) + ") "
                     "ORDER BY gstart")
                for cid, gs, ge, gr, wx in self._conn.execute(q, (tenant, *chunk)):
                    anchors.setdefault(str(cid), []).append(
                        {"date": gs, "end": ge, "granularity": gr, "said_as": wx})
                q2 = ("SELECT card_id, sess FROM cards WHERE tenant=? AND card_id IN ("
                      + ",".join("?" * len(chunk)) + ")")
                for cid, se in self._conn.execute(q2, (tenant, *chunk)):
                    sess_of[str(cid)] = str(se)
        return anchors, sess_of

    def session_ranks(self, tenant: str, sess_names: list[str]) -> dict[str, dict[str, int]]:
        """会话名 → {card_id: 该会话内按事件时间(dt)升序位次}(C2顺序红利底册)。"""
        out: dict[str, dict[str, int]] = {}
        names = [s for s in sess_names if s]
        if not names:
            return out
        with self._lock:
            for chunk in self._chunks(names):
                q = ("SELECT sess, card_id, dt FROM cards WHERE tenant=? AND sess IN ("
                     + ",".join("?" * len(chunk)) + ") ORDER BY sess, dt, card_id")
                for se, cid, _dt in self._conn.execute(q, (tenant, *chunk)):
                    lst = out.setdefault(str(se), {})
                    lst[str(cid)] = len(lst) + 1
        return out

    def cards_in_range(self, tenant: str, start: str, end: str, limit: int = 200) -> list[dict[str, Any]]:
        """时间索引范围检索API(支持按日期范围/时距查卡): gstart∈[start,end]的锚+卡。"""
        rows: list[dict[str, Any]] = []
        with self._lock:
            q = ("SELECT a.card_id, c.sess, a.gstart, a.gend, a.granularity, a.expr "
                 "FROM time_anchors a JOIN cards c ON c.tenant=a.tenant AND c.card_id=a.card_id "
                 "WHERE a.tenant=? AND a.gstart>=? AND a.gstart<=? ORDER BY a.gstart LIMIT ?")
            for cid, se, gs, ge, gr, wx in self._conn.execute(q, (tenant, start, end, int(limit))):
                rows.append({"card_id": cid, "sess": se, "date": gs, "end": ge,
                             "granularity": gr, "said_as": wx})
        return rows

    # ── SEARCH相: 注记+排序+间隔双卡 ──
    def time_annotate(self, tenant: str, query: str, data: list[dict[str, Any]],
                      k1_intent: str = "current") -> tuple[list[dict[str, Any]], str, int]:
        """返回(注记/重排后data, 档位, 命中卡数)。零命中或非时间query=原样直通(零成本)。
        k1_intent=history时跳过重排(knife1时间线dt升序为C3资产·禁打架·只注记)。"""
        if not data:
            return data, "none", 0
        tier = detect_tier(query)
        if tier == "none":
            return data, "none", 0
        ids = [str(d.get("id", "")) for d in data]
        anchors_by, sess_by = self.lookup(tenant, ids)
        n_hit = len(anchors_by)
        if tier == "order":  # C2红利: 只注记会话时间位次·不重排不注日期(零召回扰动)
            ranks = self.session_ranks(tenant, list(set(sess_by.values())))
            for d in data:
                se = sess_by.get(str(d.get("id", "")))
                r = ranks.get(se, {}).get(str(d.get("id", "")))
                if r:
                    md = d.setdefault("metadata", {}).setdefault("time", {})
                    md["session_time_rank"] = {"rank": r, "of": len(ranks.get(se, {}))}
            return data, tier, len(sess_by)
        if not n_hit:
            return data, tier, 0
        # ①归一化日期注记(metadata附注·不改写content原文)
        for d in data:
            rows = anchors_by.get(str(d.get("id", "")))
            if rows:
                d.setdefault("metadata", {}).setdefault("time", {})[
                    "normalized_dates"] = [
                        {"date": r["date"], **({"end": r["end"]} if r["end"] else {}),
                         "said_as": r["said_as"], "granularity": r["granularity"]} for r in rows]
        if k1_intent == "history":
            # knife1时间线意图(dt升序=C3资产·禁打架): 只注记·零重排
            return data, tier, n_hit
        if tier == "duration":
            self._front_interval_pair(tenant, query, data, anchors_by, sess_by)
        elif tier == "when":
            self._float_matched(data, set(anchors_by))
        return data, tier, n_hit

    def _front_interval_pair(self, tenant: str, query: str, data: list[dict[str, Any]],
                             anchors_by: dict, sess_by: dict) -> None:
        """间隔query: 同会话双日期卡必回(前插保位·两卡按日序)+日期差注记(粒度对表)。"""
        pos_of = {str(d.get("id", "")): i for i, d in enumerate(data)}
        by_sess: dict[str, list[tuple[int, str, dict]]] = {}
        for cid, rows in anchors_by.items():
            if len(rows) < 1:
                continue
            se = sess_by.get(cid)
            if se:
                for r in rows:
                    by_sess.setdefault(se, []).append((pos_of.get(cid, 10 ** 9), cid, r))
        best: tuple[int, dict] | None = None  # (rank_sum, pair描述)
        for se, items in by_sess.items():
            dates = sorted({r["date"]: (cid, r) for _, cid, r in items}.items())
            if len(dates) >= 2:  # 跨卡对(同会话≥2个不同日期)
                (ga, (ca, _)), (gb, (cb, _)) = dates[0], dates[-1]
                rs = pos_of.get(ca, 10 ** 9) + pos_of.get(cb, 10 ** 9)
                if best is None or rs < best[0]:
                    best = (rs, {"a": ca, "b": cb, "ga": ga, "gb": gb})
            elif (len(items) >= 2 and items[0][2]["date"] != items[-1][2]["date"]):
                # 单卡内双日期(如"From the April 2 kickoff to the August 19 freeze")
                rs = pos_of.get(items[0][1], 10 ** 9)
                if best is None or rs < best[0]:
                    best = (rs, {"a": items[0][1], "b": items[0][1], "ga": items[0][2]["date"],
                                 "gb": items[-1][2]["date"]})
        if best is None:
            return
        p = best[1]
        d1, d2 = date.fromisoformat(p["ga"]), date.fromisoformat(p["gb"])
        days = abs((d2 - d1).days)
        mo, rd = _months_days(d1, d2)
        ask_months = any(rx.search(query) for rx in _QSIG.get("granularity_months", ()))
        ask_years = any(rx.search(query) for rx in _QSIG.get("granularity_years", ()))
        if ask_years and mo >= 12:
            note = f"about {mo / 12:.1f} years ({days} days)"
        elif ask_months and mo:
            note = (f"about {mo} months" + (f" {rd} days" if rd else "") + f" ({days} days)")
        else:
            note = f"{days} days"  # 粒度对表默认: 问时长给天数
        iv = {"from": min(p["ga"], p["gb"]), "to": max(p["ga"], p["gb"]),
              "days": days, **({"months": mo} if mo else {}), "note": note}
        by_id = {str(d.get("id", "")): d for d in data}
        ca, cb = by_id.get(p["a"]), by_id.get(p["b"])
        if ca is None:
            return
        if cb is not None and cb is not ca:
            pair = sorted([ca, cb], key=lambda x: min(
                (r["date"] for r in x["metadata"]["time"].get("normalized_dates", [])
                 if r.get("date")), default=""))
        else:
            pair = [ca]
        for d in pair:
            d["metadata"]["time"]["interval"] = dict(iv)
        rest = [d for d in data if d is not ca and d is not cb]
        data[:] = pair + rest  # 双卡必回: 前插保位(其余相对序不变)

    def _float_matched(self, data: list[dict[str, Any]], matched: set) -> None:
        """when档: 命中卡浮前排(钉住原top3防回退·尾段命中前移相对序不变)。"""
        if len(data) <= 3:
            return
        head, tail = data[:3], data[3:]
        mt = [d for d in tail if str(d.get("id", "")) in matched]
        if not mt or len(mt) > 60:  # 噪声闸: 命中过多=索引噪声大·放弃重排
            return
        ut = [d for d in tail if str(d.get("id", "")) not in matched]
        data[:] = head + mt + ut

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "cards": g("SELECT COUNT(*) FROM cards"),
                "anchor_rows": g("SELECT COUNT(*) FROM time_anchors"),
                "anchored_cards": g("SELECT COUNT(DISTINCT card_id) FROM time_anchors"),
                "by_granularity": {k: g(f"SELECT COUNT(*) FROM time_anchors WHERE granularity='{k}'")
                                   for k in ("day", "week", "month", "year")},
                "obs_cards_total": self.n_obs, "anchor_total": self.n_anchor,
                "obs_err_total": self.n_err}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部索引行(organ内卡片本体不在本模块管辖)。"""
        with self._lock:
            n1 = self._conn.execute("DELETE FROM cards WHERE tenant=?", (tenant,)).rowcount
            n2 = self._conn.execute("DELETE FROM time_anchors WHERE tenant=?", (tenant,)).rowcount
            self._conn.commit()
        return n1 + n2


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K3: Knife3 | None = None
_K3_LOCK = threading.Lock()


def get() -> Knife3:
    global _K3
    with _K3_LOCK:
        if _K3 is None:
            _K3 = Knife3(DB_PATH)
            log.info("knife3 init db=%s bounds=[-%dd,+%dd]", _K3.db_path, _PAST_DAYS, _FUTURE_DAYS)
        return _K3


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def backfill(organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    return get().backfill(organ_cards)


def time_annotate(tenant: str, query: str, data: list[dict[str, Any]],
                  k1_intent: str = "current") -> tuple[list[dict[str, Any]], str, int]:
    return get().time_annotate(tenant, query, data, k1_intent)
