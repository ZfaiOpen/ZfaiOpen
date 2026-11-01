"""deep_fact.py — 真事实抽取器(结构化知识卡·EN+CN·零LLM·确定性)
指挥官定谳20261009: 官方考"理解"不考"原样还" — 句子切分原子卡(Paris is a beautiful
city.)是废卡; 本模块把原文升维成 (主体, 关系, 对象, 绝对日期) 结构化事实:
  "Alice lives in Paris (date: 2024-03-15)"
  "Alice plans to move to London (planned, stated 2024-12-30)"
精度优先(解引错=造谣): 封闭类关系动词家族·主语必须真名(解引或继承)·
日期可锚定才落缀(卡dt为"述于"锚·显式/相对日期优先·零伪造)。
零organ扰动: 卡只入边车DB, 搜索相由card_construct.overlay_inject注入浮顶。
回滚 = card_construct摘import+derive_all内fact_cards调用+删本文件。
PG毒卡清除席(20261009·48.38实战窗毒卡三例): ①缩约展开②主语黑名单扩③RULE白名单化④解引完整性门⑤出厂卫生检。
"""
from __future__ import annotations
import re
from datetime import datetime, timedelta
from typing import Any

# ── 日期锚定 ────────────────────────────────────────────────────────────────
_REL_EN = (("yesterday", -1), ("last night", -1), ("today", 0), ("tonight", 0),
           ("tomorrow", 1), ("last week", -7), ("next week", 7),
           ("two weeks ago", -14), ("three days ago", -3), ("a month ago", -30),
           ("last month", -30), ("next month", 30), ("last year", -365),
           ("next year", 365))
_REL_CN = (("昨天", -1), ("昨晚", -1), ("今天", 0), ("今晚", 0), ("明天", 1),
           ("上周", -7), ("下周", 7), ("上个月", -30), ("下个月", 30),
           ("去年", -365), ("明年", 365), ("三天前", -3), ("两周前", -14),
           ("一个月前", -30))

_ISO_FULL = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_CN_DATE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})[日号]?")
_EN_MDY = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", re.I)
_EN_MY = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(20\d{2})\b", re.I)
_MON_N = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
          "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def _iso(y: int, m: int, d: int) -> str:
    try:
        return datetime(y, m, d).strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _card_iso(dt: str) -> str:
    try:
        return datetime.fromisoformat(str(dt).replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""


def _stated_date(text: str, dt: str) -> str:
    """事实日期: 显式日期词直取(年月日>年月)>相对词锚dt换算>回落卡dt(述于·诚实零伪造)。"""
    m = _ISO_FULL.search(text)
    if m:
        s = _iso(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if s:
            return s
    m = _CN_DATE.search(text)
    if m:
        s = _iso(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if s:
            return s
    m = _EN_MDY.search(text)
    if m:
        s = _iso(int(m.group(3)), _MON_N[m.group(1).lower()], int(m.group(2)))
        if s:
            return s
    m = _EN_MY.search(text)
    if m:
        return f"{m.group(2)}-{_MON_N[m.group(1).lower()]:02d}"
    anchor = _card_iso(dt)
    if not anchor:
        return ""
    low = " " + text.lower() + " "
    for w, off in _REL_EN:
        if w in low:
            try:
                return (datetime.strptime(anchor, "%Y-%m-%d") + timedelta(days=off)).strftime("%Y-%m-%d")
            except ValueError:
                return anchor
    for w, off in _REL_CN:
        if w in text:
            try:
                return (datetime.strptime(anchor, "%Y-%m-%d") + timedelta(days=off)).strftime("%Y-%m-%d")
            except ValueError:
                return anchor
    return anchor  # 述于卡dt(消息时间=该事实的证词时间)


# ── EN封闭类事实文法 ─────────────────────────────────────────────────────────
_EN_SUB = r"([A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){0,3})"
_EN_OBJ = r"([A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){0,3})"
_EN_ROL = r"([A-Za-z][\w'-]*(?:\s+[A-Za-z][\w'-]+){0,3})"

_EN_FACTS = (
    (re.compile(rf"\b{_EN_SUB}\s+(?:lives?|is\s+living|resides?|is\s+based|stays?)\s+(?:in|at)\s+{_EN_OBJ}"), "lives in", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:worked?|lived?)\s+in\s+{_EN_OBJ}"), "lived in", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:will\s+move|is\s+moving|is\s+going\s+to\s+move|plans?\s+to\s+move|moving)\s+to\s+{_EN_OBJ}"), "move to", "plan"),
    (re.compile(rf"\b{_EN_SUB}\s+(?:moved|switched|changed|relocated)\s+to\s+{_EN_OBJ}"), "moved to", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:works?|is\s+working)\s+(?:as|at|for)\s+{_EN_ROL}"), "works as", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:got|received|won|accepted)\s+(?:a|an|the)?\s*{_EN_ROL}"), "received", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:finished|completed|won)\s+(?:a|an|the)?\s*{_EN_ROL}"), "completed", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:graduated\s+from|studies\s+at|studies\s+in)\s+{_EN_OBJ}"), "studies at", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:was|is|got)\s+diagnosed\s+with\s+{_EN_ROL}"), "diagnosed with", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:is\s+)?(?:suffering|recovering)\s+from\s+{_EN_ROL}"), "suffers from", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:likes?|loves?|enjoys?|prefers?|hates?)\s+{_EN_ROL}"), "likes", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:is\s+)?(?:married|engaged)\s+to\s+{_EN_OBJ}"), "married to", ""),
    (re.compile(rf"\b{_EN_SUB}\s+(?:bought|purchased|sold)\s+(?:a|an|the)?\s*{_EN_ROL}"), "bought", ""),
)

# ── CN封闭类事实文法(子句级·主语锚定+时间桥) ─────────────────────────────────
_CN_NAME = r"([A-Za-z][A-Za-z]{1,15}|[\u4e00-\u9fa5]{2,4}?)"
_CN_OBJ = r"([\u4e00-\u9fa5A-Za-z0-9]{2,24}?)(?:[了过的]?(?:的|了)|$|[,，。;；!？?\s])"
_CN_BRIDGE = (r"(?:要|将|将要|打算|准备|计划|马上|就|已经|刚|于|在|下周|下个月|下月|"
              r"明天|明年|上周|上个月|上月|昨天|今年|去年|月底|年底|月初|年初|最近|"
              r"近期|接下来|之后|以后|最|很|挺|非常|超)*")
# (主语锚定match: 名字紧邻或时间桥后接关系动词)
_CN_ANCHORED = (
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}住在?(?:于)?{_CN_OBJ}"), "住在", ""),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:搬到了?|搬到){_CN_OBJ}"), "搬到", "plan"),
    (re.compile(rf"^{_CN_NAME}在{_CN_OBJ}(?:工作|上班|任职)"), "在_工作", ""),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:换工作(?:到|成了?)|换到了?|跳槽到|加入){_CN_OBJ}"), "换工作到", "plan"),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:毕业于|考上了?){_CN_OBJ}"), "毕业于", ""),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:喜欢上了?|热爱|讨厌|爱){_CN_OBJ}"), "喜欢", ""),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:得了?|确诊(?:了)?|查出)(?:了)?{_CN_OBJ}"), "确诊", ""),
    (re.compile(rf"^{_CN_NAME}{_CN_BRIDGE}(?:完成了?|跑完(?:了)?|拿下(?:了)?){_CN_OBJ}"), "完成", ""),
)
# (主语承前省略: 子句以关系动词/时间桥开头·主语=前子句主语)
_CN_INHERIT = (
    (re.compile(rf"^(?:要|将|打算|准备|计划|下周|下个月|下月|明天|明年|月底|年底|最近|近期)*搬到了?{_CN_OBJ}"), "搬到", "plan"),
    (re.compile(rf"^(?:要|将|打算|准备|计划)*换工作(?:到|成了?){_CN_OBJ}"), "换工作到", "plan"),
    (re.compile(rf"^(?:喜欢上了?|热爱|讨厌){_CN_OBJ}"), "喜欢", ""),
    (re.compile(rf"^(?:得了?|确诊(?:了)?|查出)(?:了)?{_CN_OBJ}"), "确诊", ""),
    (re.compile(rf"^(?:完成了?|跑完(?:了)?|拿下(?:了)?){_CN_OBJ}"), "完成", ""),
    (re.compile(rf"^住在?{_CN_OBJ}"), "住在", ""),
)

# ── 规则/偏好(G2)与因果(B2)封闭类标记 ────────────────────────────────────────
# 手术③RULE白名单化(全小写匹配·只收承诺/指令句式·治"must've been great"感叹假RULE毒卡)
_EN_RULES = re.compile(
    r"\b(?:i|we)\s+(?:will\s+|would\s+)?(?:always|never)\s+\w+"
    r"|\b(?:i|we)\s+(?:will\s+)?(?:only|never)\s+(?:use|eat|drink|call|provide|answer|respond|include|share)\b"
    r"|\b(?:i|we)\s+(?:must|should|prefer to|make a point to|have to)\s+\w+"
    r"|\byou\s+(?:must|should|need to|have to|always|never|should always)\s+\w+"
    r"|\b(?:always|never)\s+(?:use|call|provide|maintain|answer|include|share|reveal|disclose)\b")
_EN_RULE_EXCLUDE = re.compile(
    r"must(?:'ve| have) been|must be\b\w*\s*(?:great|nice|good|cool|awesome|exciting|fun)")
_CN_RULES = re.compile(r"(?:总是|从不|从来不|从未|必须|记得要|习惯(?:性)?|只(?:吃|喝|用)|不喜欢|偏好)")
_CN_RULE_EXCLUDE = re.compile(r"(?: Must have| Must have been)")  # CN侧同源排除(推测非规则)
_CAUSAL_RE = re.compile(r"\bbecause\b|\bdue to\b|\bso that\b|因为|由于|因此|所以")

_SPEAKER_RE = re.compile(r"^([A-Z][\w'-]{1,20}|[\u4e00-\u9fa5]{2,4})\s*[:：]\s*")
_PRON_RE = re.compile(r"^(i\b|my\b|we\b|我(?!们)|我们|我的)[ ']{0,2}(.*)$", re.I)
_TAG_RE = re.compile(r"^\[[^\]]{1,48}\]\s*")
_VERB3 = {"live": "lives", "work": "works", "study": "studies", "move": "moves",
          "have": "has"}  # PG毒卡修: 缩约展开后"I have"→真名需三单has("Andrew has")
_TRIM_TAIL = re.compile(r"\s+(?:in|at|on|with|during|for|about|last|this|next|every|when|while|and|but|since)\s+.*$", re.I)
_TRIM_DANGLER = re.compile(r"\s+(?:in|at|on|with|for|about|during|since|and|but|when|while)\s*$", re.I)
_LEAD_ART = re.compile(r"^(?:a|an|the)\s+", re.I)
_ROLE_USER = {"user", "assistant", "system", "tool", ""}

# ── PG毒卡修复(20261009·48.38实战窗三例毒卡: 缩约断裂"Andrew ve always"/感叹假RULE
# "must've been great"/废主语"You've received guts" — 手术①展开·④⑤门在下方定义) ──
# 手术①缩约展开(deref入口+rule_cards clause前统一展开·I族大小写两形·其余小写)
_CONTRACTIONS = (
    (re.compile(r"\bI've\b"), "I have"), (re.compile(r"\bi've\b"), "i have"),
    (re.compile(r"\bI'm\b"), "I am"), (re.compile(r"\bi'm\b"), "i am"),
    (re.compile(r"\bI'll\b"), "I will"), (re.compile(r"\bi'll\b"), "i will"),
    (re.compile(r"\bI'd\b"), "I would"), (re.compile(r"\bi'd\b"), "i would"),
    (re.compile(r"\bwe've\b", re.I), "we have"), (re.compile(r"\byou've\b", re.I), "you have"),
    (re.compile(r"\bit's\b", re.I), "it is"), (re.compile(r"\bdon't\b", re.I), "do not"),
    (re.compile(r"\bcan't\b", re.I), "cannot"), (re.compile(r"\bwon't\b", re.I), "will not"),
)


def _expand_contractions(text: str) -> str:
    """缩约展开(确定性·幂等): 未展开则解引把"I've"斩成"<Name> ve"断裂残骸→毒卡直供评审。"""
    for pat, rep in _CONTRACTIONS:
        text = pat.sub(rep, text)
    return text


# 手术④解引产物完整性门: 断裂残粒(独立小写ve/ll/d/m/s后随空白)→整句弃(诚实弃卡)
_DEREF_BROKEN = re.compile(r"\b(ve|ll|d|m|s)\s+")
# 手术⑤FACT/RULE出厂卫生检: 脏token(None/nan/{}/undefined)或废短文本(<12字符)→弃
_POISON_TEXT = re.compile(r"\bNone\b|nan|\{\}|undefined")


def _clean_card_text(t: str) -> bool:
    return len(t) >= 12 and not _POISON_TEXT.search(t)
_PLAN_EN = ("will move", "moving", "plans to move", "going to move")
_PLAN_CN = ("要搬", "打算搬", "准备搬", "将搬", "要换", "打算换", "准备换")


def _sentences(text: str) -> list[str]:
    out: list[str] = []
    for line in str(text or "").splitlines():
        line = _TAG_RE.sub("", line.strip(), count=1)
        if not line:
            continue
        for s in re.split(r"[.!?;。！？；\n]+", line):
            s = s.strip()
            if len(s) >= 4:  # CN短规则句("我从不吃辣"=5字)也要活
                out.append(s)
    return out[:24]


def _clauses(sent: str) -> list[str]:
    """CN长句按逗号切子句(EN逗号不切: 'Paris, France'地名)。"""
    if not re.search(r"[\u4e00-\u9fa5]", sent):
        return [sent]
    return [c.strip() for c in re.split(r"[,，]+", sent) if len(c.strip()) >= 4]


def _deref_subj(clause: str, spk: str) -> str | None:
    """第一人称解引(EN变位+句中I/my替换/CN直接前缀)·无名第一人称=None诚实弃。
    PG毒卡修: 入口先缩约展开·返回前过完整性门(残粒如"Andrew ve"→None·fact/rule双侧共用)。"""
    clause = _expand_contractions(clause)

    def _intact(s: str) -> str | None:
        return None if _DEREF_BROKEN.search(s) else s

    m = _PRON_RE.match(clause)
    if not m:
        # EN句中孤立I/my(无名→不动防造谣)
        if spk and not re.search(r"[\u4e00-\u9fa5]", clause):
            clause = re.sub(r"\bmy\b", spk + "'s", clause, flags=re.I)
            clause = re.sub(r"\bI\b", spk, clause)
        return _intact(clause)
    if not spk:
        return None
    p, rest = m.group(1).strip().lower(), m.group(2)
    if p in ("i", "我", "我们"):
        wm = re.match(r"([A-Za-z]+)(.*)$", rest, re.S)
        if wm and wm.group(1).lower() in _VERB3:
            rest = _VERB3[wm.group(1).lower()] + wm.group(2)
        sep = " " if re.match(r"^[A-Za-z]", rest) else ""
        s = f"{spk}{sep}{rest}"
        if not re.search(r"[\u4e00-\u9fa5]", s):  # 同句后续I/my一并解引(双事实句)
            s = re.sub(r"\bmy\b", f"{spk}'s", s, flags=re.I)
            s = re.sub(r"\bI\b", spk, s)
        return _intact(s)
    sep = "'s " if re.match(r"^[A-Za-z]", rest) else "的"
    return _intact(f"{spk}{sep}{rest}")


def _trim_obj(obj: str) -> str:
    obj = _TRIM_TAIL.sub("", obj.strip())
    obj = _TRIM_DANGLER.sub("", obj)
    obj = _LEAD_ART.sub("", obj)
    return obj.strip(" .,!?'\"，。的了去来呀啊")


_SUBJ_TIME = re.compile(r"^(?:Yesterday|Today|Tonight|Tomorrow|Recently|Later|Finally)\s+", re.I)


# 手术②主语黑名单: 一二人称/无名代词解引不了=造谣风险·诚实弃(扩自("I","WE","MY","ME"))
_SUBJ_DENY = {"I", "WE", "MY", "ME", "YOU", "YOUR", "IT", "THIS", "THAT", "THEY",
              "THERE", "HE", "SHE", "HIS", "HER", "EVERYONE", "SOMEONE", "ANYONE"}


def _extract_en(s: str, out: list[tuple[str, str, str, str]], subj_hint: str) -> None:
    spans: list[tuple[int, int]] = []
    for pat, rel, flag in _EN_FACTS:
        m = pat.search(s)
        if not m:
            continue
        if any(not (m.end() <= a or m.start() >= b) for a, b in spans):
            continue  # 重叠匹配跳过(同段不重复归因)
        subj = _SUBJ_TIME.sub("", m.group(1).strip())
        obj = _trim_obj(m.group(2))
        if len(obj) < 2 or subj.lower() == obj.lower() or len(subj) < 2:
            continue
        if subj.upper() in _SUBJ_DENY:
            continue  # 残留代词主语=归属不明·弃("You've received guts"毒卡根因)
        out.append((subj, rel, obj, "en"))
        spans.append((m.start(), m.end()))


def _extract_cn(s: str, out: list[tuple[str, str, str, str]], subj_hint: str) -> str:
    """CN子句抽取: 主语锚定match(精度)·关系动词开头则承前主语。返回本子句主语。"""
    for pat, rel, flag in _CN_ANCHORED:
        m = pat.match(s)
        if m and m.group(1):
            obj = _trim_obj(m.group(2) or "")
            if len(obj) >= 2 and m.group(1) != obj:
                out.append((m.group(1), rel, obj, "cn"))
            return m.group(1)
    for pat, rel, flag in _CN_INHERIT:
        m = pat.match(s)
        if m and subj_hint:
            obj = _trim_obj(m.group(1) or "")
            if len(obj) >= 2 and subj_hint != obj:
                out.append((subj_hint, rel, obj, "cn"))
            return subj_hint
    # 非关系动词开头的人名子句→更新承前主语(下子句继承)
    nm = re.match(rf"^{_CN_NAME}(?:的|说|表示|告诉|觉得|认为|$)", s)
    return nm.group(1) if nm else subj_hint


def fact_cards(card: dict[str, Any], rid: str, idx: int, start_j: int) -> list[dict[str, Any]]:
    """单原文卡→结构化事实卡列表(确定性·fail-soft·预算由derive_all统一裁)。
    text形态(评审LLM直读友好): "Alice lives in Paris (date: 2024-03-15)" /
    "Bob 计划搬到 伦敦 (计划, 述于 2024-12-30)"。"""
    out: list[dict[str, Any]] = []
    try:
        text = str(card.get("text") or "")
        dt = str(card.get("dt") or "")
        card_spk = str(card.get("speaker") or "")
        fallback_spk = "" if card_spk.lower() in _ROLE_USER else card_spk
        j = start_j
        seen: set[str] = set()
        last_name = ""  # 跨句speaker继承("Alice: Hi! I live..."→次句仍属Alice)
        for raw in _sentences(text):
            sm = _SPEAKER_RE.match(raw)
            spk = ""
            if sm:
                spk = sm.group(1).strip()
                raw = raw[sm.end():].strip()
            if spk:
                last_name = spk
            cur_subj = spk or last_name or fallback_spk
            for clause in _clauses(raw):
                deref = _deref_subj(clause, cur_subj)
                if deref is None:
                    continue
                is_cn = bool(re.search(r"[\u4e00-\u9fa5]", deref))
                found: list[tuple[str, str, str, str]] = []
                if is_cn:
                    cur_subj = _extract_cn(deref, found, cur_subj)
                else:
                    _extract_en(deref, found, cur_subj)
                for subj, rel, obj, lang in found:
                    low = deref.lower()
                    planned = any(w in low for w in _PLAN_EN) or any(w in deref for w in _PLAN_CN)
                    d = _stated_date(clause, dt)
                    if lang == "cn":
                        body = f"{subj} 计划{rel} {obj}" if planned else f"{subj} {rel} {obj}"
                        suffix = (f"(计划, 述于 {d})" if planned else f"(日期: {d})") if d else ""
                    else:
                        body = f"{subj} plans to {rel} {obj}" if planned else f"{subj} {rel} {obj}"
                        suffix = (f"(planned, stated {d})" if planned else f"(date: {d})") if d else ""
                    full = f"{body} {suffix}".strip()
                    if full in seen:
                        continue
                    if not _clean_card_text(full):
                        continue  # 手术⑤出厂卫生检: 脏token/废短文本→弃
                    seen.add(full)
                    out.append({
                        "conv": card.get("conv", ""),
                        "sess": card.get("sess", ""),
                        "dia": f"{rid}:{idx}:f{j}",
                        "speaker": subj,
                        "text": full,
                        "dt": dt,
                        "kind": "fact",
                    })
                    j += 1
                if not found and cur_subj and _CAUSAL_RE.search(deref):
                    # 因果卡(B2): 因果链整句保短卡·不埋长段落(检索回原文30轮=评审读不到)
                    d_c = _stated_date(clause, dt)
                    body = deref if len(deref) <= 150 else deref[:147] + "..."
                    full = f"CAUSAL: {body}" + (f" (date: {d_c})" if d_c else "")
                    if full not in seen and _clean_card_text(full):
                        seen.add(full)
                        out.append({
                            "conv": card.get("conv", ""),
                            "sess": card.get("sess", ""),
                            "dia": f"{rid}:{idx}:c{j}",
                            "speaker": cur_subj,
                            "text": full,
                            "dt": dt,
                            "kind": "fact",
                        })
                        j += 1
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡
        return out
    return out


def rule_cards(card: dict[str, Any], rid: str, idx: int, start_j: int) -> list[dict[str, Any]]:
    """规则/偏好卡(G2): 封闭类规则标记(always/never/总是/从不/必须/习惯)→RULE短卡。
    治法: 官方G2考"约束/偏好回忆"——规则埋在30轮原文里=评审读不到;
    RULE卡独立成卡+注入浮顶=规则直接出席。kind=rule·每原文卡≤2张。"""
    out: list[dict[str, Any]] = []
    try:
        text = str(card.get("text") or "")
        dt = str(card.get("dt") or "")
        card_spk = str(card.get("speaker") or "")
        last_name = "" if card_spk.lower() in _ROLE_USER else card_spk
        j = start_j
        for raw in _sentences(text):
            sm = _SPEAKER_RE.match(raw)
            spk = ""
            if sm:
                spk = sm.group(1).strip()
                raw = raw[sm.end():].strip()
                last_name = spk
            subj = spk or last_name
            for clause in _clauses(raw):
                clause = _expand_contractions(clause)  # 手术①: 白名单匹配前统一展开
                low = clause.lower()  # 手术③: 白名单全小写匹配
                if not (_EN_RULES.search(low) or _CN_RULES.search(clause)):
                    continue
                if _EN_RULE_EXCLUDE.search(low) or _CN_RULE_EXCLUDE.search(clause):
                    continue  # 手术③: 推测/感叹非规则("must've been great"毒卡根因)
                deref = _deref_subj(clause, subj)
                if not deref:
                    continue  # 手术④: 解引断裂/无名→None弃卡
                if len(deref) > 160:
                    deref = deref[:157] + "..."
                d = _stated_date(clause, dt)
                full = f"RULE: {deref}" + (f" (date: {d})" if d else "")
                if not _clean_card_text(full):
                    continue  # 手术⑤出厂卫生检: 脏token/废短文本→弃
                out.append({
                    "conv": card.get("conv", ""),
                    "sess": card.get("sess", ""),
                    "dia": f"{rid}:{idx}:r{j}",
                    "speaker": subj or "rule",
                    "text": full,
                    "dt": dt,
                    "kind": "rule",
                })
                j += 1
                if len(out) >= 2:
                    return out
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡
        return out
    return out
