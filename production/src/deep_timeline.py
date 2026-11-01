"""deep_timeline.py — T1席·as-of时间线预计算引擎(D1/C1对治·2026-10-10)·v2=T-D席增补
法证根因(AML官方评测D1新值覆盖=0/C1时点=0): 人设属性随session更新("2024-12住A"→
"2025-11搬B"), 官方问"as of <日期> X的Y" — 评审拿双版本原文卡自己按日期挑值=时点推理
失败。治法: ADD相把可更新属性预计算成时间线碎片(timelines表·(主体,属性类)聚合·跨卡
多版本), SEARCH相把"按日期选值"由确定性代码算好→注入判定卡(AS-OF ANSWER: ... =
旧值或新值 + valid at <日期> + timeline全段)评审零推理直读。属性类EN+CN各≥10·纯
stdlib·零LLM·零网络·确定性·fail-soft·零organ扰动。回滚=摘patch_t1七hunk+删本件。

[TD-v2增补 2026-10-10·T-D席(缺答诚实卡)]
法证(2026-10-10 12:21-12:26官方run·Sarah Garcia六题): 该租户timelines仅2条垃圾碎片
(主体"Today"/"WilsonPatricia"·196只读实捕)→本人(Sarah Garcia)零覆盖→T1六题0开火;
其中middle name类属性全库(45+卡ledger实捕)无任何证据, gold="记忆中无此信息"类→
评审需系统明示弃答信号。治法: inject()内 query含属性词(EN+CN词表)或as-of日期+
主体可识别(≥2词人名/CN"X的"/timeline单名回认) + 该(主体,属性类)timelines零覆盖
+ 窗内无该属性类证据词(防否证窗内真实证据·T-D守卫) + 限定语(at <Corp>)未在窗内
出现 → 注入诚实卡 "MEMORY STATUS: no record found for <attr> of <subject> in
stored memory" 插位1。有覆盖→正常出AS-OF卡, 该类绝不出诚实卡(防干扰·互斥);
与AS-OF合计总注入≤2·确定性·fail-soft契约不变。"""
from __future__ import annotations
import hashlib
import logging
import re
from datetime import datetime
from typing import Any
import deep_fact as _df  # 复用句切/解引/日期锚定(纯stdlib·无循环依赖)
log = logging.getLogger("deep_timeline")
_TL_CACHE: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
_DDL = ("CREATE TABLE IF NOT EXISTS timelines(tenant TEXT NOT NULL, cid TEXT NOT NULL, subject TEXT NOT NULL, attr TEXT NOT NULL, value TEXT NOT NULL, valid_from TEXT NOT NULL, valid_until TEXT NOT NULL, card_dt TEXT NOT NULL, seq INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE INDEX IF NOT EXISTS idx_tl_ts ON timelines(tenant, subject, attr)")
_ROLE = {"user", "assistant", "system", "tool", ""}
_EW = r"[A-Za-z][\w'-]*(?:\s+[\w'-]+){0,3}"
_CV = r"(?P<v>[\u4e00-\u9fa5A-Za-z0-9]{2,12}?)(?:(?:的|了)|$|[,，。;；!？?\s])"
_EN = (
    ("residence", re.compile(rf"\b(?:lives?|lived|resides?|resided|is living|was living|has lived|stays?|stayed)\s+(?:in|at)\s+(?P<v>{_EW})", re.I), 0), ("residence", re.compile(rf"\b(?:moved|relocated|settled|has moved|just moved)\s+(?:to|into)\s+(?P<v>{_EW})", re.I), 0),
    ("job", re.compile(rf"\b(?:works?|worked|is working|was working)\s+as\s+(?:a|an|the)?\s*(?P<v>{_EW})", re.I), 0), ("job", re.compile(rf"\b(?:am|is|was|are|were)\s+(?:a|an|the)\s+(?P<v>{_EW})", re.I), 0),
    ("employer", re.compile(rf"\b(?:works?|worked|is working|was working)\s+(?:at|for|with)\s+(?P<v>{_EW})", re.I), 0), ("marital", re.compile(r"\b(?:is|was|got|gets)\s+(?P<v>married|engaged|divorced|separated|single)(?:\s+to\s+(?P<v2>[A-Z][\w'-]*(?:\s+[A-Z][\w'-]+){0,2}))?", re.I), 0),
    ("hobby", re.compile(rf"\b(?:likes?|loves?|enjoys?|hobbies?\s+(?:are|include)|hobby\s+is)\s+(?P<v>{_EW})", re.I), 0), ("hobby", re.compile(rf"\b(?:gave up|quit|stopped|dropped)\s+(?P<v>{_EW})", re.I), 1),
    ("language", re.compile(r"\b(?:learning|studying|learned|learn|speak|speaks|spoke|picking up)\s+(?P<v>English|French|Spanish|German|Chinese|Mandarin|Japanese|Korean|Italian|Russian|Portuguese|Arabic|Hindi|Dutch|Swedish|Polish|Turkish)\b", re.I), 0), ("club", re.compile(rf"\b(?:member\s+of|joined|is in|was in|is part of)\s+(?:the\s+)?(?P<v>{_EW})", re.I), 0),
    ("pet", re.compile(r"\b(?:has|have|had|got|owns|adopted)\s+(?:a|an|the|my)?\s*(?P<v>dog|cat|puppy|kitten|rabbit|hamster|parrot|bird|turtle)(?:\s+(?:named|called)\s+(?P<v2>[A-Z][\w'-]+))?", re.I), 0),
    ("diet", re.compile(r"\b(?:is|was|became|went)\s+(?P<v>vegetarian|vegan|pescatarian)\b", re.I), 0), ("school", re.compile(rf"\b(?:studies|studied|graduated|graduating)\s+(?:at|from)\s+(?P<v>{_EW})", re.I), 0),
)
_CN = (
    ("residence", re.compile(rf"住在?(?:于)?{_CV}"), 0), ("residence", re.compile(rf"(?:搬到了?|搬进|迁到了?|落户){_CV}"), 0),
    ("job", re.compile(rf"(?:担任|职位(?:是|为)|升为|升任|改做|现在做|做了){_CV}"), 0), ("employer", re.compile(r"在(?P<v>[\u4e00-\u9fa5A-Za-z0-9]{2,16}?)(?=工作|上班|任职)"), 0),
    ("marital", re.compile(r"(?:和|跟|与)(?P<v>[\u4e00-\u9fa5A-Za-z0-9·]{2,8}?)(?:结了?婚|领了证)"), 0), ("marital", re.compile(r"(?P<v>结婚了?|离婚了?|订婚了?|单身)"), 0),
    ("hobby", re.compile(rf"(?:喜欢上?|热爱|爱好是|爱上了?){_CV}"), 0),
    ("hobby", re.compile(r"(?P<v>[\u4e00-\u9fa5]{2,6}?)(?=停了|不练了|不玩了|放弃了|戒了|退坑了)"), 1), ("hobby", re.compile(rf"(?:改练|改学|开始练|开始玩|入坑){_CV}"), 0),
    ("language", re.compile(r"(?:在学|开始学|学了|自学)(?P<v>英语|法语|西班牙语|日语|韩语|德语|中文|意大利语|俄语|阿拉伯语)"), 0), ("club", re.compile(r"(?:加入了?|参加了?)(?:一?个)?(?P<v>[\u4e00-\u9fa5A-Za-z0-9]{1,10}?(?:俱乐部|社团|协会|球队))"), 0),
    ("pet", re.compile(r"养了?(?:一只|一条|只)?(?P<v>猫|狗|鸟|鱼|兔子|仓鼠|乌龟)"), 0), ("school", re.compile(rf"(?:毕业于|考上了?|考进){_CV}"), 0),
    ("diet", re.compile(r"(?:开始|改成?)(?P<v>吃素|吃斋|素食)"), 0), ("diet", re.compile(r"戒了?(?P<v>糖|酒|烟|辣|肉|咖啡)"), 0),
)
_JOB_DENY = re.compile(r"^(?:member|fan|part|lot|bit|few|couple|person|people|beginner|good|great|big|huge|little|nice|proud|happy)\b", re.I)
_TRIM = re.compile(r"\s+(?:until|till|since|before|after|ago|but|because|when|while|previously|formerly|now|and|last|this|next)\s+.*$", re.I)
_OLD_EN = re.compile(r"\b(?:used to|until|till|previously|formerly|ago|before)\b|\b(?:last|past)\s+(?:month|year|week)\b", re.I)
_OLD_CN = re.compile(r"(?:以前|曾经|之前|原来|过去|停了|戒了|放弃了|不练了|不玩了)")
_CL_EN = re.compile(r",\s*(?=(?:but\s+|and\s+)?(?:now|currently|these days|lately|recently|since|later|today)\b)", re.I)
_CL_CN = re.compile(r"[，,；;]+")
_LEAD = re.compile(r"^([A-Z][\w'-]*(?:\s+[A-Z][\w'-]+){0,2})\s|^([\u4e00-\u9fa5]{2,4})(?:(?:的|说|表示)|$)")

# ── [TD-v2] 缺答诚实卡词表: (属性类, 展示名, query属性词pattern·EN+CN) ──────────
# 同类多词条按序去重(首词条定展示名·"middle name"先于泛"name"); 类名对齐timelines表。
_ATTR_TERMS: tuple[tuple[str, str, Any], ...] = (
    ("name", "middle name", re.compile(r"middle\s+names?\b|中间名", re.I)),
    ("name", "nickname", re.compile(r"nicknames?\b|昵称|外号", re.I)),
    ("name", "first name", re.compile(r"first\s+names?\b", re.I)),
    ("name", "last name", re.compile(r"(?:last|family)\s+names?\b|surnames?\b", re.I)),
    ("name", "full name", re.compile(r"full\s+names?\b|全名", re.I)),
    ("name", "name", re.compile(r"\bnames?\b|叫什么名字", re.I)),
    ("birth date", "birth date", re.compile(r"birth\s*dates?\b|date\s+of\s+birth|\bbirthdays?\b|birth\s+year|\bborn\b|生日|出生日期|出生年", re.I)),
    ("job", "job title", re.compile(r"job\s+titles?|\bjobs?\b|\boccupations?\b|\bprofessions?\b|\bpositions?\b|\brole\b|\bworks?\b|\bworking\b|职位|职业|什么工作", re.I)),
    ("employer", "employer", re.compile(r"\bemployers?\b|which\s+company|在哪家|雇主", re.I)),
    ("residence", "city", re.compile(r"\bcities?\b|\bcity\b|\btowns?\b|which\s+city|\blives?\b|\bliving\b|\bmove[ds]?\b|\bmoving\b|哪个城市|住在哪|城市|搬家|搬到", re.I)),
    ("marital", "spouse", re.compile(r"\bspouses?\b|\bmarried\b|\bmarry\b|\bmarries\b|\bmarriage\b|\bwedding\b|\bengaged?\b|\bdivorc(?:e|ed)\b|\bhusband\b|\bwives?\b|\bpartners?\b|配偶|结婚|伴侣|婚礼", re.I)),
    ("school", "education", re.compile(r"\beducations?\b|\beducational\b|highest\s+education|\bdegrees?\b|\buniversity\b|\bcolleges?\b|\bgraduate|\bstudies\b|\bstudied\b|\bschools?\b|学历|教育|毕业", re.I)),
    ("salary", "salary", re.compile(r"\bsalaries?\b|\bincomes?\b|\bwages?\b|\bearnings?\b|工资|收入|月薪", re.I)),
    ("hobby", "hobby", re.compile(r"\bhobbies?\b|\bhobby\b|\bpastimes?\b|interests?|爱好|兴趣", re.I)),
    ("language", "language", re.compile(r"\blanguages?\b|\bspeaks?\b|fluent|语言", re.I)),
    ("club", "club", re.compile(r"\bclubs?\b|\bsociet|\bassociations?\b|俱乐部|社团|协会", re.I)),
    ("pet", "pet", re.compile(r"\bpets?\b|\bdogs?\b|\bcats?\b|宠物", re.I)),
    ("diet", "diet", re.compile(r"\bdiets?\b|vegetarian|vegan|饮食|吃素", re.I)),
)
# [TD-v2] 窗内证据pattern(按类)·命中=窗内已有该类真实证据→不出诚实卡(防否证)·fail-open
_ATTR_EV: dict[str, Any] = {
    "name": re.compile(r"(?:middle|first|last|family|given|full)\s+names?\b|nicknames?\b|中间名|全名", re.I),
    "birth date": re.compile(r"\bborn\b|\bbirth\b|\bbirthdays?\b|出生|生于", re.I),
    "job": re.compile(r"\b(?:works?|working|worked)\s+(?:as|at|in|for|on)\b|\bemploy(?:ed|ment|s)\b|\bas\s+(?:a|an|the)\s+[a-z]|任职|工作于|担任", re.I),
    "employer": re.compile(r"\b(?:works?|working|worked)\s+(?:at|for|with)\b|\bemploy(?:ed|ment|er)\b|在.{2,12}工作", re.I),
    "residence": re.compile(r"\b(?:lives?|lived|resides?|resided|stays?|stayed)\s+(?:in|at)\b|\bmoved\s+to\b|住在|搬到", re.I),
    "marital": re.compile(r"\b(?:married|engaged|divorced|separated|single)\b|\bspouses?\b|\bhusband\b|\bwives?\b|结婚|离婚|订婚", re.I),
    "school": re.compile(r"\b(?:education|educational|degrees?|diploma|phd|masters?|bachelors?|university|college|graduat\w*|studies|studied|school)\b|学历|毕业", re.I),
    "salary": re.compile(r"\b(?:salaries?|incomes?|wages?|earnings?|savings)\b|工资|收入|存款", re.I),
    "hobby": re.compile(r"\bhobbies?\b|\bhobby\b|likes?\s+to\b|loves?\s+to\b|enjoys?\b|爱好", re.I),
    "language": re.compile(r"\blanguages?\b|\bspeaks?\b|fluent|语言", re.I),
    "club": re.compile(r"\b(?:clubs?|societ\w+|associations?|member\s+of|joined)\b|俱乐部|社团|协会", re.I),
    "pet": re.compile(r"\bpets?\b|\b(?:dog|cat|puppy|kitten|rabbit|hamster|parrot|bird|turtle)s?\b|宠物", re.I),
    "diet": re.compile(r"vegetarian|vegan|pescatarian|\bdiets?\b|吃素|素食", re.I),
}
# [TD-v2] 主体识别禁词(月名/疑问词/介词·防"On Sep"误当人名) + 限定语(at <Corp>)提取
_QW_DENY = {"a", "an", "the", "on", "in", "at", "as", "of", "by", "to", "for", "with",
            "about", "from", "what", "where", "when", "who", "which", "why", "how",
            "did", "does", "do", "is", "was", "are", "were", "tell", "my", "his",
            "her", "their", "its", "and", "or", "jan", "feb", "mar", "apr", "may",
            "jun", "jul", "aug", "sep", "sept", "september", "oct", "nov", "dec",
            "march", "april", "june", "july", "january", "february", "august",
            "october", "november", "december"}
_NAME2 = re.compile(r"\b([A-Z][a-z'’\-]+(?:\s+[A-Z][a-z'’\-]+){1,3})\b")
_GEN_S = re.compile(r"['’]s$")
_CN_OF = re.compile(r"([\u4e00-\u9fa5]{2,4})的")
_QUAL = re.compile(r"\bat\s+([A-Z][\w'’-]*(?:\s+[A-Z][\w'’-]+)?)")

# [刀8·C1] 时间意图判定: WHEN型query豁免属性排窗(C1日期证据可跨属性类分布·
# 属性gate对"什么时候"类问题=类别错误·通用词族·零题面触碰 禁2✓)
_TEMPORAL_INTENT = re.compile(r"\b(?:when|which\s+year|what\s+year|how\s+long|how\s+old|what\s+date|what\s+time)\b|什么时间|什么时候|哪一年|多久", re.I)


def query_attr_classes(query: str) -> frozenset[str]:
    """[刀7·A1] query显式属性类判定(纯函数·通用属性词表·零题面触碰): 命中属性词
    →返回该query所指的属性类集合; 无属性词→空集(时间线综合query零突变)。"""
    q = str(query or "")
    return frozenset(cls for cls, _disp, pat in _ATTR_TERMS if pat.search(q))


_TL_TEXT_RE = re.compile(r"^ATTRIBUTE\s+TIMELINE\s*\[([^\]]+)\]", re.IGNORECASE)


def attr_of_timeline_text(text: str) -> str:
    """[刀7·A1] 时间线卡文本→属性类(纯函数): "ATTRIBUTE TIMELINE [attr] (subj)"→attr。"""
    m = _TL_TEXT_RE.match(str(text or "").strip())
    return m.group(1).strip() if m else ""


def _q_subject(query: str, groups: dict[tuple[str, str], dict[str, Any]] | None = None) -> str:
    """[TD-v2] query主体识别(确定性): ≥2词人名(禁词表过滤"On Sep")>CN"X的">
    timeline单名回认(仅groups内已有主体·供诚实卡主体锚定)。无名→""=不出诚实卡。"""
    q = str(query or "")
    for m in _NAME2.finditer(q):
        toks = m.group(1).split()
        if all(t.lower().strip("'’") not in _QW_DENY for t in toks):
            return _GEN_S.sub("", m.group(1)).strip()
    mc = _CN_OF.search(q)
    if mc:
        return mc.group(1)
    if groups:
        for m in re.finditer(r"\b([A-Za-z][\w'’-]{2,20})\b", q):
            wl = _GEN_S.sub("", m.group(1)).lower()
            if wl in _QW_DENY:
                continue
            for (_s, _a), g in groups.items():
                if str(g.get("subject", "")).lower() == wl:
                    return str(g.get("subject", ""))
    return ""


def _covered(groups: dict[tuple[str, str], dict[str, Any]], subj_tok: set, cls: str) -> bool:
    """[TD-v2] (主体,属性类)timeline覆盖判定: 组主体token与主体token交集≥1即覆盖。"""
    for (_s, attr), g in groups.items():
        if attr != cls:
            continue
        import card_construct as cc
        if cc._tokens(str(g.get("subject", ""))) & subj_tok:
            return True
    return False


def _qualifier(query: str) -> str:
    """[TD-v2] query限定实体提取("job title at Microsoft"→"Microsoft")·无→""。"""
    for m in _QUAL.finditer(str(query or "")):
        w = m.group(1)
        if w.lower() not in _QW_DENY:
            return w
    return ""


def _honest_inject(groups: dict[tuple[str, str], dict[str, Any]], query: str,
                   out: list, existing: set, budget: int) -> int:
    """[TD-v2] 缺答诚实卡(MEMORY STATUS): 属性词命中+主体可识别+(主体,类)零覆盖
    +窗内无该类证据+限定语不在窗内 → "MEMORY STATUS: no record found for <attr>
    of <subject> in stored memory" 插位1·与AS-OF同类互斥·预算≤(2-AS-OF数)·确定性。"""
    if budget <= 0:
        return 0
    subj = _q_subject(query, groups)
    if not subj:
        return 0
    classes: list[tuple[str, str]] = []
    for cls, disp, pat in _ATTR_TERMS:
        if pat.search(query) and all(c != cls for c, _ in classes):
            classes.append((cls, disp))
    if not classes:
        return 0
    import card_construct as cc
    stok = cc._tokens(subj)
    made = 0
    for cls, disp in classes:
        if made >= budget:
            break
        if _covered(groups, stok, cls):
            continue  # 有覆盖→AS-OF卡管辖·绝不出诚实卡(防干扰)
        qual = _qualifier(query)
        if qual and any(re.search(re.escape(qual), str(d.get("content", "")), re.I) for d in out):
            continue  # 限定实体已在窗→归属不确定·诚实让位(不造谣)
        ev = _ATTR_EV.get(cls)
        if not qual and ev and any(ev.search(str(d.get("content", ""))) for d in out):
            continue  # 无限定语且窗内已有该类证据→不出诚实卡(防否证真实证据·T-D守卫)
        # 有限定语且限定语不在窗→类证据(关于他体)不否证本陈述·照出("no record for X at Y"为真)
        text = "MEMORY STATUS: no record found for %s of %s in stored memory" % (
            disp + (" at " + qual if qual else ""), subj)
        if text in existing:
            continue
        out.insert(min(1, len(out)), {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
                                      "content": text, "created_at": ""})
        existing.add(text)
        made += 1
    return made


def _hits(text: str) -> list[tuple[str, str, int]]:
    """单子句属性抽取→[(attr, value, flag)]·每模式首匹配·值trim+job禁代词表。"""
    out: list[tuple[str, str, int]] = []
    for attr, pat, flag in _EN + _CN:
        m = pat.search(text)
        if not m:
            continue
        v = (m.group("v") or "").strip()
        v2 = (m.groupdict().get("v2") or "").strip()
        if v2:
            v = f"{v} to {v2}" if attr == "marital" else v2
        v = _TRIM.sub("", v).strip(" .,;:!?'\"，。的")
        if len(v) < 2 or (attr == "job" and _JOB_DENY.match(v)):
            continue
        out.append((attr, v, flag))
    return out


def _dedupe(segs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同值合并(保最早date/old占优)·按(date,旧值先,出现序)排: "停了X现在Y"同锚日时旧值段在前。"""
    seen: dict[str, dict[str, Any]] = {}
    for i, s in enumerate(segs):
        k = s["value"].lower()
        if k in seen:
            seen[k]["date"] = min(seen[k]["date"], s["date"])
            seen[k]["old"] = max(seen[k]["old"], s["old"])
        else:
            seen[k] = {"value": s["value"], "old": s["old"], "date": s["date"], "ord": i}
    return sorted(seen.values(), key=lambda s: (s["date"], 0 if s["old"] else 1, s["ord"]))


def _pieces(segs: list[dict[str, Any]]) -> list[str]:
    """段→展示件: 首段(有后继)=until <next> = v; 其余=from <date> = v (until <next>)。"""
    ps: list[str] = []
    for i, s in enumerate(segs):
        nxt = segs[i + 1]["date"] if i + 1 < len(segs) else ""
        head = f"until {nxt}" if (i == 0 and nxt) else f"from {s['date']}"
        ps.append(f"{head} = {s['value']}" + (f" (until {nxt})" if nxt and i else ""))
    return ps


def timeline_cards(card: dict[str, Any], rid: str, idx: int, start_j: int) -> list[dict[str, Any]]:
    """单原文卡→时间线卡(确定性·fail-soft)。同(主体,属性类)同卡多版本→复合卡·单版本→
    碎片卡(供跨卡聚合); 卡带frags供store_fragments落表。精度门: 段无日期锚→弃·组空→零卡。"""
    out: list[dict[str, Any]] = []
    try:
        text, dt = str(card.get("text") or ""), str(card.get("dt") or "")
        spk0 = str(card.get("speaker") or "")
        fallback = "" if spk0.lower() in _ROLE else spk0
        anchor = _df._card_iso(dt)
        if not anchor:
            try:
                import card_construct as cc  # 延迟import防循环·[Time:...]前缀同源剥取
                anchor = (cc.time_prefix_dt(text) or "")[:10]
            except Exception:  # noqa: BLE001
                anchor = ""
        groups: dict[tuple[str, str], dict[str, Any]] = {}
        last = ""
        for raw in _df._sentences(text):
            sm = _df._SPEAKER_RE.match(raw)
            spk = ""
            if sm:
                spk, raw, last = sm.group(1).strip(), raw[sm.end():].strip(), spk
            cur = spk or last or fallback
            for clause in ([c.strip() for c in _CL_CN.split(raw) if len(c.strip()) >= 4] if re.search(r"[\u4e00-\u9fa5]", raw) else [c.strip() for c in _CL_EN.split(raw) if len(c.strip()) >= 8]):  # EN按", now"断代(旧/新值分句)·CN按逗号(地名逗号不切)
                deref = _df._deref_subj(clause, cur)
                if not deref:
                    continue
                subj = cur
                if not subj:
                    m = _LEAD.match(deref)
                    subj = next((g for g in m.groups() if g), "") if m else ""
                if not subj:
                    continue  # 归属不明=造谣风险·诚实弃(精度优先)
                old = bool(_OLD_EN.search(deref.lower()) or _OLD_CN.search(deref))
                d = _df._stated_date(clause, anchor)
                for attr, val, flag in _hits(deref):
                    g = groups.setdefault((subj.lower(), attr), {"subject": subj, "attr": attr, "segs": []})
                    g["segs"].append({"value": val, "old": 1 if (flag or old) else 0, "date": d})
        j = start_j
        for key in sorted(groups):
            g = groups[key]
            segs = [s for s in _dedupe(g["segs"]) if s["date"]]  # 精度门: 无日期段弃
            if not segs:
                continue
            frags = [(hashlib.sha1(f"{key[0]}|{g['attr']}|{s['value']}|{s['date']}".encode("utf-8")).hexdigest()[:16], g["subject"], g["attr"], s["value"], s["date"], segs[i2 + 1]["date"] if i2 + 1 < len(segs) else "", dt) for i2, s in enumerate(segs)]
            out.append({"conv": "", "sess": card.get("sess", ""), "dia": f"{rid}:{idx}:t{j}", "speaker": g["subject"], "text": "ATTRIBUTE TIMELINE [%s] (%s): %s" % (g["attr"], g["subject"], "; ".join(_pieces(segs))), "dt": dt, "kind": "timeline", "frags": frags})
            j += 1
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡
        return out
    return out


def store_fragments(tenant: str, cards: list[dict[str, Any]]) -> int:
    """ADD相落表: timeline卡frags→timelines表(幂等OR REPLACE·写时失效·fail-soft)。"""
    if not tenant or not cards:
        return 0
    rows = [(tenant,) + tuple(f) for c in cards for f in (c.get("frags") or [])]
    try:
        import card_construct as cc  # 延迟import防循环(复用_db连接/_DB_LOCK)
        with cc._DB_LOCK:
            conn = cc._db()
            list(map(conn.execute, _DDL))
            conn.executemany("INSERT OR REPLACE INTO timelines(tenant,cid,subject,attr,value,valid_from,valid_until,card_dt) VALUES(?,?,?,?,?,?,?,?)", rows)
            conn.commit()
        _TL_CACHE.pop(tenant, None)
        return len(rows)
    except Exception as exc:  # noqa: BLE001 — 落表失败只丢碎片·绝不影响add
        log.error("deep_timeline store FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0


def timeline_map(tenant: str) -> dict[tuple[str, str], dict[str, Any]]:
    """读侧聚合: 全部碎片→{(主体lower,attr): {subject, segs[]}}·跨卡多版本按(valid_from,seq)排·同值合并·缓存+写时失效。"""
    cached = _TL_CACHE.get(tenant)
    if cached is not None:
        return cached
    rows: list[tuple] = []
    try:
        import card_construct as cc  # 延迟import防循环
        with cc._DB_LOCK:
            conn = cc._db()
            list(map(conn.execute, _DDL))
            rows = conn.execute("SELECT subject,attr,value,valid_from,seq FROM timelines WHERE tenant=? ORDER BY valid_from,seq", (tenant,)).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.error("deep_timeline map FAILED tenant=%s err=%s", tenant, type(exc).__name__)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for subj, attr, val, frm, seq in rows:
        g = groups.setdefault((str(subj).lower(), str(attr)), {"subject": str(subj), "attr": str(attr), "segs": []})
        if any(s["value"].lower() == str(val).lower() for s in g["segs"]):
            continue
        g["segs"].append({"value": str(val), "date": str(frm), "seq": int(seq)})
    for g in groups.values():
        g["segs"].sort(key=lambda s: (s["date"], s["seq"]))
    _TL_CACHE[tenant] = groups
    return groups


_MO = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}  # as-of解析: ISO>中文年月[日]>英文月日/日月年>英文月年·优先as-of/截至前导词
_MARK = re.compile(r"(?:as\s+of|as-of|截至|截止|在|到了?|时)\s*$", re.I)
_QPATS = (
    (0, re.compile(r"\b((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})\b"), lambda g: _fmt(g[0], g[1], g[2])), (1, re.compile(r"((?:19|20)\d{2})年(\d{1,2})月(?:(\d{1,2})[日号]?)?"), lambda g: _fmt(g[0], g[1], g[2] or 1)),
    (2, re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d{2})\b", re.I), lambda g: _fmt(g[2], _MO.get(g[0][:3].lower(), 0), g[1])), (2, re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+((?:19|20)\d{2})\b", re.I), lambda g: _fmt(g[2], _MO.get(g[1][:3].lower(), 0), g[0])),
    (3, re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+((?:19|20)\d{2})\b", re.I), lambda g: _fmt(g[1], _MO.get(g[0][:3].lower(), 0), 1)),
)


def _fmt(y: Any, m: Any, d: Any) -> str:
    try:
        return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return ""


def asof_date(query: str) -> str:
    """query→as-of日期(YYYY-MM-DD)·无→""。确定性: 前导词>文法优先级>出现序。"""
    q, cands = str(query or ""), []
    for pri, pat, fx in _QPATS:
        for m in pat.finditer(q):
            d = fx(m.groups())
            if d:
                mk = 1 if _MARK.search(q[max(0, m.start() - 12):m.start()]) else 0
                cands.append((mk, pri, m.start(), d))
    cands.sort(key=lambda c: (-c[0], c[1], c[2]))
    return cands[0][3] if cands else ""


def _pick(segs: list[dict[str, Any]], qd: str) -> str:
    """时点取值: date≤qd的最后一段; 全部>qd→最早段(早期状态向过去延伸)。"""
    val = segs[0]["value"]
    for s in segs:
        if s["date"] <= qd:
            val = s["value"]
    return val


def inject(tenant: str, query: str, data: list[dict[str, Any]], top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相钩子(rank0不动·判定卡插位1·总注入≤2·确定性·fail-soft=原窗直通)·
    query人名token与主体token交集≥1才出席; 有as-of→判定卡(按日期选值已算好)·无→完整时间线卡。
    [TD-v2] 另: 属性词+主体可识别+(主体,类)零覆盖+窗内无该类证据→MEMORY STATUS诚实卡
    插位1(与AS-OF同类互斥·合计≤2)·零覆盖也让评审能诚实弃答(gold="记忆中无此信息"类)。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        groups = timeline_map(tenant)
        import card_construct as cc  # 延迟import防循环(复用_tokens)
        query = str(query or "")
        qt = cc._tokens(query)
        if not qt:
            return data, 0
        qd = asof_date(query)
        scored: list[tuple[int, str, str, dict[str, Any]]] = []
        for (slow, attr), g in groups.items():
            inter = qt & cc._tokens(g["subject"])
            if inter:
                scored.append((-(len(inter) + 2 * len(qt & cc._tokens(attr))), slow, attr, g))
        if not scored and not groups:
            pass  # [TD-v2] 空timeline租户也要走诚实卡判定(零覆盖=真无记录)
        # 刀7·A1(20261015·60.34战报§三): 时间线属性gate — query显式属性词(通用属性
        # 词表_ATTR_TERMS)命中时只出席该属性类时间线卡(按query意图分派窗构成: 单点
        # 事实query→事实卡优先留窗·错属性卡不进窗·rank位让还事实证据); 无属性词query
        # 零突变(时间线综合query行为不变)。通用词类信号·零题面触碰(禁2✓)。
        _qcls = query_attr_classes(query)
        if _qcls and not _TEMPORAL_INTENT.search(query):
            scored = [s for s in scored if str(s[3].get("attr", "")) in _qcls]
        scored.sort(key=lambda x: (x[0], x[1], x[2]))
        out, existing, n = list(data), {str(d.get("content", "")) for d in data}, 0
        for _, _, attr, g in scored[:2]:
            segs = g["segs"]
            tail = "; ".join(_pieces(segs))
            text = (f"AS-OF ANSWER: {g['subject']} {attr} = {_pick(segs, qd)} (valid at {qd}; timeline: {tail})" if qd else f"ATTRIBUTE TIMELINE [{attr}] ({g['subject']}): {tail}")
            if text in existing:
                continue
            out.insert(min(1, len(out)), {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16], "content": text, "created_at": segs[-1]["date"] or ""})
            n += 1
        n += _honest_inject(groups, query, out, existing, 2 - n)  # [TD-v2] 缺答诚实卡(预算=2-AS-OF)
        if n == 0:
            return data, 0
        return out, n
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_timeline inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0
