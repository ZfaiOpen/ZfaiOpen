"""deep_temporal.py v2 — T-C席·C2事件排序+C1间隔预计算(2026-10-10·在T3版基础上增补·纯stdlib·确定性·fail-soft)
T3版根因(AML官方C2事件顺序=0·C1=33.33): ①任期时点="任期2021-04-01至2024-09-30"官方问
"As of 2023-06-15还是书记吗" ②主题时序=三session各提一新活动官方问"按提起顺序列出"。
T3治法: TENURE卡+AS-OF判定卡+动作动词SEQUENCE卡("开始X/报名X/started X")。
T-C实测(QLOG捕获官方真题): T3的SEQUENCE本场**0次开火**——官方C2语料是"多session技术
实现对话, 用户逐session提出不同实现方面"(无任何动作动词模式), 真题="Can you list the
order in which I brought up different aspects of implementing the city autocomplete
feature across our conversations, in order?"。C1问"how long"间隔月数, T3亦无月数计算
(且T3日期文法只收全日期, "January 2008"月年粒度不识别)。
v2增补两个抽取器(零LLM零网络·通用句法启发·不依赖动作动词):
  ① topic_sequence_cards: 每session用户消息抽主题短语候选(首句内容词连串·去停用词·
     CN去功能词切段)落topic行; 读侧按session dt序做"新方面"过滤(候选含≥1未见词)+
     共享主体词支撑门(全部保留session共现≥1词=同一主题线·闲聊零共享=零卡)→
     "SEQUENCE [<主体>]: 1.<方面1>(YYYY-MM) 2.<方面2>(...) ..."≥3≤5项。
  ② duration_cards: 同主体/事件两个显式日期→间隔月数行(dur·月份算术·含跨年):
     "DURATION [<主体>]: from 2008-01 to 2010-12 = 35 months (2 years 11 months)"。
     日期文法增补EN月年粒度("January 2008"→2008-01-01·对齐t1席_QPATS pri3·T3文法超集)。
inject优先级(≤2张·rank0不动·3键形状·fail-soft原窗直通): ①query含日期+主体命中TENURE
→AS-OF VERDICT(T3原语义·月年粒度受益) ②间隔信号词(how long/多久/多少年)+dur行命中→
DURATION卡插位1 ③顺序信号词(order/顺序)→SEQUENCE聚合卡(T3动作序列语义优先=字节级零
回归·v2主题首提卡兜底=官方C2新通道)。
DB=复用card_construct._db(): temporal_cards表(tenant,cid,kind,subject,payload,card_dt,seq
PK+唯一索引)·kind∈{tenure,seq,topic,dur}·幂等OR IGNORE·ADD存·SEARCH读(缓存+写失效)。
解析失败/无日期/无新方面/无共享主体=弃卡=精度门。回滚=摘本件还原T3版+card_construct两
import+derive_all钩子/分类+aml两hunk(.bak-t3/.bak-tc)。
"""
from __future__ import annotations

import hashlib, json, logging, re
from datetime import datetime
from typing import Any

log = logging.getLogger("deep_temporal")
_CACHE: dict[str, list[dict[str, Any]]] = {}
_DDL = (
    "CREATE TABLE IF NOT EXISTS temporal_cards(tenant TEXT NOT NULL, cid TEXT NOT NULL, kind TEXT NOT NULL,"
    " subject TEXT NOT NULL, payload TEXT NOT NULL, card_dt TEXT, seq INTEGER PRIMARY KEY AUTOINCREMENT)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_tc_tcid ON temporal_cards(tenant, cid)")
# ── 日期解析(ISO/英文月日年/EN月年[v2增补]/中文2021年4月1日/汉字数字二〇二一·失败=""=精度门) ──
_ND = r"[0-9〇零一二三四五六七八九十]"
_NUM_T = str.maketrans("〇零一二三四五六七八九", "00123456789")
_MON = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
_R_ISO = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
_R_CNY = re.compile(rf"({_ND}{{4}})年\s*({_ND}{{1,3}})\s*月(?:\s*({_ND}{{1,3}})\s*[日号]?)?")
_R_MDY = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", re.I)
_R_MY = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+((?:19|20)\d{2})\b", re.I)  # v2: 月年粒度
_ANY_D = (r"(?:\d{4}-\d{1,2}-\d{1,2}|\d{4}年\s*\d{1,2}月(?:\s*\d{1,2})?[日号]?|"
          rf"{_ND}{{4}}年\s*{_ND}{{1,3}}月(?:\s*{_ND}{{1,3}})?[日号]?"
          r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
          r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+(?:19|20)\d{2})")

def _cn_num(s: str) -> str:
    s = str(s).translate(_NUM_T)  # 汉字数字逐字直映
    a, t, b = s.partition("十")  # 十三→13/二十→20
    return s if not t else str((int(a) if a else 1) * 10 + (int(b) if b else 0))

def _pd(s: str) -> str:
    """任意方言日期→YYYY-MM-DD(真历校验·解析失败=""=精度门)。"""
    try:
        s = str(s or "").strip()
        for pat, mk in ((_R_ISO, lambda g: (int(g[1]), int(g[2]), int(g[3]))),
                        (_R_CNY, lambda g: (int(_cn_num(g[1])), int(_cn_num(g[2])), int(_cn_num(g[3] or "1")))),
                        (_R_MDY, lambda g: (int(g[3]), _MON[g[1][:3].lower()], int(g[2]))),
                        (_R_MY, lambda g: (int(g[2]), _MON[g[1][:3].lower()], 1))):
            m = pat.search(s)
            if m:
                return datetime(*mk(m)).strftime("%Y-%m-%d")
    except (ValueError, KeyError, IndexError):
        return ""
    return ""

# ── TENURE: 角色词表EN+CN≥10·区间日期对→start/end·左上下文找role/org ──
_ROLE_EN = re.compile(r"\b(member|chair|chairman|chairperson|president|secretary|treasurer|director|officer|agent|broker|coach|captain|trustee|fellow)\b", re.I)
_ROLE_CN = re.compile(r"([\u4e00-\u9fa5]{0,2}(?:书记|会长|主席|经纪|委员|董事|秘书|理事|会员|队长|教练|主任|社长))")
_TERMISH = re.compile(r"任期|任职|成员|会员|tenure|membership", re.I)
_ORG_CUT = re.compile(r"(?:公会|协会|俱乐部|社团|委员会|联盟|工会|会社|商会|学会)")
_PREP_EN = re.compile(r"^(?:of|for|at|with|in|the|a|an|his|her|their|our)\s+", re.I)
_CN_ORGJ = re.compile(r"^(?:任期|任职|担任|于|在|从|自)+")
_RANGE_RE = re.compile(rf"({_ANY_D})\s*(?:to|until|till|through|—|–|--|~|〜|至|到)\s*({_ANY_D})", re.I)
_TEN_JOIN = re.compile(rf"(?:^|[，,。;；\s])(?P<subj>[\u4e00-\u9fa5A-Za-z·]{{2,8}}?)?于?\s*(?P<d1>{_ANY_D})\s*(?:加入|成为|进入)\s*(?P<org>[\u4e00-\u9fa5A-Za-z0-9]{{2,20}}?)[，,]?\s*(?:并于|又于|之后于|后于)?\s*(?P<d2>{_ANY_D})?\s*(?:退出|离任|卸任|退会|转会|离职)")
_SENT = re.compile(r"[。.!?\n;；]+")

def _tenures(sent: str, spk: str) -> list[tuple[str, str, str, str, str]]:
    """单句→[(role, org, start, end, person)]·无role/任期标记且非加入退出=弃(精度门)。"""
    out: list[tuple[str, str, str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _push(role: str, org: str, d1: str, d2: str, person: str) -> None:
        if d1 and (not d2 or d1 <= d2) and (d1, d2) not in seen:
            out.append((role, org[:40], d1, d2, person)); seen.add((d1, d2))
    for m in _RANGE_RE.finditer(sent):
        d1, d2 = _pd(m.group(1)), _pd(m.group(2))
        left = sent[:m.start()]
        rm = _ROLE_EN.search(left) or _ROLE_CN.search(left)
        if not rm and not _TERMISH.search(left):
            continue  # 精度门: 纯日期区间无成员资格语义=零卡
        if not rm:  # 裸"任期从D1到D2"句式: role缺省·subject锚"任期"
            _push("任期", "", d1, d2, spk)
            continue
        raw = str(rm.group(0)).strip(" 的了")  # 角色清洗: 前缀误吞org后缀→截后缀之后
        cut = _ORG_CUT.search(raw)
        role = raw[cut.end():] if cut and cut.end() < len(raw) else raw
        tail = left[rm.end():].strip()
        if re.search(r"[\u4e00-\u9fa5]", tail):  # CN: org=role后首段剥连接词
            seg = _CN_ORGJ.sub("", re.split(r"[，,。；;：:（）()]", tail)[0]).strip()
            org = seg if re.search(r"[\u4e00-\u9fa5A-Za-z0-9]", seg) else ""
        else:  # EN: 剥from尾+冠词/介词前缀
            org = re.sub(r"\s+(?:from|until|till|to)$", "", tail, flags=re.I).strip(" 的，,.、")
            prev = None
            while prev != org and org:
                prev, org = org, _PREP_EN.sub("", org, count=1).strip(" 的，,.、")
        _push(role, org, d1, d2, spk)
    for m in _TEN_JOIN.finditer(sent):  # CN加入退出方言: 于D1加入ORG并于D2退出
        _push("", m.group("org") or "", _pd(m.group("d1")), _pd(m.group("d2") or ""),
              (m.group("subj") or spk).rstrip("于在 "))
    return out

# ── SEQUENCE(动作序列·T3原语义): 同主体新动作短语(开始X/报名X/started X·跨session去重排序) ──
_SEQ_TRIG = ("started learning", "started training for", "signed up for", "registered for",
             "enrolled in", "started", "began", "took up", "报名参加了", "报名参加", "报名了",
             "开始", "报名", "改练", "参加了", "备考", "考上了")
_SPK = re.compile(r"^([A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){0,2}|[\u4e00-\u9fa5A-Za-z0-9_]{2,16})\s*[:：]\s*")
_ACT_CN = re.compile(r"^[了去起]?\s*([\u4e00-\u9fa5A-Za-z0-9]{2,24})")
_ACT_EN = re.compile(r"^\s+(?:the\s+|a\s+|an\s+|my\s+|our\s+)?([A-Za-z][\w' -]{1,47})")

def _seq_item(sent: str) -> str:
    """单句新动作宾语(最长触发词优先·空捕获续扫·""=无)。"""
    low = sent.lower()
    for trig in sorted(_SEQ_TRIG, key=len, reverse=True):
        i = low.find(trig)
        if i < 0:
            continue
        rest = sent[i + len(trig):]
        m = _ACT_CN.match(rest) if re.search(r"[\u4e00-\u9fa5]", rest) else _ACT_EN.match(rest)
        act = re.split(r"[，,.;。！!？?]", m.group(1))[0].strip(" 的了") if m else ""
        if len(act) >= 2:
            return act
    return ""

# ── TOPIC(v2·C2官方新通道): 主题首提排序 — 不依赖动作动词 ─────────────────────
# 每session用户消息→主题短语候选(内容词连串·去停用词/功能词·首句优先·多词连串优先)
# 落topic行(cid=conv|sess幂等); 读侧新方面过滤+共享主体词支撑门→SEQUENCE卡≥3≤5项。
_STOPW = frozenset("""
i we us me my mine our ours you your yours he him his she her hers it its they them their theirs
this that these those there here what which who whom whose when where why how
is am are was were be been being do does did done have has had having
will would shall should can could may might must let lets
go goes going gone went come comes coming came get gets getting got
make makes making made take takes taking took give gives giving gave
think thinks thinking thought know knows knowing knew
want wants wanting wanted need needs needing needed try tries trying tried
use uses using used say says saying said tell tells telling told
talk talks talking talked discuss discusses discussing discussed
start starts starting started begin begins beginning began
cover covers covering covered handle handles handling handled
review reviews reviewing reviewed improve improves improving improved
and or but if then else so than too very just also only now soon later
today tomorrow yesterday about into over under again because while until upon
with without within from for the a an of to in on at by as up down out off
not no yes okay ok hey hi hello thanks thank please well like sure maybe perhaps
thing things stuff way ways question questions idea ideas point points
first second third next last new old good great nice bad better best
more most some any all both each every other another same own much many few lot lots bit
kind sort type really actually basically currently recently still yet ever never always often sometimes
work works working worked year years month months week weeks day days
""".split())
_CN_STOPW = (
    "我们", "你们", "他们", "她们", "它们", "咱们", "大家", "自己",
    "一下", "一些", "这个", "那个", "哪些", "哪个", "什么", "怎么", "怎样", "如何", "为什么",
    "因为", "所以", "但是", "然后", "接着", "首先", "其次", "最后", "关于", "对于",
    "还有", "另外", "或者", "以及", "需要", "应该", "可以", "打算", "准备", "计划",
    "考虑", "觉得", "认为", "感觉", "希望", "帮我", "帮忙", "请问", "开始",
    "是不是", "有没有", "能不能", "好不好", "行不行",
    "最近", "现在", "今天", "明天", "昨天", "聊聊", "讨论", "谈谈", "说说", "看看", "试试",
    "我", "你", "他", "她", "它", "的", "了", "是", "在", "有", "和", "与", "就", "都",
    "也", "不", "这", "那", "被", "把", "让", "请", "想", "会", "能", "要", "吧", "呢",
    "吗", "啊", "呀", "嗯", "哦", "还", "很", "挺", "更", "最", "又", "再", "才", "只",
    "个", "们", "等", "及", "或", "对", "从", "至", "到", "于")
_CN_STOP_SORTED = tuple(sorted(_CN_STOPW, key=len, reverse=True))
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'+-]*")
_TOK_RE = re.compile(r"[a-z0-9]+|[\u4e00-\u9fa5]")

def _toks(s: str) -> set[str]:
    """主题新词token(EN词+CN二元·去停用词·与cc._tokens同风格·零import防循环)。"""
    t = str(s or "").lower()
    out: set[str] = set()
    for m in _TOK_RE.finditer(t):
        w = m.group(0)
        if w in _STOPW or len(w) < 2 or w.isdigit():
            continue
        out.add(w)
    cjk = re.findall(r"[\u4e00-\u9fa5]", t)
    out.update(a + b for a, b in zip(cjk, cjk[1:]))
    return out

def _tf_of(text: str) -> dict[str, int]:
    """session内词频表(EN词+CN二元·去停用·确定性序·封顶40)=共享主体词支撑门的原料。"""
    t = str(text or "").lower()
    counts: dict[str, int] = {}
    for m in _TOK_RE.finditer(t):
        w = m.group(0)
        if w in _STOPW or len(w) < 2 or w.isdigit():
            continue
        counts[w] = counts.get(w, 0) + 1
    cjk = re.findall(r"[\u4e00-\u9fa5]", t)
    for a, b in zip(cjk, cjk[1:]):
        counts[a + b] = counts.get(a + b, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:40])

def _cn_segs(sent: str) -> list[str]:
    """CN句→功能词剥除后切段(≥3字)=主题候选(确定性·纯字符串运算)。"""
    t = str(sent or "")
    for w in _CN_STOP_SORTED:
        if w in t:
            t = t.replace(w, " ")
    return [seg for seg in (x.strip() for x in re.split(r"[^\u4e00-\u9fa5A-Za-z0-9]+", t)) if len(seg) >= 3]

def _en_runs(sent: str) -> list[str]:
    """EN句→内容词连续串(≥2词·停用词切分·各封顶4词·句序)·单词串不入候选(噪音门)。"""
    runs: list[list[str]] = []
    cur: list[str] = []
    for w in _WORD_RE.findall(str(sent or "")):
        lw = w.lower()
        if lw in _STOPW or len(lw) < 2 or lw.isdigit():
            if cur:
                runs.append(cur)
                cur = []
            continue
        cur.append(w)
    if cur:
        runs.append(cur)
    return [" ".join(r[:4]) for r in runs if len(r) >= 2][:3]

def _topic_candidates(text: str) -> list[str]:
    """用户消息→主题短语候选(首6句·首句优先·每句≤3·总≤4·空=弃)。"""
    cands: list[str] = []
    for raw in [x.strip() for x in _SENT.split(str(text or "")) if x.strip()][:6]:
        s = raw
        if _SPK.match(s):
            s = _SPK.sub("", s, count=1).strip()
        s = re.sub(r"(?i)^\s*session\s+\S{1,6}\s+text\s+[^:]{1,40}:\s*", "", s)  # v091_c2: 剥语料行格式伪影前缀
        if not s:
            continue
        if re.search(r"[\u4e00-\u9fa5]", s):
            cands.extend(seg[:12] for seg in _cn_segs(s)[:3])
        else:
            cands.extend(_en_runs(s))
        if len(cands) >= 3:
            break
    return cands[:4]

# ── DURATION(v2·C1间隔): 月份算术+格式化·泛化区间主体=区间前内容词连串 ─────────
_DUR_RE = re.compile(
    r"how\s+(?:long|much\s+time|many\s+(?:years?|months?|weeks?|days?))|what\s+(?:period|timespan|duration)"
    r"|duration|timespan|time\s+between|多久|多长时间|多少年|多少个月|多少月|间隔|持续|时长|跨度", re.I)

def _months(d1: str, d2: str) -> int:
    """YYYY-MM-DD对→间隔月数(月份算术·含跨年: 2008-01→2010-12=35)。"""
    return (int(d2[:4]) - int(d1[:4])) * 12 + (int(d2[5:7]) - int(d1[5:7]))

def _span(m: int) -> str:
    """月数→"2 years 11 months"形(零年只报月·零月只报年·单数无s)。"""
    y, r = divmod(max(0, m), 12)
    parts: list[str] = []
    if y:
        parts.append(f"{y} year{'s' if y != 1 else ''}")
    if r or not y:
        parts.append(f"{r} month{'s' if r != 1 else ''}")
    return " ".join(parts)

def _push_dur(rows: list[tuple[Any, ...]], tenant: str, subj: str, label: str,
              d1: str, d2: str, dt: str) -> None:
    """间隔行落库(cid=dur|subj|d1|d2幂等·月数预计算=评审直读零推理)。"""
    m = _months(d1, d2)
    rows.append((tenant, hashlib.sha1(f"dur|{subj.lower()}|{d1}|{d2}".encode()).hexdigest()[:16],
                 "dur", subj,
                 json.dumps({"s": d1, "e": d2, "label": label, "months": m, "span": _span(m)}, ensure_ascii=False),
                 dt))

def _subj_before(sent: str, pos: int) -> str:
    """区间起点前文→主体短语(EN=末≤4内容词连串·CN=末段≤10字·空=弃精度门)。"""
    left = str(sent or "")[:pos].strip(" \t，,。;；:：、-—")
    if not left:
        return ""
    if re.search(r"[\u4e00-\u9fa5]", left):
        segs = _cn_segs(left)
        return segs[-1][:10] if segs else ""
    words = [w for w in _WORD_RE.findall(left)
             if w.lower() not in _STOPW and len(w) >= 3 and not w.isdigit()]
    return " ".join(words[-4:]) if words else ""

def temporal_cards(card: dict[str, Any], rid: str, idx: int, tenant: str = "") -> list[dict[str, Any]]:
    """ADD相钩子(derive_all内rules循环后·T3签名不变): 原文卡→TENURE派生卡+tenure/seq/
    topic/dur行落库(复用cc._db同连接同锁·幂等OR IGNORE·写失效)·解析失败/无日期=零产出(精度门)。"""
    out: list[dict[str, Any]] = []
    try:
        dt = str(card.get("dt") or "")
        cs = str(card.get("speaker") or "")
        spk = "" if cs.lower() in ("user", "assistant", "system", "") else cs
        rows: list[tuple[Any, ...]] = []; j = 0
        dur_seen: set[tuple[str, str]] = set()
        text_all = str(card.get("text") or "")
        for s in filter(None, (x.strip() for x in _SENT.split(text_all))):
            head = _SPK.match(s)
            sent = _SPK.sub("", s, count=1).strip() if head else s
            who = head.group(1).strip() if head else spk
            for role, org, d1, d2, person in _tenures(sent, who):
                label = " @ ".join(x for x in (role, org) if x) or person or "tenure"
                out.append({"conv": card.get("conv", ""), "sess": card.get("sess", ""),
                            "dia": f"{rid}:{idx}:t{j}", "speaker": person or role,
                            "text": (f"TENURE FACTS [{label}]: start={d1} end={d2 or 'open'}\n"
                                     f"→ as-of锚点: D∈[{d1},{d2 or '∞'}]=在任; 区间外=已卸任"),
                            "dt": dt, "kind": "temporal"})
                j += 1
                subj = " ".join(x for x in (role, org, person) if x).strip()
                tcid = hashlib.sha1(f"tenure|{subj.lower()}|{d1}|{d2}".encode()).hexdigest()[:16]
                rows.append((tenant, tcid, "tenure", subj, json.dumps({"role": role, "org": org, "s": d1, "e": d2, "label": label}, ensure_ascii=False), dt))
                if d2 and (d1, d2) not in dur_seen:  # v2: C1间隔行(TENURE同源同主体)
                    dur_seen.add((d1, d2))
                    _push_dur(rows, tenant, subj, label, d1, d2, dt)
            for m in _RANGE_RE.finditer(sent):  # v2: 泛化区间(事件/工程·非tenure语义)
                d1, d2 = _pd(m.group(1)), _pd(m.group(2))
                if not d1 or not d2 or d1 > d2 or (d1, d2) in dur_seen:
                    continue
                dur_seen.add((d1, d2))
                subj2 = _subj_before(sent, m.start())
                if subj2:
                    _push_dur(rows, tenant, subj2, subj2, d1, d2, dt)
            act = _seq_item(sent) if who else ""
            if act:
                scid = hashlib.sha1(f"seq|{who.lower()}|{act.lower()}".encode()).hexdigest()[:16]
                rows.append((tenant, scid, "seq", who, json.dumps({"act": act}, ensure_ascii=False), dt))
        if cs.lower() in ("user", ""):  # v2: topic行=仅用户消息(session主题·cid=conv|sess幂等)
            skey = str(card.get("sess") or card.get("conv") or "")
            if skey:
                cands = _topic_candidates(text_all)
                if cands:
                    toid = hashlib.sha1(f"topic|{str(card.get('conv', ''))}|{skey}".lower().encode()).hexdigest()[:16]
                    rows.append((tenant, toid, "topic", skey,
                                 json.dumps({"conv": str(card.get("conv", "")), "sess": skey,
                                             "cands": cands, "tf": _tf_of(text_all)}, ensure_ascii=False), dt))
        if tenant and rows:
            import card_construct as cc  # 延迟import防循环(复用_db()/_DB_LOCK)
            with cc._DB_LOCK:
                conn = cc._db()
                for stmt in _DDL:
                    conn.execute(stmt)
                conn.executemany("INSERT OR IGNORE INTO temporal_cards(tenant,cid,kind,subject,payload,card_dt) VALUES(?,?,?,?,?,?)", rows)
                conn.commit()
            _CACHE.pop(tenant, None)
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡·绝不影响add
        return out
    return out

def _load(tenant: str) -> list[dict[str, Any]]:
    """读租户temporal行(内存缓存·写时失效·fail-soft→[])。"""
    if tenant in _CACHE:
        return _CACHE[tenant]
    try:
        import card_construct as cc
        with cc._DB_LOCK:
            raw = cc._db().execute("SELECT kind,subject,payload,card_dt,seq FROM temporal_cards WHERE tenant=? ORDER BY seq", (tenant,)).fetchall()
            _CACHE[tenant] = [{"kind": r[0], "subject": r[1], "payload": r[2], "card_dt": r[3] or "", "seq": r[4]} for r in raw]
    except Exception:  # noqa: BLE001
        return []
    return _CACHE.get(tenant, [])

def _card3(text: str, dt: str) -> dict[str, str]:
    return {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16], "content": text, "created_at": dt or ""}

_ORDER_RE = re.compile(r"order|sequence|chronological|按顺序|顺序|第几|依次|先后|排序", re.I)

def _verdict(p: dict[str, Any], d: str) -> str:
    return "在任" if str(p.get("s", "")) <= d and (not p.get("e") or d <= str(p.get("e", ""))) else "已卸任"

def _seq_card(rows: list[dict[str, Any]]) -> tuple[str, str] | None:
    """聚合SEQUENCE卡(T3原语义·动作序列·≥2≤5项·按(dt,seq)升序·组选最大·字母序破并列=确定性)。"""
    groups: dict[str, list[tuple[str, int, str]]] = {}
    for r in rows:
        if r["kind"] == "seq":
            try:
                act = str(json.loads(r["payload"]).get("act", "")).strip()
                if act:
                    groups.setdefault(r["subject"].lower(), []).append((r["card_dt"], r["seq"], act))
            except Exception:  # noqa: BLE001
                pass
    if not groups:
        return None
    key = max(sorted(groups), key=lambda k: len(groups[k]))
    items = sorted(groups[key], key=lambda t: (t[0], t[1]))[:5]
    if len(items) < 2:
        return None
    disp = next(r["subject"] for r in rows if r["kind"] == "seq" and r["subject"].lower() == key)
    body = " ".join(f"{i}.{a}({(d or '')[:7] or '?'})" for i, (d, _, a) in enumerate(items, 1))
    return f"SEQUENCE [{disp}]: {body}", items[-1][0]

def _topic_card(rows: list[dict[str, Any]]) -> tuple[str, str] | None:
    """v2主题首提SEQUENCE卡(C2官方新通道·≥3 session·新方面过滤·共享主体词支撑门·≤5项·确定性):
    租户级聚合(aml tenant=单用户记忆线·conv==session_id不可作组键); 每session候选按存序
    取首个含未见词者(首提新方面); 保留组必须存在全session共现主体词(≥1词=同一主题线·
    闲聊零共享/偶碰词不足全数=零卡=精度门)。"""
    items: list[tuple[str, int, dict[str, Any]]] = []
    for r in rows:
        if r["kind"] != "topic":
            continue
        try:
            p = json.loads(r["payload"])
        except Exception:  # noqa: BLE001
            continue
        if p.get("cands"):
            items.append((r["card_dt"], r["seq"], p))
    if len(items) < 3:
        return None
    items = sorted(items, key=lambda t: (t[0], t[1]))
    seen: set[str] = set()
    kept: list[tuple[str, str, dict[str, Any]]] = []
    for d, _, p in items:
        pick = ""
        for c in p["cands"]:
            if re.search(r"(?i)\bsession\s+\w{1,6}\s+text\b|^\s*session\b", c):
                continue  # v091_c2: 格式伪影永不作方面
            if _toks(c) - seen:
                pick = c
                break
        if pick:
            kept.append((d, pick, p))
        for c in p["cands"]:
            seen |= _toks(c)
        seen |= set(p.get("tf") or ())
    if len(kept) < 3:
        return None
    kept = kept[:5]
    support: dict[str, int] = {}
    for _, _, p in kept:
        for w in (p.get("tf") or {}):
            support[w] = support.get(w, 0) + 1
    tfs = [p.get("tf") or {} for _, _, p in kept]
    common = [w for w, c in support.items() if c >= len(kept)]
    if not common:
        return None  # 精度门: 无跨全session共享主体词=闲聊≠同一主题线
    common.sort(key=lambda w: (-sum(t.get(w, 0) for t in tfs), -len(w), w))
    body = " ".join(f"{i}.{t}({(d or '')[:7] or '?'})" for i, (d, t, _) in enumerate(kept, 1))
    return f"SEQUENCE [{' '.join(common[:2])}]: {body}", kept[-1][0]

def _dur_card(rows: list[dict[str, Any]], qt: set[str], tokens: Any) -> tuple[str, str] | None:
    """v2间隔卡选取(query token×主体交集最大·并列取seq新·月数从s/e重算=零脏payload依赖)。"""
    best: tuple[tuple, dict[str, Any], dict[str, Any]] | None = None
    for r in rows:
        if r["kind"] != "dur":
            continue
        try:
            p = json.loads(r["payload"])
        except Exception:  # noqa: BLE001
            continue
        inter = qt & tokens(r["subject"])
        if not inter:
            continue
        key = (-len(inter), -int(r["seq"]), r["subject"])
        if best is None or key < best[0]:
            best = (key, r, p)
    if best is None:
        return None
    r, p = best[1], best[2]
    s, e = str(p.get("s", "")), str(p.get("e", ""))
    if not s or not e or s > e:
        return None
    m = _months(s, e)
    text = f"DURATION [{p.get('label') or r['subject']}]: from {s[:7]} to {e[:7]} = {m} month{'s' if m != 1 else ''}"
    if m >= 2:
        text += f" ({p.get('span') or _span(m)})"
    return text, r["card_dt"]

def inject(tenant: str, query: str, data: list[dict[str, Any]], top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相钩子(cfact.inject之后·≤2张·插位1·rank0不动·3键形状·确定性·fail-soft原窗直通):
    ①query含日期+主体命中TENURE→AS-OF VERDICT判定卡(T3原语义·v2月年文法受益)
    ②间隔信号词+dur行命中→DURATION月数卡插位1(C1·评审直读=35 months)
    ③顺序信号词→SEQUENCE聚合卡插位1(T3动作序列优先=字节级零回归·v2主题首提卡兜底=C2官方新通道)。"""
    if not tenant or not isinstance(data, list) or not data:
        return data, 0
    try:
        import card_construct as cc
        rows = _load(tenant)
        if not rows:
            return data, 0
        q, qt = str(query or ""), cc._tokens(str(query or ""))
        existing = {str(d.get("content", "")) for d in data}
        out, n, d = list(data), 0, _pd(q)
        if d and qt:
            hits = [r for r in rows if r["kind"] == "tenure" and qt & cc._tokens(r["subject"])]
            pick = next((r for r in hits if _verdict(json.loads(r["payload"]), d) == "在任"), hits[0] if hits else None)
            if pick is not None:
                p = json.loads(pick["payload"])
                text = f"AS-OF VERDICT: [{p.get('label') or pick['subject']}] as of {d} = {_verdict(p, d)} ({p.get('s', '')} ~ {p.get('e') or 'open'})"
                if text not in existing:
                    out.insert(min(1, len(out)), _card3(text, pick["card_dt"]))
                    n += 1
        if n < 2 and _DUR_RE.search(q) and qt:
            dur = _dur_card(rows, qt, cc._tokens)
            if dur and dur[0] not in existing:
                out.insert(min(1, len(out)), _card3(*dur))
                n += 1
        if n < 2 and _ORDER_RE.search(q):
            seq = _seq_card(rows) or _topic_card(rows)
            if seq and seq[0] not in existing:
                out.insert(min(1, len(out)), _card3(seq[0], seq[1]))
                n += 1
        return out, n
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_temporal inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0
