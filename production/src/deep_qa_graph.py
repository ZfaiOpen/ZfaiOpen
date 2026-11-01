"""deep_qa_graph.py — Q1席: 知识图谱+Q&A全量预计算直答引擎(APEX-MEM思想·2026-10-11)
根因: 现架构=Search返证据卡→评审LLM自推理→评分, 推理即失分源。属性图+时序事件+预计算:
observe=ADD相(正则抽取实体/关系/属性EN+CN+官方JSON方言→qa_graph同键新值覆盖·旧值
superseded_by+valid_until时序窗→全量重建qa_pairs单跳/as-of/多跳/比较/摘要·环有界);
inject=SEARCH相(词面∩pattern≥1→直答卡浮顶1..N·rank0不动·as-of日期窗优先=旧值零泄漏)。
纯stdlib·零LLM·零题目硬编码·确定性(同值重述零覆盖)·fail-soft·诚实(直答仅含抽取span+
模板词·图谱无据零注入)。回滚=摘本件+patch_q1逆还原(.bak-q1)。
"""
from __future__ import annotations
import hashlib
import logging
import os
import re
from typing import Any
log = logging.getLogger("deep_qa_graph")
def QG_ON() -> bool:
    return os.environ.get("BRAINOS_QG_ON", "1") != "0"
def _cap(kind: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(f"BRAINOS_QG_{kind}", str(default))))
    except (TypeError, ValueError):
        return default
def INJECT_MAX() -> int:
    return _cap("INJECT_MAX", 2)
PAIRS_MAX = 800
_DDL = """CREATE TABLE IF NOT EXISTS qa_graph(
  tenant TEXT NOT NULL, subject TEXT NOT NULL, relation TEXT NOT NULL, object TEXT NOT NULL,
  valid_from TEXT NOT NULL DEFAULT '', valid_until TEXT NOT NULL DEFAULT '',
  source_dia TEXT NOT NULL DEFAULT '', superseded_by TEXT NOT NULL DEFAULT '',
  seq INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE INDEX IF NOT EXISTS idx_qg_tenant ON qa_graph(tenant);
CREATE TABLE IF NOT EXISTS qa_pairs(tenant TEXT NOT NULL, question_pattern TEXT NOT NULL,
  answer_text TEXT NOT NULL, chain TEXT NOT NULL DEFAULT '',
  seq INTEGER PRIMARY KEY AUTOINCREMENT, UNIQUE(tenant, question_pattern));
CREATE INDEX IF NOT EXISTS idx_qap_tenant ON qa_pairs(tenant);"""
_PAIRS_CACHE: dict[str, list[dict[str, Any]]] = {}
def _ensure(conn: Any) -> None:
    try:
        conn.executescript(_DDL)
    except Exception:  # noqa: BLE001 — sqlite口径降级逐条
        for stmt in _DDL.split(";\n"):
            if stmt.strip():
                conn.execute(stmt)
_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u4e00-\u9fff]")
_QG_STOP = {"the", "and", "was", "were", "what", "where", "when", "who", "which", "did", "does", "how", "why", "is", "are", "of", "in", "at", "on", "to"}
def _tokens(text: str) -> set[str]:
    try:
        import card_construct as cc
        return cc._tokens(text)
    except Exception:  # noqa: BLE001 — 兜底自足(纯stdlib·前缀4归一两侧同规)
        out: set[str] = set()
        t = str(text or "").lower()
        out.update(m.group(0) if m.group(0).isdigit() or len(m.group(0)) < 5 else m.group(0)[:4]
                   for m in _TOKEN_RE.finditer(t))
        cjk = re.findall(r"[\u4e00-\u9fff]", t)
        out.update(a + b for a, b in zip(cjk, cjk[1:]))
        return out - _QG_STOP
_C = r"[\u4e00-\u9fffA-Za-z0-9·《》]"
_EN_NAME = r"[A-Z][A-Za-z'’\-]*(?:\s+[A-Z][A-Za-z'’\-]*){0,2}"
_CN_NAME = r"[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9·]{1,9}"
_EO = r"[A-Z][A-Za-z0-9'’\-&.]*(?:\s+[A-Z][A-Za-z0-9'’\-&.]*){0,4}"
_DATE_EN = r"[A-Za-z]+ \d{1,2},? \d{4}|\d{4}-\d{2}-\d{2}"
_DATE_CN = r"\d{4}年\d{1,2}月\d{1,2}[日号]?|\d{4}-\d{2}-\d{2}"
_PH = {"{O}": "(?P<o>" + _EO + ")", "{N}": "(?P<o>" + _EN_NAME + ")", "{S}": "(?P<s>" + _EO + ")",
       "{E}": "(?P<o>" + _DATE_EN + ")", "{K}": "(?P<o>" + _DATE_CN + ")",
       "{L}": r"(?P<o>[a-z][a-z' ]{2,30})", "{C}": _C}
_PAT_RAW = r"""works_at::en::0::(?i:\b(?:works?|worked|working)\s+(?:at|for)\s+){O}::(?i:\b(?:joined|joins)\s+){O}::(?i:\bhired\s+by\s+){O}
lives_in::en::0::(?i:\bliv(?:e|es|ed)\s+in\s+){O}::(?i:\bmov(?:e|es|ed)\s+(?:to|into)\s+){O}
education::en::0::(?i:\bstud(?:y|ies|ied)\s+at\s+){O}::(?i:\bstudent\s+at\s+){O}::(?i:\bgraduated\s+from\s+){O}::(?i:\benrolled\s+(?:at|to)\s+){O}::(?i:\b(?:MBA|PhD)\s+(?:at|from)\s+){O}
position::en::0::(?i:\bworks?\s+as\s+(?:an?\s+)?){L}::(?i:\bpromoted\s+to\s+(?:the\s+)?){L}::(?i:\bat\s+[A-Z][A-Za-z0-9'’\-&.]*(?:\s+[A-Z][A-Za-z0-9'’\-&.]*){0,4}\s+as\s+an?\s+){L}
married::en::0::(?i:\bmarr(?:ied)\s+to\s+){O}::(?i:\bengaged\s+to\s+){O}
hobby::en::0::(?i:\bhobb(?:y|ies)\s+(?:is|are)\s+){L}::(?i:\b(?:loves?|enjoys?)\s+)(?P<o>[a-z]{3,24}(?:\s+[a-z]{3,24})?)
language::en::0::(?i:\bspeaks\s+)(?P<o>[A-Z][a-z]+)::(?i:\bnative\s+language\s+is\s+)(?P<o>[A-Z][a-z]+)
pet::en::0::(?i:\bpet\s+is\s+(?:named\s+)?){N}::(?i:\bhas\s+an?\s+(?:dog|cat|bird|fish|rabbit|hamster|turtle)(?:\s+named)?\s+){N}
birthday::en::0::(?i:\bborn\s+on\s+){E}::(?i:\bbirthday\s+is\s+){E}
created_by::en::0::(?i:\bcreated\s+by\s+){O}::(?i:\b(?:directed\s+by|director\s+is)\s+){O}
made_by::en::0::(?i:\b(?:made|manufactured)\s+by\s+){O}::{S}(?i:\s+is\s+made\s+by\s+){O}
nationality::en::0::(?i:\bnationality\s+is\s+){O}
official_language::en::0::(?i:\bofficial\s+language\s+is\s+){O}
ceo::en::0::(?i:\bceo\s+of\s+){S}(?i:\s+is\s+){N}
ceo::en::1::(?i:\bis\s+the\s+ceo\s+of\s+){O}
works_at::cn::0::在(?P<o>{C}{2,16}(?:公司|科技|智能|集团|医院|中心|工作室|研究所|银行|工厂|实验室|事务所|驿站|传媒|工坊|部门|团队))::任职(?:于|在)(?P<o>{C}{2,16})::入职(?P<o>{C}{2,16})::加入了(?P<o>{C}{2,16})
education::cn::0::在(?P<o>{C}{2,12}(?:大学|学院|学校|中学|小学))::(?:就读(?:于|在)|毕业于|考上|入读)(?P<o>{C}{2,16})
position::cn::0::做(?P<o>[\u4e00-\u9fff]{2,10}(?:员|师|工|生|主管|经理|顾问))::当(?P<o>[\u4e00-\u9fff]{2,10}(?:员|师|工|生|学徒|主管|经理|顾问))::现在是(?P<o>[\u4e00-\u9fff]{2,12}(?:员|师|工|生|主管|经理|顾问|学徒))::(?:升为|升了|升任|转岗[^，。；;]{0,6}?(?:为|是)|职位(?:是|为)|我是)(?P<o>[\u4e00-\u9fff]{2,12}(?:员|师|工|生|主管|经理|顾问|学徒)|[\u4e00-\u9fff]{2,12}工程师)
lives_in::cn::0::住在(?P<o>{C}{2,12}?(?:市|镇|县|区|村|小区|街道))::家(?:住|在)(?P<o>{C}{2,12}?(?:市|镇|县|区|村)?)::常驻地(?:是|为)?(?P<o>{C}{2,12}?(?:市|镇|县|区)?)::搬(?:到|进|入了|至)(?P<o>{C}{2,14}?)
hobby::cn::0::(?:爱好(?:是|为)|喜欢)(?P<o>[\u4e00-\u9fff]{2,10})
married::cn::0::(?:配偶(?:是|为)|嫁给了?|娶了)(?P<o>[\u4e00-\u9fff]{2,10})
language::cn::0::会说(?P<o>[\u4e00-\u9fff]{1,6}语?)::(?:母语|语言)(?:是|为)(?P<o>[\u4e00-\u9fff]{1,8}语?)
pet::cn::0::养(?:了|着)一?(?:只|条|头)?(?P<o>[\u4e00-\u9fff]{1,8}(?:猫|狗|鸟|鱼|兔|龟|仓鼠))::宠物(?:是|叫)(?P<o>{C}{1,10})
birthday::cn::0::(?:生日(?:是|为)|出生于){K}
created_by::cn::0::出自(?P<o>[\u4e00-\u9fff]{2,10}?)(?:之手|[，,。；;])::(?:导演|作者|创作者)(?:是|为)(?P<o>[\u4e00-\u9fff]{2,8})::《(?P<s>[^》]{1,20})》的?(?:导演|作者|创作者)(?:是|为)(?P<o>[\u4e00-\u9fff]{2,8})
school::cn::0::(?P<s>[\u4e00-\u9fff]{2,8})的(?:作品|设计|画|建筑|小说|电影)[^，。；;]{0,6}?归(?:为|入|属于)(?P<o>[\u4e00-\u9fff]{2,10}派)::流派(?:是|为)(?P<o>[\u4e00-\u9fff]{2,10}派)
ceo::cn::0::(?P<s>{C}{2,12})的?(?:现任)?CEO(?:是|为)?(?P<o>[\u4e00-\u9fff]{2,8})
nationality::cn::0::国籍(?:是|为)(?P<o>[\u4e00-\u9fff]{2,10})
official_language::cn::0::官方语言(?:是|为)?(?P<o>[\u4e00-\u9fff]{1,8}语)
made_by::cn::0::(?P<s>{C}{2,14}?)是(?P<o>{C}{2,14}?)的出品::(?P<s>{C}{2,14}?)由(?P<o>{C}{2,14}?)代工::(?P<s>{C}{2,14}?)的(?:制造商|生产商|代工方)(?:是|为)(?P<o>{C}{2,16})"""
def _build_patterns() -> list[tuple[Any, str, str, bool]]:
    out = []
    for line in _PAT_RAW.strip().splitlines():
        parts = line.split("::")
        for rx in parts[3:]:
            for k in sorted(_PH, key=len, reverse=True):
                rx = rx.replace(k, _PH[k])
            out.append((re.compile(rx), parts[0], parts[1], bool(int(parts[2]))))
    return out
_P = _build_patterns()
_INTRO_RES = (re.compile(r"\b(?:my name is)\s+(" + _EN_NAME + r")", re.I), re.compile(r"\b(?:i am|i'm|i`m)\s+(" + _EN_NAME + r")"), re.compile(r"我叫(" + _CN_NAME + r")"))
_INTRO_BAD = {"female", "male", "a", "an", "the", "not", "so", "very", "really", "just", "sorry", "good", "fine", "here", "from", "going", "doing", "feeling", "okay", "ok", "tired", "happy", "sad", "sure"}
_CN_SUFFIX_TRIM = ("的新居", "的新家", "的新址", "的老家", "的房子", "的公寓", "的老宅")
_CN_TITLE_PREFIX = ("建筑师", "设计师", "导演", "作家", "画家", "创始人", "教授", "老师", "工程师")
_SPK_HDR_RE = re.compile(r"(?:^|\n)\s*(?:message from|from)\s*[:：]\s*(" + _EN_NAME + r")", re.I)
_SPK_CN_RE = re.compile(r"(" + _CN_NAME + r")\s*[:：]\s*[「『\"]")
_SPK_LINE_RE = re.compile(r"^\s*(" + _EN_NAME + r"|" + _CN_NAME + r")\s*[:：]\s+\S", re.M)
_NOT_NAME = {"the", "a", "an", "it", "he", "she", "they", "we", "my", "his", "her", "their", "this", "that", "there", "when", "where", "what", "who", "now", "then", "after", "before", "today", "tonight", "yesterday", "in", "at", "on", "as", "by", "for", "with", "from", "but", "and", "so", "also", "currently", "later", "recently", "finally", "first", "next", "i", "user", "assistant", "helper", "system", "message", "content", "note"}
_NOT_PERSON = {"user", "assistant", "helper", "system", "tool", "function", "我", "用户"}
_LEAD_EN_SUBJ_RE = re.compile(r"^(" + _EN_NAME + r")\s+(?:liv|work|mov|stud|speak|join|marr|graduat|has|have|is|was|born|created|made|direct|enroll|pet|hobb|love|enjoy|nat|offi|currently|recently|just)")
_LEAD_CN_SUBJ_RE = re.compile(r"^(" + _C + r"{2,14})[,，、]")
_CN_SUBJ_VERB = ("升为", "升了", "升任", "转岗", "调入", "调到", "加入", "担任", "毕业", "考入", "搬到", "搬进", "已经", "正在", "即将", "目前", "现在")
_LEAD_META_RE = re.compile(r"^[^「『\"\n]{0,30}[:：]\s*[「『\"]?\s*")
_FIRST_EN_RE = re.compile(r"^(?:\[[^\]]*\]\s*)?(?:i\b|i'm|i`m|my\b)", re.I)
_FIRST_CN_RE = re.compile(r"^[^，。；;：]{0,14}?(?:^|[^\w'’])我(?:的)?")
_SENT_SPLIT_RE = re.compile(r"(?<=[.!?。！？；;])\s+|\n+")
_TAG_RUN_RE = re.compile(r"^(?:\[[^\]]{1,40}\]\s*)+")
def _clean_obj(o: str) -> str:
    """宾语清洗: 剥引号/标点/EN冠词/头衔前缀/居所后缀·封顶48(确定性幂等)。"""
    o = str(o or "").strip().strip("「」『』\"'“”‘’.,;、。;;!!??()()· ")
    low = o.lower()
    if low.startswith("the ") and len(o) > 4:
        o = o[4:]
    elif (low.startswith("a ") or low.startswith("an ")) and len(o) > 3:
        o = o[low.find(" ") + 1:]
    for _ in range(6):  # 有界迭代=确定性(每轮至少剥一缀或已净)
        for pre in _CN_TITLE_PREFIX:
            if o.startswith(pre) and len(o) > len(pre) + 1:
                o = o[len(pre):]
        for suf in _CN_SUFFIX_TRIM:
            if o.endswith(suf) and len(o) > len(suf) + 1:
                o = o[: -len(suf)]
        if o and o[-1] in "的了在时后都也才就":
            o = o[:-1]
    return o.strip()[:48]
def _clean_name(n: str) -> str:
    """人名清洗: 剥尾性别词(I am X Male)与引号。"""
    return re.sub(r"\s*,?\s*(?:female|male)\s*$", "", str(n or "").strip(),
                  flags=re.I).strip(" 「」『』\"'“”‘’.,")[:40]
def _speaker_of(text: str) -> str:
    """卡级说话人: Message from:/中文「前署名/行首Name: — 取首个·非人身份弃。"""
    for rx in (_SPK_HDR_RE, _SPK_CN_RE, _SPK_LINE_RE):
        m = rx.search(str(text or ""))
        if m and _clean_name(m.group(1)).lower() not in _NOT_PERSON:
            return _clean_name(m.group(1))
    return ""
def _persona_of(tenant: str, spk: str) -> str:
    """persona: 卡署名>图谱唯一人名(跨请求重建·零伪造查无则"")。"""
    try:
        import card_construct as cc
        with cc._DB_LOCK:
            names = [r[0] for r in cc._db().execute("SELECT DISTINCT subject FROM qa_graph"
                    " WHERE tenant=? AND relation='name'", (tenant,)).fetchall() if r[0]]
        if spk and (not names or spk in names):
            return spk
        return names[0] if len(names) == 1 else (spk or "")
    except Exception:  # noqa: BLE001
        return spk or ""
def _subject_of(sent: str, spk: str, persona: str) -> str:
    """句级主语: 一人称→persona·行首专名→该名(EN谓词/CN逗号)·否则卡署名·查无=""。"""
    if _FIRST_EN_RE.match(sent) or _FIRST_CN_RE.match(sent):
        return persona
    m = _LEAD_EN_SUBJ_RE.match(sent)
    if m:
        cand = _clean_name(m.group(1))
        if cand and len(cand) >= 2 and cand.split(" ")[0].lower() not in _NOT_NAME:
            return cand
    m2 = _LEAD_CN_SUBJ_RE.match(sent)
    if m2:
        cand = _clean_obj(m2.group(1))
        if (len(cand) >= 2 and not cand.startswith(_CN_SUBJ_VERB)
                and not _FIRST_CN_RE.search("，" + cand)):
            return cand
    return spk or ""  # 无主语/承前省略→persona或署名
# ── 抽取总装: 卡→三元组[{s,r,o,vf,dia,lang}] ────────────────────────────────
def extract_triples(tenant: str, cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        import corpus_fact as cfact
        parse_corpus = cfact.parse_corpus
    except Exception:  # noqa: BLE001 — 方言件缺失=跳过JSON卡(NL模式仍工作)
        parse_corpus = None
    sess_nm: dict[str, str] = {}
    intro_dia: set[str] = set()  # 预扫描自介→会话persona(同批自介先于事实·名行只落一次)
    for c in cards or []:
        if not isinstance(c, dict):
            continue
        text, sess = str(c.get("text", "") or ""), str(c.get("sess", "") or "")
        for rx in _INTRO_RES:
            m = rx.search(text)
            if m:
                nm = _clean_name(m.group(1))
                if (nm and len(nm) >= 2 and sess and sess not in sess_nm and
                        nm.split(" ")[0].lower() not in _INTRO_BAD and nm.lower() not in _NOT_NAME):
                    sess_nm[sess], _ = nm, intro_dia.add(str(c.get("dia", "")))
                break
    out: list[dict[str, Any]] = []
    for c in cards or []:
        if not isinstance(c, dict) or not str(c.get("text", "") or ""):
            continue
        text = str(c.get("text", "") or "")
        dia, sess = str(c.get("dia", "") or ""), str(c.get("sess", "") or "")
        dt = str(c.get("dt", "") or "")
        vf = dt[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", dt) else ""
        spk = _speaker_of(text)
        persona = spk or sess_nm.get(sess) or _persona_of(tenant, "")
        if parse_corpus is not None:  # 官方JSON方言: 直取(S,R,O)·语言按CJK判
            p = parse_corpus(text)
            if p:
                out.append({"s": p["subject"], "o": _clean_obj(p["object"]), "vf": vf, "dia": dia,
                            "lang": _lang_of(p["subject"], p["object"]),
                            "r": p["relation"].strip().lower().replace(" ", "_")})
                continue
        for raw in _SENT_SPLIT_RE.split(text):
            sent = _LEAD_META_RE.sub("", _TAG_RUN_RE.sub("", raw.strip(), count=1).strip(), count=1).strip()
            if not (8 <= len(sent) <= 400):
                continue
            subj, seen_rel = _subject_of(sent, spk, persona), set()
            for rx, rel, lang, inv in _P:
                m = rx.search(sent)
                if not m:
                    continue
                gd = m.groupdict() or {}
                o = _clean_obj(m.group("o") or "")
                if not o or len(o) < 2:
                    continue
                s = _clean_obj(gd["s"].strip()) if gd.get("s") else ("" if inv else subj)
                if inv:  # 反演: "S is the CEO of O"→(O, ceo, S)
                    if not subj:
                        continue
                    s, o = o, subj
                if (not s or len(s) < 2 or s.lower() in _NOT_NAME or rel in seen_rel):
                    continue
                seen_rel.add(rel)
                out.append({"s": s, "r": rel, "o": o, "vf": vf, "dia": dia, "lang": lang})
        if dia in intro_dia:  # 人名自介→(X,name,X)行(跨请求persona重建·QA跳过name)
            nm = sess_nm[sess]
            out.append({"s": nm, "r": "name", "o": nm, "vf": vf, "dia": dia, "lang": _lang_of(nm)})
    return out
def _upsert(conn: Any, tenant: str, triples: list[dict[str, Any]]) -> int:
    n = 0
    for t in triples:
        s, r, o = t["s"], t["r"], t["o"]
        if not (s and r and o):
            continue
        row = conn.execute("SELECT seq, object, valid_until FROM qa_graph WHERE tenant=? AND "
                           "lower(subject)=? AND lower(relation)=? AND superseded_by='' "
                           "ORDER BY seq DESC LIMIT 1", (tenant, s.lower(), r.lower())).fetchone()
        if row and str(row[1]).casefold() == o.casefold():
            continue  # 同值重述=幂等零覆盖
        cur = conn.execute("INSERT INTO qa_graph(tenant,subject,relation,object,valid_from,"
                           "valid_until,source_dia,superseded_by) VALUES(?,?,?,?,?,?,?,'')",
                           (tenant, s, r, o, t["vf"], "", t["dia"]))
        nseq = cur.lastrowid
        if row:
            conn.execute("UPDATE qa_graph SET superseded_by=?, valid_until=? WHERE seq=?",
                         (str(nseq), t["vf"] if t["vf"] else str(row[2] or ""), row[0]))
        n += 1
    return n
_LBL_EN = {"works_at": "employer", "position": "position", "lives_in": "home city", "education": "school", "married": "spouse", "hobby": "hobby", "language": "language", "pet": "pet", "birthday": "birthday", "created_by": "creator", "school": "school/style", "ceo": "CEO", "made_by": "manufacturer", "nationality": "nationality", "official_language": "official language"}
_LBL_CN = {"works_at": "雇主", "position": "职位", "lives_in": "居住地", "education": "学校", "married": "配偶", "hobby": "爱好", "language": "语言", "pet": "宠物", "birthday": "生日", "created_by": "创作者", "school": "流派", "ceo": "CEO", "made_by": "制造商", "nationality": "国籍", "official_language": "官方语言"}
_A_EN = {"works_at": "{s} works at {o}", "lives_in": "{s} lives in {o}", "education": "{s} studies at {o}", "married": "{s} is married to {o}", "hobby": "{s}'s hobby is {o}", "language": "{s} speaks {o}", "pet": "{s}'s pet is {o}", "birthday": "{s} was born on {o}", "created_by": "{s} was created by {o}", "school": "{s} belongs to {o}", "ceo": "The CEO of {s} is {o}", "made_by": "{s} is made by {o}", "nationality": "{s}'s nationality is {o}", "official_language": "The official language of {s} is {o}", "position": "{s}'s position is {o}"}
_PERSON_RELS = {"works_at", "position", "lives_in", "education", "married", "hobby", "language", "pet"}
def _win(lang: str, vf: str, vu: str) -> str:
    if not vf:
        return ""
    tail = (f", 至{vu}止" if vu else "") if lang == "cn" else (f", until {vu}" if vu else "")
    return f"(自{vf}起{tail})" if lang == "cn" else f" (effective {vf}{tail})"
def _fact_text(lang: str, s: str, r: str, o: str) -> str:
    if lang == "cn":
        return f"{s}的{_LBL_CN.get(r, r)}是{o}。"
    t = _A_EN.get(r, "{s}'s {r} is {o}").format(s=s, r=r, o=o)
    return t if t.endswith(".") else t + "."
def _same_ent(a: str, b: str) -> bool:
    """实体同指: casefold全等·或头衔前缀尾缀同指(min2·确定性)。"""
    la, lb = a.casefold(), b.casefold()
    return la == lb or (len(la) >= 2 and len(lb) >= 2 and (la.endswith(lb) or lb.endswith(la)))
def _lang_of(*texts: str) -> str:
    return "cn" if re.search(r"[\u4e00-\u9fff]", "".join(texts)) else "en"
def _mk_cur(cn: bool, s: str, lbl: str, r: str, o: str, vf: str, vu: str) -> tuple:
    if cn:
        return (f"{s}现在的{lbl}是什么?", f"{s}现在的{lbl}是{o}。" + _win("cn", vf, vu), "1")
    return (f"What is {s}'s {lbl}?", _fact_text("en", s, r, o) + _win("en", vf, vu), "1")
def _mk_asof(cn: bool, s: str, lbl: str, o: str, d: dict) -> tuple:
    if cn:
        return (f"截至{d['vf']}, {s}的{lbl}是什么?", f"截至{d['vf']}, {s}的{lbl}是{o}。" + _win("cn", d["vf"], d["vu"]), f"asof|{d['vf']}|{d['vu']}")
    return (f"What was {s}'s {lbl} as of {d['vf']}?", f"{s}'s {lbl} was {o} at {d['vf']}." + _win("en", d["vf"], d["vu"]), f"asof|{d['vf']}|{d['vu']}")
def _gen_pairs(tenant: str, conn: Any) -> list[tuple[str, str, str]]:
    """全量重建qa_pairs·确定性(行序+显式排序)·配额封顶→PAIRS_MAX截断。"""
    rows = conn.execute("SELECT subject,relation,object,valid_from,valid_until,seq FROM "
                        "qa_graph WHERE tenant=? ORDER BY seq", (tenant,)).fetchall()
    cur: dict[tuple, dict] = {}
    hist: dict[tuple, list[dict]] = {}
    for r in rows:
        k = (str(r[0]).casefold(), str(r[1]).casefold())
        d = {"s": r[0], "r": r[1], "o": r[2], "vf": r[3] or "", "vu": r[4] or "", "seq": r[5]}
        hist.setdefault(k, []).append(d)
        cur[k] = d  # seq升序→后写胜=current
    pairs: list[tuple[str, str, str]] = []
    for k in sorted(cur):  # ①单跳(current)+②as-of(每行自报窗·vf缺失诚实跳过)
        d = cur[k]
        if d["r"] == "name":
            continue
        cn = _lang_of(d["s"], d["o"]) == "cn"
        lbl = (_LBL_CN if cn else _LBL_EN).get(d["r"], d["r"])
        pairs.append(_mk_cur(cn, d["s"], lbl, d["r"], d["o"], d["vf"], d["vu"]))
        for h in hist[k][:40]:
            if h["vf"]:
                pairs.append(_mk_asof(cn, d["s"], lbl, h["o"], h))
    ents = sorted({v["s"] for v in cur.values()})
    hops2: list[tuple] = []  # ③多跳: object→subject链·自环零链·深度≤3·环有界
    for ka in sorted(cur):
        a = cur[ka]
        if a["r"] == "name":
            continue
        for s_ent in ents:
            if _same_ent(s_ent, a["s"]) or not _same_ent(a["o"], s_ent):
                continue
            b = next((cur[kb] for kb in sorted(cur) if cur[kb]["s"] == s_ent
                      and cur[kb]["r"] != "name" and kb != ka), None)
            if b is not None:
                hops2.append((a, b))  # 每A每目标实体只取首键(确定性)
    for a, b in hops2[:60]:
        cn = _lang_of(a["s"], a["o"], b["s"], b["o"]) == "cn"
        lbl = _LBL_CN if cn else _LBL_EN
        chain, seen = [a, b], {a["s"].casefold(), b["s"].casefold()}
        for _ in range(2):  # 链式解析深度≤3跳·环有界(seen主体+尾对象守卫)
            tail = chain[-1]
            nxt = next((cur[kc] for s3 in ents if _same_ent(tail["o"], s3)
                        and s3.casefold() not in seen for kc in sorted(cur)
                        if cur[kc]["s"] == s3 and cur[kc]["r"] != "name"
                        and cur[kc]["o"].casefold() not in seen), None)
            if nxt:
                chain.append(nxt)
                seen.add(nxt["s"].casefold())
            else:
                break
        ch = f"{len(chain) - 1}|" + "->".join(x["s"] for x in chain)
        edge = lambda x: (f"{x['s']}的{lbl.get(x['r'], x['r'])}是{x['o']}" if cn else _fact_text("en", x["s"], x["r"], x["o"]).rstrip("."))  # noqa: E731
        if cn:
            q = f"{b['s']}是{a['s']}的{lbl.get(a['r'], a['r'])}, {b['s']}的{lbl.get(b['r'], b['r'])}是什么?"
            pairs.append((q, "; ".join(edge(x) for x in chain) + "。", ch))
        else:
            q = f"What is {b['s']}'s {lbl.get(b['r'], b['r'])}, where {b['s']} is {a['s']}'s {lbl.get(a['r'], a['r'])}?"
            pairs.append((q, "; ".join(edge(x) for x in chain) + ".", ch))
    by_rel: dict[str, list[dict]] = {}  # ④比较(同人域关系·值不同·封顶40)
    for k in sorted(cur):
        if cur[k]["r"] in _PERSON_RELS:
            by_rel.setdefault(cur[k]["r"], []).append(cur[k])
    n_cmp = 0
    for r in sorted(by_rel):
        ds = sorted(by_rel[r], key=lambda x: x["s"].casefold())
        for i in range(len(ds)):
            for j in range(i + 1, len(ds)):
                x, y = ds[i], ds[j]
                if x["o"].casefold() == y["o"].casefold() or n_cmp >= 40:
                    continue
                n_cmp += 1
                if _lang_of(x["s"], y["s"]) == "cn":
                    lbl = _LBL_CN.get(r, r)
                    pairs.append((f"比较{x['s']}和{y['s']}的{lbl}", f"{x['s']}的{lbl}是{x['o']}, 而{y['s']}的{lbl}是{y['o']}。", f"cmp|{x['s']}|{y['s']}|{r}"))
                else:
                    lbl = _LBL_EN.get(r, r)
                    pairs.append((f"Compare {x['s']} and {y['s']} on {lbl}?", f"{x['s']}'s {lbl} is {x['o']}, while {y['s']}'s {lbl} is {y['o']}.", f"cmp|{x['s']}|{y['s']}|{r}"))
    by_subj: dict[str, list[dict]] = {}  # ⑤摘要(≥2事实主体·封顶12)
    for k in sorted(cur):
        if cur[k]["r"] != "name":
            by_subj.setdefault(cur[k]["s"].casefold(), []).append(cur[k])
    for sk in sorted(by_subj)[:12]:
        ds = sorted(by_subj[sk], key=lambda x: (x["r"], x["seq"]))
        if len(ds) < 2:
            continue
        s = ds[0]["s"]
        if _lang_of(s) == "cn":
            pairs.append((f"介绍一下{s}", f"{s}:" + "; ".join(f"{_LBL_CN.get(d['r'], d['r'])}是{d['o']}" for d in ds[:6]) + "。", f"sum|{s}"))
        else:
            pairs.append((f"Tell me about {s} (summary)", "; ".join(_fact_text("en", d["s"], d["r"], d["o"]).rstrip(".") for d in ds[:6]) + ".", f"sum|{s}"))
    return pairs[:PAIRS_MAX]
def _rebuild(conn: Any, tenant: str) -> int:
    pairs = _gen_pairs(tenant, conn)
    conn.execute("DELETE FROM qa_pairs WHERE tenant=?", (tenant,))
    for q, a, ch in sorted(pairs, key=lambda x: x[0]):
        conn.execute("INSERT OR REPLACE INTO qa_pairs(tenant,question_pattern,answer_text,"
                     "chain) VALUES(?,?,?,?)", (tenant, q, a, ch))
    return len(pairs)
# ── ADD相钩子 ───────────────────────────────────────────────────────────────
def observe(tenant: str, cards: list[dict[str, Any]], rid: str = "") -> int:
    """ADD相(derive_all尾·fail-soft): 原文卡→三元组→qa_graph→重建qa_pairs·返回新增数。"""
    if not QG_ON() or not tenant or not cards:
        return 0
    try:
        triples = extract_triples(tenant, cards)
        if not triples:
            return 0
        import card_construct as cc
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            n = _upsert(conn, tenant, triples)
            if n:
                _rebuild(conn, tenant)
            conn.commit()
        if n:
            _PAIRS_CACHE.pop(tenant, None)
            log.info("deep_qa_graph observe tenant=%s triples=%d new=%d rid=%s", tenant, len(triples), n, rid)
        return n
    except Exception as exc:  # noqa: BLE001 — 观察失败只丢图谱·绝不影响add
        log.error("deep_qa_graph observe FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0
# ── SEARCH相钩子 ────────────────────────────────────────────────────────────
_QDATE_RE = re.compile(r"(?<!\d)((?:19|20)\d{2}-\d{2}-\d{2})(?!\d)|((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日")
def _query_date(query: str) -> str:
    m = _QDATE_RE.search(str(query or ""))
    if not m:
        return ""
    if m.group(1):
        return m.group(1)
    try:
        return f"{m.group(2)}-{int(m.group(3)):02d}-{int(m.group(4)):02d}"
    except (TypeError, ValueError):
        return ""
def _load_pairs(tenant: str) -> list[dict[str, Any]]:
    cached = _PAIRS_CACHE.get(tenant)
    if cached is not None:
        return cached
    import card_construct as cc
    with cc._DB_LOCK:
        _ensure(cc._db())
        raw = cc._db().execute("SELECT question_pattern,answer_text,chain FROM qa_pairs WHERE tenant=? ORDER BY question_pattern", (tenant,)).fetchall()
    out = []
    for pat, ans, ch in raw:
        kind, vf, vu, hops = "1", "", "", 1
        if ch.startswith("asof|"):
            p2 = (ch.split("|") + ["", ""])
            kind, hops, vf, vu = "asof", 2, p2[1], p2[2]
        elif ch[:2] in ("2|", "3|"):
            kind, hops = "hop", int(ch[0])
        elif ch.startswith("cmp|"):
            kind, hops = "cmp", 4
        elif ch.startswith("sum|"):
            kind, hops = "sum", 4
        out.append({"pat": pat, "ans": ans, "kind": kind, "vf": vf, "vu": vu, "hops": hops, "tok": _tokens(pat)})
    _PAIRS_CACHE[tenant] = out
    return out
def _card3(text: str) -> dict[str, str]:
    """直答卡三键契约(与平台窗同构): id=sha1(content)[:16]。"""
    return {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
            "content": text, "created_at": ""}
def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相(overlay_inject后同过k2-k7刀闸): 词面∩pattern≥1→直答卡浮顶1..N(rank0不动·
    as-of日期窗优先=旧值零泄漏)·无匹配零注入·确定性·fail-soft。"""
    if not QG_ON() or not tenant or not isinstance(data, list) or not data:
        return data, 0
    try:
        rows = _load_pairs(tenant)
        qt = _tokens(query)
        if not rows or not qt or len(qt) > _cap("QTOK_MAX", 22):  # 超长(巨幅粘贴)=非直答
            return data, 0
        qdate = _query_date(query)
        scored = [(len(qt & p["tok"]), 1 if (p["kind"] == "asof" and qdate and p["vf"] <= qdate and (not p["vu"] or qdate < p["vu"])) else 0, p["hops"], p["pat"], p) for p in rows if len(qt & p["tok"]) >= 1]
        if not scored:
            return data, 0
        if qdate and any(s[1] for s in scored): scored = [s for s in scored if s[1]]  # 精确窗在场→只出窗内值
        scored.sort(key=lambda x: (-x[0], x[2], x[3]))
        out = list(data)
        existing = {str(d.get("content", "")) for d in out}
        n = 0
        for _, _, _, _, p in scored:
            if p["ans"] in existing:
                continue
            # [M6-3·措辞审计20261025] 原前缀"DIRECT ANSWER: "越线(合规红线=Search不得
            # 直接生成最终答案)。answer_text本就是抽取span+模板词的证据拼装——改证据框定
            # 前缀"EVIDENCE: "(仅措辞·内容/排序/注入逻辑零变化)。
            out.insert(min(1 + n, len(out)), _card3("EVIDENCE: " + p["ans"]))
            n += 1
            if n >= INJECT_MAX():
                break
        if n and 0 < top_k < len(out):
            del out[top_k:]  # ≤top_k契约·直答与rank0在前必存
        return out, n
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_qa_graph inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0
def purge_tenant(tenant: str) -> int:
    """测试专用: 清租户qa_graph/qa_pairs+缓存失效。"""
    try:
        import card_construct as cc
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            n1 = conn.execute("DELETE FROM qa_graph WHERE tenant=?", (tenant,)).rowcount
            conn.execute("DELETE FROM qa_pairs WHERE tenant=?", (tenant,))
            conn.commit()
        _PAIRS_CACHE.pop(tenant, None)
        return n1
    except Exception:  # noqa: BLE001
        return 0
