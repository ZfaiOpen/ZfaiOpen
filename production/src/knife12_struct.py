# knife12_struct.py — 部署线第12刀·A3结构卡刀(集合/枚举/计数字段抽取→专卡·原型v1·20261022)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方AML A3域(结构化字段/集合/枚举抽取)=23.23(冠军76.1) — 考题形态"列出所有X"/
#       "how many Y"/"字段值"。现引擎只有叙事型事实卡: 集合成员散落多会话多卡, 任何单卡
#       都不含全量并集; "how many"需要跨卡聚合计数——叙事卡无此物; 字段值被新旧值/上下文
#       噪声淹没。冠军做法=把对话中的枚举/集合/计数字段抽成结构化专卡, 查询时按意图路由。
# 既有件复用(对齐家规·零重复造轮):
#   knife2_sensitive.py   = 边车骨架范本(observe/门/超采/purge·本件同构)。
#   corpus_fact.py        = observe+inject双钩子先例(_CACHE写时失效/三键卡注入位1)。
#   knife9_g4.py          = 枚举卡注入位形先例([card]+data·挂接位csan前·k9同位)。
#   knife_intent.py       = 意图路由先例(本件用纯正则意图门·零嵌入·更省)。
# 三段架构(边车式·零organ改动·零LLM·全确定性):
#   ADD相   observe(tenant,cards)  extract_struct(text): 逐卡纯正则抽取三类结构化事实
#           →struct_items表(sqlite WAL·幂等):
#             kind='set'   集合成员(row/条目): 显式枚举句+跨会话单成员句累计; 否定守卫
#                          (不抽取)+撤回标记(active=0·"no longer/sold/送人"→成员退出);
#                          计数不落盘(查询时由active成员数现算=零陈旧计数)。
#             kind='field' 字段键值: 最新值胜出(ON CONFLICT UPDATE·旧值入prev可溯源)。
#   SEARCH相 inject(tenant,query,data,top_k): A3意图门(列出所有/how many/字段值·正则
#           三门)→集合路由(set_key别名+成员词面交)→专卡注入位1-2(rank0不动):
#             [StructSet]   全量清单卡: "Label — n in total: m1 / m2 / m3 (…dates)"
#             [StructCount] 计数卡: "Label count = n: m1, m2, m3"
#             [StructField] 字段现值卡: "key = value (as of dt; previously old)"
#   SELFTEST 自检段(--selftest): 10合成用例(枚举/计数/字段×正反例·中英)全绿门+性能预算门。
# 挂接(3处·沙箱先验·回滚=摘3行+删本件):
#   ①aml_service import区(k7同列): import knife12_struct as k12
#   ②_knives_process: k12.observe(tenant, cards)  (原文卡流·时间线卡不吃防重复抽取)
#   ③search相g4块后(_mark("g4")之后·csan之前): data,k12_n=k12.inject(tenant_of(uid),query,data,top_k)
# 敏感域守护: health/allergy/medication/finance/secret类集合仅当query显式命中该域别名
#   才注入(k2精神·词面路由天然隐含: 问"allergies"才路由allergies集合·泛化query零旁路)。
# 预算: observe≈0.1ms/卡(25条编译正则·批提交); inject<10ms/查询(租户行内存缓存·写时失效
#   ·词面交·零查询期DB读[缓存命中时]·cfact同款)。日志零内容: 只记计数/键名哈希(k2纪律)。
# 合规: 测试数据全部自造合成·官方AML题库零接触; 沙箱租户sb_*前缀·KNIFE12_DB env隔离。
# env总闸 BRAINOS_K12(缺省开·=0全直通)。
# ═══════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("knife12")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KNIFE12_DB", str(APP_DIR.parent / "data" / "knife12.db")))

_MAX_TEXT = 600          # 超长卡不抽(叙述长文枚举价值低·噪声高)
_MAX_MEMBERS = 24        # 单集合成员上限(防爆表)
_MAX_INJECT = 3          # 单查询注入专卡上限
_MAX_SETS_ROUTE = 2      # 单查询路由集合上限
_TOKEN_MIN = 2           # 词面token最小长度


def _master_on() -> bool:
    return os.environ.get("BRAINOS_K12", "1") != "0"


# ═══════════════════════════ L1 词典: 类别×别名(中英) ═══════════════════════════
# (set_key, 查询路由别名tuple, 敏感域?)
#   别名小写; 词面交路由用。敏感域=仅query显式命中别名才注入(k2精神守护)。
_CATS: tuple[tuple[str, tuple[str, ...], bool], ...] = (
    ("pets",       ("pet", "pets", "dog", "dogs", "cat", "cats", "hamster", "animal", "animals", "宠物", "狗", "猫", "仓鼠", "宠物", "养"), False),
    ("hobbies",    ("hobby", "hobbies", "pastime", "pastimes", "free time", "spare time", "爱好", "业余", "兴趣"), False),
    ("languages",  ("language", "languages", "speak", "spoken", "语言", "会说", "说"), False),
    ("instruments",("instrument", "instruments", "play the", "乐器", "弹"), False),
    ("sports",     ("sport", "sports", "exercise", "workout", "运动", "锻炼"), False),
    ("travelled",  ("visited", "visit", "been to", "travel", "travelled", "traveled", "countries", "cities", "去过", "旅行", "国家", "城市"), False),
    ("team",       ("team", "member", "members", "people", "colleague", "colleagues", "coworker", "crew", "staff", "团队", "成员", "同事", "人"), False),
    ("club",       ("club", "book club", "group", "society", "俱乐部", "读书会", "小组", "社团"), False),
    ("allergies",  ("allergic", "allergy", "allergies", "过敏", "过敏源"), True),
    ("medications",("medication", "medications", "medicine", "medicines", "pill", "药", "服药"), True),
    ("vehicles",   ("car", "cars", "vehicle", "vehicles", "bike", "motorcycle", "车", "汽车", "摩托"), False),
    ("skills",     ("skill", "skills", "programming", " proficient", "技能", "熟练"), False),
    ("plants",     ("plant", "plants", "flower", "flowers", "garden", "植物", "花", "花园"), False),
    ("subscriptions", ("subscription", "subscriptions", "订阅", "会员"), False),
    ("devices",    ("device", "devices", "gadget", "laptop", "phone model", "设备", "电脑"), False),
)
_CAT_ALIAS: dict[str, tuple[str, ...]] = {k: a for k, a, _s in _CATS}
_SENSITIVE: frozenset[str] = frozenset(k for k, _a, s in _CATS if s)

# 字段键: (key, 查询路由别名tuple, 抽取正则异名列表, 敏感域?)
_FIELDS: tuple[tuple[str, tuple[str, ...], bool], ...] = (
    ("address",  ("address", "live", "home", "住", "地址", "住在"), False),
    ("phone",    ("phone", "number", "telephone", "电话", "手机号"), True),
    ("email",    ("email", "e-mail", "邮箱", "电子邮件"), True),
    ("employer", ("work", "employer", "company", "job", "公司", "工作", "单位"), False),
    ("job_title",("title", "role", "position", "职位", "职务"), False),
    ("birthday", ("birthday", "born", "生日", "出生"), True),
    ("spouse",   ("wife", "husband", "spouse", "partner", "妻子", "丈夫", "配偶"), True),
    ("hometown", ("hometown", "from", "家乡", "老家"), False),
    ("emergency_contact", ("emergency", "紧急联系人", "应急"), True),
    ("name",     ("name", "名字", "姓名"), False),
)

# ═══════════════════════════ L1 抽取正则(编译一次·全确定性) ═══════════════════════════
_WS = re.compile(r"\s+")
_WORD = re.compile(r"[a-zA-Z]{2,}|[\u4e00-\u9fff]+")
_QSTOP = frozenset("""a an the all my me mine our of in on at do does did i am is are was
were have has had what which who whose how many much list name every each some any to for
about tell give show please and or 的 我 你 他 她 它 所有 全部 都 有 没有 列出 哪些 什么
怎么 多少 几个 几只 几位 和 与 跟 了 吗 呢 是 在""".split())

# 否定守卫(成员/值前的左窗内出现→该条不抽取)
_NEG_RE = re.compile(
    r"\b(?:no|not|don'?t|doesn'?t|didn'?t|won'?t|never|without|neither|nor|none|zero)\b"
    r"|没有|不曾|不是|不含|没有|没有|不过敏|不喜欢|不会|没", re.I)
# 撤回话型(成员退出集合)
_RETRACT_RES = (
    re.compile(r"\b(?:no\s+longer|don'?t\s+(?:play|do|use|keep|have)|sold|gave\s+(?:away|up)"
               r"|rehomed|got\s+rid\s+of|stopped\s+(?:playing|doing|using)|quit|cancelled|canceled"
               r"|returned\s+my)\s+(?:the\s+|my\s+|our\s+)?(.{2,40}?)(?=[.,;!?]|$)", re.I),
    re.compile(r"(?:不再|再也不|戒了|送人了|卖掉了|送走|退掉了|退了|取消了一?个?)([\u4e00-\u9fffa-zA-Z]{1,20})"),
)
# 显式枚举句·EN: intro + list(≥2成员效果好·单成员也收)
_ENUM_EN = (
    # my hobbies are X, Y and Z / my pets include X / my languages: X, Y
    re.compile(r"\bmy\s+(?P<cat>[a-z][a-z ]{2,28}?)\s*(?:\bmembers\b)?\s*"
               r"(?:\b(?:are|is|were|was|include|included|consist\s+of|consists\s+of)\b|:)\s*"
               r"(?P<list>[^\.\!\?]{2,220})", re.I),
    # I enjoy/like/love/play X, Y and Z  (≥2成员才收·防"I like pizza"噪声)
    re.compile(r"\bI\s+(?:really\s+)?(?:enjoy|like|love|play)\s+(?P<list>[^\.\!\?]{2,220})", re.I),
    # I speak/know X, Y and Z
    re.compile(r"\bI\s+(?:can\s+)?(?:speak|know)\s+(?P<list>[^\.\!\?]{2,220})", re.I),
    # I have visited / I've been to X, Y and Z
    re.compile(r"\bI(?:'ve|\s+have)?\s+(?:visited|been\s+to)\s+(?P<list>[^\.\!\?]{2,220})", re.I),
    # I'm allergic to X, Y
    re.compile(r"\bI(?:'m|\s+am)\s+allergic\s+to\s+(?P<list>[^\.\!\?]{2,160})", re.I),
    # Header-colon: "Hobbies: X, Y" / "Pets: X and Y"
    re.compile(r"^\s*(?P<cat>Hobbies|Hobby|Pets|Languages|Allergies|Medications|Skills|Team"
               r"|Instruments|Sports|Subscriptions|Plants|Devices|Club|Book\s+club)\s*:\s*"
               r"(?P<list>[^\n]{2,220})", re.I | re.M),
)
# 显式枚举句·CN
_ENUM_CN = (
    re.compile(r"我(?:的)?(?P<cat>爱好|兴趣|语言|宠物|过敏源|特长|技能|收藏|乐器|订阅)(?:是|有|包括|为|：|:)\s*(?P<list>[^\。\!\?；\n]{2,120})"),
    re.compile(r"我会?(?:说|讲)(?P<list>[^\。\!\?；\n]{2,80})"),
    re.compile(r"我(?:去?过|旅游?去过)(?P<list>[^\。\!\?；\n]{2,80})"),
    re.compile(r"我对(?P<list>[^\。\!\?；\n]{2,60})过敏"),
)
# 单成员句·EN(species词表驱动): "I adopted a dog named Rex" / "my cat is called Mia" /
# "We got a new cat, Mia —" / "X joined my team"
_PET_SPECIES = r"(?:dog|puppy|cat|kitten|hamster|rabbit|bunny|bird|parrot|fish|turtle|tortoise|goldfish|gerbil|guinea\s+pig)"
_EN_MEMBER = (
    re.compile(r"\b(?:have|had|own(?:ed)?|adopted|got|bought|keep|feed)\s+(?:a|an|one|another|our|two|three)?\s*"
               r"(?:(?:new|little|old|golden|brown|black|white|small|big|rescue|tiny|young|fat|lazy|silly|first|second)\s+)?"
               r"(?:[a-z]{3,10}\s+)?(?P<sp>" + _PET_SPECIES + r")\b"
               r"(?:\s*,?\s*(?:named|called)\s+(?P<nm1>[A-Z][a-z]{1,15})"
               r"|,\s*(?P<nm2>[A-Z][a-z]{1,15})\b(?=\s*[—–-]|$|\s+(?:who|which|she|he)))?", re.I),
    re.compile(r"\bmy\s+(?P<sp>" + _PET_SPECIES + r")\s*(?:'s\s+name)?\s*"
               r"(?:\bis\s+(?:called|named)?|is\s+called|is\s+named|\bcalled\b|\bnamed\b)\s+"
               r"(?P<nm1>[A-Z][a-z]{1,15})", re.I),
    re.compile(r"\bI(?:'m|\s+am)\s+allergic\s+to\s+(?:the\s+)?(?P<sp>peanuts?|shellfish|latex|pollen|dust(?:\s+mites)?|milk|lactose|gluten|soya?|eggs?|bee\s+stings?|penicillin|cats?|dogs?)\b", re.I),
    re.compile(r"\b(?:the|our|my)\s+(?P<sp>" + _PET_SPECIES + r")\s+(?P<nm1>[A-Z][a-z]{1,15})\b"),  # 无re.I: 专名须大写("The cat videos"不入·"the hamster Pip joined"入)
    re.compile(r"\b(?P<nm2>[A-Z][a-z]{1,15}(?:\s*,\s*[A-Z][a-z]{1,15})*(?:\s+and\s+[A-Z][a-z]{1,15})?)\s+"
               r"(?:just\s+)?(?:joined|is\s+on|is\s+in|are\s+on|are\s+in|is\s+part\s+of|are\s+part\s+of)\s+"
               r"(?:my|the|our)\s+(?P<cat>book\s*club|team|class|study\s+group|gym|club|committee|crew|band|choir)", re.I),
)
# 单成员句·CN
_CN_MEMBER = (
    re.compile(r"(?:我(?:们)?(?:家)?|家里|最近|今年|去年|上个月)(?:最近|今年|去年|上个月|早些时候)?"
               r"(?:养了|领养了|买了|收养了|抱回来)(?:一只|一条|一个|只)?"
               r"(?P<sp>狗|猫|仓鼠|兔子|乌龟|金鱼|鹦鹉|鸟)(?:叫(?P<nm>[\u4e00-\u9fa5A-Za-z]{1,12}))?"),
    re.compile(r"我家(?:的)?(?P<sp>狗|猫|仓鼠|兔子)(?:名字)?叫(?P<nm>[\u4e00-\u9fa5A-Za-z]{1,12})"),
    re.compile(r"我对(?P<sp>花生|海鲜|花粉|尘螨|牛奶|乳糖|麸质|鸡蛋|青霉素)(?:)?过敏"),
    re.compile(r"(?P<nm2>[\u4e00-\u9fa5]{2,4}|[A-Z][a-z]{1,15})加入了我(?:的)?(?P<cat>读书会|团队|小组|俱乐部|班|合唱团|乐队)"),
)
# 字段键值·EN
_FIELD_EN = (
    re.compile(r"\bmy\s+(?P<key>address|phone(?:\s*number)?|email|e-mail|birthday|date\s+of\s+birth|hometown"
               r"|wife|husband|spouse|partner|employer|job|boss|name|job\s+title|emergency\s+contact)\s*"
               r"(?:'s\s+name|\bname\b)?\s*\bis\s+(?P<val>[^\.\!\?\n]{2,120})", re.I),
    re.compile(r"\bI\s+(?:currently\s+)?(?:live|am\s+living)\s+(?:at|in)\s+(?P<val>[^\.\!\?\n]{3,120})", re.I),
    re.compile(r"\bI\s+(?:work|am\s+working)\s+(?:at|for|in)\s+(?P<val>[^\.\!\?\n]{2,120})", re.I),
    re.compile(r"\bI\s+(?:just\s+)?moved\s+to\s+(?P<val>[^\.\!\?\n]{3,120})", re.I),
    re.compile(r"^\s*(?P<key>address|phone|email|birthday|emergency\s+contact)\s*[:：]\s*(?P<val>[^\n]{2,120})",
               re.I | re.M),
    re.compile(r"\bmy\s+(?P<key>emergency\s+contact)\s*[:：]\s*(?P<val>[^\n]{2,120})", re.I),
)
# 字段键值·CN
_FIELD_CN = (
    re.compile(r"我住在(?P<val>[^\。\!\?\n，,]{3,80})"),
    re.compile(r"我的(?P<key>电话|手机|邮箱|地址|生日|老家|家乡|老板|名字)(?:号码)?(?:是|为)(?P<val>[^\。\!\?\n]{2,80})"),
    re.compile(r"我在(?P<val>[^\。\!\?\n，,]{2,60})(?:上班|工作|任职)"),
)
# 枚举成员切分
_SPLIT_RE = re.compile(r"[,;、；]|(?:\s+and\s+)|(?:\s*&\s*)|和|与|跟")
_MEMBER_CLEAN = re.compile(r"^(?:a|an|the|my|our|some|both|一只|一条|一个|种|些)\s+|\s+$")
_QUANT_PRE = re.compile(r"^(?:a\s+little|a\s+bit\s+of|some|several|a\s+few|fluent|basic|conversational|一点|一点)\s+", re.I)
_STOPMEMBER = frozenset({"etc", "and", "or", "the", "others", "stuff", "things", "so", "much",
                         "lot", "lots", "等等", "之类", "等等", "很多", "一些"})
# 垃圾引导词: 枚举串越过句界拖尾(", so careful with...")→首词命中即弃
_JUNK_LEAD = re.compile(r"^(?:so|which|but|that|because|then|although|though|however|while)\b|^I\b", re.I)
# 成员→类别词表(set推断用·EN species/物品→key)
_MEMBER2CAT: tuple[tuple[str, str], ...] = (
    (r"^(?:dog|puppy|cat|kitten|hamster|rabbit|bunny|bird|parrot|fish|turtle|tortoise|goldfish|gerbil|guinea\s*pig)", "pets"),
    (r"^(?:english|french|spanish|german|mandarin|chinese|japanese|italian|portuguese|russian|korean|arabic|hindi|dutch)", "languages"),
    (r"^(?:reading|swimming|chess|hiking|painting|gardening|photography|knitting|cycling|running|yoga|cooking|baking|climbing|skiing|fishing)", "hobbies"),
    (r"^(?:peanut|shellfish|latex|pollen|dust|lactose|gluten|soy|soya|penicillin)", "allergies"),
    (r"^(?:piano|violin|guitar|drums|flute|cello|trumpet)", "instruments"),
    (r"^(?:tennis|football|soccer|basketball|volleyball|badminton|baseball|golf)", "sports"),
    (r"^(?:paris|london|rome|madrid|tokyo|berlin|vienna|amsterdam|barcelona|prague|lisbon|athens|new\s+york|kyoto|oslo)", "travelled"),
)
_MEMBER2CAT_CN: tuple[tuple[str, str], ...] = (
    (r"^(?:狗|猫|仓鼠|兔子|乌龟|金鱼|鹦鹉|鸟)", "pets"),
    (r"^(?:中文|英文|英语|法语|日语|德语|西班牙语|韩语|俄语)", "languages"),
    (r"^(?:读书|游泳|下棋|爬山|画画|摄影|园艺|骑行|跑步|瑜伽|烹饪|钓鱼|滑雪)", "hobbies"),
    (r"^(?:花生|海鲜|花粉|尘螨|牛奶|乳糖|麸质|鸡蛋|青霉素)", "allergies"),
    (r"^(?:巴黎|伦敦|罗马|东京|柏林|维也纳|巴塞罗那|布拉格|纽约|京都)", "travelled"),
)
# 花名册/场所类枚举的类别词面自证(list前导名词含这些→该set)
_INTRO2CAT: tuple[tuple[str, str], ...] = (
    (r"pet", "pets"), (r"hobb(?:y|ies)", "hobbies"), (r"language", "languages"),
    (r"allerg", "allergies"), (r"medic", "medications"), (r"instrument", "instruments"),
    (r"sport", "sports"), (r"skill", "skills"), (r"plant|flower", "plants"),
    (r"subscrip", "subscriptions"), (r"device", "devices"),
    (r"book\s*club|club", "club"), (r"team|crew|staff|committee", "team"),
    (r"宠物", "pets"), (r"爱好|兴趣", "hobbies"), (r"语言", "languages"),
    (r"过敏", "allergies"), (r"特长|技能", "skills"), (r"乐器", "instruments"),
    (r"运动", "sports"), (r"订阅", "subscriptions"), (r"读书会|俱乐部|小组|社团", "club"),
    (r"团队", "team"),
)
_FIELDKEY_NORM = {"phone number": "phone", "e-mail": "email", "date of birth": "birthday",
                  "job": "employer", "boss": "employer", "job title": "job_title",
                  "电话": "phone", "手机": "phone", "手机号": "phone"}
_FIELD_KEYS = frozenset(k for k, _a, _s in _FIELDS)
_LEX_KEYS = frozenset(k for k, _a, _s in _CATS) | {"travelled", "misc"}


def _norm_ws(s: str) -> str:
    return _WS.sub(" ", s).strip()


def _qtokens(q: str) -> list[str]:
    """query词面token(去停用词·小写·中英)·路由与别名交配用。"""
    out = []
    for m in _WORD.finditer(str(q or "").lower()):
        w = m.group(0)
        if len(w) >= _TOKEN_MIN and w not in _QSTOP:
            out.append(w)
    return out


def _tokens_of(s: str) -> set[str]:
    return {m.group(0).lower() for m in _WORD.finditer(str(s or "").lower()) if len(m.group(0)) >= _TOKEN_MIN}


def _cat_of_intro(intro: str) -> str | None:
    """intro名词短语→set_key(词典优先·泛化回落=短语本身规范化)。"""
    il = intro.lower().strip()
    for pat, key in _INTRO2CAT:
        if re.search(pat, il):
            return key
    g = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", il).strip("_")
    return g[:24] or None


def _cat_of_member(member: str) -> str | None:
    ml = member.lower().strip()
    for pat, key in _MEMBER2CAT:
        if re.match(pat, ml):
            return key
    for pat, key in _MEMBER2CAT_CN:
        if re.match(pat, member):
            return key
    return None


def _negated(text: str, start: int, span: int = 42) -> bool:
    """左窗否定守卫: 候选起点前span字符内出现否定词→该候选不抽取(反例防线)。"""
    lo = max(0, start - span)
    return bool(_NEG_RE.search(text[lo:start]))


def _split_members(list_str: str) -> list[str]:
    """枚举串→成员列表(切分/清洗/去量词前缀/去停成员·保原样大小写供卡面摘录)。"""
    parts: list[str] = []
    for seg in _SPLIT_RE.split(list_str):
        seg = _QUANT_PRE.sub("", _MEMBER_CLEAN.sub("", _norm_ws(seg)))
        seg = seg.strip(" .。,，;；、-—")
        if not seg or seg.lower() in _STOPMEMBER or seg in _STOPMEMBER:
            continue
        if _JUNK_LEAD.match(seg):
            continue
        if len(seg) > 60:
            seg = seg[:60]
        parts.append(seg)
    return parts


def _member_norm(member: str, name: str | None) -> str:
    """成员规范化身份: 有专名用专名(小写)·否则短语小写。"""
    if name:
        return name.lower()
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", member.lower())[:40]


def extract_struct(text: str) -> list[dict[str, Any]]:
    """L1: 卡文本→结构化事实列表[{kind,set_key,member_norm,member_raw,detail,retract}]。
    顺序: 撤回→字段→显式枚举→单成员句。零LLM·确定性·单卡≈0.1ms级。"""
    t = _norm_ws(str(text or "").strip())
    if not t or len(t) > _MAX_TEXT:
        return []
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    # ── 撤回(成员退出·优先级最高) ──
    for rx in _RETRACT_RES:
        for m in rx.finditer(t):
            target = m.group(1) if m.lastindex else ""
            target = _MEMBER_CLEAN.sub("", _norm_ws(target)).strip(" .。,，")
            target = re.sub(r"^(?:do|play|use|keep|have|own|practice|collect|any|玩|做|用|练)\s+", "", target)
            target = re.sub(r"\s+(?:anymore|again|now|these\s+days|at\s+the\s+moment)$", "", target, flags=re.I)
            if not target or len(target) < 2:
                continue
            key = _cat_of_member(target) or _cat_of_intro(target) or "misc"
            nm = _member_norm(target, None)
            if (key, nm) in seen:
                continue
            seen.add((key, nm))
            out.append({"kind": "set", "set_key": key, "member_norm": nm,
                        "member_raw": target, "retract": True, "detail": ""})

    # ── 字段键值(最新值胜·抽取层不判新旧) ──
    for rx in _FIELD_EN + _FIELD_CN:
        for m in rx.finditer(t):
            gd = m.groupdict()
            val = _norm_ws(gd.get("val") or "").strip(" .。,，;；")
            key_raw = (gd.get("key") or "").lower().strip()
            if not val:
                continue
            vl = val.lower()
            if vl.startswith(("no ", "not ", "nothing", "没有", "不是")) or vl in ("anything", "nowhere"):
                continue  # 值级否定门("I'm not allergic to anything"形不产字段)
            if not key_raw:  # 无键形(live in/work at/moved to/我住在/我在…工作)→固定映射
                low = m.group(0).lower()
                if re.search(r"\b(?:live|living|moved)\b|住", low):
                    key = "address"
                elif re.search(r"\bwork(?:ing|s)?\b|上班|工作|任职", low):
                    key = "employer"
                else:
                    continue
            else:
                key = _FIELDKEY_NORM.get(key_raw, key_raw)
            if _negated(t, m.start()):
                continue
            if (key, "") in seen:
                continue
            seen.add((key, ""))
            out.append({"kind": "field", "set_key": key[:24], "member_norm": "",
                        "member_raw": val, "retract": False, "detail": ""})

    # ── 显式枚举句 ──
    for rx in _ENUM_EN:
        for m in rx.finditer(t):
            gd = m.groupdict()
            members = _split_members(gd.get("list") or "")
            if not members:
                continue
            cat_raw = gd.get("cat")
            if cat_raw:
                key = _cat_of_intro(cat_raw)
                if key in _FIELD_KEYS:
                    continue  # "my address is X"形归字段刀管·不产杂集(field键与类目冲突守卫)
                if key not in _LEX_KEYS and len(members) < 2:
                    continue  # 泛化类目单成员="my X is Y"叙述句·宁缺勿滥
            else:  # 无intro动词形(enjoy/speak/visited/allergic)→按成员多数投票
                cats = [_cat_of_member(x) for x in members]
                votes: dict[str, int] = {}
                for c in cats:
                    if c:
                        votes[c] = votes.get(c, 0) + 1
                if not votes:
                    continue  # 无法归类→宁缺勿滥(防噪声set)
                # 动词形修正: speak→languages·enjoy/like→hobbies·visited→travelled·allergic→allergies
                if rx is _ENUM_EN[2] and "languages" in votes:
                    key = "languages"
                elif rx is _ENUM_EN[1] and "hobbies" in votes:
                    key = "hobbies"
                elif rx is _ENUM_EN[3] and "travelled" in votes:
                    key = "travelled"
                elif rx is _ENUM_EN[4] and "allergies" in votes:
                    key = "allergies"
                else:
                    key = max(votes.items(), key=lambda kv: (kv[1], kv[0]))[0]
            if key is None or _negated(t, m.start()):
                continue
            if rx is _ENUM_EN[1] and cat_raw is None and len(members) < 2:
                continue  # "I like pizza"单成员like形噪声门
            for mem in members[:_MAX_MEMBERS]:
                nm = _member_norm(mem, None)
                if nm and (key, nm) not in seen:
                    seen.add((key, nm))
                    out.append({"kind": "set", "set_key": key, "member_norm": nm,
                                "member_raw": mem, "retract": False, "detail": ""})
    for rx in _ENUM_CN:
        for m in rx.finditer(t):
            gd = m.groupdict()
            members = _split_members(gd.get("list") or "")
            if not members:
                continue
            cat_raw = gd.get("cat") or ""
            cmap = {"爱好": "hobbies", "兴趣": "hobbies", "语言": "languages", "宠物": "pets",
                    "过敏源": "allergies", "特长": "skills", "技能": "skills", "收藏": "misc",
                    "乐器": "instruments", "订阅": "subscriptions"}
            if cat_raw:
                key = cmap.get(cat_raw) or _cat_of_intro(cat_raw)
            else:
                cats = [_cat_of_member(x) for x in members]
                votes: dict[str, int] = {}
                for c in cats:
                    if c:
                        votes[c] = votes.get(c, 0) + 1
                if not votes:
                    continue
                if rx is _ENUM_CN[1] and "languages" in votes:
                    key = "languages"
                elif rx is _ENUM_CN[2] and "travelled" in votes:
                    key = "travelled"
                elif rx is _ENUM_CN[3] and "allergies" in votes:
                    key = "allergies"
                else:
                    key = max(votes.items(), key=lambda kv: (kv[1], kv[0]))[0]
            if key is None or _negated(t, m.start()):
                continue
            for mem in members[:_MAX_MEMBERS]:
                nm = _member_norm(mem, None)
                if nm and (key, nm) not in seen:
                    seen.add((key, nm))
                    out.append({"kind": "set", "set_key": key, "member_norm": nm,
                                "member_raw": mem, "retract": False, "detail": ""})

    # ── 单成员句(跨会话累计的主力) ──
    def _emit(key: str | None, pairs: list[tuple[str, str | None]], where: str) -> None:
        if key is None:
            return
        for raw, nm in pairs[:_MAX_MEMBERS]:
            if not raw or _negated(t, m.start()):
                continue
            mnm = _member_norm(raw, nm)
            if mnm and (key, mnm) not in seen:
                seen.add((key, mnm))
                out.append({"kind": "set", "set_key": key, "member_norm": mnm,
                            "member_raw": raw, "retract": False, "detail": ""})
    for rx in _EN_MEMBER:
        for m in rx.finditer(t):
            gd = m.groupdict()
            sp = gd.get("sp") or ""
            nm = gd.get("nm1") or gd.get("nm2") or ""
            cat_raw = gd.get("cat") or ""
            if cat_raw:  # joined-my-club形: 成员=专名(名单逗号/and型拆开)·set=类别短语
                names = [p for p in _split_members(nm) if p] or ([nm] if nm else [])
                _emit(_cat_of_intro(cat_raw), [(x, x) for x in names], "en_club")
            elif sp:
                if nm:
                    raw = f"{sp} named {nm}" if gd.get("nm1") else f"{sp}, {nm}"
                    _emit(_cat_of_member(sp), [(raw, nm)], "en_pet")
                else:
                    _emit(_cat_of_member(sp), [(sp, None)], "en_pet")
    for rx in _CN_MEMBER:
        for m in rx.finditer(t):
            gd = m.groupdict()
            sp = gd.get("sp") or ""
            nm = gd.get("nm") or gd.get("nm2") or ""
            cat_raw = gd.get("cat") or ""
            if cat_raw:
                cmap = {"读书会": "club", "团队": "team", "小组": "club", "俱乐部": "club",
                        "班": "team", "合唱团": "club", "乐队": "club"}
                key = cmap.get(cat_raw) or _cat_of_intro(cat_raw)
                _emit(key, [(nm, nm) for _ in [0] if nm], "cn_club")
            elif sp:
                if nm:
                    _emit(_cat_of_member(sp), [(f"{sp}叫{nm}", nm)], "cn_pet")
                else:
                    _emit(_cat_of_member(sp), [(sp, None)], "cn_pet")
    return out


# ═══════════════════════════ 持久化+缓存(sqlite WAL·cfact同款写时失效) ═══════════════════════════
_DDL = (
    """CREATE TABLE IF NOT EXISTS struct_items(
        tenant TEXT NOT NULL, kind TEXT NOT NULL, set_key TEXT NOT NULL,
        member_norm TEXT NOT NULL, member_raw TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '',
        retract INTEGER NOT NULL DEFAULT 0, cid TEXT NOT NULL DEFAULT '', sess TEXT NOT NULL DEFAULT '',
        dt TEXT NOT NULL DEFAULT '', obs_ms INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(tenant, kind, set_key, member_norm))""",
    "CREATE INDEX IF NOT EXISTS idx_k12_tenant ON struct_items(tenant, kind)",
)


class Knife12:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        for stmt in _DDL:
            self._conn.execute(stmt)
        self._conn.commit()
        self._lock = threading.Lock()
        self._cache: dict[str, list[dict[str, Any]]] = {}   # tenant→rows(写时失效)
        self.n_obs = 0       # 观察卡数
        self.n_extract = 0   # 抽出结构事实数
        self.n_inject = 0    # 注入专卡数
        self.n_route = 0     # A3意图命中次数
        self.n_err = 0

    # ── ADD相 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> int:
        """逐卡观察: extract_struct→struct_items落库(幂等·批提交·写时失效缓存)。
        set成员: INSERT OR IGNORE(成员单调累计); 撤回行: UPSERT置retract=1;
        field行: ON CONFLICT UPDATE最新值胜(旧值入detail.prev可溯源)。
        fail-soft: 单卡失败只计数不抛(绝不影响add)。返回抽出事实数。"""
        if not cards or not _master_on():
            return 0
        rows_new: list[tuple] = []
        rows_retract: list[tuple] = []
        rows_field: list[tuple] = []
        for c in cards or []:
            try:
                text = str(c.get("text", ""))
                if not text:
                    continue
                facts = extract_struct(text)
                self.n_obs += 1
                if not facts:
                    continue
                cid = hashlib.sha1(f"{c.get('sess', '')}|{c.get('dt', '')}|{text}".encode("utf-8")).hexdigest()[:16]
                dt = str(c.get("dt", "") or "")
                for f in facts:
                    self.n_extract += 1
                    if f["kind"] == "field":
                        rows_field.append((tenant, "field", f["set_key"], "", f["member_raw"],
                                           cid, str(c.get("sess", "")), dt,
                                           int(time.time() * 1000)))
                    elif f.get("retract"):
                        rows_retract.append((tenant, "set", f["set_key"], f["member_norm"],
                                             f["member_raw"], cid, str(c.get("sess", "")), dt,
                                             int(time.time() * 1000)))
                    else:
                        rows_new.append((tenant, "set", f["set_key"], f["member_norm"],
                                         f["member_raw"], cid, str(c.get("sess", "")), dt,
                                         int(time.time() * 1000)))
            except Exception:  # noqa: BLE001 — 单卡失败不拖垮批
                self.n_err += 1
        if rows_new or rows_retract or rows_field:
            with self._lock:
                for r in rows_new:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO struct_items"
                        "(tenant,kind,set_key,member_norm,member_raw,cid,sess,dt,obs_ms,retract,detail)"
                        " VALUES(?,?,?,?,?,?,?,?,?,0,'')", r)
                for r in rows_retract:
                    self._conn.execute(
                        "INSERT INTO struct_items"
                        "(tenant,kind,set_key,member_norm,member_raw,cid,sess,dt,obs_ms,retract,detail)"
                        " VALUES(?,?,?,?,?,?,?,?,?,1,'')"
                        " ON CONFLICT(tenant,kind,set_key,member_norm)"
                        " DO UPDATE SET retract=1, member_raw=excluded.member_raw,"
                        " cid=excluded.cid, sess=excluded.sess, dt=excluded.dt, obs_ms=excluded.obs_ms",
                        r)
                for r in rows_field:
                    self._conn.execute(
                        "INSERT INTO struct_items"
                        "(tenant,kind,set_key,member_norm,member_raw,cid,sess,dt,obs_ms,retract,detail)"
                        " VALUES(?,?,?,?,?,?,?,?,?,0,'')"
                        " ON CONFLICT(tenant,kind,set_key,member_norm)"
                        " DO UPDATE SET"
                        " detail=CASE WHEN member_raw!=excluded.member_raw"
                        "   THEN json_object('prev',member_raw,'prev_dt',dt) ELSE detail END,"
                        " member_raw=excluded.member_raw, cid=excluded.cid, sess=excluded.sess,"
                        " dt=excluded.dt, obs_ms=excluded.obs_ms",
                        r)
                self._conn.commit()
                self._cache.pop(tenant, None)   # 写时失效(cfact同款)
        return self.n_extract

    def _load(self, tenant: str) -> list[dict[str, Any]]:
        cached = self._cache.get(tenant)
        if cached is not None:
            return cached
        with self._lock:
            raw = self._conn.execute(
                "SELECT kind,set_key,member_norm,member_raw,detail,retract,dt,sess"
                " FROM struct_items WHERE tenant=? ORDER BY obs_ms", (tenant,)).fetchall()
        rows = [{"kind": r[0], "set_key": r[1], "member_norm": r[2], "member_raw": r[3],
                 "detail": r[4], "retract": int(r[5]), "dt": r[6] or "", "sess": r[7] or ""}
                for r in raw]
        self._cache[tenant] = rows
        return rows

    # ── SEARCH相: A3意图门 + 集合路由 + 专卡注入 ──
    _LIST_Q = re.compile(
        r"\b(?:list|name|enumerate|mention|recite)\s+(?:all|every|each|out|them)\b"
        r"|\ball\s+(?:of\s+)?(?:my|the|our)\b|\bwhat\s+(?:are|were)\b[^?]{0,40}\?"
        r"|\bwhich\s+\w+\b|\bwhat\s+\w+\s+do\s+I\s+have\b|\bdo\s+I\s+have\b"
        r"|列出|所有|哪些|都有什[么么]|全部|都有些什么", re.I)
    _COUNT_Q = re.compile(
        r"\bhow\s+(?:many|much)\b|\bnumber\s+of\b|\bwhat(?:'s| is)\s+the\s+(?:total|number|count)\b"
        r"|多少|几个|几只|几位|几条|数量|几个", re.I)
    _FIELD_Q = re.compile(
        r"\bwhat(?:'s| is)\s+(?:my|the)\b|\bwhich\s+(?:address|phone|email)\b"
        r"|我的|地址|电话|邮箱|是什么", re.I)

    def _sets_of(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """rows→{set_key: {members:[(norm,raw,dt)], retracted:set}}·确定性序。"""
        sets: dict[str, dict[str, Any]] = {}
        for r in rows:
            if r["kind"] != "set":
                continue
            s = sets.setdefault(r["set_key"], {"members": [], "retracted": set(),
                                               "seen": {}, "dts": []})
            if r["retract"]:
                s["retracted"].add(r["member_norm"])
            elif r["member_norm"] not in s["seen"]:
                s["seen"][r["member_norm"]] = r
                s["members"].append(r)
                if r["dt"]:
                    s["dts"].append(r["dt"][:10])
        for s in sets.values():  # 撤回剔除(后写胜: obs_ms序撤回行在成员行后)
            s["members"] = [m for m in s["members"] if m["member_norm"] not in s["retracted"]]
            s["dts"] = [d for _i, m in enumerate(s["members"]) for d in [m["dt"][:10]] if d]
        return sets

    def route(self, query: str, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        """A3意图门+路由: →(命中sets列表[相关性降序≤_MAX_SETS_ROUTE], 命中fields, intent)。
        意图: list/count/field/none。词面交打分·零嵌入零LLM。"""
        q = str(query or "")
        qt = _qtokens(q)
        is_list = bool(self._LIST_Q.search(q))
        is_count = bool(self._COUNT_Q.search(q))
        is_field = bool(self._FIELD_Q.search(q))
        intent = "list" if is_list and not is_count else ("count" if is_count else ("field" if is_field else "none"))
        if intent == "none" and not qt:
            return [], [], "none"
        sets = self._sets_of(rows)
        scored: list[tuple[int, int, str, dict[str, Any]]] = []
        for key, s in sets.items():
            aliases = _CAT_ALIAS.get(key, ())
            alias_tok: set[str] = set()
            for a in aliases:
                alias_tok |= _tokens_of(a)
                if a and a in q.lower():
                    alias_tok.add(a)
            alias_hit = len(set(qt) & alias_tok) + sum(1 for a in aliases if a and a in q.lower())
            mem_hit = 0
            for m in s["members"]:
                mem_hit += len(set(qt) & (_tokens_of(m["member_raw"]) | _tokens_of(m["member_norm"])))
            if intent in ("list", "count"):
                score = alias_hit * 2 + mem_hit
                gate = alias_hit >= 1 or mem_hit >= 1
            else:
                score = alias_hit * 2
                gate = alias_hit >= 1
            if gate:
                scored.append((score, len(s["members"]), key, s))
        scored.sort(key=lambda x: (-x[0], -x[1], x[2]))
        picked = scored[:_MAX_SETS_ROUTE]
        # 敏感域守护: 泛化命中(仅成员词面·无域别名显式命中)不注入敏感集合(k2精神)
        picked = [p for p in picked
                  if p[2] not in _SENSITIVE or any(a and a in q.lower() for a in _CAT_ALIAS.get(p[2], ()))]
        # 字段路由: query词面与field别名交
        fhit: list[tuple[int, str]] = []
        if intent in ("field", "list"):
            for r in rows:
                if r["kind"] != "field":
                    continue
                aliases = dict((_fk, _fa) for _fk, _fa, _s in _FIELDS).get(r["set_key"], (r["set_key"],))
                hit = sum(1 for a in aliases if a and a in q.lower())
                if hit >= 1:
                    fhit.append((hit, r["set_key"]))
        fields: list[dict[str, Any]] = []
        seen_f: set[str] = set()
        for _h, key in sorted(fhit, key=lambda x: (-x[0], x[1])):
            for r in rows:
                if r["kind"] == "field" and r["set_key"] == key and key not in seen_f:
                    seen_f.add(key)
                    fields.append(r)
                    break
        if intent == "none" and not picked and not fields:
            return [], [], "none"
        return [p[3] and {"key": p[2], **p[3]} for p in picked], fields, intent

    def _card(self, content: str, dt: str) -> dict[str, str]:
        return {"id": hashlib.sha1(content.encode("utf-8")).hexdigest()[:16],
                "content": content, "created_at": dt or ""}

    def build_cards(self, query: str, sets: list[dict[str, Any]],
                    fields: list[dict[str, Any]], intent: str) -> list[dict[str, str]]:
        """专卡生成(确定性·纯摘录零编造·禁4✓): list→[StructSet] count→[StructCount]
        field→[StructField]。敏感集建卡同路由门(不过门不建)。"""
        cards: list[dict[str, str]] = []
        for s in sets[:_MAX_INJECT]:
            members = s["members"][:_MAX_MEMBERS]
            if not members:
                continue
            label = _CAT_ALIAS.get(s["key"], (s["key"],))[0] if s["key"] in _CAT_ALIAS else s["key"].replace("_", " ")
            names = " / ".join(m["member_raw"] for m in members)
            if intent == "count":
                content = (f"[StructCount] {label} count = {len(members)}: {names}"
                           f" (from {len(members)} recorded statements"
                           + (f", latest {max(s['dts'])}" if s["dts"] else "") + ")")
            else:
                content = (f"[StructSet] {label} — {len(members)} in total: {names}"
                           + (f" (recorded: {', '.join(sorted(set(s['dts'])))})" if s["dts"] else ""))
            cards.append(self._card(content, max(s["dts"]) if s["dts"] else ""))
        for f in fields[:max(_MAX_INJECT - len(cards), 0)]:
            detail = ""
            if f["detail"]:
                try:
                    import json as _j
                    _d = _j.loads(f["detail"])
                    if _d.get("prev"):
                        detail = f"; previously {_d['prev']}"
                except Exception:  # noqa: BLE001
                    detail = ""
            if f["dt"] and detail:
                content = (f"[StructField] {f['set_key'].replace('_', ' ')} = {f['member_raw']}"
                           f" (updated {f['dt'][:10]}{detail})")
            elif f["dt"]:
                content = (f"[StructField] {f['set_key'].replace('_', ' ')} = {f['member_raw']}"
                           f" (as of {f['dt'][:10]})")
            elif detail:
                content = (f"[StructField] {f['set_key'].replace('_', ' ')} = {f['member_raw']}"
                           f" ({detail.lstrip('; ')})")
            else:
                content = f"[StructField] {f['set_key'].replace('_', ' ')} = {f['member_raw']}"
            cards.append(self._card(content, f["dt"] or ""))
        return cards[:_MAX_INJECT]

    def inject(self, tenant: str, query: str, data: list[dict[str, Any]],
               top_k: int) -> tuple[list[dict[str, Any]], int]:
        """SEARCH相钩子(g4块后·csan前·k9同位): 非A3 query字节级直通(零突变);
        A3 query→专卡注入位1-3(rank0不动)·已在窗跳过·满窗自尾挤位守top_k契约·
        确定性·fail-soft(异常=原窗直通)。"""
        if not _master_on() or not tenant or not isinstance(data, list) or not str(query or "").strip():
            return data, 0
        try:
            t0 = time.perf_counter()
            rows = self._load(tenant)
            if not rows:
                return data, 0
            sets, fields, intent = self.route(query, rows)
            if intent == "none" or (not sets and not fields):
                return data, 0
            self.n_route += 1
            cards = self.build_cards(query, sets, fields, intent)
            if not cards:
                return data, 0
            existing = {str(d.get("content", "")) for d in data}
            out = list(data)
            n = 0
            pos = 1  # rank0不动·位1起插
            for c in cards:
                if c["content"] in existing:
                    continue
                out.insert(min(pos, len(out)), c)
                existing.add(c["content"])
                pos += 1
                n += 1
            if n and len(out) > max(int(top_k), 1):  # top_k契约: 原卡保(cap-n)张·注入卡全保·原序
                cap = max(int(top_k), 1)
                inj_ids = {c["id"] for c in cards}
                keep_inj_left = n
                keep_orig_left = cap - n
                o2: list[dict[str, Any]] = []
                for d in out:
                    if str(d.get("id", "")) in inj_ids:
                        if keep_inj_left > 0:
                            o2.append(d)
                            keep_inj_left -= 1
                    elif keep_orig_left > 0:
                        o2.append(d)
                        keep_orig_left -= 1
                out = o2
            dt_ms = (time.perf_counter() - t0) * 1000
            self.n_inject += n
            log.info("[k12] tenant=%s intent=%s injected=%d kept=%d %.1fms",
                     tenant[:16], intent, n, len(out), dt_ms)
            return out, n
        except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
            self.n_err += 1
            log.error("knife12 inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
            return data, 0

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {"db": str(self.db_path),
                    "rows": g("SELECT COUNT(*) FROM struct_items"),
                    "set_members": g("SELECT COUNT(*) FROM struct_items WHERE kind='set' AND retract=0"),
                    "retracted": g("SELECT COUNT(*) FROM struct_items WHERE retract=1"),
                    "fields": g("SELECT COUNT(*) FROM struct_items WHERE kind='field'"),
                    "tenants": g("SELECT COUNT(DISTINCT tenant) FROM struct_items"),
                    "obs_cards_total": self.n_obs, "extract_total": self.n_extract,
                    "route_total": self.n_route, "inject_total": self.n_inject,
                    "err_total": self.n_err, "master": _master_on()}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部struct行(organ卡不在本模块管辖)。"""
        with self._lock:
            cur = self._conn.execute("DELETE FROM struct_items WHERE tenant=?", (tenant,))
            self._conn.commit()
            self._cache.pop(tenant, None)
        return cur.rowcount


# ── 模块级单例(aml_service经此挂接·knife2/11同款) ──────────────────────────────
_K12: Knife12 | None = None
_K12_LOCK = threading.Lock()


def get() -> Knife12:
    global _K12
    with _K12_LOCK:
        if _K12 is None:
            _K12 = Knife12(DB_PATH)
            log.info("knife12 init db=%s cats=%d fields=%d", _K12.db_path, len(_CATS), len(_FIELDS))
        return _K12


def observe(tenant: str, cards: list[dict[str, Any]]) -> int:
    return get().observe(tenant, cards)


def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int) -> tuple[list[dict[str, Any]], int]:
    return get().inject(tenant, query, data, top_k)


def stats() -> dict[str, Any]:
    return get().stats()


def purge_tenant(tenant: str) -> int:
    return get().purge_tenant(tenant)


# ═══════════════════════════ 自检段(--selftest·10合成用例) ═══════════════════════════
# 测试数据全部自造合成(零官方题库内容)·正反例×枚举/计数/字段·中英+性能预算门。

_SELFTEST_CASES: list[dict[str, Any]] = [
    # P1 单句枚举EN(list intent·set卡全量)
    dict(name="P1_单句枚举EN_list", tenant="sb_t1",
         cards=[{"text": "My hobbies are reading, swimming, chess and photography.",
                 "sess": "s1", "dt": "2026-01-05T10:00:00"}],
         query="List all of my hobbies.", intent="list",
         set_key="hobbies", want_members=["reading", "swimming", "chess", "photography"],
         want_card="[StructSet]"),
    # P2 跨会话单成员累计(count intent·计数卡=3)
    dict(name="P2_跨会话宠物累计_count", tenant="sb_t2",
         cards=[{"text": "I adopted a dog named Rex last spring.", "sess": "s1", "dt": "2026-01-05T10:00:00"},
                {"text": "We got a new cat, Mia — she rules the house.", "sess": "s2", "dt": "2026-01-20T10:00:00"},
                {"text": "Earlier this year the hamster Pip joined the family.", "sess": "s3", "dt": "2026-02-01T10:00:00"}],
         query="How many pets do I have?", intent="count",
         set_key="pets", want_members=["rex", "mia", "pip"], want_card="[StructCount]"),
    # P3 字段现值+更新溯源(field intent·最新值胜·prev保留)
    dict(name="P3_字段更新溯源", tenant="sb_t3",
         cards=[{"text": "My address is 221B Baker Street, London.", "sess": "s1", "dt": "2026-01-05T10:00:00"},
                {"text": "Big news: I moved to 12 Arcadia Gardens, Oxford.", "sess": "s4", "dt": "2026-03-10T10:00:00"}],
         query="What is my address?", intent="field",
         want_field=("address", "12 Arcadia Gardens, Oxford"), want_prev="221B Baker Street, London",
         want_card="[StructField]"),
    # P4 中文枚举(CN list intent)
    dict(name="P4_中文枚举_list", tenant="sb_t4",
         cards=[{"text": "我会说中文、英文和日语。", "sess": "s1", "dt": "2026-01-08T10:00:00"}],
         query="列出所有我会说的语言", intent="list",
         set_key="languages", want_members=["中文", "英文", "日语"], want_card="[StructSet]"),
    # P5 中文宠物计数(CN count)
    dict(name="P5_中文宠物计数", tenant="sb_t5",
         cards=[{"text": "我们家养了一只狗叫大黄。", "sess": "s1", "dt": "2026-01-02T10:00:00"},
                {"text": "最近领养了一只猫叫咪咪。", "sess": "s2", "dt": "2026-01-15T10:00:00"}],
         query="我有几只宠物？", intent="count",
         set_key="pets", want_members=["大黄", "咪咪"], want_card="[StructCount]"),
    # P6 人群计数(joined-club形·count=3)
    dict(name="P6_人群计数_joined", tenant="sb_t6",
         cards=[{"text": "Alice, Bob and Carol joined my book club this year.", "sess": "s1", "dt": "2026-02-02T10:00:00"}],
         query="How many people are in my book club?", intent="count",
         set_key="club", want_members=["alice", "bob", "carol"], want_card="[StructCount]"),
    # N1 否定守卫: 否定句零抽取(反例)
    dict(name="N1_否定句零抽取", tenant="sb_n1",
         cards=[{"text": "I don't have any pets, our building forbids animals.", "sess": "s1", "dt": "2026-01-05T10:00:00"},
                {"text": "Luckily I'm not allergic to anything.", "sess": "s1", "dt": "2026-01-05T10:01:00"}],
         query="List all of my pets.", intent="list",
         set_key="pets", want_members=[], want_zero=True),
    # N2 非A3 query零突变(字节级直通·反例)
    dict(name="N2_非A3查询零突变", tenant="sb_n2",
         cards=[{"text": "My hobbies are reading and swimming.", "sess": "s1", "dt": "2026-01-05T10:00:00"}],
         query="Tell me about our last conversation.", intent="none",
         want_zero=True, window=[{"id": "w1", "content": "narrative card one", "created_at": "2026-01-05T10:00:00"},
                                 {"id": "w2", "content": "narrative card two", "created_at": "2026-01-06T10:00:00"}]),
    # N3 撤回成员退出集合(list卡不再含photography)
    dict(name="N3_撤回成员退出", tenant="sb_n3",
         cards=[{"text": "My hobbies are reading, swimming, chess and photography.", "sess": "s1", "dt": "2026-01-05T10:00:00"},
                {"text": "Update: I no longer do photography, sold my camera.", "sess": "s4", "dt": "2026-03-10T10:00:00"}],
         query="What are all of my hobbies?", intent="list",
         set_key="hobbies", want_members=["reading", "swimming", "chess"], want_absent=["photography"],
         want_card="[StructSet]"),
    # N4 敏感域守护: 泛化query不注入医疗集合(仅成员词面命中不够)
    dict(name="N4_敏感域守护", tenant="sb_n4",
         cards=[{"text": "I'm allergic to peanuts and shellfish.", "sess": "s1", "dt": "2026-01-05T10:00:00"}],
         query="What foods should the restaurant avoid for me?", intent="list",
         set_key="allergies", want_members=["peanuts", "shellfish"],
         want_no_inject=True, want_card=None),
]


def selftest() -> int:
    """10项烟测: P1-P6正例+N1-N4反例+性能预算门。返回失败数(0=全绿)。"""
    import tempfile
    fails: list[str] = []
    tmp = tempfile.mkdtemp(prefix="k12self_")
    t_all = time.perf_counter()
    for i, tc in enumerate(_SELFTEST_CASES):
        name = tc["name"]
        try:
            db = Path(tmp) / f"{name}.db"
            k = Knife12(db)
            cards = [{"text": c["text"], "sess": c.get("sess", "s"), "dt": c.get("dt", "")}
                     for c in tc["cards"]]
            k.observe(tc["tenant"], cards)
            rows = k._load(tc["tenant"])
            window = list(tc.get("window") or [{"id": "w0", "content": "filler", "created_at": ""}])
            out, n = k.inject(tc["tenant"], tc["query"], window, top_k=100)
            # 断言1: 抽取层(set成员)
            if "want_members" in tc:
                sets = k._sets_of(rows)
                got = sorted(m["member_norm"] for m in sets.get(tc["set_key"], {}).get("members", []))
                want = sorted(tc["want_members"])
                if tc.get("want_zero"):
                    ok = len(got) == 0
                else:
                    ok = all(w in got for w in want) and (len(got) == len(want) if tc.get("want_absent") else True)
                    if tc.get("want_absent"):
                        ok = ok and not any(a in got for a in tc["want_absent"])
                if not ok:
                    fails.append(f"{name}: members want={want} got={got}")
            # 断言2: 字段(最新值+prev)
            if "want_field" in tc:
                fk, fv = tc["want_field"]
                frow = next((r for r in rows if r["kind"] == "field" and r["set_key"] == fk), None)
                if frow is None or fv.lower() not in frow["member_raw"].lower():
                    fails.append(f"{name}: field {fk} want={fv} got={frow and frow['member_raw']}")
                elif tc.get("want_prev"):
                    prev = ""
                    if frow["detail"]:
                        import json as _j
                        try:
                            prev = str(_j.loads(frow["detail"]).get("prev", ""))
                        except Exception:  # noqa: BLE001
                            pass
                    if tc["want_prev"].lower() not in prev.lower():
                        fails.append(f"{name}: prev want={tc['want_prev']} got={prev!r}")
            # 断言3: 注入行为
            if tc.get("want_zero"):
                if n != 0:
                    fails.append(f"{name}: want zero inject got n={n}")
                if "window" in tc and out != window:
                    fails.append(f"{name}: want byte-identical window")
            elif tc.get("want_no_inject"):
                if n != 0:
                    fails.append(f"{name}: want no-inject(sensitive guard) got n={n}")
            else:
                if n == 0:
                    fails.append(f"{name}: want inject got n=0")
                elif tc.get("want_card") and not any(str(d.get("content", "")).startswith(tc["want_card"]) for d in out):
                    fails.append(f"{name}: card prefix {tc['want_card']} missing in {[d['content'][:40] for d in out[:3]]}")
                elif "want_members" in tc and not tc.get("want_zero"):
                    joined = " ".join(str(d.get("content", "")) for d in out)
                    missing = [w for w in tc["want_members"] if w.lower() not in joined.lower()]
                    if missing:
                        fails.append(f"{name}: members missing from window: {missing}")
            k.purge_tenant(tc["tenant"])
        except Exception as exc:  # noqa: BLE001
            fails.append(f"{name}: EXC {type(exc).__name__}: {exc}")
    # 性能预算门: observe<5ms/卡·inject<10ms/查询
    db = Path(tmp) / "perf.db"
    k = Knife12(db)
    bulk = [{"text": f"Session note {i}: I adopted a dog named Dog{i} and my hobby is reading.",
             "sess": f"s{i % 50}", "dt": "2026-01-05T10:00:00"} for i in range(3000)]
    t0 = time.perf_counter()
    k.observe("sb_perf", bulk)
    obs_ms = (time.perf_counter() - t0) * 1000 / max(len(bulk), 1)
    for i in range(600):  # 第二租户600成员(倒逼路由打分走真实词面交路径)
        k.observe("sb_perf2", [{"text": f"I adopted a dog named Dog{i} last month.", "sess": "p", "dt": ""}])
    t0 = time.perf_counter()
    for _ in range(50):
        out_p, n_p = k.inject("sb_perf2", "List all of my pets.",
                              [{"id": "w", "content": "x", "created_at": ""}], 100)
    inj_ms = (time.perf_counter() - t0) * 1000 / 50
    if n_p < 1:  # 注入路径必须真实发生(防空租户早退使性能门空转)
        fails.append("PERF: inject produced no card on populated tenant (routing dead?)")
    if obs_ms >= 5.0:
        fails.append(f"PERF: observe {obs_ms:.3f}ms/card >= 5ms budget")
    if inj_ms >= 10.0:
        fails.append(f"PERF: inject {inj_ms:.2f}ms/query >= 10ms budget")
    total_s = time.perf_counter() - t_all
    print(f"knife12 selftest: {len(_SELFTEST_CASES)} cases + perf gate · fails={len(fails)} · {total_s:.2f}s")
    print(f"  observe={obs_ms:.3f}ms/card (budget<5) · inject={inj_ms:.2f}ms/query (budget<10)")
    for f_ in fails:
        print(f"  FAIL {f_}")
    print("  RESULT:", "ALL GREEN" if not fails else "RED")
    return len(fails)


if __name__ == "__main__":
    import sys
    sys.exit(0 if selftest() == 0 else 1)
