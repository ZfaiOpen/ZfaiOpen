# knife11_forget.py — 部署线第11刀·D3删除遗忘抑制刀(删除指令识别→tombstone→检索过滤→FSRS遗忘衰减·原型v1·20261022)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方D3(删除/遗忘/抑制)=0.00 — 引擎无删除语义: 评测在add内容里发删除指令类话型,
#       之后考"已删信息是否还能/不应再被检索到"; 现引擎把删除指令当普通记忆存档并照常返回。
# 既有件复用(对齐家规·零重复造轮):
#   knife2_sensitive.py   = 边车骨架范本(observe标注/filter_gate放行门/extra_fetch超采补偿/
#                           purge_tenant·本件逐一同构)。
#   knife1_supersede.py   = 双时相语义(软删不硬删·旧卡保留只在当前态排除·C3时间线禁回退)。
#   deep_supersede.py     = "标记不删除"先例(superseded_by列)。
#   memory_service.py:605-675 = /forget端点(服务级·评测期不背调用) + _svc_forget(硬删组织件
#                           —本件不复用其硬删: 软删零organ改动=冻结核心铁律)。
#   fsrs_engine.py:35/94-95 = retrievability + <0.1跳过 — 遗忘衰减的既有度量(本件衰减=把
#                           tombstoned卡在重排先验中视ret=0, 等效exp(-∞); 物理purge走30天删除律)。
# 四层架构:
#   L1 识别(ADD相)  detect_deletion(text): 词典+模式三槽(动作类型del/forget/suppress,
#                   目标指称span, 实词集) — 零LLM·逐卡正则≈0.1ms。
#   L2 标记(ADD相)  tombstone落盘: 指令→历史卡按(域词面倒排优先→实词交≥2兜底)匹配 →
#                   tombstones表(tenant,card_id,instr_cid,action,marked_ms)。约束: 有证据交
#                   才标(零过度删除·孤指令只记instructions); organ卡本体不动(软删);
#                   卡文镜像card_texts供匹配(k7_cards存文先例)。
#   L3 过滤(SEARCH相) filter_gate: 非审计意图→tombstoned卡整卡摘除(禁4✓卡不出现=字面量必
#                   不出现·knife2滤除选型三理由); 超采补偿extra_fetch(knife2同款);
#                   删除审计意图(query问删除事件本身)→豁免: 全窗放行+删除指令原句卡注入
#                   (评审直读"删除已发生"·用户原句摘录=纯抽取零编造)。
#   L4 衰减(SEARCH相) decay_ids: 重排先验里tombstoned卡ret视为0(防过滤漏网的派生新卡浮顶);
#                   物理purge由30天删除律家规接管(不在本件)。
# 挂接(4处·回滚=摘4行+删本件): ①aml_service import区 ②_knives_process加k11.observe一行
#   ③search相超采extra_fetch加进ik(k1/k2同位aml_service.py:1044-1054) ④k2闸
#   (aml_service.py:1311)后加filter_gate+审计注入。
# digest联动: observe返回受影响会话 — 调用方可触发k7 digest v+1重建(被删句不再进代表句引文)。
# 持久化   knife11.db (sqlite WAL + synchronous=NORMAL·与knife1/2/7同款纪律·重启不丢)。
# 卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与aml_service._evidence_id同源同值(knife1/2同法)。
# 合规: 日志零内容 — 只记计数/id哈希/动作类型; 不落query/文本/主语。env总闸BRAINOS_K11(缺省开)。
# [W1沙箱修正20261023·实证驱动] ①detect_deletion中文段切2元组(整段子串匹配使zh指令matched=0);
#   ②filter_gate双通道: id门+内容包含网(organ卡面=fact-time面文本→mirror id漂移, 纯id门空转;
#   网覆盖原文/时间线/cc派生原子卡/指令卡全通道), 快路径改租户级存在性判定。
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

log = logging.getLogger("knife11")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KNIFE11_DB", str(APP_DIR.parent / "data" / "knife11.db")))


# ── 总闸与阈值(env可调) ─────────────────────────────────────────────────────
def _master_on() -> bool:
    return os.environ.get("BRAINOS_K11", "1") != "0"


_MAX_MATCH_PER_INSTR = max(1, int(os.environ.get("BRAINOS_K11_MAX_MATCH", "32")))  # 单指令最多标卡数(防误伤面失控)
_MAX_INJECT = max(1, int(os.environ.get("BRAINOS_K11_MAX_INJECT", "1")))           # 审计意图指令卡注入上限
_MIN_WORD_HITS = max(1, int(os.environ.get("BRAINOS_K11_MIN_WORD_HITS", "2")))     # 实词交兜底门(卡文须含≥N指令实词)
_SCAN_CAP = max(200, int(os.environ.get("BRAINOS_K11_SCAN_CAP", "5000")))          # 兜底实词交扫描卡数上限(评测租户小库·分块)

# ── L1: 删除指令识别词典(话型×动作类型) ────────────────────────────────────
# 话型三类: 硬删除(del) / 遗忘请求(forget) / 抑制撤回(suppress)。动词组命中才判指令
# (纯"delete"词面不判·防误伤); 动作类型按下标映射, 改词典须同步改_ACTION_OF。
_DEL_VERBS = (
    r"\b(?:please\s+)?(?:permanently\s+)?delete\s+(?:my|the|that|all|everything|it\b)",  # 0 del
    r"\bremove\s+(?:my|the|that|all|everything|it\b)",                                    # 1 del
    r"\berase\s+(?:my|the|that|all)\b",                                                   # 2 del
    r"\bwipe\s+(?:my|the|that|all)\b",                                                    # 3 del
    r"\bforget\s+(?:that|about|everything|all|what)\b",                                   # 4 forget
    r"\bdon'?t\s+(?:remember|keep|store|use|mention|save|bring)\b",                       # 5 suppress
    r"\bstop\s+(?:remembering|keeping|storing|tracking)\b",                               # 6 suppress
    r"\b(?:I|I'd|i)\s+(?:retract|withdraw)\b",                                            # 7 suppress
    r"\b(?:actually\s+)?ignore\s+(?:what\s+I\s+said|my\s+earlier|that\s+I)",              # 8 suppress
    r"\b(?:strike|scratch)\s+that\b",                                                     # 9 suppress
    r"(?:删除|删掉|清除|抹掉)(?:我|我说的|关于|我的|那个|之前|所有)",                       # 10 del
    r"(?:请)?(?:忘记|忘掉)(?:我|关于|之前|我说过|那)",                                     # 11 forget
    r"(?:当作?没说过|不算了|撤回|作废)",                                                   # 12 suppress
    r"(?:不要?再?(?:记|存|保留|提)|别再?记)",                                              # 13 suppress
)
_DEL_VERB_RES = tuple(re.compile(rx, re.IGNORECASE) for rx in _DEL_VERBS)
_ACTION_OF = ("del", "del", "del", "del", "forget", "suppress", "suppress", "suppress",
              "suppress", "suppress", "del", "forget", "suppress", "suppress")

# 目标域类别(D3高频域·与卡面词面匹配): 指令实词/域词 ∩ 卡域模式 → 候选。
_DOMAINS: tuple[tuple[str, tuple[re.Pattern, ...]], ...] = (
    ("address", (re.compile(r"\b(?:liv(?:es?|ed|ing)\s+(?:in|at)|address|home\s+at|moved\s+to|apartment|street)\b|住在|地址|搬到", re.I),)),
    ("phone", (re.compile(r"\b(?:phone|cell|mobile|telephone)\s*(?:number|no\.?)?\b|电话|手机号", re.I),)),
    ("email", (re.compile(r"\b(?:email|e-mail)\b|邮箱|电子邮件", re.I),)),
    ("finance", (re.compile(r"\b(?:salary|income|bank\s*account|credit\s*card|debt|loan|savings)\b|工资|收入|银行|存款|欠款", re.I),)),
    ("health", (re.compile(r"\b(?:diagnos\w+|illness|disease|medication|therapy|mental\s+health|pregnan\w+)\b|确诊|病|吃药|怀孕", re.I),)),
    ("family", (re.compile(r"\b(?:wife|husband|partner|divorce|married|daughter|son|sister|brother|affair)\b|妻子|丈夫|离婚|结婚|外遇", re.I),)),
    ("job", (re.compile(r"\b(?:work(?:s)?\s+(?:at|as)|job|employer|resign\w*|fired|quit)\b|工作|辞职|离职|被开", re.I),)),
    ("preference", (re.compile(r"\b(?:(?:my|our)\s+)?(?:favorite|favourite|preference|hobby|allergic)\w*\b|喜欢|爱好|过敏", re.I),)),
    ("secret", (re.compile(r"\b(?:secret|confidential|private|don'?t\s+tell\s+anyone)\b|秘密|保密|别告诉别人", re.I),)),
    ("plan", (re.compile(r"\b(?:plan(?:s|ning)?\s+to|going\s+to|intend\w*\s+to)\b|计划|打算", re.I),)),
    # [修20261025-7] 标识符/凭证域: "office code/pin/badge/编号"类通用值删除无域可撞
    # →_tombstone_match域门拦下(实测remove my office code→墓碑0)。补域+CN。
    ("identifier", (re.compile(r"\b(?:code|pin|passcode|password|badge|serial|license|plate|account\s*(?:id|no\.?|number)?|user(?:name)?)\b|编号|密码|口令|车牌|证件", re.I),)),
)

_GENERIC_WORD = re.compile(r"[a-zA-Z]{3,}|[\u4e00-\u9fff]{2,}")
_TOK_CJ = re.compile(r"[\u4e00-\u9fff]+")
# [W1沙箱修正20261023] organ卡面=w2tl.face_cards前置"[fact-time ...]"+原文→纯id门失配
# (sha1(sess|dt|原文)≠organ侧sha1(sess|dt|面文本)·时间线/派生卡同理漂移)→filter_gate加
# 内容包含网(面文本剥除后与tombstone/instruction文本互含)兜底全通道; id门保留(id对齐时零成本)。
_FACE_PREFIX_RE = re.compile(r"^\[fact-time [^\]]*\]\s*")
# [W1沙箱修正20261023] 硬字面量通道: 指令span中的数字串(≥4位含连字符·排除年份)跨语言命中 —
# 治"字面量在卡A(EN指令)→删除指令→卡B(ZH卡)仍漏"的词面盲区(zh卡与EN指令零词面交)。
_LIT_RE = re.compile(r"\d[\d\-]{3,}")
_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
# [修20261025-3a] 日期形态字面量排除: YYYY-MM / YYYY-MM-DD 不参与墓碑字面量封口
# (时间线卡文本全是日期串·不排除=含同日期的无关卡被连坐)。
_DATE_LIT_RE = re.compile(r"\d{4}-\d{2}(?:-\d{2})?")
# [修20261025-3b] 派生/系统卡前缀守卫: 这些卡会"引用"用户指令原文(如SESSION TIMELINE
# 引指令全文)→被detect_deletion二次识别成指令→其模板词(session/events/日期)把全租户
# 同类卡连坐墓碑(实测1条remove指令→7张墓碑→全窗清零)。派生卡只镜像、永不作指令源。
_DERIVED_PREFIXES = ("[SESSION TIMELINE]", "SESSION DIGEST", "KNOWLEDGE DIGEST",
                     "[fact-time", "ATTRIBUTE TIMELINE", "[SUPERSEDED]", "[CURRENT]",
                     "Data pattern:", "[dates:", "TASK BRIEF", "PATTERN CARD")


def literals_of(text: str) -> list[str]:
    out = []
    for m in _LIT_RE.finditer(str(text or "")):
        s = m.group(0)
        if not _YEAR_RE.fullmatch(s):
            out.append(s)
    return out

_AUDIT_RES = tuple(re.compile(rx, re.IGNORECASE) for rx in (
    # 删除审计意图: query在问"删除事件本身"(豁免门)
    r"\b(?:did\s+I|have\s+I|did\s+you)\s+(?:ever\s+)?(?:ask|tell|request).{0,24}\b(?:delete|forget|remove|erase)\b",
    r"\b(?:what|which)\b.{0,24}\b(?:deleted|erased|removed|forgotten)\b",
    r"\b(?:delete|deletion|removal|erasure|forgetting)\s+(?:request|instruction|history|record)\b",
    r"(?:删(?:除|掉)过|要求?删除|删除请求|哪些.{0,8}删|忘掉过|要求?忘记)",
))


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16]·knife1/2同法)。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def detect_deletion(text: str) -> dict[str, Any] | None:
    """L1: 卡文本→删除指令{action, span, words}|None(零LLM·纯正则·确定性)。"""
    t = str(text or "").strip()
    if not t or len(t) > 600:
        return None
    for i, rx in enumerate(_DEL_VERB_RES):
        if rx.search(t):
            words = {m.group(0).lower() for m in _GENERIC_WORD.finditer(t)}
            words -= {"delete", "forget", "remove", "please", "that", "about", "everything",
                      "said", "want", "would", "like", "have", "told", "erase", "wipe",
                      "yourself", "记录", "删除", "删掉", "清除", "抹掉", "忘记", "忘掉",
                      "撤回", "作废", "说过",
                      # [修20261025-3c] 功能词入停用表: from/your/memory等词几乎每卡都有
                      # →实词交≥2门被这些词凑数→无辜卡连坐(实测remove my phone number
                      # from your memory的words含from/your/memory→时间线卡全体命中)。
                      "from", "your", "my", "our", "their", "his", "her", "the", "and",
                      "or", "all", "any", "what", "this", "these", "those",
                      "now", "again", "just", "also", "was", "were", "into", "out",
                      "memory", "mind", "there", "here", "when", "then"}
            # [W1沙箱修正20261023] 中文段切2元组(tokens_of同款): 原实现中文指令词=整段
            # (如"请删除我之前说的家庭住址信息"), 子串匹配对历史卡几乎必落空→zh指令matched=0。
            # 切2元组后按卡面子串计数; 泛用2元组入停用表防误标。
            for seg in _TOK_CJ.findall(t):
                for k in range(len(seg) - 1):
                    words.add(seg[k:k + 2])
            words -= {"之前", "说的", "那个", "我的", "自己", "请删", "一下", "是我"}
            return {"action": _ACTION_OF[i], "span": t[:160], "words": words}
    return None


def is_audit_query(query: str) -> bool:
    """删除审计意图判定: query在问删除事件本身(豁免门·零LLM)。"""
    q = str(query or "")
    return any(rx.search(q) for rx in _AUDIT_RES)


# ── 持久化(sqlite WAL·knife1/2/7同款纪律) ───────────────────────────────────
class Knife11:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS instructions(
            tenant TEXT NOT NULL, instr_cid TEXT NOT NULL, sess TEXT NOT NULL,
            dt TEXT NOT NULL, action TEXT NOT NULL, span TEXT NOT NULL,
            obs_ms INTEGER NOT NULL, PRIMARY KEY(tenant, instr_cid))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS tombstones(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, instr_cid TEXT NOT NULL,
            action TEXT NOT NULL, marked_ms INTEGER NOT NULL,
            PRIMARY KEY(tenant, card_id, instr_cid))""")
        # 卡文镜像(匹配用·k7_cards存文先例; 主卡本体仍在organ·本表纯边车派生态)
        self._conn.execute("""CREATE TABLE IF NOT EXISTS card_texts(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, sess TEXT NOT NULL,
            dt TEXT NOT NULL, text TEXT NOT NULL, obs_ms INTEGER NOT NULL,
            PRIMARY KEY(tenant, card_id))""")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0        # 观察卡数(累计)
        self.n_instr = 0      # 新识别删除指令数
        self.n_tomb = 0       # 新tombstone对数
        self.n_drop = 0       # search相滤除卡数
        self.n_audit = 0      # 审计意图放行次数
        self.n_err = 0

    # ── ADD相: 识别+镜像+匹配+tombstone ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> list[str]:
        """逐卡观察: ①卡文镜像入card_texts; ②删除指令卡→落instructions并匹配本租户历史卡
        标tombstone。返回受影响会话列表(调用方可触发k7 digest重建)。fail-soft: 单卡失败
        只计数不抛(绝不影响add成功语义)。幂等: 同卡重放零增量。"""
        if not cards or not _master_on():
            return []
        affected: set[str] = set()
        for c in cards or []:
            try:
                self._observe_card(str(tenant), c, affected)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 不拖垮add
                self.n_err += 1
                log.error("knife11 observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return sorted(affected)

    def _observe_card(self, tenant: str, c: dict[str, Any], affected: set[str]) -> None:
        text = str(c.get("text", ""))
        if not text:
            return
        cid = card_id_of(str(c.get("sess", "")), str(c.get("dt", "")), text)
        now_ms = int(time.time() * 1000)
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO card_texts VALUES(?,?,?,?,?,?)",
                (tenant, cid, str(c.get("sess", "")), str(c.get("dt", "")), text, now_ms))
            self._conn.commit()
        # [修20261025-3b] 派生/系统卡永不作指令源: 其文本引用用户指令原文(时间线引全文)
        # →二次识别成指令→模板词连坐全租户同类卡(实测7墓碑/全窗清零)。镜像保留(可被
        # 内容包含网罩住)、指令识别跳过。
        if text.lstrip().startswith(_DERIVED_PREFIXES):
            return
        instr = detect_deletion(text)
        if instr is None:
            return
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO instructions VALUES(?,?,?,?,?,?,?)",
                (tenant, cid, str(c.get("sess", "")), str(c.get("dt", "")),
                 instr["action"], instr["span"], now_ms))
            self._conn.commit()
            self.n_instr += 1
        n_new = self._tombstone_match(tenant, cid, instr)
        if n_new:
            affected.add(str(c.get("sess", "")))
        log.info("knife11 instr tenant=%s action=%s matched=%d id=%s",
                 tenant, instr["action"], n_new, cid[:8])

    def _tombstone_match(self, tenant: str, instr_cid: str, instr: dict[str, Any]) -> int:
        """指令→历史卡匹配: ①域优先: 指令实词LIKE命中镜像卡文且卡命中域模式; ②兜底:
        实词交≥_MIN_WORD_HITS(强证据门·零过度删除)。上限_MAX_MATCH_PER_INSTR。"""
        words = sorted(instr["words"])
        lits = literals_of(instr.get("span", ""))
        if not words and not lits:
            return 0
        cands: set[str] = set()
        with self._lock:
            rows = self._conn.execute(
                "SELECT card_id, text FROM card_texts WHERE tenant=?"
                " ORDER BY obs_ms DESC LIMIT ?", (tenant, _SCAN_CAP)).fetchall()
        domain_res = [(d, rxs) for d, rxs in _DOMAINS]
        for cid, card_text in rows:
            t = str(card_text or "")
            tl = t.lower()
            word_hit_generic = any(rx.search(t) for rx in _DEL_VERB_RES)  # 卡本身也是指令→不自标
            if word_hit_generic:
                continue
            if any(lit in t for lit in lits):  # 硬字面量命中=强证据·直通候选
                cands.add(str(cid))
                continue
            hits = sum(1 for w in words if w in tl)
            if hits < _MIN_WORD_HITS:
                continue
            domain_hit = any(rx.search(t) for _d, rxs in domain_res for rx in rxs)
            if hits >= _MIN_WORD_HITS and (domain_hit or hits >= _MIN_WORD_HITS + 1):
                cands.add(str(cid))
        return self._tombstone_mark(tenant, instr_cid, instr["action"], sorted(cands))

    def _tombstone_mark(self, tenant: str, instr_cid: str, action: str, cids: list[str]) -> int:
        if not cids:
            return 0
        n = 0
        with self._lock:
            for cid in cids[:_MAX_MATCH_PER_INSTR]:
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO tombstones VALUES(?,?,?,?,?)",
                    (tenant, cid, instr_cid, action, int(time.time() * 1000)))
                n += cur.rowcount
            self._conn.commit()
        self.n_tomb += n
        return n

    # ── SEARCH相: 过滤门(knife2 filter_gate同构) ──
    def tombstoned_ids(self, tenant: str, card_ids: list[str]) -> set[str]:
        """card_ids → 其中被tombstone的id集(分块IN·零全库扫)。"""
        ids = [c for c in card_ids if c]
        if not ids:
            return set()
        out: set[str] = set()
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = ("SELECT DISTINCT card_id FROM tombstones WHERE tenant=? AND card_id IN ("
                     + ",".join("?" * len(chunk)) + ")")
                for (cid,) in self._conn.execute(q, (tenant, *chunk)):
                    out.add(str(cid))
        return out

    def extra_fetch(self, tenant: str) -> int:
        """超采补偿量=本租户tombstoned卡数(封顶64·knife2同款: 滤除后仍按top_k填满)。"""
        try:
            with self._lock:
                n = int(self._conn.execute(
                    "SELECT COUNT(DISTINCT card_id) FROM tombstones WHERE tenant=?",
                    (tenant,)).fetchone()[0])
            return min(n, 64)
        except Exception:  # noqa: BLE001
            return 0

    def filter_gate(self, tenant: str, query: str,
                    data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
        """D3放行门: 非审计意图→tombstoned卡+删除指令卡整卡摘除(指令span引原文=被删值
        泄漏面·烟测P3实捉; 指令卡在审计意图下仍可用); 审计意图(query问删除事件本身)→
        全窗放行+返回指令卡供注入位1(用户原句摘录·零编造·禁4✓)。
        [W1沙箱修正20261023] 双通道匹配: ①id门(tombstoned_ids/instructions·id对齐时零成本)
        ②内容包含网(剥[fact-time]面后与tombstone卡文/instruction span互含·core≥8) — 治
        organ卡面文本id漂移(face前缀/时间线卡/cc派生原子卡均与mirror id失配→纯id门空转)。
        返回(过滤后data, 摘除数, 审计注入卡列表)。门决策落[tomb-gate]日志(零内容)。"""
        if not data or not _master_on():
            return data, 0, []
        # 快路径=租户级存在性(非窗口id匹配): id漂移(face前缀/时间线/派生卡)下窗口id匹配恒空,
        # 若按窗口id早退则内容包含网永不生效→必须按租户是否有tombstone/instruction判定。
        with self._lock:
            _n_t = int(self._conn.execute(
                "SELECT COUNT(*) FROM tombstones WHERE tenant=?", (tenant,)).fetchone()[0])
            _n_i = int(self._conn.execute(
                "SELECT COUNT(*) FROM instructions WHERE tenant=?", (tenant,)).fetchone()[0])
        if not _n_t and not _n_i:
            return data, 0, []
        ids = [str(d.get("id", "")) for d in data]
        dead = self.tombstoned_ids(tenant, ids)
        if is_audit_query(query):
            self.n_audit += 1
            log.info("[tomb-gate] tenant=%s audit=1 kept=%d", tenant, len(data))
            return data, 0, self.audit_cards(tenant)
        with self._lock:
            tombs = self._conn.execute(
                "SELECT DISTINCT ct.card_id, ct.text FROM tombstones t"
                " JOIN card_texts ct ON ct.tenant=t.tenant AND ct.card_id=t.card_id"
                " WHERE t.tenant=? LIMIT 256", (tenant,)).fetchall()
            spans = self._conn.execute(
                "SELECT instr_cid, span FROM instructions WHERE tenant=? LIMIT 128",
                (tenant,)).fetchall()
            instr_ids = {str(r[0]) for r in self._conn.execute(
                "SELECT instr_cid FROM instructions WHERE tenant=?", (tenant,))}
        dead |= instr_ids
        t_texts = [str(x[1]) for x in tombs if x[1]]
        s_spans = [str(x[1])[:160] for x in spans if x[1]]
        lit_set = {lit for sp in s_spans for lit in literals_of(sp)}
        # [修20261025-3d] 墓碑卡字面量封口: 被删卡文本中的数字串(电话/证件号·排除日期形态
        # YYYY-MM[-DD])出现在窗卡→同删。治谓词卡绕过: knife1谓词卡"phone = 139…"在墓碑门
        # 之后注入(S11边界)·内容包含网与指令span字面量均罩不住它(指令里没数字·原文不互含)。
        tomb_lit_set = {lit for t in t_texts for lit in literals_of(t)
                        if not _DATE_LIT_RE.fullmatch(lit)}
        kept: list[dict[str, Any]] = []
        for d in data:
            cid = str(d.get("id", ""))
            content = str(d.get("content", ""))
            core = _FACE_PREFIX_RE.sub("", content, count=1).strip()
            drop = cid in dead
            if not drop and lit_set and any(lit in content for lit in lit_set):
                drop = True  # 硬字面量(指令span数字串)跨语言封口
            if not drop and tomb_lit_set and any(lit in content for lit in tomb_lit_set):
                drop = True  # 被删卡数字字面量(谓词卡/派生卡绕过兜底)
            if not drop:
                for t in t_texts:  # 被删卡原文整段嵌在卡面(face前缀/时间线引文/派生卡)
                    if t in content or (len(core) >= 8 and core in t):
                        drop = True
                        break
            if not drop:
                for sp in s_spans:  # 指令卡span(引被删原文)同删
                    if sp in content or (len(core) >= 8 and core in sp):
                        drop = True
                        break
            if drop:
                continue
            kept.append(d)
        dropped = len(data) - len(kept)
        self.n_drop += dropped
        log.info("[tomb-gate] tenant=%s audit=0 dropped=%d kept=%d", tenant, dropped, len(kept))
        return kept, dropped, []

    def sweep_literals(self, tenant: str,
                       data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """[修20261025-3e] 尾注清扫: k9/k12等末端注入的结构卡(knife12 "field = 值 (as of
        日期)"族)在k11门之后入窗=绕过墓碑。本方法在csan前再扫一遍: 命中墓碑卡数字字面量
        (排除日期形态)或被删卡原文互含→摘除。无审计豁免(审计卡由audit_cards专供)。
        无墓碑租户零开销直通。"""
        if not data or not _master_on():
            return data
        try:
            with self._lock:
                n_t = int(self._conn.execute(
                    "SELECT COUNT(*) FROM tombstones WHERE tenant=?", (tenant,)).fetchone()[0])
                if not n_t:
                    return data
                tombs = self._conn.execute(
                    "SELECT DISTINCT ct.text FROM tombstones t"
                    " JOIN card_texts ct ON ct.tenant=t.tenant AND ct.card_id=t.card_id"
                    " WHERE t.tenant=? LIMIT 256", (tenant,)).fetchall()
            t_texts = [str(x[0]) for x in tombs if x[0]]
            tomb_lit_set = {lit for t in t_texts for lit in literals_of(t)
                            if not _DATE_LIT_RE.fullmatch(lit)}
            kept = []
            for d in data:
                content = str(d.get("content", ""))
                core = _FACE_PREFIX_RE.sub("", content, count=1).strip()
                drop = bool(tomb_lit_set and any(lit in content for lit in tomb_lit_set))
                if not drop:
                    for t in t_texts:
                        if t in content or (len(core) >= 8 and core in t):
                            drop = True
                            break
                if not drop:
                    kept.append(d)
            n_drop = len(data) - len(kept)
            if n_drop:
                self.n_drop += n_drop
                log.info("[tomb-sweep] tenant=%s dropped=%d kept=%d", tenant, n_drop, len(kept))
            return kept
        except Exception:  # noqa: BLE001 — 清扫失败=原窗直通
            return data

    def _has_instr(self, tenant: str, card_ids: list[str]) -> bool:
        """窗内是否含删除指令卡(快路径·决定是否进入过滤分支)。"""
        ids = [c for c in card_ids if c]
        if not ids:
            return False
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = ("SELECT 1 FROM instructions WHERE tenant=? AND instr_cid IN ("
                     + ",".join("?" * len(chunk)) + ") LIMIT 1")
                if self._conn.execute(q, (tenant, *chunk)).fetchone():
                    return True
        return False

    def audit_cards(self, tenant: str) -> list[dict[str, Any]]:
        """审计意图注入卡: 删除指令原句摘录(用户自己的话=证据·可溯源·位1注入)。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT instr_cid, action, span, dt FROM instructions"
                " WHERE tenant=? ORDER BY obs_ms DESC LIMIT ?", (tenant, _MAX_INJECT)).fetchall()
        out = []
        for icid, action, span, dt in rows:
            out.append({"id": f"k11{str(icid)[:13]}",
                        "content": f"[DeletionRecord] 用户删除指令({action}·{str(dt)[:10]}): «{span}»",
                        "created_at": str(dt or "")})
        return out

    def decay_ids(self, tenant: str) -> set[str]:
        """L4: 本租户全部tombstoned卡id — 调用方在重排先验里把其FSRS ret视为0
        (fsrs_engine.py:35度量·等效exp(-∞)强衰减·防过滤漏网派生卡浮顶)。"""
        with self._lock:
            return {str(r[0]) for r in self._conn.execute(
                "SELECT DISTINCT card_id FROM tombstones WHERE tenant=?", (tenant,))}

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "instructions": g("SELECT COUNT(*) FROM instructions"),
                "tombstone_pairs": g("SELECT COUNT(*) FROM tombstones"),
                "tombstoned_cards": g("SELECT COUNT(DISTINCT card_id) FROM tombstones"),
                "card_texts": g("SELECT COUNT(*) FROM card_texts"),
                "obs_cards_total": self.n_obs, "instr_total": self.n_instr,
                "tomb_total": self.n_tomb, "drop_total": self.n_drop,
                "audit_total": self.n_audit, "err_total": self.n_err,
                "master": _master_on()}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部knife11状态行(organ卡本体不在本模块管辖)。"""
        with self._lock:
            n = 0
            for tbl in ("tombstones", "instructions", "card_texts"):
                cur = self._conn.execute(f"DELETE FROM {tbl} WHERE tenant=?", (tenant,))  # noqa: S608 — 常量表名
                n += cur.rowcount
            self._conn.commit()
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K11: Knife11 | None = None
_K11_LOCK = threading.Lock()


def get() -> Knife11:
    global _K11
    with _K11_LOCK:
        if _K11 is None:
            _K11 = Knife11(DB_PATH)
            log.info("knife11 init db=%s verbs=%d domains=%d",
                     _K11.db_path, len(_DEL_VERB_RES), len(_DOMAINS))
        return _K11


def observe(tenant: str, cards: list[dict[str, Any]]) -> list[str]:
    return get().observe(tenant, cards)


def filter_gate(tenant: str, query: str,
                data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    return get().filter_gate(tenant, query, data)


def extra_fetch(tenant: str) -> int:
    return get().extra_fetch(tenant)


# is_audit_query为模块级纯函数(上方定义)·不再包一层(防自引用)

def decay_ids(tenant: str) -> set[str]:
    return get().decay_ids(tenant)


def sweep_literals(tenant: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """[修20261025-3e] 模块级包装(aml_service按模块函数调): 尾注清扫k9/k12后注入卡。"""
    return get().sweep_literals(tenant, data)
