# card_construct.py — 基地重构席·证据内容层三刀(2026-10-09)
# ═════════════════════════════════════════════════════════════════════════════
# 法证根因(SMOKE46_FORENSICS/FORENSICS_card_construct): 官方冒烟46.10结构性根因=
# 官方卡是消息流1:1直通副本(零切块零原子化零日期+毒dt):
#   病灶① 毒dt: _dt_from_ts缺timestamp→datetime.now()冒充事件时间(43.4%卡[Time:Unknown]碎片带毒)
#   病灶② build_cards 1:1直通非切块器 → 检索窗给的是整段叙事非事实
#   病灶③ 日期仅3.7%卡文本含 → C1时间题无文本通道可命中
#
# 三刀(全部只增不改·原文卡text逐字节不变=回归基线·派生卡走同一add流过k1-k7门=零旁路):
#   刀1 DT_HONEST   (BRAINOS_CC_NODTPOISON, 缺省1): 缺timestamp→dt=""不造假;
#                   官方合成步进dt(毫秒epoch)原样保留; 下游全容错(实证:
#                   organ._parse_dt("")→None/k1时间线"9999"排尾/k3 _parse_base→None)
#   刀2 DATES       (BRAINOS_CC_DATES, 缺省1): 归一化日期入卡text通道 — 派生
#                   [dates: 1986-03-26]注记卡(锚定源=消息dt+k3归一+绝对日期直抽),
#                   与原文卡双轨并存 — 治C1/C2(日期进BM25/语义/呈现三通道)
#   刀3 ATOM        (BRAINOS_CC_ATOM, 缺省1): 句级原子事实卡(拆句+真名speaker
#                   抽取+主语代词解引+时间继承)作附加卡 — 治D1/A1/B2
#
# 纪律: 确定性(禁now()/随机→幂等重放安全·同body必产出同卡流)·fail-soft(单句脏数据
#       只跳过绝不影响add语义)·零新依赖·零LLM·零题目硬编码(通用句法启发)。
# 回滚: 摘除本件 + aml_service三处hunk还原pre_rebuild_1009/aml_service.py(a4f369b9)。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("card_construct")


# ── 开关(env·运行期读·关门即回官方1:1直通行为) ─────────────────────────────
import deep_fact as dfact  # deep_v3: 真事实抽取(结构化知识卡EN+CN)
import corpus_fact as cfact  # deep_jf: 官方JSON事实方言(Corpus UPDATE)
import deep_policy as dpol  # deep_t4: 策略合规地图预计算引擎(H域/K6披露策略)
import deep_temporal as dtmp  # t3: 时序/成员资格预计算(TENURE/SEQUENCE判定卡)
import deep_closure as dcl  # deep_t2: 链闭包终值卡(B2多跳预计算)
import deep_timeline as dtl  # deep_t1: as-of时间线预计算(D1/C1)
import deep_supersede as dsup  # deep_d1: 新旧值对撞(superseded_by)


import deep_digest as ddig  # deep_f1: 跨请求聚合摘要
_TEMPORAL_INTENT_RE = re.compile(r"\b(?:when|which\s+year|what\s+year|how\s+long|how\s+old|what\s+date|what\s+time)\b|什么时间|什么时候|哪一年|多久", re.I)


def DT_HONEST() -> bool:
    return os.environ.get("BRAINOS_CC_NODTPOISON", "1") != "0"


def DATES_ON() -> bool:
    return os.environ.get("BRAINOS_CC_DATES", "1") != "0"


def ATOM_ON() -> bool:
    return os.environ.get("BRAINOS_CC_ATOM", "1") != "0"


# [P0fix_f 20261025·巨卡派生解析窗钳制] 派生层(fact/atom/temporal/timeline/dates/
# pattern/policy…)全是逐遍线性扫描·5MB无结构单消息实测derive_all整体≈8s全在add
# 请求线程(build_cards内联)→契约内巨消息即可拖死200回包。钳=巨卡只把头窗喂派生层
# (窗缺省256KB·env BRAINOS_CC_PARSE_WIN可调); 原文卡out.append(c)逐字节不动=organ
# 全量入库零损失·派生卡本就是增强层(真 trails 单卡远小于窗·≤窗逐字节同行为)。
def _parse_win() -> int:
    try:
        return max(4096, int(os.environ.get("BRAINOS_CC_PARSE_WIN", "262144")))
    except Exception:  # noqa: BLE001 — fail-soft缺省
        return 262144


def _cap(kind: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(f"BRAINOS_CC_{kind}", str(default))))
    except (TypeError, ValueError):
        return default


def MAX_ATOM_PER_MSG() -> int:
    return _cap("ATOM_PER_MSG", 3)        # 每原文卡原子卡上限


def MAX_DERIVED_PER_MSG() -> int:
    return _cap("DERIVED_PER_MSG", 4)     # 每原文卡派生卡总上限(注记+原子)


def MAX_DERIVED_PER_REQ() -> int:
    return _cap("DERIVED_PER_REQ", 48)    # 每请求派生卡总上限(embed预算硬门)


# ── 刀1: 毒dt止血 ──────────────────────────────────────────────────────────
def dt_of(ts: Any) -> str:
    """消息timestamp(毫秒epoch,兼容秒,ISO-8601字符串)→organ dt; 缺/脏→空串(诚实·绝不now()冒充)。
    官方合成步进dt原样保留(合法路径与原实现逐字节同形·ISO无时区尾Z)。
    W2-1透传修复(20261014·梯队一第一刀): ISO-8601字符串("2023-05-08T13:56:00.00Z")直析。
    丢日期根因(A2/A4实证): 源数据session日期字段为ISO串→原实现float()失败→dt="unknown"
    →search契约守卫把非ISO created_at回填入库收据时刻=全库时间轴失真(temporal 259错题83%)。
    数值路径与原实现逐字节不变; ISO解析确定性(UTC归一)·禁now()。"""
    try:
        v = float(ts)
    except (TypeError, ValueError):
        s = str(ts or "").strip()
        if not s:
            return ""
        try:  # ISO-8601直析(Z尾/毫秒小数/裸日期皆容; 时区→UTC归一)
            dtv = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith(("Z", "z")) else s)
        except ValueError:
            return ""
        if dtv.tzinfo is not None:
            dtv = dtv.astimezone(timezone.utc).replace(tzinfo=None)
        return dtv.strftime("%Y-%m-%dT%H:%M:%S")
    if v != v or v in (float("inf"), float("-inf")):  # nan/inf脏值→诚实空串(原实现此处500)
        return ""
    if v > 1e11:  # 毫秒
        v /= 1000.0
    try:
        return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return ""


def honest_dt(text: str) -> str:
    """刀1诚实dt(终端摸底修正·2026-10-09): organ冻结核EG审计#3守卫跳过空dt卡
    (空dt=证据蒸发·stream_smoke 10/16实证)——故缺payload timestamp时:
    ①卡文本[Time: ...]前缀=真事件源→直用(非伪造·消息自报);
    ②无任何时间源→哨兵"unknown"(自述未知·零伪造: _parse_dt→None=FSRS跳过·
      k1时间线str排序稳定排尾·k3 _parse_base→None零锚·可溯源=如实unknown)。"""
    return time_prefix_dt(text) or "unknown"


# ── 刀2: 时间前缀解析(第二锚定源·治"真实事件时间只在文本前缀"族) ───────────
_TIME_PREFIX_RE = re.compile(r"^\s*\[Time:\s*([^\]]+)\]\s*", re.IGNORECASE)
_TAG_RE = re.compile(r"^\s*\[[^\]]*\]\s*")
_TIME_PREFIX_FMTS = (
    "%b %d, %Y, %H:%M:%S", "%b %d, %Y", "%B %d, %Y, %H:%M:%S", "%B %d, %Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
    "%d %b %Y, %H:%M:%S", "%d %b %Y", "%d %B %Y",
)


def time_prefix_dt(text: str) -> str:
    """卡文本首[Time: ...]前缀→ISO dt(与_dt_from_ts同形); Unknown/解析不出→""。"""
    m = _TIME_PREFIX_RE.match(str(text or ""))
    if not m:
        return ""
    raw = m.group(1).strip()
    if not raw or raw.lower() in ("unknown", "none", "n/a", "na"):
        return ""
    for f in _TIME_PREFIX_FMTS:
        try:
            return datetime.strptime(raw, f).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return ""


def _iso_date(s: str) -> str:
    return s[:10] if len(s) >= 10 else s


# 绝对日期直抽(k3门控外补充·自证无需base): ISO完整日期 + 月名,年 + 裸年
_ABS_ISO_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_ABS_MONYEAR_RE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|"
    r"June|July|August|September|October|November|December)\.?,?\s+(\d{4})\b", re.IGNORECASE)
_ABS_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
_MON_NUM = {"jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
            "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"}


def direct_dates(text: str) -> list[str]:
    """卡文本绝对日期直抽(确定性·按出现序去重): ISO→原形; 月名,年→YYYY-MM; 年→YYYY。"""
    t = str(text or "")
    out: list[str] = []
    for m in _ABS_ISO_RE.finditer(t):
        d = m.group(1)
        if d not in out:
            out.append(d)
    for m in _ABS_MONYEAR_RE.finditer(t):
        mon = _MON_NUM.get(m.group(1)[:3].lower())
        d = f"{m.group(2)}-{mon}" if mon else ""
        if d and d not in out:
            out.append(d)
    for m in _ABS_YEAR_RE.finditer(t):
        y = m.group(1)
        if not any(o.startswith(y) for o in out) and y not in out:
            out.append(y)
    return out


def dates_note_text(text: str, dt: str, k3_mod: Any) -> str:
    """刀2核心(纯函数·确定性): 归一化日期→"[dates: d1, d2]"注记文本; 无日期→""。
    三源合并: ①k3.parse_anchors(dt为base·相对/区间归一·±门内) ②[Time:...]前缀
    ③绝对日期直抽(门控外·自证)。排序去重封顶DATES_MAX。零来源→""(诚实跳过)。"""
    try:
        found: list[str] = []
        base = str(dt or "")
        if base and k3_mod is not None:
            for a in k3_mod.parse_anchors(text, base) or []:
                g = _iso_date(str(a.get("gstart", "")))
                if g and g not in found:
                    found.append(g)
        tp = time_prefix_dt(text)
        if tp and _iso_date(tp) not in found:
            found.append(_iso_date(tp))
        for d in direct_dates(text):
            if d not in found:
                found.append(d)
        if not found:
            return ""
        # 裸年包含过滤: "2025"被"2025-09-04"包含→弃(注记零冗余)
        found = [d for d in found
                 if len(d) <= 4 or not any(o != d and o.startswith(d + "-") for o in found)]
        return "[dates: " + ", ".join(sorted(found)[:_cap("DATES_MAX", 6)]) + "]"
    except Exception:  # noqa: BLE001 — fail-soft: 单卡注记失败不影响add
        return ""


# ── 刀3: 句级原子事实卡 ─────────────────────────────────────────────────────
_SENT_SPLIT_RE = re.compile(r"(?<=[.!?。！？])\s+")
_SPEAKER_RE = re.compile(r"^([A-Z][A-Za-z'’\-]*(?:\s+[A-Z][A-Za-z'’\-]*){0,2}|"
                         r"[\u4e00-\u9fff]{2,8})\s*[:：]\s+")
_FACT_HINTS = (" is ", " was ", " are ", " were ", " has ", " have ", " had ",
               " lives", " lived", " works", " worked", " born", " moved",
               " married", " graduated", " died", " killed", " lost ", " got ",
               " started ", " joined ", " bought ", " sold ", " is a ", " was a ",
               " is the ", " was the ", " went to ", " studies ", " studied ",
               " live ", " teaches ", " taught ", " work ", " study ",
               " came from ")
_PRON_RE = re.compile(r"^(I|My|We)\b(.*)$", re.DOTALL)
_PRONISH_RE = re.compile(r"^(I|My|We|He|She|It|They|This|That|There|His|Her|Their|Its)\b")
_SUBJECTISH_RE = re.compile(r"^[A-Z\u4e00-\u9fff\"']")
_IDENTITY_RE = re.compile(r"\b(my name is|i am|i'm|i`m|so am i)\b", re.IGNORECASE)
_JSONISH_RE = re.compile(r"[{\"}]|\bSUBJECT\b|\bOBJECT\b", re.IGNORECASE)
_OPINION_RE = re.compile(r"^I\s+(think|believe|feel|guess|wonder|said|told|heard|know|"
                         r"don't|do not|can't|cannot|could|would|will|may|might|"
                         r"just|never|always)\b", re.IGNORECASE)
_MID_PRON_RE = re.compile(r"\b(my|me|mine|ours|we|us|you|your|yours|i)\b", re.IGNORECASE)
# 最小变位表(只收事实提示词家族·第一人称单数→第三人称·过去时/第三人称原形安全)
_VERB_MAP = {"am": "is", "have": "has", "live": "lives", "work": "works",
             "like": "likes", "want": "wants", "need": "needs", "walk": "walks",
             "study": "studies", "go": "goes", "do": "does"}
_VERB_RE = re.compile(r"^((?:currently|now|also|still)\s+)?"
                      r"(am|have|live|work|like|want|need|walk|study|go|do)\b\s*(.*)$",
                      re.IGNORECASE)


def _split_sentences(text: str) -> list[str]:
    out: list[str] = []
    for line in str(text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        for s in _SENT_SPLIT_RE.split(line):
            s = _TAG_RE.sub("", s, count=1).strip()  # 剥首[Time:...]/[tag]前缀(speaker可见)
            if s:
                out.append(s)
    return out


def _atomic_sentences(text: str) -> list[tuple[str, str]]:
    """→[(speaker, sentence)]: 真名speaker抽取(行首"Name:")+跨句继承; 无名→""。"""
    out: list[tuple[str, str]] = []
    last_spk = ""
    for s in _split_sentences(text)[:24]:  # 扫描上限(超长叙事卡封顶·预算硬门)
        m = _SPEAKER_RE.match(s)
        if m:
            last_spk = m.group(1).strip()
            out.append((last_spk, s[m.end():].strip()))
        else:
            out.append((last_spk, s))
    return out


def _deref(sentence: str, speaker: str) -> str | None:
    """主语代词解引(保守·语法安全): 有真名speaker且句为事实家族时
    I→<Name>(带最小变位·currently等副词保位)·My→<Name>'s; 意见句/自介句/
    集体主语/无名→None(弃)。精度优先: 解引错=造谣, 弃卡只丢增益。"""
    if not speaker:
        return sentence  # 无名不改写(交由裸句门判归属·第三人称事实句保留)
    if _IDENTITY_RE.search(sentence):
        return None  # 自我介绍("My name is X"/"I'm X")→speaker字段已承载·原子卡零增益
    if _OPINION_RE.match(sentence):
        return None  # 意见/心理/能力句非事实(且"I think"→"X thinks"易语法翻车)
    m = _PRON_RE.match(sentence)
    if not m:
        return sentence
    p, rest = m.group(1), m.group(2)
    if p == "My":
        return f"{speaker}'s{rest}"
    if p == "We":
        return None  # 集体主语不解引(解引=造谣风险·诚实弃)
    vm = _VERB_RE.match(rest.strip())
    if vm:
        adv, verb, tail = vm.group(1) or "", vm.group(2).lower(), vm.group(3)
        return f"{speaker} {adv}{_VERB_MAP.get(verb, verb)} {tail}".rstrip()
    return f"{speaker}{rest}"


def _atomic_ok(sentence: str) -> bool:
    if not (12 <= len(sentence) <= 280):  # 12=短事实句下限("I have a dog."=13)
        return False
    if _JSONISH_RE.search(sentence):
        return False  # JSON blob/UPDATE载荷→k1已专业处理·原子化=噪音
    low = " " + sentence.lower() + " "
    return any(h in low for h in _FACT_HINTS)


def atomic_cards(card: dict[str, Any], rid: str, idx: int,
                 start_j: int) -> list[dict[str, Any]]:
    """刀3核心: 单原文卡→原子事实卡列表(确定性·fail-soft)。
    dia="{rid}:{i}:{j}" 数字尾(与原卡{rid}:{i}无碰撞·确定性键=幂等重放基础);
    speaker=真名(解引后)·dt=时间继承原卡。无名裸句仅保留大写主语开头(第三人称
    事实句·"Aldric Voss is a member of...")·代词主语弃·与原文全等弃(防近重复)。"""
    out: list[dict[str, Any]] = []
    try:
        parent_bare = _TAG_RE.sub("", str(card.get("text") or ""), count=1).strip()
        j = start_j
        for spk, sent in _atomic_sentences(card.get("text", "")):
            if len(out) >= min(MAX_ATOM_PER_MSG(),
                               max(0, MAX_DERIVED_PER_MSG() - start_j)):
                break
            if not _atomic_ok(sent):
                continue
            body = _deref(sent, spk)
            if not body:
                continue
            if body.strip() == parent_bare:
                continue  # 单句消息原子卡与原文全等→零增益弃(防近重复)
            if body == sent and not spk:
                if _PRONISH_RE.match(sent) or not _SUBJECTISH_RE.match(sent):
                    continue  # 无名且代词/小写主语→归属不明弃(精度优先)
            if _MID_PRON_RE.search(body):
                continue  # 解引后仍残留一二称代词("my job")→归属不明弃
            out.append({
                "conv": card.get("conv", ""),
                "sess": card.get("sess", ""),
                "dia": f"{rid}:{idx}:{j}",
                "speaker": spk or str(card.get("speaker", "")),
                "text": body,
                "dt": card.get("dt", ""),
            })
            j += 1
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡
        return out
    return out


# ── 总装: build_cards后处理(aml_service单hunk调用点) ────────────────────────
def _sidecar(conv: str) -> str:
    """派生卡边车conv(仅index档用): "{sid}:a" — 边车卡sess原样(证据session字段
    显原会话)·租户前缀过滤不受影响。"""
    c = str(conv or "")
    return c if c.endswith(":a") else c + ":a"


def ATOM_MODE() -> str:
    """原子/注记卡投运模式(床分归因矩阵2026-10-09·四轮床分实证):
    index=入organ索引(回归铁证: 单张注记卡即翻organ FSRS 0.97归一带→原文卡
    rank2→36·r9ctl0全关门=基线逐位复现钉死因果) — 仅存档A/B用;
    overlay(缺省)=派生卡入边车DB·搜索相尾注(organ索引=纯原文卡流→零回归由构造
    保证·注入位=knife5之后knife2闸之前=同过k2-k7门零旁路)。"""
    return os.environ.get("BRAINOS_CC_ATOM_MODE", "overlay")


# ── 边车DB(派生卡·overlay态) ────────────────────────────────────────────────
_DB_LOCK = threading.Lock()
_DB: Any = None
_TENANT_CACHE: dict[str, list[dict[str, Any]]] = {}


def _db():
    global _DB
    if _DB is None:
        import sqlite3
        from pathlib import Path as _P
        path = os.environ.get("BRAINOS_CC_DB", str(_P(__file__).parent / "card_construct.db"))
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS derived(
            tenant TEXT NOT NULL, cid TEXT NOT NULL, sess TEXT NOT NULL,
            dia TEXT NOT NULL, dt TEXT NOT NULL, speaker TEXT NOT NULL,
            text TEXT NOT NULL, kind TEXT NOT NULL, seq INTEGER NOT NULL,
            PRIMARY KEY(tenant, cid))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cc_tenant ON derived(tenant)")
        conn.commit()
        _DB = conn
    return _DB


def _cid_of(sess: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16])。"""
    import hashlib
    return hashlib.sha1(f"{sess or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def store_derived(tenant: str, derived: list[dict[str, Any]], kind: str) -> int:
    """派生卡落边车(幂等OR REPLACE·单事务·fail-soft)。缓存失效按租户。"""
    if not derived:
        return 0
    try:
        import time as _t
        seq = _t.time_ns()
        with _DB_LOCK:
            conn = _db()
            for d in derived:
                sess, dt, text = str(d.get("sess", "")), str(d.get("dt", "")), str(d.get("text", ""))
                # deep_d1: 显式列名 — ADD COLUMN后裸9值位置插入失配(12列)·必修
                conn.execute("INSERT OR REPLACE INTO derived"
                             "(tenant,cid,sess,dia,dt,speaker,text,kind,seq)"
                             " VALUES(?,?,?,?,?,?,?,?,?)",
                             (tenant, _cid_of(sess, dt, text), sess, str(d.get("dia", "")),
                              dt, str(d.get("speaker", "")), text, kind, seq))
            conn.commit()
        _TENANT_CACHE.pop(tenant, None)
        return len(derived)
    except Exception as exc:  # noqa: BLE001 — 边车失败只丢派生卡·绝不影响add
        log.error("card_construct store_derived FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0


def load_derived(tenant: str) -> list[dict[str, Any]]:
    """读租户派生卡(内存缓存·fail-soft→[])。"""
    try:
        cached = _TENANT_CACHE.get(tenant)
        if cached is not None:
            return cached
        with _DB_LOCK:
            rows = _db().execute(
                "SELECT sess, dt, speaker, text, kind, superseded_by FROM derived WHERE tenant=? "
                "ORDER BY seq", (tenant,)).fetchall()
        out = [{"sess": r[0], "dt": r[1], "speaker": r[2], "text": r[3], "kind": r[4],
                "superseded_by": r[5]}
               for r in rows]
        _TENANT_CACHE[tenant] = out
        return out
    except Exception:  # noqa: BLE001
        return []


_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u4e00-\u9fff]")
_STOP = {"the", "and", "was", "were", "what", "where", "when", "who", "whom", "which",
         "did", "does", "how", "why", "her", "his", "their", "its", "she", "him",
         "that", "this", "with", "for", "from", "after", "before", "about", "have",
         "has", "had", "are", "you", "your", "new"}
# deep_v3b: 关系标记词(高精度factoid锚)——CN bigram+EN词干(_tokens已归一)
_REL_BIGRAMS = {"搬到", "住在", "工作", "毕业", "确诊", "完成", "喜欢", "换工",
                "结婚", "离职", "退休", "买入", "卖出", "跑完", "考上", "搬家",
                "liv", "work", "mov", "marri", "graduat", "diagnos", "complet",
                "规则", "习惯", "偏好", "日常", "总是", "从不", "alway", "rule",
                "habit", "prefer", "never", "summar", "digest", "overview",
                "总结", "汇总", "概要", "causal", "becaus", "因为", "由于", "summariz",
                "global", "timeline", "overal", "全局", "整体", "时间线"}


def _stem(w: str) -> str:
    """轻词形归一(查询/卡两侧同规·仅len≥3): lives→liv/moves→mov/studies→study。
    目的: factoid问句(live)与事实卡(lives)词面相交·治OVERLAY_MIN门槛误杀。"""
    for suf, add in (("ies", "y"), ("es", ""), ("ing", ""), ("ed", ""), ("s", "")):
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            w = w[: -len(suf)] + add
            break
    if w.endswith("e") and len(w) > 3:
        w = w[:-1]
    return w


def _tokens(text: str) -> set[str]:
    t = str(text or "").lower()
    out: set[str] = set()
    for m in _TOKEN_RE.finditer(t):
        w = m.group(0)
        if w.isdigit():
            out.add(w)
        elif len(w) >= 3:
            out.add(_stem(w))
        elif len(w) >= 2:
            out.add(w)  # 短词保留(专名/缩写)
    cjk = re.findall(r"[\u4e00-\u9fff]", t)
    out.update(a + b for a, b in zip(cjk, cjk[1:]))  # 中文二元
    return out - _STOP


# deep_d1: 意图检测 — current(现在值)/history(过往)决定superseded卡skip与浮顶
_D1_CURRENT = re.compile(r"\b(still|currently|now|these days|nowadays|today)\b", re.I)
_D1_HISTORY = re.compile(r"\b(ever|used to|before|in the past|history)\b", re.I)
_D1_CURRENT_CN = re.compile(r"(现在|如今|目前|现在还)")
_D1_HISTORY_CN = re.compile(r"(曾经|以前|之前|过去)")


# ══ A1单点事实 + G4经验发现增强(2026-10-24·mem0式实体倒排) ═══════════════════
# 参照mem0记忆检索(实体倒排索引+0.5权重boost+4x超采)新增·总门BRAINOS_CC_ENTITY_IDX
# (缺省0=关=旧行为逐字节·本节全部新增代码只在门开时执行):
#   增① EntityIndex 实体倒排索引(entity_inv sqlite表={entity_lower: set(card_ids)}):
#        add相从卡文本抽实体(大写开头词+中文名词run)落倒排; search相query实体词查
#        索引→命中卡+0.5权重boost·4x超采补录→优先入窗; 索引空/构建失败=降级线性扫。
#   增② overlay槽位扩容: BRAINOS_CC_MAX_INJECT(1..6·未设env=原OVERLAY_MAX链路不变)
#   增③ 数据归纳卡(kind=pattern): 含数字/百分比/趋势的数据卡→"Data pattern:
#        X increased Y% between Z and W"·注入窗位2。
# 纪律同全件: 确定性(禁now()/随机)·fail-soft·零LLM·零新依赖·只增不改(签名零变动)。
def ENTITY_IDX_ON() -> bool:
    """A1/G4总门(缺省0=关·旧行为逐字节): =1启用实体倒排索引+0.5boost+4x超采+
    pattern归纳卡插位2+BRAINOS_CC_MAX_INJECT槽位扩容通道。"""
    return os.environ.get("BRAINOS_CC_ENTITY_IDX", "0") == "1"


def _inject_slot_cap(default_tok: int) -> int:
    """overlay槽位扩容门(上限6·下限1): 未设BRAINOS_CC_MAX_INJECT→原值透传
    (缺省不变=旧行为)·设6即从3扩到6·非法值→原值(fail-soft)。"""
    try:
        v = os.environ.get("BRAINOS_CC_MAX_INJECT")
        if not v:
            return default_tok
        return max(1, min(6, int(v)))
    except (TypeError, ValueError):
        return default_tok


_ENT_EN_RE = re.compile(
    r"\b((?:[A-Z]{2,}|[A-Z][a-z][a-z'’\-]+)(?:\s+(?:[A-Z]{2,}|[A-Z][a-z][a-z'’\-]+)){0,2})\b")
_ENT_CN_RE = re.compile(r"[\u4e00-\u9fff]{2,}")
# 实体停用集(功能词/日历词/系统词·防句首大写词污染倒排)
_ENT_STOP = {
    "the", "this", "that", "these", "those", "there", "then", "they", "she", "her",
    "his", "him", "when", "what", "where", "while", "after", "before", "because",
    "and", "but", "for", "with", "who", "how", "why", "was", "were", "has", "have",
    "had", "are", "did", "does", "will", "its", "our", "your", "not", "all", "any",
    "january", "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december", "jan", "feb", "mar", "apr",
    "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec", "monday", "tuesday",
    "wednesday", "thursday", "friday", "saturday", "sunday", "data", "pattern",
    "time", "date", "session", "user", "assistant", "note", "fact", "rule",
    "digest", "timeline", "summary", "member", "members", "yesterday", "today",
    "tomorrow", "unknown", "update",
}
_SUBJ_PRON_STOP = {"it", "this", "that", "there", "he", "she", "they", "we", "you", "i"}


class EntityIndex:
    """A1·实体倒排索引(mem0式·2026-10-24): {entity_lower: set(card_ids)}的sqlite
    落地形态(entity_inv表·PK(tenant,entity,cid)·INSERT OR IGNORE幂等)。
    add相: extract_entities(大写开头词+中文名词run)→倒排落库(derive_all尾部);
    search相: query实体词→cid集合(0.5权重boost与4x超采由_entity_boost_rescore消费)。
    fail-soft: 建表/写入/查询任何异常→空结果=降级回线性扫(旧行为)·零旁路。"""

    _READY = False

    @staticmethod
    def _conn() -> Any:
        conn = _db()
        if not EntityIndex._READY:
            conn.execute("""CREATE TABLE IF NOT EXISTS entity_inv(
                tenant TEXT NOT NULL, entity TEXT NOT NULL, cid TEXT NOT NULL,
                PRIMARY KEY(tenant, entity, cid))""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_einv_tenant ON entity_inv(tenant)")
            conn.commit()
            EntityIndex._READY = True
        return conn

    @staticmethod
    def extract_entities(text: str) -> list[str]:
        """实体抽取(纯函数·确定性·按出现序去重): EN=大写开头词(含≤3连词专名·
        剥's·首词停用弃·月/星期弃)小写化; CN=2-4字run整体·≥5字run取全部bigram
        (零分词依赖·add/query两侧同规=倒排可相交)。"""
        try:
            t = _TAG_RE.sub("", str(text or ""), count=1)
            out: list[str] = []
            for m in _ENT_EN_RE.finditer(t):
                e = re.sub(r"['’]s\b", "", m.group(1)).strip().lower()
                if len(e) < 3:
                    continue
                toks = e.replace("-", " ").split()
                if not toks or toks[0] in _ENT_STOP or all(tk in _ENT_STOP for tk in toks):
                    continue
                if e not in out:
                    out.append(e)
            for m in _ENT_CN_RE.finditer(t):
                run = m.group(0)
                if len(run) <= 4:
                    if run not in out:
                        out.append(run)
                else:
                    for a, b in zip(run, run[1:]):
                        bg = a + b
                        if bg not in out:
                            out.append(bg)
            return out
        except Exception:
            return []

    def add_cards(self, tenant: str, cards: list[dict[str, Any]]) -> int:
        """add相倒排落库(单事务·OR IGNORE幂等·fail-soft→0)。cid=_cid_of同源
        (sha1(session|dt|text)[:16])=load_derived侧cid反查键。"""
        if not tenant or not cards:
            return 0
        try:
            rows: list[tuple[str, str, str]] = []
            for c in cards:
                cid = _cid_of(str(c.get("sess", "")), str(c.get("dt", "")), str(c.get("text", "")))
                for e in self.extract_entities(str(c.get("text", ""))):
                    rows.append((tenant, e, cid))
            if not rows:
                return 0
            with _DB_LOCK:
                conn = self._conn()
                conn.executemany(
                    "INSERT OR IGNORE INTO entity_inv(tenant,entity,cid) VALUES(?,?,?)", rows)
                conn.commit()
            _EIDX_QCACHE.clear()
            return len(rows)
        except Exception as exc:  # noqa: BLE001 — 建索引失败=降级线性扫
            log.error("card_construct EntityIndex.add FAILED tenant=%s err=%s",
                      tenant, type(exc).__name__)
            return 0

    def query_cids(self, tenant: str, query: str) -> set[str]:
        """search相: query实体词→命中cid集合(小缓存·fail-soft→空集=降级线性扫)。"""
        try:
            ents = self.extract_entities(query)
            if not tenant or not ents:
                return set()
            ck = (tenant, "|".join(ents))
            hit = _EIDX_QCACHE.get(ck)
            if hit is not None:
                return set(hit)
            with _DB_LOCK:
                conn = self._conn()
                qmarks = ",".join("?" * len(ents))
                rows = conn.execute(
                    f"SELECT cid FROM entity_inv WHERE tenant=? AND entity IN ({qmarks})",
                    (tenant, *ents)).fetchall()
            out = frozenset(r[0] for r in rows)
            if len(_EIDX_QCACHE) < 256:
                _EIDX_QCACHE[ck] = out
            return set(out)
        except Exception:  # noqa: BLE001 — 查询失败=降级线性扫
            return set()


_EIDX_QCACHE: dict[tuple[str, str], frozenset[str]] = {}
EIDX = EntityIndex()


def _entity_boost_rescore(tenant: str, scored: list[tuple[Any, str, dict]],
                          atoms: list[dict[str, Any]], existing: set[str],
                          qt: set[str], query: str, max_tok: int,
                          q_cur: bool, q_hist: bool) -> None:
    """A1(20261024·mem0式)·原地改scored(确定性稳定排序):
    ① 0.5权重boost — scored池内cid命中倒排的卡 s+=0.5 重排(实体命中卡优先入窗);
    ② 4x超采 — OVERLAY_MIN阈值下被线性扫淘汰的实体命中卡补录(白名单kind·词面
      ≥1·非在窗·非current过期·池≤4*max_tok)。
    索引空/任何异常→零操作=降级回线性扫(fail-soft)。"""
    try:
        ehit = EIDX.query_cids(tenant, query)
        if not ehit:
            return
        boosted: list[tuple[Any, str, dict]] = []
        for s, key, a in scored:
            cid = _cid_of(str(a.get("sess", "")), str(a.get("dt", "")), str(a.get("text", "")))
            boosted.append(((float(s) + 0.5) if cid in ehit else s, key, a))
        boosted.sort(key=lambda x: (-float(x[0]), x[1]))
        have = {key for _, key, _ in boosted}
        cand: list[tuple[Any, str, dict]] = []
        for a in atoms:
            if len(cand) >= 4 * max(1, max_tok):
                break  # 4x超采池上限(mem0同式)
            if a.get("kind") not in ("atom", "fact", "rule", "digest", "pattern"):
                continue  # note/timeline等自有门控kind不超采(防旁路)
            txt = str(a.get("text", ""))
            if txt in existing:
                continue  # 已在窗(organ命中)零重复
            cid = _cid_of(str(a.get("sess", "")), str(a.get("dt", "")), txt)
            if cid not in ehit:
                continue
            key = a["dia"] if "dia" in a else txt[:16]
            if key in have:
                continue
            s = len(qt & _tokens(txt))
            if s < 1:
                continue  # 零词面→零相关(诚实弃·不造相关性)
            if a.get("superseded_by") and q_cur and not q_hist:
                continue  # deep_d1同门: current意图→过期旧值卡不超采
            cand.append((float(s) + 0.5, key, a))
            have.add(key)
        if cand:
            cand.sort(key=lambda x: (-float(x[0]), x[1]))
            boosted.extend(cand)
            boosted.sort(key=lambda x: (-float(x[0]), x[1]))
        scored[:] = boosted
    except Exception as exc:  # noqa: BLE001 — 增强失败=纯线性扫
        log.error("card_construct entity_boost FAILED err=%s", type(exc).__name__)


def _pattern_slot2(out: list[dict[str, Any]], injected: list[dict[str, Any]],
                   scored: list[tuple[Any, str, dict]], injected_all: list[dict[str, Any]],
                   max_tok: int) -> None:
    """G4: 数据归纳卡插位2 — pattern卡已入选(picks)且未被浮顶占用窗位时, 从尾插
    列表提到窗位2(rank0 organ最优不动·fact/rule浮顶位1优先·确定性·fail-soft零操作)。"""
    try:
        for k, (_s, _key, a) in enumerate(scored[:max(1, max_tok)]):
            if a.get("kind") != "pattern":
                continue
            if k < len(injected_all):
                inj = injected_all[k]
                if inj in injected:
                    injected.remove(inj)
                    out.insert(min(2, len(out)), inj)
            break
    except Exception as exc:  # noqa: BLE001 — 插位失败=pattern照常尾插
        log.error("card_construct pattern_slot2 FAILED err=%s", type(exc).__name__)


_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")
_NUM_RE = re.compile(r"(?<![\w.,])(\d[\d,]*(?:\.\d+)?)(?![\w.%])")
_YEAR_NUM_RE = re.compile(r"^(?:19|20)\d{2}$")
_TREND_UP_RE = re.compile(
    r"\b(increas(?:e|es|ed|ing)|grew|growth|rose|rises?|risen|rising|climb(?:ed|s)?|"
    r"surged?|surging|jump(?:ed|s)?|doubled|tripled|improve[ds]?|improving)\b"
    r"|上升|增长|提高|增加|上涨|涨了|翻倍|翻番", re.IGNORECASE)
_TREND_DOWN_RE = re.compile(
    r"\b(decreas(?:e|es|ed|ing)|declin(?:e|es|ed|ing)|drop(?:ped|s)?|fell|fallen|"
    r"falls?|falling|reduc(?:e|es|ed|ing)|shrank|shrunk|halved)\b"
    r"|下降|减少|降低|下跌|跌了|萎缩|减半", re.IGNORECASE)


def _pattern_subject(body: str, trend_start: int) -> str:
    """归纳主语(紧邻趋势词左侧·精度优先防张冠李戴): 最近token=NP中心名词(小写
    亦可·"revenue")·更远token仅专名形(大写/CJK)才并入("Acme Corp revenue"·
    "reported revenue"停在动词=只取revenue)·剥标点与's·代词主语弃; 空则退首
    原形实体(诚实有据·停用词弃)。"""
    try:
        pre = body[:trend_start].rstrip(" ,;:、—-·")
        toks = pre.split()
        cand: list[str] = []
        n = len(toks)
        for i in range(n - 1, max(-1, n - 5), -1):
            tk = toks[i].strip(",;:、。！？!'\"’“”")
            if tk.endswith(("'s", "’s")):
                tk = tk[:-2]
            if not tk or not re.match(r"^[A-Za-z\u4e00-\u9fff]", tk):
                break
            if i == n - 1:  # 最近词=NP中心(小写亦可·功能词则左侧无主语)
                if len(tk) < 2 or tk.lower() in _ENT_STOP:
                    break
                cand.insert(0, tk)
                continue
            if re.match(r"^[A-Z\u4e00-\u9fff]", tk) and tk.lower() not in _ENT_STOP:
                cand.insert(0, tk)  # 专名形前缀并入NP
                continue
            break
        subj = " ".join(cand).strip()
        if subj and len(subj) >= 3 and subj.lower() not in _SUBJ_PRON_STOP:
            return subj
        m = _ENT_EN_RE.search(body)
        if m and len(m.group(1)) >= 3 and m.group(1).lower() not in _ENT_STOP:
            return m.group(1)
        cm = _ENT_CN_RE.search(body)
        return cm.group(0) if cm else ""
    except Exception:
        return ""


def pattern_text(text: str) -> str:
    """G4核心(纯函数·确定性·诚实归纳): 数据卡(趋势词+百分比/非年份数字)→
    "Data pattern: {subj} increased/decreased {Y}% between {Z} and {W}"。
    无趋势/方向双向并存/缺主语/缺量值→""(宁缺勿造)·日期direct_dates: 两个→
    between·单个→since·零个→省略时段从句。"""
    try:
        body = _TAG_RE.sub("", str(text or ""), count=1).strip()
        if not body:
            return ""
        up = bool(_TREND_UP_RE.search(body))
        down = bool(_TREND_DOWN_RE.search(body))
        if up == down:
            return ""  # 无趋势或双向并存→方向不诚实·不归纳
        tm = _TREND_UP_RE.search(body) if up else _TREND_DOWN_RE.search(body)
        pm = _PCT_RE.search(body)
        mag = ""
        if pm:
            mag = pm.group(1) + "%"
        else:
            for nm in _NUM_RE.finditer(body):
                if _YEAR_NUM_RE.match(nm.group(1)):
                    continue  # 年份非量值(2021 increased by 2021=造谣)
                mag = "by " + nm.group(1)
                break
            if not mag:
                return ""
        subj = _pattern_subject(body, tm.start() if tm else 0)
        if not subj:
            return ""
        verb = "increased" if up else "decreased"
        ds = direct_dates(body)
        if len(ds) >= 2:
            when = f" between {ds[0]} and {ds[1]}"
        elif len(ds) == 1:
            when = f" since {ds[0]}"
        else:
            when = ""
        return f"Data pattern: {subj} {verb} {mag}{when}"
    except Exception:  # noqa: BLE001 — 单卡归纳失败只丢归纳卡
        return ""


def pattern_cards(card: dict[str, Any], rid: str, idx: int) -> list[dict[str, Any]]:
    """G4: 原文卡→数据归纳卡(≤1张·kind=pattern·确定性·fail-soft)。dia尾"pat"
    非数字(同{rid}:digest先例)与原子/事实数字尾无碰撞·dt继承原卡·speaker=""
    (引擎归纳非人言·与digest同规)。"""
    out: list[dict[str, Any]] = []
    try:
        txt = pattern_text(card.get("text", ""))
        if not txt or len(txt) > 280:
            return out
        out.append({
            "conv": card.get("conv", ""),
            "sess": card.get("sess", ""),
            "dia": f"{rid}:{idx}:pat",
            "speaker": "",
            "text": txt,
            "dt": card.get("dt", ""),
            "kind": "pattern",
        })
    except Exception:  # noqa: BLE001 — 单卡派生失败只丢派生卡
        return out
    return out


def overlay_inject(tenant: str, query: str, data: list[dict[str, Any]],
                   top_k: int) -> tuple[list[dict[str, Any]], int]:
    """搜索相尾注(零organ扰动·零回归由构造保证): 边车派生卡按词面重叠选强匹配
    →去已在窗→窗有空位尾插·满窗仅替换无query词面尾位卡(死重)→上限OVERLAY_MAX。
    注入位=knife5后knife2闸前→同过k2-k7全门=零旁路。确定性: 同入参同输出。"""
    if not ATOM_ON() and not DATES_ON():
        return data, 0
    if ATOM_MODE() != "overlay" or not data:
        return data, 0
    try:
        atoms = load_derived(tenant)
        if not atoms:
            return data, 0
        qt = _tokens(query)
        if not qt:
            return data, 0
        # deep_d1: current/history意图(EN+CN) — history优先(问过去→过期卡照常出席)
        q_cur = bool(_D1_CURRENT.search(query)) or bool(_D1_CURRENT_CN.search(query))
        q_hist = bool(_D1_HISTORY.search(query)) or bool(_D1_HISTORY_CN.search(query))
        max_tok = _cap("OVERLAY_MAX", 3)
        max_tok = _inject_slot_cap(max_tok)  # A1/G4: BRAINOS_CC_MAX_INJECT槽位扩容(1..6·未设env=原值→旧行为)
        existing = {str(d.get("content", "")) for d in data}
        scored: list[tuple[int, str, dict]] = []
        strong_rel: set[str] = set()
        for a in atoms:
            if a["text"] in existing:
                continue  # 已在窗(organ命中)零重复
            # 刀7·A1(20261015·60.34战报§三): 时间线卡属性gate — query显式属性词时,
            # 错属性时间线卡零进窗(按实体命中而非属性命中的错配卡挤位病灶·与
            # dtl.inject同款gate=单一实现路径在deep_timeline.query_attr_classes)。
            if a.get("kind") == "timeline" and not _TEMPORAL_INTENT_RE.search(query):
                _qcls = dtl.query_attr_classes(query)
                if _qcls:
                    _ac = dtl.attr_of_timeline_text(a["text"])
                    if _ac and _ac not in _qcls:
                        continue
            if a.get("superseded_by") and q_cur and not q_hist:
                continue  # deep_d1: current意图→过期旧值卡零注入(防评审读到旧值)
            at = _tokens(a["text"])
            ov = qt & at
            s = len(ov)
            # [修20261025-6] 版本链对卡打分加成: [SUPERSEDED]/[CURRENT]是预消化的转变知识
            # (C3/D3设计意图=成对进出窗)·与原文卡同词面分数时被挤出前6槽→+3保对卡可见。
            if a["text"].startswith(("[SUPERSEDED]", "[CURRENT]")):
                s += 3
            if a["kind"] == "note":  # 注记卡仅query显式日期词才出席(免噪音)
                if not (qt & at and any(ch.isdigit() for ch in " ".join(qt & at))):
                    continue
            rel_hit = bool(ov & _REL_BIGRAMS)
            q_rel = qt & _REL_BIGRAMS
            key = a["dia"] if "dia" in a else a["text"][:16]
            kind_special = a.get("kind") in ("rule", "digest")
            if s < _cap("OVERLAY_MIN", 2) and not (
                    (rel_hit or (kind_special and q_rel)) and len(qt) <= 10):
                continue  # v3b: CN关系bigram直通; v3c: rule/digest问句词直通(G2/F1)
            scored.append((s, key, a))
            if rel_hit or (kind_special and q_rel):
                strong_rel.add(key)
        scored.sort(key=lambda x: (-x[0], x[1]))
        if ENTITY_IDX_ON():  # A1: 实体倒排0.5boost+4x超采(门关=逐字节旧行为·索引空=纯线性扫)
            _entity_boost_rescore(tenant, scored, atoms, existing, qt, query,
                                  max_tok, q_cur, q_hist)
        picks = [a for _, _, a in scored[:max_tok]]
        if not picks:
            return data, 0
        injected_all = [{"id": _cid_of(a["sess"], a["dt"], a["text"]),
                         "content": a["text"], "created_at": a["dt"]} for a in picks]
        out = list(data)
        # deep_v3浮顶: 覆盖≥半数query内容词的fact卡插位1+(rank0 organ最优不动·
        # ≤OVERLAY_FLOAT_MAX张) — 治注入沉底(评审LLM读不到rank9知识卡)
        fmin = _cap("OVERLAY_FLOAT_MIN", 2)
        fmax = _cap("OVERLAY_FLOAT_MAX", 2)
        injected: list[dict[str, Any]] = []
        fpos = 1
        for (s, key, a), inj in zip(scored[:max_tok], injected_all):
            float_ok = (s >= fmin and s * 2 >= len(qt)) or (key in strong_rel and len(qt) <= 10)
            if (a.get("kind") in ("fact", "rule", "digest") and float_ok
                    and fpos <= fmax and (not a.get("superseded_by") or q_hist)):
                out.insert(min(fpos, len(out)), inj)
                fpos += 1
            else:
                injected.append(inj)
        if ENTITY_IDX_ON():  # G4: pattern归纳卡占窗位2(浮顶之后·尾插之前)
            _pattern_slot2(out, injected, scored, injected_all, max_tok)
        room = max(0, top_k - len(out))
        if room >= len(injected):
            out.extend(injected)
        else:  # 满窗: 仅替换尾位无query词面卡(死重)·有词面位不动(零证据损失)
            qtok_any = list(qt)
            for inj in injected:
                placed = False
                for k2i in range(len(out) - 1, 0, -1):
                    tail_txt = str(out[k2i].get("content", "")).lower()
                    if not any(t in tail_txt for t in qtok_any):
                        out.pop(k2i)
                        out.append(inj)
                        placed = True
                        break
                if not placed and len(out) < top_k:
                    out.append(inj)
        while len(out) > top_k:
            out.pop()
        n = len(out) - len(data)
        return out, n
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("card_construct overlay FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0


def derive_all(cards: list[dict[str, Any]], rid: str,
               k3_mod: Any = None, tenant: str = "") -> list[dict[str, Any]]:
    """add相派生总装: 原文卡流原样返回(organ索引=纯原文卡流·回归由构造保证);
    派生卡(日期注记/原子)→边车DB(overlay缺省)或卡流(index档·A/B存档用)。
    预算: 每卡派生≤DERIVED_PER_MSG·每请求≤DERIVED_PER_REQ。fail-soft整体。"""
    if not cards or not rid:
        return cards
    if not (DATES_ON() or ATOM_ON()):
        return cards
    try:
        mode = ATOM_MODE()
        out: list[dict[str, Any]] = []
        notes_all: list[dict[str, Any]] = []
        atoms_all: list[dict[str, Any]] = []
        facts_all: list[dict[str, Any]] = []
        rules_all: list[dict[str, Any]] = []
        pmaps_all: list[dict[str, Any]] = []  # deep_t4: 合规地图卡清单
        temporals_all: list[dict[str, Any]] = []
        timelines_all: list[dict[str, Any]] = []
        patterns_all: list[dict[str, Any]] = []  # G4: 数据归纳卡清单
        budget = MAX_DERIVED_PER_REQ()
        for i, c in enumerate(cards):
            out.append(c)  # 原文卡: 逐字节不动(回归基线)
            # [P0fix_f] 巨卡派生窗钳: 派生层只吃头窗·原文organ全量(行为对表见_PARSE_WIN注)
            if len(str(c.get("text", ""))) > _parse_win():
                c = {**c, "text": str(c.get("text", ""))[:_parse_win()]}
            if budget <= 0:
                continue
            derived: list[dict[str, Any]] = []
            if DATES_ON() and k3_mod is not None:
                note = dates_note_text(c.get("text", ""), c.get("dt", ""), k3_mod)
                if note:
                    derived.append({
                        "conv": _sidecar(c.get("conv", "")), "sess": c.get("sess", ""),
                        "dia": f"{rid}:{i}:0", "speaker": str(c.get("speaker", "")),
                        "text": note, "dt": c.get("dt", ""),
                    })
            if ATOM_ON():
                j0 = 1 if derived else 0
                n_atom = 0
                for a_card in atomic_cards(c, rid, i, j0):
                    a_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(a_card)
                    n_atom += 1
                # deep_v3: 结构化事实卡(主体/关系/对象/绝对日期)·同边车流同预算
                n_fact = 0
                for f_card in dfact.fact_cards(c, rid, i, j0 + n_atom + 4):
                    f_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(f_card)
                    n_fact += 1
                # deep_v3c: 规则/偏好卡(G2)·kind=rule
                for rl_card in dfact.rule_cards(c, rid, i, j0 + n_atom + n_fact + 8):
                    rl_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(rl_card)
                # deep_t4: 策略合规地图卡(H域/K6)·kind=pmap(识别失败fail-soft→[]·地图表由钩子内落库)
                for pm_card in dpol.policy_cards(c, rid, i, tenant=tenant):
                    pm_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(pm_card)
                # t3: 时序/成员资格预计算(TENURE/SEQUENCE)·kind=temporal
                for t_card in dtmp.temporal_cards(c, rid, i, tenant):
                    t_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(t_card)
                # deep_t1: as-of时间线碎片
                for tl in dtl.timeline_cards(c, rid, i, j0 + n_atom + n_fact + 12):
                    tl["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(tl)
            # G4(20261024): 数据归纳卡(kind=pattern) — 含数字/百分比/趋势的数据卡→
            # "Data pattern: X increased Y% between Z and W"·仅ENTITY_IDX门开时执行
            if ENTITY_IDX_ON() and budget > 0:
                for p_card in pattern_cards(c, rid, i):
                    p_card["conv"] = _sidecar(c.get("conv", ""))
                    derived.append(p_card)
            took = derived[:min(MAX_DERIVED_PER_MSG(), budget)]
            for d in took:
                budget -= 1
                if d.get("kind") == "fact":
                    facts_all.append(d)
                elif d.get("kind") == "rule":
                    rules_all.append(d)
                elif d.get("kind") == "pmap":  # deep_t4: 合规地图卡分类
                    pmaps_all.append(d)
                elif d.get("kind") == "temporal":
                    temporals_all.append(d)
                elif d.get("kind") == "timeline":
                    timelines_all.append(d)
                elif d.get("kind") == "pattern":  # G4: 归纳卡分类(入patterns_all)
                    patterns_all.append(d)
                elif d["text"].startswith("[dates:"):
                    notes_all.append(d)
                else:
                    atoms_all.append(d)
                if mode == "index":
                    out.append(d)
        if mode != "index" and tenant:
            store_derived(tenant, notes_all, "note")
            store_derived(tenant, atoms_all, "atom")
            # deep_d1: store前置备 — 批内+跨请求新旧值对撞(改text缀出处·算最终cid)
            d1_collisions = dsup.prepare(tenant, facts_all)
            store_derived(tenant, facts_all, "fact")
            store_derived(tenant, rules_all, "rule")
            store_derived(tenant, pmaps_all, "pmap")  # deep_t4: 地图卡入边车(overlay共用·地图表已由钩子落库)
            store_derived(tenant, temporals_all, "temporal")
            store_derived(tenant, timelines_all, "timeline")
            dtl.store_fragments(tenant, timelines_all)
            store_derived(tenant, patterns_all, "pattern")  # G4: 归纳卡入边车(overlay同流)
            # A1: 实体倒排索引add相构建(仅ENTITY_IDX门开·fail-soft→降级线性扫)
            if ENTITY_IDX_ON():
                try:
                    EIDX.add_cards(tenant, notes_all + atoms_all + facts_all
                                   + rules_all + pmaps_all + temporals_all
                                   + timelines_all + patterns_all)
                except Exception:
                    pass
            # deep_v3c: 会话摘要卡(F1) — 每请求一张·facts/rules零LLM拼装·评审问
            # "summarize/总结"类问题时的稠密出口
            try:
                seg_src = (rules_all + facts_all)[:12]
                if seg_src:
                    d0 = seg_src[0]
                    digest_text = ("SESSION DIGEST: "
                                   + "; ".join((str(d.get("dt", ""))[:10] + " " + d["text"])
                                               if d.get("dt") else d["text"]
                                               for d in seg_src))[:900]
                    store_derived(tenant, [{
                        "conv": _sidecar(d0.get("conv", "")),
                        "sess": d0.get("sess", ""),
                        "dia": f"{rid}:digest",
                        "dt": d0.get("dt", ""),
                        "speaker": "",
                        "text": digest_text,
                        "kind": "digest",
                    }], "digest")
                    # deep_f1: 跨请求聚合master卡 — 本请求digest入历史, 聚合最近
                    # ≤12会话(≤900字符·保最新)成KNOWLEDGE DIGEST; 聚合生效(返回
                    # ≠本请求digest)时prune旧master再落新master(dia=rid:master,
                    # kind=digest→overlay直通+浮顶): master覆盖跨请求视野,
                    # session digest保留本请求细节, 两者并存。fail-soft降级=现状。
                    try:
                        master_text = ddig.append_and_build(
                            tenant, digest_text, d0.get("dt", ""))
                        if master_text != digest_text:
                            ddig.prune_master(tenant)
                            store_derived(tenant, [{
                                "conv": _sidecar(d0.get("conv", "")),
                                "sess": d0.get("sess", ""),
                                "dia": f"{rid}:master",
                                "dt": d0.get("dt", ""),
                                "speaker": "",
                                "text": master_text,
                                "kind": "digest",
                            }], "digest")
                    except Exception:
                        pass
            except Exception:
                pass
            # deep_d1: 对撞落库 — 旧行superseded_by·新卡fact_key/fact_obj·租户缓存失效
            # [修20261025-5] 传原文卡: fact语法不保留from-to措辞·版本链需原文才能触发
            dsup.commit(tenant, d1_collisions, facts_all, raw_cards=cards)
        # deep_jf: 官方JSON事实方言观察(Corpus/UPDATE卡→jfacts·fail-soft)
        try:
            cfact.observe(tenant, cards, rid)
        except Exception:
            pass

        # deep_t2: 链闭包终值卡预计算(RESOLVED卡→closures自管表·fail-soft)
        try:
            dcl.closure_cards(tenant, rid)
        except Exception:
            pass
        return out
    except Exception as exc:  # noqa: BLE001 — 内容层整体fail-soft
        log.error("card_construct derive_all FAILED err=%s", type(exc).__name__)
        return cards
