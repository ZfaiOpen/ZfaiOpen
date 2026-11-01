"""deep_supersede.py — deep_d1·新旧值对撞引擎(知识更新D1维度·纯stdlib·确定性·fail-soft)
D1考"知识更新": 先"Alice lives in Paris"后"Alice lives in London" — 评审LLM必须看到
新值生效·旧值过期。机制(零LLM·零网络·同入参同出参):
  fact卡按 (租户, 关系组, 主体) 归组 — 同组不同对象=新旧值对撞:
    批内:   同请求多卡按(dt,批内序)升序·旧对撞给新;
    跨请求: 新卡查库同key开放行(superseded_by空)·不同对象旧行将被标记;
    新卡text缀出处: "(changed from 旧obj, was 旧dt[:10])" — 评审LLM直读;
    旧行UPDATE superseded_by=新卡cid — 注入层按意图(current→skip/history→正常/默认→禁浮顶)。
  planned卡("plans to"/"计划")永不参战 — "计划搬家"不是事实变更。
接线(card_construct两行): store_derived(fact)前 dsup.prepare · rules/digest store后 dsup.commit。
DB: 必须同库同连接 — 一律经 card_construct._db()/_DB_LOCK(函数内延迟import防循环)。

v2新增(全部受env门 BRAINOS_SUPE_EXPANDED 控制·缺省0=与旧版行为完全一致):
  ①关系词开放类: 封闭类_REL_LIST之外, 用正则模式(而非词表枚举)识别变化陈述 —
    EN: "A was/is/got/became/turned/switched/changed/moved/upgraded/downgraded from|to ..."、
    EN变化标记: "no longer|used to|previously|formerly|stopped|started|began|ended"、
    CN: "A从X变成Y | A不再B | A停止B | A开始B" (mem0式: 变化即转变陈述·开放类覆盖);
  ②版本链: 显式"from X to Y / 从X变成Y"转变→为旧值生成[SUPERSEDED]卡·新值生成[CURRENT]卡,
    两张都经store_derived入检索窗(评审LLM直读mem0式转变卡·带双时戳出处);
  ③env门: 所有新增路径先查_expanded()·门关=零行为差异; 匹配失败fail-soft返回原结果。
"""
from __future__ import annotations

import os
import re
from typing import Any

# ── 关系表(与deep_fact封闭类文法同源·longest-first匹配防子串遮蔽) ────────────
_REL_LIST = (
    "lives in", "lived in", "move to", "moved to", "works as", "works at",
    "received", "completed", "studies at", "diagnosed with", "suffers from",
    "likes", "married to", "bought",
    "住在", "搬到", "在_工作", "换工作到", "毕业于", "喜欢", "确诊", "完成",
)
_RELS = tuple(sorted(_REL_LIST, key=len, reverse=True))
# 关系归组(同组不同对象才算新旧值对撞): LOC=居址/JOB=职位·其余relation各自成组
_LOC_GROUP = {"lives in", "lived in", "住在", "moved to", "搬到"}
_JOB_GROUP = {"works as", "works at", "在_工作", "换工作到"}
# 非fact卡前缀(kind=fact的CAUSAL卡/RULE卡/摘要/注记→不可解析·永不参战)
_NON_FACT_PREFIX = ("CAUSAL:", "RULE:", "SESSION DIGEST:", "[dates:", "UPDATE:")

# ── [v2新增] 扩展模式区(全部仅 env门 BRAINOS_SUPE_EXPANDED=1 时生效) ──────────
# [P0fix_f 20261025·灾难回溯修复] 原量化词全部无界(\w+/.+?)→无空白巨文本(平台允许
# ≤30MiB单请求)上 O(n²) 回溯: 实测"A"*5MB单消息 EN_FROMTO.search 楔死>6min(9次
# faulthandler取证100%命中deep_supersede.py parse_change_pair L153; 10→80KB每次
# 翻倍耗时×4.0=纯二次), 外推5MB≈49小时→add主路径(build_cards→derive_all→commit)
# 全程饿死/health。修复=量化词钳到卫哨语义界(_ok_subj≤64/_ok_obj≤160·越界本就弃
# →接受集不变的消回溯), 另加输入窗钳 BRAINOS_SUPE_PARSE_WIN(缺省256KB·尾部截断
# 扫描·变化陈述为句级短语永不越窗)。双保险后任意巨输入本模块解析≤毫秒级。
_EXPANDED_ENV = "BRAINOS_SUPE_EXPANDED"
_PARSE_WIN = max(4096, int(os.environ.get("BRAINOS_SUPE_PARSE_WIN", "262144")))
# 开放类EN动词集(规格给定·左侧主体/右侧宾语开放→开放类覆盖)
# 主体锚\b=整词语义(与旧版"\w+贪词+≤64卫哨"接受集同形·且全字母巨文本每位置O(1)灭火)
_EXP_EN_VERBS = r"(?:was|is|got|became|turned|switched|changed|moved|upgraded|downgraded)"
_EXP_EN_FROMTO = re.compile(  # "A switched from X to Y" → 主体+动词+旧值+新值
    r"\b(\w{1,64})\b\s+(" + _EXP_EN_VERBS + r")\s+from\s+(.{1,160}?)\s+to\s+(.{1,160}?)\s*(?=\(|$)",
    re.IGNORECASE)
_EXP_EN_TO = re.compile(      # "A changed to Y" → 主体+动词+新值(无显式旧值)
    r"\b(\w{1,64})\b\s+(" + _EXP_EN_VERBS + r")\s+to\s+(.{1,160}?)\s*(?=\(|$)", re.IGNORECASE)
_EXP_EN_MARK = re.compile(    # EN变化标记词(主体=标记前缀·对象=标记后文)
    r"\b(no longer|used to|previously|formerly|stopped|started|began|ended)\b\s*(.*)",
    re.IGNORECASE)
# CN主体锚(?:[^\w]|^)=词run起点(旧lazy左最左=整run·>64旧版卫哨拒→新版不中·同形);
# subject惰性=旧lazy"首现标记即停"同形; CN新值(?!\w)=旧"整run后卫哨≤160拒"同形
_EXP_CN_FROMTO = re.compile(r"(?:[^\w]|^)(\w{1,64}?)从(\w{1,160}?)变成(\w{1,160}(?!\w))")  # "A从X变成Y"
_EXP_CN_MARK = re.compile(r"(?:[^\w]|^)(\w{1,64}?)(不再|停止|开始)(\w+)")   # "A不再B | A停止B | A开始B"
# 主体若落在虚词上=抽取可疑(如"plans to switch"的"to")→诚实弃(弃卡只丢增益)
_SUBJ_STOP = frozenset(
    {"to", "from", "and", "the", "a", "an", "of", "in", "on", "at",
     "for", "with", "by"})
_VC_PREFIX = ("[SUPERSEDED]", "[CURRENT]")  # 版本链自产卡前缀→永不二次参战


def _expanded() -> bool:
    """[v2新增] env门: BRAINOS_SUPE_EXPANDED=1(或true)→扩展开放类模式;
    其余(含未设/0)→旧封闭词表。现读不缓存(同env同入参同出参·确定性不变)。异常→关。"""
    try:
        return str(os.environ.get(_EXPANDED_ENV, "")).strip().lower() in ("1", "true")
    except Exception:  # noqa: BLE001 — fail-soft→默认旧词表
        return False


def _ok_subj(s: str) -> bool:
    """[v2新增] 主体卫哨: 非空·≤64字·非虚词(防"plans to X"的to被当主体)。"""
    try:
        return bool(s) and len(s) <= 64 and str(s).strip().lower() not in _SUBJ_STOP
    except Exception:  # noqa: BLE001
        return False


def _ok_obj(s: str) -> bool:
    """[v2新增] 对象卫哨: 非空·≤160字。"""
    try:
        return bool(s) and len(s) <= 160
    except Exception:  # noqa: BLE001
        return False


def _parse_expanded(t: str) -> tuple[str, str, str] | None:
    """[v2新增·仅扩展模式] 开放类变化陈述→(subj, rel, obj)|None。
    尝试序: EN from-to > EN to > EN标记 > CN从..变成 > CN标记 — 先specific后泛化。
    planned("plans to"/"计划")与自产版本链卡永不参战(与旧词表同纪律)。
    任何不确定→None(诚实弃·fail-soft只丢增强)。确定性纯函数。"""
    try:
        if not t or t.startswith(_VC_PREFIX):
            return None  # 自产[SUPERSEDED]/[CURRENT]卡不二次参战
        if len(t) > _PARSE_WIN:
            t = t[:_PARSE_WIN].strip()  # [P0fix_f] 巨文本扫描窗钳制(fail-soft)
        if re.search(r"\bplans to\b", t, re.IGNORECASE) or ("计划" in t):
            return None  # planned永不参战
        m = _EXP_EN_FROMTO.search(t)
        if m:
            subj = m.group(1).strip()
            verb = str(m.group(2)).strip().lower()
            new = m.group(4).strip()
            if _ok_subj(subj) and _ok_obj(new) and subj.lower() != new.lower():
                return (subj, verb, new)  # rel=动词→同动词同主体跨对象对撞
            return None
        m = _EXP_EN_TO.search(t)
        if m:
            subj = m.group(1).strip()
            verb = str(m.group(2)).strip().lower()
            new = m.group(3).strip()
            if _ok_subj(subj) and _ok_obj(new) and subj.lower() != new.lower():
                return (subj, verb, new)
            return None
        m = _EXP_EN_MARK.search(t)
        if m:
            subj = t[:m.start()].strip()
            rel = str(m.group(1)).strip().lower()
            obj = str(m.group(2)).strip()
            if _ok_subj(subj) and _ok_obj(obj) and subj.lower() != obj.lower():
                return (subj, rel, obj)
            return None
        m = _EXP_CN_FROMTO.search(t)
        if m:
            subj, _old, new = m.group(1), m.group(2), m.group(3)
            if _ok_subj(subj) and _ok_obj(new) and subj != new:
                return (subj, "变成", new)
            return None
        m = _EXP_CN_MARK.search(t)
        if m:
            subj, rel, obj = m.group(1), m.group(2), m.group(3)
            if _ok_subj(subj) and _ok_obj(obj) and subj != obj:
                return (subj, rel, obj)
            return None
        return None
    except Exception:  # noqa: BLE001 — fail-soft: 匹配失败只丢增强
        return None


def parse_change_pair(text: str) -> tuple[str, str, str, str] | None:
    """[v2新增·仅扩展模式] 显式变化陈述→(subj, old, new, verb)|None。
    认 EN "A <verb> from X to Y" 与 CN "A从X变成Y"(mem0式转变对·旧值新值双提)。
    供版本链用; 无显式新旧值对(markers类)→None。纯函数·fail-soft。"""
    try:
        if not _expanded():
            return None
        t = str(text or "").strip()
        if len(t) > _PARSE_WIN:
            t = t[:_PARSE_WIN].strip()  # [P0fix_f] 巨文本扫描窗钳制(句级短语永不越窗·fail-soft)
        if (not t or t.startswith(_NON_FACT_PREFIX) or t.startswith(_VC_PREFIX)
                or re.search(r"\bplans to\b", t, re.IGNORECASE) or ("计划" in t)):
            return None
        base = t.split(" (")[0].strip()  # 剥日期/出处缀(与parse_fact_text同法)
        if not base:
            return None
        m = _EXP_EN_FROMTO.search(base)
        if m:
            subj = m.group(1).strip()
            verb = str(m.group(2)).strip().lower()
            old = m.group(3).strip()
            new = m.group(4).strip()
            if (_ok_subj(subj) and _ok_obj(old) and _ok_obj(new)
                    and old.lower() != new.lower()):
                return (subj, old, new, verb)
            return None
        m = _EXP_CN_FROMTO.search(base)
        if m:
            subj, old, new = m.group(1), m.group(2), m.group(3)
            if (_ok_subj(subj) and _ok_obj(old) and _ok_obj(new)
                    and old != new):
                return (subj, old, new, "变成")
            return None
        return None
    except Exception:  # noqa: BLE001 — fail-soft
        return None


_VC_TAG_RE = re.compile(r"^(?:\[[^\]]*\]\s*)+")          # [修20261025-5] 官方方言标签前缀
_VC_SPK_RE = re.compile(r"^[A-Za-z\u4e00-\u9fff][\w\u4e00-\u9fff]{0,19}\s*[:：]\s*")  # 说话人前缀


def _vc_clean(text: str) -> str:
    """[修20261025-5] 原文卡→可解析变化句: 剥[官方标签]前缀·剥说话人前缀·取首行。
    治集成断点: deep_fact谓词语法只产"X moved to Y"新值式·从不保留"from A to B"
    →原版只吃facts_all时版本链在产线不可达·故v2增吃原文卡(保留原措辞)。"""
    try:
        t = str(text or "")
        t = _VC_TAG_RE.sub("", t, count=1)
        t = t.split("\n", 1)[0]
        t = _VC_SPK_RE.sub("", t, count=1)
        return t.strip()
    except Exception:  # noqa: BLE001
        return ""


def _version_chain_cards(tenant: str,
                         facts_all: list[dict[str, Any]],
                         raw_cards: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """[v2新增·仅扩展模式] 版本链: 每条显式变化陈述→成对转变卡
    旧值"[SUPERSEDED] subj: old (...)" + 新值"[CURRENT] subj: new (...)" —
    mem0式转变卡·带双时戳出处·两张都经store_derived入检索窗。
    dia=源卡cid:vc_old/vc_new(幂等)·最多8对·纯函数不占conn·fail-soft→[]。
    [修20261025-5] 输入=facts+原文卡(fact语法不保留from-to·原文卡才保留)。"""
    try:
        import card_construct as cc
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for d in list(facts_all or []) + list(raw_cards or []):
            if len(out) >= 16:
                break  # 最多8对·防刷窗
            if not isinstance(d, dict):
                continue
            text = str(d.get("text", ""))
            if d.get("kind") in ("atom", "fact", "rule", "note", "pattern"):
                pr = parse_change_pair(text)          # 派生卡: 已是干净句·直接解析
            else:
                pr = parse_change_pair(_vc_clean(text))  # 原文卡: 清洗后解析
            if pr is None:
                continue
            subj, old, new, _verb = pr
            dt = str(d.get("dt", ""))
            sess = str(d.get("sess", ""))
            conv = str(d.get("conv", "") or sess)
            cid = cc._cid_of(sess, dt, text)
            if cid in seen:
                continue
            seen.add(cid)
            when = dt[:10] or "unknown"
            # [修20261025-5] 值钳制: 原文句尾常带时间状语/杂讯·超长截断保卡面可读
            subj = subj[:48]
            old = old[:64].rstrip(". ")
            new = new[:64].rstrip(". ")
            out.append({
                "conv": "_sidecar_" + conv, "sess": sess, "dia": f"{cid}:vc_old",
                "dt": dt, "speaker": "", "kind": "atom",
                "text": (f"[SUPERSEDED] {subj}: {old} "
                         f"(superseded by {new}, stated {when})")})
            out.append({
                "conv": "_sidecar_" + conv, "sess": sess, "dia": f"{cid}:vc_new",
                "dt": dt, "speaker": "", "kind": "atom",
                "text": (f"[CURRENT] {subj}: {new} "
                         f"(changed from {old}, stated {when})")})
        return out
    except Exception as exc:  # noqa: BLE001 — 版本链失败只丢增强不丢卡
        print(f"[deep_supersede] version-chain fail-soft tenant={tenant}: "
              f"{type(exc).__name__}: {exc}")
        return []


def parse_fact_text(text: str) -> tuple[str, str, str] | None:
    """fact卡text→(subj, rel, obj)|None(非现在时事实·含planned→None=不参战)。
    形态(deep_fact产出·固定): "A lives in B (date: ...)" /
    "A plans to move to B (planned, stated ...)" / "李明 住在 北京 (日期: ...)" /
    "Bob 计划搬到 伦敦 (计划, 述于 ...)"。确定性纯函数。
    [v2] env门BRAINOS_SUPE_EXPANDED=1时·封闭词表未中→开放类正则兜底(_parse_expanded)。"""
    try:
        t = str(text or "").strip()
        if not t or t.startswith(_NON_FACT_PREFIX):
            return None
        base = t.split(" (")[0].strip()  # 剥日期/planned/changed-from缀(首个" ("前=三元组体)
        if not base:
            return None
        for rel in _RELS:
            for probe, planned in ((f" plans to {rel} ", True),   # EN planned
                                   (f" 计划{rel} ", True),        # CN planned
                                   (f" {rel} ", False)):           # 现在时事实
                pos = base.find(probe)
                if pos < 0:
                    continue
                subj = base[:pos].strip()
                obj = base[pos + len(probe):].strip()
                if not subj or not obj or subj.lower() == obj.lower():
                    return None  # 归属不明→诚实弃(弃卡只丢增益)
                return None if planned else (subj, rel, obj)
        if _expanded():  # [v2新增·env门] 开放类兜底·门关=到不了这里(行为同旧版)
            return _parse_expanded(base)
        return None
    except Exception:  # noqa: BLE001 — fail-soft: 解析失败只丢增强
        return None


def fact_key(tenant: str, subj: str, rel: str) -> str:
    """归组键: f"{tenant}|{组}|{subject.lower()}" — LOC/JOB组内跨relation对撞,
    其余relation各自成组(组名=relation本身)。
    [v2] 扩展模式rel(动词/标记词/变成/不再..)照走同函数→各自成组·无需改本函数。"""
    group = ("LOC" if rel in _LOC_GROUP else "JOB" if rel in _JOB_GROUP else rel)
    return f"{tenant}|{group}|{str(subj).lower()}"


_SCHEMA_DONE = False


def ensure_schema() -> bool:
    """边车DB加列 fact_key/fact_obj/superseded_by + 索引idx_d1_key(幂等·进程一次)。
    连接=card_construct._db()(同库同连接·绝不另开)。duplicate column=忽略。"""
    global _SCHEMA_DONE
    if _SCHEMA_DONE:
        return True
    try:
        import card_construct as cc
        with cc._DB_LOCK:
            conn = cc._db()
            for ddl in ("ALTER TABLE derived ADD COLUMN fact_key TEXT",
                        "ALTER TABLE derived ADD COLUMN fact_obj TEXT",
                        "ALTER TABLE derived ADD COLUMN superseded_by TEXT"):
                try:
                    conn.execute(ddl)
                except Exception:  # noqa: BLE001 — duplicate column等→幂等忽略
                    pass
            conn.execute("CREATE INDEX IF NOT EXISTS idx_d1_key ON derived(tenant, fact_key)")
            conn.commit()
        _SCHEMA_DONE = True
        return True
    except Exception as exc:  # noqa: BLE001 — 派生失败只丢增强不丢卡
        print(f"[deep_supersede] ensure_schema fail-soft: {type(exc).__name__}: {exc}")
        return False


def prepare(tenant: str, facts_all: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """store_derived之前调用: 批内+跨请求新旧值对撞。
    新卡dict的text就地追加出处缀(cid由最终text算·store用同text同cid);
    返回对撞清单[(old_cid, new_cid)]供commit。任何异常→[](绝不抛出)。
    [v2] 扩展模式经parse_fact_text兜底自动入组·本函数零改动。"""
    try:
        if not ensure_schema() or not facts_all:
            return []
        import card_construct as cc
        groups: dict[str, list[tuple[str, int, dict[str, Any], str]]] = {}
        for i, d in enumerate(facts_all):
            pr = parse_fact_text(str(d.get("text", "")) if isinstance(d, dict) else "")
            if pr is None:
                continue  # planned/CAUSAL/不可解析→永不参战
            subj, rel, obj = pr
            groups.setdefault(fact_key(tenant, subj, rel), []).append(
                (str(d.get("dt", "")), i, d, obj))
        collisions: list[tuple[str, str]] = []
        with cc._DB_LOCK:
            conn = cc._db()
            for key in sorted(groups):  # 键序确定性
                items = sorted(groups[key], key=lambda x: (x[0], x[1]))  # dt升序→批内序tie
                surv_dt, _si, surv, s_obj = items[-1]  # 批内最新=存活者
                olds: list[tuple[str, str, str]] = []  # (dt, cid, obj)
                for dt_i, _i, d_i, obj_i in items[:-1]:
                    if obj_i != s_obj:  # 同对象=重述非变更·不对撞
                        olds.append((dt_i, cc._cid_of(str(d_i.get("sess", "")), dt_i,
                                                      str(d_i.get("text", ""))), obj_i))
                try:
                    rows = conn.execute(
                        "SELECT cid, dt, fact_obj FROM derived "
                        "WHERE tenant=? AND fact_key=? AND (superseded_by IS NULL OR superseded_by='')",
                        (tenant, key)).fetchall()
                except Exception:  # noqa: BLE001 — 查询失败只丢跨请求对撞
                    rows = []
                for cid_o, dt_o, obj_o in rows:
                    if obj_o and str(obj_o) != s_obj:
                        olds.append((str(dt_o or ""), str(cid_o), str(obj_o)))
                if not olds:
                    continue
                olds.sort(key=lambda x: (x[0], x[1]))  # dt升序·cidtie→最近旧值=note出处
                note_dt, _oc, note_obj = olds[-1]
                suffix = (f" (changed from {note_obj}, was {note_dt[:10]})" if note_dt
                          else f" (changed from {note_obj})")
                surv["text"] = str(surv.get("text", "")) + suffix
                new_cid = cc._cid_of(str(surv.get("sess", "")), surv_dt, str(surv.get("text", "")))
                collisions.extend((old_cid, new_cid) for _dt, old_cid, _ob in olds)
        return collisions
    except Exception as exc:  # noqa: BLE001 — 派生失败只丢增强不丢卡
        print(f"[deep_supersede] prepare fail-soft tenant={tenant}: {type(exc).__name__}: {exc}")
        return []


def commit(tenant: str, collisions: list[tuple[str, str]],
           facts_all: list[dict[str, Any]],
           raw_cards: list[dict[str, Any]] | None = None) -> None:
    """store_derived之后调用: ①旧行标superseded_by=new_cid(逐条fail-soft)
    ②新卡落fact_key/fact_obj(跨请求对撞依据·planned不落=永不被发现)
    ③失效该租户内存缓存(写时变更·load_derived必须重读)。任何异常→打印log返回。
    [v2·仅env门开启] ④版本链: 显式from-X-to-Y/从X变成Y→[SUPERSEDED]+[CURRENT]成对卡
    两张都store_derived入窗(dia幂等·最多8对·失败不伤主链)。"""
    try:
        import card_construct as cc
        n_mark = n_key = 0
        update_cards: list[dict[str, Any]] = []
        vc_cards: list[dict[str, Any]] = (_version_chain_cards(tenant, facts_all, raw_cards)
                                          if _expanded() else [])  # [v2新增·env门]先建(纯函数·自fail-soft)
        with cc._DB_LOCK:
            conn = cc._db()
            for old_cid, new_cid in collisions or []:
                try:
                    cur = conn.execute(
                        "UPDATE derived SET superseded_by=? WHERE tenant=? AND cid=?",
                        (str(new_cid), tenant, str(old_cid)))
                    n_mark += max(0, cur.rowcount)
                    if cur.rowcount and len(update_cards) < 8:
                        orow = conn.execute(
                            "SELECT dt, fact_obj, conv, sess FROM derived WHERE tenant=? AND cid=?",
                            (tenant, str(old_cid))).fetchone()
                        nrow = conn.execute(
                            "SELECT dt, fact_obj, conv, sess FROM derived WHERE tenant=? AND cid=?",
                            (tenant, str(new_cid))).fetchone()
                        if orow and nrow and orow[1] and nrow[1]:
                            subj = str(fact_key(tenant, "", "")).split("|")
                            fkey = conn.execute(
                                "SELECT fact_key FROM derived WHERE tenant=? AND cid=?",
                                (tenant, str(new_cid))).fetchone()
                            subj = (fkey[0].split("|")[2] if fkey and fkey[0] else "该主体")
                            update_cards.append({
                                "conv": "_sidecar_" + str(nrow[2] or ""),
                                "sess": str(nrow[3] or ""),
                                "dia": f"{new_cid}:update",
                                "dt": str(nrow[0] or ""),
                                "speaker": "",
                                "text": (f"UPDATE: {subj} changed from {orow[1]} to {nrow[1]}"
                                         f" (stated {str(orow[0] or '')[:10]} -> {str(nrow[0] or '')[:10]})"),
                                "kind": "atom",
                            })
                except Exception:  # noqa: BLE001 — 单条失败不影响其余
                    continue
            for d in facts_all or []:
                if not isinstance(d, dict):
                    continue
                pr = parse_fact_text(str(d.get("text", "")))
                if pr is None:
                    continue  # planned/不可解析→不落key(lookup永不见=永不参战)
                subj, rel, obj = pr
                cid = cc._cid_of(str(d.get("sess", "")), str(d.get("dt", "")),
                                 str(d.get("text", "")))
                try:
                    conn.execute(
                        "UPDATE derived SET fact_key=?, fact_obj=? WHERE tenant=? AND cid=?",
                        (fact_key(tenant, subj, rel), obj, tenant, cid))
                    n_key += 1
                except Exception:  # noqa: BLE001
                    continue
            conn.commit()
        if update_cards:
            try:
                cc.store_derived(tenant, update_cards, "atom")
            except Exception as exc:  # noqa: BLE001 — 叙事卡失败不伤主链
                print(f"[deep_supersede] update-cards fail-soft: {type(exc).__name__}: {exc}")
        if vc_cards:  # [v2新增·版本链] 旧值+新值两张都入窗(store_derived→检索窗)
            try:
                cc.store_derived(tenant, vc_cards, "atom")
            except Exception as exc:  # noqa: BLE001 — 版本链卡失败不伤主链
                print(f"[deep_supersede] version-cards fail-soft: {type(exc).__name__}: {exc}")
        cc._TENANT_CACHE.pop(tenant, None)  # 写时变更→必须失效租户缓存
        print(f"[deep_supersede] commit tenant={tenant} superseded={n_mark} keyed={n_key}")
    except Exception as exc:  # noqa: BLE001 — 派生失败只丢增强不丢卡
        print(f"[deep_supersede] commit fail-soft tenant={tenant}: {type(exc).__name__}: {exc}")
