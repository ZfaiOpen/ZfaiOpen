# deep_expdistill.py — M3 经验蒸馏器(FIX/ANTI/COMPR 英雄卡·纯规则零LLM·2026-10-25 轨道C)
# ═════════════════════════════════════════════════════════════════════════════
# 依据(CODING_PLAN §M3): SWE-Exp(arXiv 2507.23361)经验双面=理解面(COMPR)+修改面(FIX)
# ·k=1最优; ReasoningBank(arXiv 2509.25140)失败经验与成功等价(ANTI)·Title/Body两段式。
# 三卡(英文·≤220 token·Title/Body两段):
#   FIX   成功修改经验: errsig+失败测试+根因短语+改动文件/函数+验证命令
#   ANTI  失败经验(防走死路): 试过什么+Exit n+仍失败测试
#   COMPR 理解经验: issue表象→实际根因模块映射(diff归属)
#
# 终态判定(核心防线=erank v3"中途假pass"血泪教训): 会话事件流(Exit code/FAILED/
#   Traceback/passed摘要/"All tests pass"短语各带位置)·成功=diff存在 ∧ 末事件=成功
#   ∧ 其后无失败痕迹 ∧ 会话内先有过失败(SYMPTOM有源·宁缺毋滥); 失败=末事件=失败
#   ∧ (同一测试≥2次失败 ∨ 有diff仍FAIL); 其余→不产卡。
# [U3 2026-10-26] 产率门BRAINOS_EXP_RELAX(缺省0=关·关=上面严口径逐字节等价):
#   开=末事件pass场景三选二(C1 diff/C2 末pass/C3 先有失败·任二即FIX)+两道新闸——
#   (a)非具体pass(裸OK/无数量)不采信放宽分支 (b)VERIFY命令必须轨迹可提取·无→降级
#   COMPR。治P3: rt7实测FIX产率44%(11/25合格分母)→放宽后60%+且假FIX=0(对抗组)。
#
# 存储(实测card_construct惯例后定): **独立表**(同DB文件 card_construct.db·经
#   cc._db()/cc._DB_LOCK·deep_digest先例)·不入 derived 表 — derived 会被
#   overlay_inject 当atom池词面打分→英雄卡会在overlay位被二次注入且窗位失控;
#   独立表 exp_cards 使 aml_service.py 成为唯一挂接面(hero_cards→窗位1-3由
#   搜索相hunk精确控制)。表:
#     exp_trace(tenant,sess,dia PK,dt,text) — 会话原文卡累积(跨请求会话·INSERT OR
#       IGNORE by dia=幂等重放安全·ORDER BY dt,rowid=稳定时序)
#     exp_cards(tenant,cid PK,sess,dia,dt,kind,errsig,files,syms,test,text,seq)
#       — 英雄卡(expfix/expanti/expcmpr)·每会话先DELETE后INSERT=会话增长自然supersede
#
# 检索 hero_cards(query, tenant, fingerprint=None) ≤3张: errsig精确(100)>文件重叠(60)>
#   符号重叠(40)>词面兜底(≤30·cc._tokens同规); ANTI在query含失败语境时+25提权。
#   fingerprint可选参数=轨道B knife13_repo.fingerprint(text)输出dict(files/symbols/
#   errsig键名防御性适配); 缺省自己尝试import knife13_repo·import失败→跳过(fail-soft)。
#
# 纪律: 确定性(禁now()/随机·同轨迹同卡)·fail-soft(任何异常只丢英雄卡绝不影响add/search)
#   ·零LLM零外部API纯规则·env门BRAINOS_EXP_DISTILL缺省关(关=observe no-op/hero空)。
# 挂接: aml_service.py 两处hunk(add相observe_session_end·搜索相erank后hero_cards置顶)。
# 回滚: 还原aml_service两hunk + 摘除本件(exp表可留档·无organ扰动=零回归由构造保证)。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

log = logging.getLogger("expdistill")

_ENV_GATE = "BRAINOS_EXP_DISTILL"
# [U3·2026-10-26] FIX产率门(CODING_PLAN §7 U3·治P3=56% bugFix查询英雄卡空窗):
# 缺省0=关=终态判定与pre-U3逐字节等价(严口径三全)。开=末事件pass场景三选二放宽+两道新闸。
_RELAX_ENV = "BRAINOS_EXP_RELAX"


def ON() -> bool:
    """env门BRAINOS_EXP_DISTILL(缺省0=关): 关=observe no-op·hero_cards空·零DB触碰。"""
    try:
        return str(os.environ.get(_ENV_GATE, "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def relax_on() -> bool:
    """env门BRAINOS_EXP_RELAX(缺省0=关·逐字节等价铁律): 关=_judge严口径不变;
    开=末事件pass场景三选二(C1 diff存在/C2 末pass/C3 先有失败·任二即FIX)+两道新闸
    防假FIX——闸(a)非具体pass(裸OK/"all tests pass"/纯exit0·无测试数量可锚)不采信
    放宽分支(需三全=退回严口径); 闸(b)VERIFY命令必须从轨迹提取到场·无→卡降级COMPR。
    只放宽PASS终态·失败终态(ANTI)与_judge拒判路径不受影响。"""
    try:
        return str(os.environ.get(_RELAX_ENV, "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _hero_max() -> int:
    try:
        return max(1, min(3, int(os.environ.get("BRAINOS_EXP_HERO_MAX", "3"))))
    except (TypeError, ValueError):
        return 3


# ── [R1·2026-10-28] 英雄卡社区一致门(CODING_PLAN §7 R1·治冒烟5暴露的跨题污染源①) ──
# 病灶(FFPF2_REPORT/V双模式归因): errsig家族前缀匹配(he/qf errsig首段同类型+70)让
#   异库/异题英雄卡撞进窗——实测 astropy 题窗内出现 pandas/progressbar 英雄卡
#   (ModuleNotFoundError 家族前缀撞卡)。英雄卡是跨任务经验, 错库经验=锚定错误既往修法
#   (V实验"富窗全败"归因的成分之一)。
# 处方: hero_cards 出口加社区一致校验——英雄卡只在「query的社区==英雄卡源的社区」时
#   入窗; 跨库/跨题 errsig 家族撞卡直接拒。query社区= fingerprint 参自带 "community"
#   字段(caller预计算时优先)∨ k13.query_community; 英雄卡源社区= k13 mirror 按源会话
#   (sess)反查 communities_of_sess(英雄卡是蒸馏新卡不在k13索引, 但其源会话原文卡在
#   add相已被k13.observe镜像·sess即源任务身份)。
# fail-soft 四放行(兼容承诺): 门关 / k13缺席·库坏·异常 / query无社区(纯描述退化·
#   老租户无k13数据) / 该英雄卡源无社区(未被k13镜像过的老会话) → 一律放行原样。
# env BRAINOS_HERO_COMM 缺省0=关=逐字节等价(英雄卡选取·排序·cid·content全不变)。
_COMM_ENV = "BRAINOS_HERO_COMM"


def hero_comm_on() -> bool:
    """env门BRAINOS_HERO_COMM(缺省0=关): 关=hero_cards与pre-R1逐字节等价。"""
    try:
        return str(os.environ.get(_COMM_ENV, "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _query_community(tenant: str, query: str,
                     fingerprint: dict[str, Any] | None = None) -> str:
    """query社区: fingerprint参"community"字段(str·caller预计算)优先, 否则
    k13.query_community。k13缺席/异常/无社区→""(调用方按无信号放行)。"""
    try:
        if isinstance(fingerprint, dict):
            v = fingerprint.get("community") or fingerprint.get("comm")
            if isinstance(v, str) and v.strip():
                return v.strip()
        import knife13_repo as k13  # noqa: PLC0415 — 延迟import(缺席=无门)
        return str(k13.query_community(str(tenant), str(query or "")) or "")
    except Exception:  # noqa: BLE001
        return ""


def _src_comms(tenant: str, sess: str) -> set[str]:
    """英雄卡源会话→k13社区集。空sess/k13缺席/无镜像→set()(=无社区·放行兼容)。"""
    try:
        import knife13_repo as k13  # noqa: PLC0415
        m = k13.communities_of_sess(str(tenant), str(sess or ""))
        return {str(v) for v in (m or {}).values() if v}
    except Exception:  # noqa: BLE001
        return set()


# ── 侧车DB(同card_construct.db·复用连接与锁·deep_digest先例) ────────────────
_DDL = (
    """CREATE TABLE IF NOT EXISTS exp_trace(
        tenant TEXT NOT NULL, sess TEXT NOT NULL, dia TEXT NOT NULL,
        dt TEXT NOT NULL, speaker TEXT NOT NULL DEFAULT '', text TEXT NOT NULL,
        PRIMARY KEY(tenant, dia))""",
    "CREATE INDEX IF NOT EXISTS idx_exp_tr ON exp_trace(tenant, sess)",
    """CREATE TABLE IF NOT EXISTS exp_cards(
        tenant TEXT NOT NULL, cid TEXT NOT NULL, sess TEXT NOT NULL,
        dia TEXT NOT NULL, dt TEXT NOT NULL, kind TEXT NOT NULL,
        errsig TEXT NOT NULL DEFAULT '', files TEXT NOT NULL DEFAULT '[]',
        syms TEXT NOT NULL DEFAULT '[]', test TEXT NOT NULL DEFAULT '',
        text TEXT NOT NULL, seq INTEGER NOT NULL,
        PRIMARY KEY(tenant, cid))""",
    "CREATE INDEX IF NOT EXISTS idx_exp_hc ON exp_cards(tenant, sess)",
)
_READY = False
_HCACHE: dict[str, list[dict[str, Any]]] = {}  # tenant→英雄卡行缓存(observe时失效)


def _db():
    import card_construct as cc  # 延迟import: 复用_db()连接/_DB_LOCK(BRAINOS_CC_DB同源)
    global _READY
    conn = cc._db()
    if not _READY:
        for stmt in _DDL:
            conn.execute(stmt)
        conn.commit()
        _READY = True
    return conn, cc


# ── 痕迹正则(每行·[xxx]平台前缀宽容) ────────────────────────────────────────
_TAGP = r"(?:\[[^\]]*\]\s*)?"
_EXIT_RE = re.compile(r"^\s*" + _TAGP + r"[Ee]xit\s+code\s+(\d+)\b")
_EXC_RE = re.compile(r"^\s*" + _TAGP +
                     r"((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*(?:Error|Exception|Interrupt|"
                     r"Timeout|Failure|Exit))\s*[:：]\s*(.+)$")
_FAILED_RE = re.compile(r"^\s*" + _TAGP + r"=*\s*(?:FAILED|ERROR)\s+"
                        r"([^\s:]+(?:::[\w.\[\]'\"<>-]+)?)")
_FAILSUM_RE = re.compile(r"^\s*" + _TAGP + r"=*»?\s*(?:\[?short test summary\]?\s*)?"
                         r"(?:(\d+)\s+failed\b|\bfailed\D(\d+))", re.I)
_TB_RE = re.compile(r"^\s*" + _TAGP + r"Traceback \(most recent call last\)")
_PASSED_RE = re.compile(r"^\s*" + _TAGP + r"(\d+)\s+passed\b([^\n]*)$", re.I)
_ALLPASS_RE = re.compile(r"\ball\s+(?:\d+\s+)?tests?\s+(?:pass|passed|passing)\b", re.I)
_OK_LINE_RE = re.compile(r"^\s*" + _TAGP + r"OK\s*$")  # unittest/nose2 全绿摘要
_DIFF_RE = re.compile(r"diff --git a/(\S+)(?:\s+b/(\S+))?")
_HUNK_FUNC_RE = re.compile(r"^@@ [^@]*@@\s*(?:async\s+def|def|class)\s+(\w+)", re.M)
_CMD_RE = re.compile(
    r"(?:^|\n)\s*" + _TAGP + r"(?:\$ |>>> )?" +
    r"((?:\./)?(?:python3?|pypy3?)?\s*(?:-m\s+)?(?:pytest|py\.test|unittest|"
    r"nosetests|tox|make\s+check|make\s+test|go\s+test|npm\s+test|yarn\s+test|"
    r"cargo\s+test|rspec|mvn\s+test|gradle\s+test)\b[^\n]*)", re.I)
# 根因短语(成功前最后thinking/总结句抽取锚)
_ROOT_RE = re.compile(
    r"(root\s+cause|the\s+(?:issue|problem|bug|error|failure)\s+(?:is|was)|"
    r"turns?\s+out|caused\s+by|due\s+to|the\s+(?:real|actual)\s+(?:problem|cause|bug)|"
    r"the\s+fix(?:\s+is)?|misunderstood|mismatch(?:ed)?|wrong(?:ly)?|incorrect|"
    r"missing|off\s+by|forgot|typo)", re.I)
# 平台标记剥除(契约: 无平台标记泄漏)
_PLAT_MARK_RE = re.compile(
    r"\s*\[(?:thinking|tool_result|tool_use|Time:[^\]]*|tool\w*|part \d+/\d+)\]\s*")
_SPEAKER_RE = re.compile(r"^(?:[A-Z][a-z]{1,12}|User|Assistant|Agent)\s*[:：]\s*")
_OPENER_RE = re.compile(
    r"^(?:i\s+(?:think|believe|found|see|notice[d]?|realiz(?:e|ed)|was)|we\s+"
    r"(?:think|found|see|realiz(?:e|ed))|it\s+turns\s+out|turns\s+out|so\s+"
    r"(?:i|we|the)|basically|actually)[,\s]+", re.I)
_ERR_STOP = {"error", "exception", "assertion", "assert", "failed", "failure", "raise",
             "when", "while", "test", "tests", "the", "and", "with", "that", "this",
             "from", "line", "file", "code", "could", "should", "would", "cannot",
             "must", "unable", "instead", "still", "every", "after", "before",
             "during", "because", "value", "type"}


def _clean(text: str, cap: int = 160) -> str:
    """片段净化: 剥平台标记/说话人前缀/引号噪声·压空白·钳长(词边界截)。"""
    t = _PLAT_MARK_RE.sub(" ", str(text or ""))
    t = _SPEAKER_RE.sub("", t.strip())
    t = _OPENER_RE.sub("", t.strip())
    t = re.sub(r"\s+", " ", t).strip().strip("`\"'“”‘’ ")
    if len(t) > cap:
        cut = t.rfind(" ", 0, cap)
        t = t[:cut if cut > cap // 2 else cap].rstrip(",;: ")
    return t


# ── errsig: 异常类型+消息关键摘要(确定性归一) ────────────────────────────────
def normalize_errsig(exc_type: str, msg: str = "") -> str:
    """→"TypeError-none-strict"式errsig(≤48字符): 类型原形+消息前3个显著词(≥4字·
    小写·去停用·下划线连字)。query侧与卡侧同规=可精确相交。空→""。"""
    et = re.sub(r"[^A-Za-z0-9_.]", "", str(exc_type or "").split(".")[-1])
    if not et:
        return ""
    words: list[str] = []
    for w in re.findall(r"[A-Za-z][A-Za-z0-9_]{3,}", str(msg or "")):
        wl = w.lower()
        if wl in _ERR_STOP or wl == et.lower():
            continue
        if wl not in words:
            words.append(wl)
        if len(words) >= 3:
            break
    sig = "-".join([et] + words) if words else et
    return sig[:48]


_EXC_ANY_RE = re.compile(r"\b((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*(?:Error|Exception|"
                         r"Interrupt|Timeout|Failure|Exit))\s*[:：]\s*([^\n]{1,140})")


def errsig_of_text(text: str) -> str:
    """文本首条异常→errsig: 行首异常(_EXC_RE)∨行内任意位置异常(FAILED后缀/
    traceback末行·_EXC_ANY_RE)。query侧与卡侧同规。无→""。"""
    for line in str(text or "").splitlines():
        m = _EXC_RE.match(line) or _EXC_ANY_RE.search(line)
        if m:
            return normalize_errsig(m.group(1), m.group(2))
    return ""


# ── 会话事件流(终态判定核心) ────────────────────────────────────────────────
def _events(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按(dt,rowid序)卡流→事件流: {kind, ci(卡序), payload}。
    kind: exit0/exitN(Exit code)·fail(FAILED/ERROR test)·exc(异常行)·tb(traceback)
          ·failsum(N failed)·pass(通过摘要/All tests pass/OK)。中性行无事件。"""
    ev: list[dict[str, Any]] = []
    for ci, c in enumerate(cards):
        for line in str(c.get("text", "")).splitlines():
            m = _EXIT_RE.match(line)
            if m:
                n = int(m.group(1))
                ev.append({"kind": "exit0" if n == 0 else "exitN", "ci": ci,
                           "payload": n, "line": line})
                continue
            m = _FAILED_RE.match(line)
            if m:
                ev.append({"kind": "fail", "ci": ci, "payload": m.group(1), "line": line})
                continue
            m = _FAILSUM_RE.match(line)
            if m:
                ev.append({"kind": "failsum", "ci": ci,
                           "payload": m.group(1) or m.group(2) or "", "line": line})
                continue
            if _TB_RE.match(line):
                ev.append({"kind": "tb", "ci": ci, "payload": "", "line": line})
                continue
            pm = _PASSED_RE.match(line)
            if pm:
                tail = pm.group(2) or ""
                if not re.search(r"\bfailed\b|\berror", tail, re.I):
                    ev.append({"kind": "pass", "ci": ci,
                               "payload": pm.group(1) + " passed", "line": line})
                continue
            if _ALLPASS_RE.search(line) or _OK_LINE_RE.match(line):
                ev.append({"kind": "pass", "ci": ci, "payload": "all tests pass", "line": line})
                continue
            m = _EXC_RE.match(line)
            if m:
                ev.append({"kind": "exc", "ci": ci,
                           "payload": normalize_errsig(m.group(1), m.group(2)),
                           "line": m.group(1) + ": " + m.group(2)})
                continue
    return ev


_FAIL_KINDS = ("exitN", "fail", "tb", "exc", "failsum")
_PASS_KINDS = ("exit0", "pass")


def _diff_info(cards: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """diff头→(改动文件有序去重, 捕获函数名)。文件/函数=CHANGED字段与COMPR模块归属源。"""
    files: list[str] = []
    funcs: list[str] = []
    for c in cards:
        t = str(c.get("text", ""))
        for m in _DIFF_RE.finditer(t):
            f = m.group(1) or m.group(2) or ""
            if f and f not in files:
                files.append(f)
        for m in _HUNK_FUNC_RE.finditer(t):
            f = m.group(1)
            if f and f not in funcs:
                funcs.append(f)
    return files[:8], funcs[:6]


def _last_cmd(cards: list[dict[str, Any]], upto_ci: int) -> str:
    """成功/失败位置前最后一条测试命令(VERIFY/tried字段源)。无→""。"""
    best = ""
    for ci in range(min(upto_ci, len(cards) - 1), -1, -1):
        t = str(cards[ci].get("text", ""))
        ms = _CMD_RE.findall(t)
        if ms:
            best = _clean(ms[-1], 64)
            break
    return best


def _terminal_test(events: list[dict[str, Any]]) -> str:
    """末位失败事件关联的测试名(最近的FAILED/ERROR行)。无→""。"""
    for ev in reversed(events):
        if ev["kind"] == "fail" and ev["payload"]:
            return str(ev["payload"])
    return ""


def _judge(events: list[dict[str, Any]], has_diff: bool
           ) -> tuple[str, dict[str, Any]]:
    """终态判定(伪码见模块头·宁缺毋滥):
    success ⇔ has_diff ∧ 末事件∈{exit0,pass} ∧ 全流先有过失败事件(SYMPTOM有源)
    fail    ⇔ 末事件∈失败族 ∧ (同一测试≥2次失败 ∨ has_diff)
    其余 → ("", ctx)。"""
    if not events:
        return "", {}
    last = events[-1]
    n_fail_ev = sum(1 for e in events if e["kind"] in _FAIL_KINDS)
    if last["kind"] in _PASS_KINDS:
        # 终端位置通过 ∧ diff存在 ∧ 先有失败(修复语义成立·无失败=非FIX不产卡)
        if has_diff and n_fail_ev >= 1:
            first_fail = next(e for e in events if e["kind"] in _FAIL_KINDS)
            return "success", {"first_fail": first_fail, "ci": last["ci"]}
        return "", {}
    if last["kind"] in _FAIL_KINDS:
        ttest = _terminal_test(events)
        attempts = sum(1 for e in events
                       if e["kind"] == "fail" and ttest and str(e["payload"]) == ttest)
        exit_n = last["payload"] if last["kind"] == "exitN" else ""
        if attempts >= 2 or has_diff:
            first_fail = next(e for e in events if e["kind"] in _FAIL_KINDS)
            return "fail", {"first_fail": first_fail, "ci": last["ci"],
                            "test": ttest, "exit": exit_n, "attempts": attempts}
    return "", {}


# ── [U3] RELAX放宽判定(仅RELAX开·_judge严口径已拒后调用) ────────────────────
_PASS_SPECIFIC_RE = re.compile(r"^\d+\s+passed\b")
_SBCTX_LINE_RE = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*\[SBCTX[^\]]*\]")


def _pass_specific(events: list[dict[str, Any]]) -> bool:
    """闸(a)具体性: 末次失败事件之后的终态块里存在带数量的pass行("3 passed")=可信
    (中途假pass在末次失败前·不计入); 裸OK/"all tests pass"/纯exit0=非具体(无测试
    规模可锚)→放宽分支不采信(需三全=退回严口径)。"""
    last_fail_i = -1
    for i, e in enumerate(events):
        if e["kind"] in _FAIL_KINDS:
            last_fail_i = i
    return any(e["kind"] == "pass" and _PASS_SPECIFIC_RE.match(str(e.get("payload", "")))
               for e in events[last_fail_i + 1:])


def _issue_title_relax(cards: list[dict[str, Any]]) -> str:
    """RELAX专用issue表象: 最早user卡首个实质行(SBCTX任务标记行/diff行整行跳过·
    防平台标记泄漏卡面)。无→""。仅RELAX路径使用·不动_issue_title(逐字节等价)。"""
    for c in cards:
        if str(c.get("speaker", "")) != "user":
            continue
        for ln in str(c.get("text", "")).splitlines():
            if _SBCTX_LINE_RE.match(ln) or _DIFF_RE.search(ln):
                continue
            t = _clean(ln, 90)
            if t:
                return t
    return ""


def _judge_relax(events: list[dict[str, Any]], has_diff: bool,
                 cards: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    """U3三选二放宽(仅末事件=pass场景·失败终态/拒判路径不经过此处):
    C1=diff存在 C2=末事件∈{exit0,pass} C3=先有失败 → 任二即success;
    闸(a): 具体性=末次失败后的终态块存在带数量pass行("N passed")·裸OK/all-tests-
    pass/纯exit0需三全=本分支全拒;
    闸(b): VERIFY命令在场→调用方建卡时强制(observe内·无命令降级COMPR不入此)。
    无失败事件分支SYMPTOM回源=最早user卡实质首句(议题表象·无→不产卡宁缺毋滥)。"""
    if not events:
        return "", {}
    last = events[-1]
    if last["kind"] not in _PASS_KINDS:
        return "", {}
    c1 = bool(has_diff)
    c3 = any(e["kind"] in _FAIL_KINDS for e in events)
    if (c1 and c3) or not (c1 or c3):
        # 三全=_judge已判success不到此(防御); 零分=仅C2不过闸
        return "", {}
    if not _pass_specific(events):  # 闸(a): 二选二必须带具体pass(末次失败后的终态块)
        return "", {}
    ctx: dict[str, Any] = {"ci": last["ci"], "relax": "nodiff" if c3 else "nofail"}
    if c3:
        ctx["first_fail"] = next(e for e in events if e["kind"] in _FAIL_KINDS)
    else:
        itt = _issue_title_relax(cards)
        if not itt:
            return "", {}
        ci_tt = 0
        for i, c in enumerate(cards):
            if str(c.get("speaker", "")) == "user":
                ci_tt = i
                break
        ctx["first_fail"] = {"kind": "issue", "ci": ci_tt, "line": itt}
    return "success", ctx


def _downgrade_compr(cards: list[dict[str, Any]], fix: dict[str, Any],
                     files: list[str]) -> dict[str, Any] | None:
    """闸(b)降级: FIX字段齐但轨迹无可提取验证命令→降COMPR(理解经验·宁实勿虚)。
    文件已知→ISSUE-MAP模块归属(与既有COMPR同式); 否则退化errsig类型/诚实未知。
    议题表象缺源→None(不硬塞)。"""
    itt = _issue_title_relax(cards)
    if not itt:
        return None
    frag = _clean(itt, 60)
    if files:
        target = _module_of(files)
    else:
        et = str(fix.get("errsig", "") or "").split("-")[0]
        target = et if et else "an unparsed module"
    return {"kind": "expcmpr", "text": f"ISSUE-MAP: '{frag}' was actually {target}",
            "errsig": str(fix.get("errsig", "")), "files": files, "syms": [], "test": ""}


# ── 蒸馏卡构造(英文·Title/Body·≤220token≈880字符) ───────────────────────────
def _root_phrase(cards: list[dict[str, Any]], upto_ci: int) -> str:
    """成功位置前(含)最近的根因句: 根因短语锚定的句子片段·剥平台标记/说话人/开场白。
    无→""(FIX卡ROOT记"(not captured)"·诚实)。"""
    for ci in range(min(upto_ci, len(cards) - 1), -1, -1):
        for line in str(cards[ci].get("text", "")).splitlines():
            m = _ROOT_RE.search(line)
            if not m:
                continue
            frag = line[m.start():]
            frag = re.split(r"(?<=[.!?;])\s", frag)[0]
            frag = _clean(frag, 140)
            if len(frag) >= 12:
                return frag
    return ""


def _issue_title(cards: list[dict[str, Any]]) -> str:
    """issue标题卡: 最早user卡首句(COMPR的'表象'侧)。无→""。"""
    for c in cards:
        if str(c.get("speaker", "")) != "user":
            continue
        t = _clean(str(c.get("text", "")).splitlines()[0] if str(c.get("text", "")).splitlines() else "", 90)
        if t and not _DIFF_RE.search(str(c.get("text", ""))):
            return t
    return ""


def _module_of(files: list[str]) -> str:
    """diff归属模块: 多文件时取最长公共前缀路径段(≥1段)·单文件取全路径·空→""。"""
    if not files:
        return ""
    if len(files) == 1:
        return files[0]
    segs = [f.split("/") for f in files]
    common: list[str] = []
    for parts in zip(*segs):
        if all(p == parts[0] for p in parts):
            common.append(parts[0])
        else:
            break
    if common and len(common) < max(len(s) for s in segs):
        return "/".join(common)
    return files[0]


def _build_fix(cards: list[dict[str, Any]], ctx: dict[str, Any],
               files: list[str], funcs: list[str]) -> dict[str, Any] | None:
    """成功会话→FIX卡(+COMPR卡当标题误导)。字段抽取失败到拼不出SYMPTOM→None(宁缺毋滥)。"""
    ff = ctx.get("first_fail") or {}
    errsig = str(ff.get("payload") or "") if ff.get("kind") == "exc" else ""
    if not errsig:
        errsig = errsig_of_text(str(ff.get("line", "")))
    if not errsig and cards:
        fci = min(int(ff.get("ci", 0) or 0), len(cards) - 1)
        errsig = errsig_of_text(str(cards[fci].get("text", "")))
    symptom_line = _clean(ff.get("line", ""), 110)
    if not symptom_line:  # SYMPTOM无源不产卡
        return None
    verify = _last_cmd(cards, int(ctx.get("ci", 0)))
    root = _root_phrase(cards, int(ctx.get("ci", 0))) or "(not captured)"
    primary = os.path.basename(files[0]) if files else "code"
    et = errsig.split("-")[0] if errsig else "tests"
    title = f"fix {et} in {primary}"
    changed_items: list[str] = list(files[:3])
    for fn in funcs[:2]:
        changed_items.append(f"{primary}::{fn}" if files else fn)
    changed = ", ".join(changed_items[:5])
    if not changed:
        changed = "(changed paths unparsed)"  # 经典success路径恒有files·此分支仅RELAX可达
    text = (f"FIX[{errsig or 'fixed'}] {title}\n"
            f"SYMPTOM: {symptom_line}\n"
            f"ROOT: {root}\n"
            f"CHANGED: {changed}\n"
            f"VERIFY: {verify or 'run tests'}")
    if len(text) > 880:  # ≤220token硬门: 先裁CHANGED再裁ROOT(词边界)
        text = text[:880 - 3].rsplit(" ", 1)[0] + " ..."
    out = {"kind": "expfix", "text": text, "errsig": errsig,
           "files": files, "syms": funcs, "test": ""}
    # COMPR: issue标题与diff模块词面不相交=标题误导→理解经验卡
    # [U3] RELAX开时用SBCTX标记行感知的表象抽取(防平台标记泄漏卡面·闸关=原函数逐字节等价)
    title_txt = _issue_title_relax(cards) if relax_on() else _issue_title(cards)
    if title_txt and files:
        import card_construct as cc
        tt = cc._tokens(title_txt)
        ft = cc._tokens(" ".join(files) + " " + " ".join(
            os.path.splitext(os.path.basename(f))[0] for f in files))
        if tt and not (tt & ft):
            frag = _clean(title_txt, 60)
            out["compr"] = (f"ISSUE-MAP: '{frag}' was actually {_module_of(files)}")
    return out


def _build_anti(cards: list[dict[str, Any]], ctx: dict[str, Any],
                files: list[str], funcs: list[str]) -> dict[str, Any] | None:
    """失败会话→ANTI卡: tried(末位失败前最后命令∨编辑概述)+Exit n+仍失败测试。"""
    ff = ctx.get("first_fail") or {}
    errsig = str(ff.get("payload") or "") if ff.get("kind") == "exc" else ""
    if not errsig:
        errsig = errsig_of_text(str(ff.get("line", "")))
    if not errsig and cards:
        fci = min(int(ff.get("ci", 0) or 0), len(cards) - 1)
        errsig = errsig_of_text(str(cards[fci].get("text", "")))
    ci = int(ctx.get("ci", 0))
    tried = _last_cmd(cards, ci)
    if not tried and files:
        tried = "editing " + ", ".join(files[:2])
    test = str(ctx.get("test") or "") or "(unknown test)"
    exit_n = str(ctx.get("exit") or "") or "N"
    text = (f"ANTI[{errsig or 'uncategorized'}] tried {tried or 'previous fixes'}, "
            f"Exit {exit_n}, {test} still failing")
    if len(text) > 400:
        text = text[:397] + "..."
    return {"kind": "expanti", "text": text, "errsig": errsig,
            "files": files, "syms": funcs, "test": test}


# ── add相: 会话观察(总入口) ─────────────────────────────────────────────────
def observe_session_end(cards: list[dict[str, Any]], tenant: str) -> int:
    """会话终态观察→英雄卡落库。cards=add请求原文卡(derive_all返回流·任意顺序·
    防御性按dt稳定排序); 跨请求会话经exp_trace累积(同dia INSERT OR IGNORE=幂等)。
    返回落库英雄卡数(门关/无新卡/异常→0·绝不抛)。每会话先DELETE后INSERT=会话
    增长时自然supersede·同轨迹重复observe幂等(cid稳定·行数稳定)。"""
    if not ON() or not cards:
        return 0
    try:
        tenant = str(tenant or "default")
        conn, cc = _db()
        # 1. 累积: 本次卡入exp_trace(dia幂等)·收集触碰的sess
        sess_set: list[str] = []
        with cc._DB_LOCK:
            for c in cards:
                sess = str(c.get("sess", "") or "")
                dia = str(c.get("dia", "") or "")
                text = str(c.get("text", "") or "")
                if not sess or not dia or not text:
                    continue
                conn.execute("INSERT OR IGNORE INTO exp_trace(tenant,sess,dia,dt,speaker,text)"
                             " VALUES(?,?,?,?,?,?)",
                             (tenant, sess, dia, str(c.get("dt", "") or ""),
                              str(c.get("speaker", "") or ""), text))
                if sess not in sess_set:
                    sess_set.append(sess)
            # 2. 逐会话终态判定+蒸馏+落库
            import time as _t
            seq = _t.time_ns()
            stored = 0
            for sess in sess_set:
                rows = conn.execute(
                    "SELECT dt, speaker, text FROM exp_trace WHERE tenant=? AND sess=? "
                    "ORDER BY dt, rowid", (tenant, sess)).fetchall()
                scards = [{"sess": sess, "dt": r[0], "speaker": r[1], "text": r[2]}
                          for r in rows]
                if not scards:
                    continue
                # 快速预筛: 无任何代码痕迹的纯文本会话不过重解析(省钱·不影响判定)
                joined = "\n".join(str(r[2]) for r in rows)
                if not re.search(r"Exit code|diff --git|FAILED|Traceback|\bpassed\b|"
                                 r"\berror\b|Error", joined, re.I):
                    continue
                events = _events(scards)
                files, funcs = _diff_info(scards)
                verdict, ctx = _judge(events, bool(files))
                if not verdict and relax_on():
                    # [U3] 严口径拒判→RELAX三选二复核(仅末事件pass场景·自带闸a)
                    verdict, ctx = _judge_relax(events, bool(files), scards)
                heroes: list[dict[str, Any]] = []
                if verdict == "success":
                    fix = _build_fix(scards, ctx, files, funcs)
                    if fix and relax_on():
                        # [U3] 闸(b): FIX必须带轨迹提取的真实验证命令·无→降级COMPR
                        mv = re.search(r"^VERIFY: (.+)$", fix["text"], re.M)
                        if not (mv and mv.group(1).strip() not in ("", "run tests")):
                            dg = _downgrade_compr(scards, fix, files)
                            if dg:
                                heroes.append(dg)
                            fix = None
                    if fix:
                        h = dict(fix)
                        compr = h.pop("compr", "")
                        heroes.append(h)
                        if compr:
                            heroes.append({"kind": "expcmpr", "text": compr,
                                           "errsig": h["errsig"], "files": files,
                                           "syms": [], "test": ""})
                elif verdict == "fail":
                    anti = _build_anti(scards, ctx, files, funcs)
                    if anti:
                        heroes.append(anti)
                # supersede: 本会话旧英雄卡先删(会话增长→重判定覆盖)·再落新卡
                conn.execute("DELETE FROM exp_cards WHERE tenant=? AND sess=?",
                             (tenant, sess))
                for h in heroes:
                    dt = str(scards[min(int(ctx.get("ci", 0)), len(scards) - 1)].get("dt", "")
                             ) if ctx else str(scards[-1].get("dt", ""))
                    cid = cc._cid_of(sess, dt, h["text"])
                    conn.execute(
                        "INSERT OR REPLACE INTO exp_cards"
                        "(tenant,cid,sess,dia,dt,kind,errsig,files,syms,test,text,seq)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (tenant, cid, sess, f"{sess}:{h['kind']}", dt, h["kind"],
                         str(h.get("errsig", "")),
                         json.dumps(h.get("files", [])[:6]),
                         json.dumps(h.get("syms", [])[:6]),
                         str(h.get("test", "")), h["text"], seq))
                    stored += 1
            conn.commit()
        _HCACHE.pop(tenant, None)
        return stored
    except Exception as exc:  # noqa: BLE001 — fail-soft: 蒸馏失败只丢英雄卡
        log.error("expdistill observe FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0


# ── search相: 英雄卡检索(≤3) ────────────────────────────────────────────────
_Q_FAIL_RE = re.compile(r"\b(error|errors|fail|fails|failing|failed|not\s+working|"
                        r"broken|crash|crashes|traceback|exception|bug|issue|"
                        r"cannot|can't)\b", re.I)
_Q_FILE_RE = re.compile(r"(?:[\w.\-]+/)*[\w.\-]+\.(?:py|js|ts|tsx|jsx|go|rs|java|c|cc|"
                        r"cpp|h|hpp|rb|php|sh|toml|yaml|yml|json)\b")
_Q_SYM_RE = re.compile(r"\b(?:[a-z_][a-z0-9_]{3,}|[A-Z][A-Za-z0-9]{2,})\b")


def _q_features(query: str, fingerprint: dict[str, Any] | None = None
                ) -> dict[str, Any]:
    """query侧特征: errsig/文件/符号/失败语境/词面token。fingerprint(轨道B)提供时
    其files/symbols/errsig优先并入(键名防御性适配·值形态宽容)。"""
    q = str(query or "")
    feats: dict[str, Any] = {
        "errsig": "", "files": set(), "syms": set(),
        "fail": bool(_Q_FAIL_RE.search(q)), "tokens": set(),
    }
    exc = errsig_of_text(q) or normalize_errsig("", "")
    m = re.search(r"\b([A-Za-z_]\w*(?:Error|Exception|Interrupt|Timeout|Failure))\b", q)
    if m:
        exc = normalize_errsig(m.group(1), q)
    feats["errsig"] = exc
    feats["files"] = set(_Q_FILE_RE.findall(q))
    import card_construct as cc
    feats["tokens"] = cc._tokens(q)
    raw = set(_Q_SYM_RE.findall(q))
    feats["syms"] = {s for s in raw if s.lower() not in _ERR_STOP} - feats["tokens"]
    # 轨道B fingerprint并入(缺省自行尝试import·import失败→跳过)
    fp = fingerprint
    if fp is None:
        try:
            import knife13_repo  # type: ignore  # noqa: PLC0415
            fp = knife13_repo.fingerprint(q)
        except Exception:
            fp = None
    if isinstance(fp, dict):
        for k in ("files", "paths", "file_paths", "file"):
            v = fp.get(k)
            if isinstance(v, (list, tuple, set)):
                feats["files"].update(str(x) for x in v if isinstance(x, str))
            elif isinstance(v, str) and v:
                feats["files"].add(v)
        for k in ("symbols", "defs", "symbol", "def_names"):
            v = fp.get(k)
            if isinstance(v, (list, tuple, set)):
                feats["syms"].update(str(x) for x in v if isinstance(x, str))
        for k in ("errsig", "error_sig", "error", "errors"):
            v = fp.get(k)
            if isinstance(v, str) and v:
                feats["errsig"] = feats["errsig"] or normalize_errsig(v, v)
                break
            if isinstance(v, (list, tuple)) and v and isinstance(v[0], str):
                feats["errsig"] = feats["errsig"] or normalize_errsig(v[0], v[0])
                break
    return feats


def _load_heroes(tenant: str) -> list[dict[str, Any]]:
    cached = _HCACHE.get(tenant)
    if cached is not None:
        return cached
    conn, cc = _db()
    with cc._DB_LOCK:
        rows = conn.execute(
            "SELECT cid, dia, dt, kind, errsig, files, syms, test, text, sess"
            " FROM exp_cards WHERE tenant=? ORDER BY seq, dia", (tenant,)).fetchall()
    out = []
    for r in rows:
        try:
            out.append({"cid": r[0], "dia": r[1], "dt": r[2], "kind": r[3],
                        "errsig": r[4] or "", "files": json.loads(r[5] or "[]"),
                        "syms": json.loads(r[6] or "[]"), "test": r[7] or "",
                        "text": r[8], "sess": r[9]})
        except Exception:  # noqa: BLE001
            continue
    _HCACHE[tenant] = out
    return out


# ── 源归因标记(轨道A记分板记功锚·2026-10-25快修) ───────────────────────────
# A的评测器(dev_a/m0/sbctx_lib.py MARKER_RE)对content任意位置grep
# "[SBCTX task=<instance_id>]"记功。A灌池时session_id=纯instance_id(实测
# build_cards把sess=sid原样入库·memory_service只给conv加"{tenant}:"前缀·sess
# 不动)；防御性剥"{tenant}:"前缀与边车尾":a"(若未来从conv侧取源)。
# 真实评测无害: SRC行=溯源元数据(哪个会话蒸馏的)·对平台agent是合理上下文非泄漏。
# 注意: 追加只发生在hero_cards出口·DB存储text不变→cid(id)稳定·幂等不受影响。
_SRC_LINE = "\nSRC: [SBCTX task=%s]"


def _src_id(sess: str, tenant: str) -> str:
    """sess→纯instance_id: 剥"{tenant}:"前缀(仅精确匹配本租户前缀·instance_id
    本身不含冒号故不误伤)·剥边车尾":a"。空→""。"""
    s = str(sess or "").strip()
    t = str(tenant or "")
    if t and s.startswith(t + ":"):
        s = s[len(t) + 1:]
    if s.endswith(":a"):
        s = s[:-2]
    return s


def hero_cards(query: str, tenant: str,
               fingerprint: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """检索侧英雄卡(≤3·窗位1-3由aml_service hunk注入):
    errsig精确(100)>errsig同类型(70)>文件重叠(60)>符号重叠(40)>词面兜底(≤30·非空交集);
    query失败语境 ∧ ANTI卡 → +25提权(防走死路经验优先); 词面≥1才入围(零相关不硬塞)。
    [R1] env BRAINOS_HERO_COMM=1 时社区一致校验: query社区≠英雄卡源社区→拒
    (跨库/跨题errsig家族撞卡防线·四放行fail-soft见模块头R1注·门关=逐字节等价)。
    返回[{"id","content","created_at"}](id=cc._cid_of稳定·门关/无卡/异常→[])。"""
    if not ON():
        return []
    try:
        import card_construct as cc
        tenant = str(tenant or "default")
        heroes = _load_heroes(tenant)
        if not heroes:
            return []
        qf = _q_features(str(query or ""), fingerprint)
        # [R1] query社区一次预取(门关=空串=整段门逻辑短路·零开销)
        q_comm = _query_community(tenant, str(query or ""), fingerprint) \
            if hero_comm_on() else ""
        n_rej = 0
        scored: list[tuple[int, str, dict[str, Any]]] = []
        qfiles = {f.lower() for f in qf["files"]}
        qbase = {os.path.basename(f) for f in qfiles}
        for h in heroes:
            s = 0
            he = str(h.get("errsig", "") or "")
            if he and qf["errsig"]:
                if he == qf["errsig"]:
                    s += 100
                elif he.split("-")[0] == qf["errsig"].split("-")[0]:
                    s += 70
            hfiles = {str(f).lower() for f in (h.get("files") or [])}
            hbase = {os.path.basename(f) for f in hfiles}
            if qfiles & hfiles or qbase & hbase:
                s += 60
            qsyms = {str(x) for x in qf["syms"]}
            if qsyms & {str(x) for x in (h.get("syms") or [])}:
                s += 40
            if qsyms & set(h.get("test", "").split("::")):
                s += 40
            lex = len(qf["tokens"] & cc._tokens(h.get("text", "")))
            s += min(30, lex)
            if qf["fail"] and h.get("kind") == "expanti":
                s += 25
            if s < 1:
                continue
            # [R1] 社区一致校验(评分后·截断前): 有信号仍跨社区→拒(下一张同社区卡
            # 可递补英雄位); 源无社区=放行(老库兼容)。
            if q_comm:
                sc = _src_comms(tenant, str(h.get("sess", "")))
                if sc and q_comm not in sc:
                    n_rej += 1
                    log.info("hero comm gate REJECT tenant=%s q_comm=%s src=%s kind=%s",
                             tenant, q_comm, ",".join(sorted(sc))[:60],
                             str(h.get("kind", "")))
                    continue
            scored.append((s, str(h.get("dia", "")), h))
        if n_rej:
            log.info("hero comm gate tenant=%s rejected=%d kept=%d q_comm=%s",
                     tenant, n_rej, len(scored), q_comm)
        scored.sort(key=lambda x: (-x[0], x[1]))
        out = []
        for s, _k, h in scored[:_hero_max()]:
            content = h["text"]
            iid = _src_id(h.get("sess", ""), tenant)
            if iid:
                content = content + _SRC_LINE % iid
            out.append({"id": h["cid"], "content": content,
                        "created_at": h["dt"] or ""})
        return out
    except Exception as exc:  # noqa: BLE001 — fail-soft: 检索失败=空窗(原窗直通)
        log.error("expdistill hero_cards FAILED err=%s", type(exc).__name__)
        return []
