# coding_window.py — 轨道E·M5编程方言窗口编排器(20261025)
# ═══════════════════════════════════════════════════════════════════════════════
# 依据(CODING_PLAN §M5): 编程赛道query携带强代码方言(路径/报错签名/diff/命令), 需要
#   ① FIX/ANTI/COMPR英雄蒸馏卡置顶位1-3(SWE-Exp k=1最优→宁少勿滥) ②旁路文本刀注入
#   (pattern/struct/digest对编程query是噪声) ③自适应小窗k∈[3,15]+字符预算≤16K
#   (Oracle紧凑摘要+8.1 vs 完整轨迹仅+1=选得对+压得紧缺一负收益)。
#
# 方言检测 is_coding(query) -> bool (纯query侧信号·确定性·零LLM·不碰租户数据):
#   信号打分: ①knife13.fingerprint(query)五类(paths/errsig/tests/cmds/symbols)任一
#   非空 +1  ②代码标记正则五类各+1: diff --git / Exit code / Traceback / ``` /
#   代码文件扩展名(foo.py|ts|go|...·带词边界与前置边界保护)。
#   score ≥ BRAINOS_CW_TH(缺省2) → 编程模式。纯文本问题("Where was this
#   metastasis located?"类)五类标记零命中+指纹零命中=0分→恒非编程。
#
# 编程模式动作 apply(data, query, tenant, top_k)(单挂接点=aml_service k13b后
#   _qa_window_trim/终窗收口前; 文本刀注入抑制由aml_service三处_cw_coding守卫承担):
#   编程模式管线序(轨道D接线语义): organ → k13/crf boost(aml_service k13b块) →
#     [M1.5预留 recall_union·B hunk到货前=原样直通] → rerank(前TOPN证据卡·轨道D) →
#     英雄卡置顶1-3 → 自适应窗裁剪。
#   伪码:
#     if not ON() or not is_coding(query): return data          # 零改动路径(原样)
#     fp   = k13.fingerprint(query)                             # fail-soft→{}
#     heroes = expd.hero_cards(query, tenant, fingerprint=fp)   # ≤3张·fail-soft→[]
#     heroes = dedupe(heroes, by id+content)[:min(3, top_k)]    # 英雄互去重
#     evid = [d for d in data if d.id∉heroes and d.content∉heroes]   # 窗内id+content去重
#     evid = rerank_hook(evid, query)                           # ④轨道D·env BRAINOS_RERANK门
#     n_sig = #{d∈evid: d.content含任一fp值}                    # 指纹支撑卡计数
#     k_t  = clamp(KMIN + len(heroes) + min(n_sig, KMAX-KMIN),  # 自适应目标k(信号扩张)
#                  KMIN, min(KMAX, top_k))
#     keep = evid[:max(0, k_t - len(heroes))]
#     win  = heroes + keep
#     while sum(len(content)) > BUDGET and 尾部非英雄卡: 弹尾    # 预算淘汰·英雄豁免
#     win  = [d for d in win if d.id and d.content][:top_k]     # 硬契约(id/content非空·≤top_k)
#     return win
#
# [M1.5预留] recall_union(data, query, tenant): 轨道B指纹直召回合并点(aml_service在
#   k13/crf boost块之前调用)——env BRAINOS_K13_RECALL门缺省0=原样返回; B的M1.5 hunk
#   到货后在此实现"直召回卡与data按id去重合并(直召回卡放队首)再进boost"。
#
# ④ rerank_hook(data, query): 轨道D rerank_client 接线(Qwen3-Reranker·127.0.0.1:8098)。
#   env BRAINOS_RERANK总闸缺省0=不调(字节直通); 开=前BRAINOS_RERANK_TOPN(30)证据卡送
#   {query, candidates}→P(yes)降序回排, 超窗候选原相对序沉尾, 长度守恒校验后才换序。
#   三层fail-soft: 门关零开销 / 客户端超时(8s)/死端口(实测15ms降级)→原序+计数 /
#   本hook外层try任何异常→原序。只重排证据卡(英雄卡在其后置顶=位1-3契约不被D破坏)。
#
# 非编程模式: apply()原样返回输入列表(不复制不改序不改集)——门关(BRAINOS_CODING_WINDOW
#   缺省0)时is_coding恒False=文本赛道字节级零回归由构造保证。
# 纪律: 零LLM零外部API(rerank_hook除外且env门缺省关)·确定性·fail-soft·env总闸
#   BRAINOS_CODING_WINDOW缺省0=关。回滚=摘aml_service挂接+删本件(无独立DB零状态)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger("coding_window")


def ON() -> bool:
    """env总闸BRAINOS_CODING_WINDOW(缺省0=关): 关=方言检测恒False+apply原样直通。"""
    try:
        return str(os.environ.get("BRAINOS_CODING_WINDOW", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)


# ── 参数(env可调) ────────────────────────────────────────────────────────────
def TH() -> int:        return max(1, _i("BRAINOS_CW_TH", "2"))     # 编程判定阈值
def KMIN() -> int:      return max(1, _i("BRAINOS_CW_KMIN", "15"))  # 自适应窗下限
# [KFIX 20261026] KMIN 3→15 固化(VPS env 修复实证): agent 的检索窗=它的工作口粮,
#   KMIN=3+K10 上限把含 gold 的检索结果砍到 3-4 卡=饿死(官方冒烟 arm-v3 astropy-13033
#   0 分根因; K30 窗同查询 11 张金卡)。KMIN=15 后派生逻辑回归(见本文件 apply()):
#   k_t=KMIN+len(heroes)+min(n_sig,KMAX-KMIN) 语义不变——heroes 仍在上限内扩张,
#   n_sig 仍占 KMAX-KMIN=15 个扩张槽; 小 top_k(<KMIN) 由 kmin_e=min(KMIN(),top_k)
#   钳制回原语义。env BRAINOS_CW_KMIN 仍可调(消融臂 K3 复刻)。
# [R3复核 2026-10-28] KMIN=15/KMAX=30 固化复核: KMIN 缺省15 ✓(本行); SPAN 开时上限
#   KMAX_SPAN 缺省30 ✓(见下 KMAX_SPAN)——R3 臂 env 只需 BRAINOS_CW2_KMAX=30(覆盖
#   arm-final 遗留 env 的 60)·代码零改动。
def KMAX() -> int:      return max(KMIN(), _i("BRAINOS_CW_KMAX", "15"))  # 窗上限
def BUDGET() -> int:    return max(1024, _i("BRAINOS_CW_BUDGET", "16000"))  # 字符预算
def HERO_MAX() -> int:  return max(0, min(3, _i("BRAINOS_CW_HERO_MAX", "3")))


# ── [L轨·SPAN] BRAINOS_SPAN=1 时窗上限自动放宽到 BRAINOS_CW2_KMAX(缺省30) ────
# 依据(L1规格): 行级裁剪后单卡≈SPAN_CARD(缺省1400)字符, 同预算容纳2-3倍卡数→k上限
#   同步放宽才有意义(否则裁出的空间被闲置)。SPAN关/span_cut缺席→逐位旧值(零改动路径)。
#   编程模式生效域不变: 非编程query在apply()早退=本放宽对文本赛道不可达。
def KMAX_SPAN() -> int:
    km = max(KMIN(), _i("BRAINOS_CW_KMAX", "15"))
    try:
        import span_cut  # noqa: PLC0415 — L轨行级裁剪(缺席/门关=旧值)
        if span_cut.ON():
            km = max(KMIN(), _i("BRAINOS_CW2_KMAX", "30"))
    except Exception:  # noqa: BLE001 — fail-soft: 模块缺席=旧值
        pass
    return km


# ── [M10/K] 增敏v2总门+打分(env BRAINOS_CW_V2缺省0=不引入新信号·字节级等价) ──
def V2_ON() -> bool:
    try:
        return str(os.environ.get("BRAINOS_CW_V2", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def coding_score_v2(query: str) -> int:
    """v2打分: coding_score(query)+cw_signals.bonus(query)(fail-soft=0退化现版)。"""
    s = coding_score(query)
    if not V2_ON() or not str(query or "").strip():
        return s
    try:
        import cw_signals  # noqa: PLC0415 — M10增敏信号源
        return s + cw_signals.bonus(query)
    except Exception:  # noqa: BLE001
        return s


def _k_content_ok(ct: str) -> bool:
    return bool(ct and str(ct).strip())


# ── [R2·2026-10-28] F1叙事卡禁入编程窗(CODING_PLAN §7 R2·治冒烟5暴露的污染源②) ──
# 病灶: KNOWLEDGE DIGEST/SESSION DIGEST(文本赛道叙事摘要·deep_digest/card_construct
#   digest家族)按词面直通规则可经 organ 遗留窗/ens融合列表/未来注入面混入编程窗——
#   对编程query是纯叙事噪声(V实验"富窗全败"归因成分·挤占证据位)。
# 既有守卫(本防线前置·不在本防线重复): aml_service 三处注入位(digest route 1564/
#   cc.overlay 1536/k7.inject 1798)编程模式已抑制; 本防线=**窗编排入口统一收口**——
#   is_coding=True 的 query 在 apply() 组窗前把 digest 家族卡整段剥除, 一律不入窗,
#   覆盖上述守卫之外的一切到达路径(注入点 coding 分支守卫的终态形态)。
# 文本查询(非编程)在 apply() 早退=文本赛道弹药照常·零影响。
# fail-soft: 剥除为纯列表过滤不可抛; env BRAINOS_CW_NODIGEST 缺省0=关=逐字节等价。
_DIGEST_PREFIXES = ("SESSION DIGEST:", "KNOWLEDGE DIGEST")


def NODIGEST_ON() -> bool:
    try:
        return str(os.environ.get("BRAINOS_CW_NODIGEST", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def strip_digest(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """digest家族卡剥除(卡面SECRET前缀识别·确定性纯函数·返回新列表)。"""
    return [d for d in data
            if not str(d.get("content", "")).startswith(_DIGEST_PREFIXES)]


# ── [R3·2026-10-28] 窗尺寸遥测(CODING_PLAN §7 R3·供冒烟后窗波动分析) ─────────
# 每编程窗收口落一行结构化遥测(k/chars/heroes 数+k_t/n_sig/kmin/kmax/预算·query头
# 80字符供对题)。env BRAINOS_CW_TELEM 缺省0=关=不落(与现版日志面逐字节等价);
# 开=在既有 "coding window applied" 行之外**追加**一行 CWTELEM(机器可grep·零行为差)。
def TELEM_ON() -> bool:
    try:
        return str(os.environ.get("BRAINOS_CW_TELEM", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _telem(tenant: str, query: str, win: list[dict[str, Any]],
           heroes_n: int, evid_n: int, k_t: int, n_sig: int,
           kmin: int, kmax: int, budget: int) -> None:
    try:
        chars = sum(len(str(d.get("content", ""))) for d in win)
        log.info("CWTELEM tenant=%s k=%d chars=%d heroes=%d evid=%d k_t=%d n_sig=%d "
                 "kmin=%d kmax=%d budget=%d q=%r",
                 str(tenant), len(win), chars, heroes_n, evid_n, k_t, n_sig,
                 kmin, kmax, budget, str(query)[:80])
    except Exception:  # noqa: BLE001 — 遥测绝不影响窗
        pass


# ── 代码标记正则(五类·每类命中+1·纯函数) ────────────────────────────────────
_RX_DIFF = re.compile(r"diff --git a/")
_RX_EXIT = re.compile(r"\b[Ee]xit\s+code\s+\d+\b")
_RX_TB = re.compile(r"\bTraceback\b")
_RX_FENCE = re.compile(r"```")
# 文件扩展名: 前置边界防版本号/域名误切(e.g. "v1.2"不中·"3.5"不中·数字ext不收)
_RX_EXT = re.compile(
    r"(?<![\w.@:-])(?:[A-Za-z0-9_\-.]+/)*[A-Za-z0-9_\-.]+\."
    r"(?:py|pyi|pyw|mjs|js|cjs|ts|tsx|jsx|go|rs|c|h|cpp|cc|cxx|hpp|hh|java|kt|rb|php|sh"
    r"|bash|zsh|ps1|yaml|yml|toml|cfg|ini|proto|sql|mk|lua|dart|swift|scala)\b",
    re.I)


def _query_fingerprint(query: str) -> dict[str, list[str]]:
    """k13.fingerprint懒加载fail-soft(k13缺席/异常→空dict=零指纹信号)。"""
    try:
        import knife13_repo as k13  # noqa: PLC0415
        return k13.fingerprint(str(query or ""))
    except Exception:  # noqa: BLE001
        return {}


def coding_score(query: str) -> int:
    """query侧方言信号打分(纯函数·零异常): 指纹非空+1·五类标记各+1。"""
    q = str(query or "")
    if not q.strip():
        return 0
    score = 0
    try:
        fp = _query_fingerprint(q)
        if any(fp.get(k) for k in ("paths", "errsig", "tests", "cmds", "symbols")):
            score += 1
    except Exception:  # noqa: BLE001
        pass
    if _RX_DIFF.search(q):
        score += 1
    if _RX_EXIT.search(q):
        score += 1
    if _RX_TB.search(q):
        score += 1
    if _RX_FENCE.search(q):
        score += 1
    if _RX_EXT.search(q):
        score += 1
    return score


def is_coding(query: str) -> bool:
    """方言检测: 总闸开 ∧ score≥TH。门关恒False(文本赛道零回归的构造保证)。
    v2(BRAINOS_CW_V2=1·M10/K增敏): 分数=coding_score_v2(=coding_score+新信号bonus,
    阈值语义不变); 缺省V2关=coding_score_v2≡coding_score=与integ现版逐位等价。"""
    if not ON():
        return False
    try:
        return coding_score_v2(query) >= TH()
    except Exception:  # noqa: BLE001
        return False


# ── [M1.5预留] 轨道B指纹直召回合并点(aml_service在k13/crf boost前调用) ────────
def recall_union(data: list[dict[str, Any]], query: str,
                 tenant: str) -> list[dict[str, Any]]:
    """B的M1.5 hunk到货前=原样直通(env BRAINOS_K13_RECALL缺省0)。到货后语义:
    直召回卡(≤40)与data按id去重合并·直召回卡放队首·再进k13/crf boost。fail-soft。"""
    if os.environ.get("BRAINOS_K13_RECALL", "0") != "1":
        return data
    try:
        import knife13_repo as k13  # noqa: PLC0415,F401 — M1.5 hunk 到货后启用
        _recall = getattr(k13, "recall_by_fingerprint", None)
        if _recall is None:
            return data
        cards = _recall(str(tenant), str(query or "")) or []
        if not cards:
            return data
        _ids = {str(d.get("id", "")) for d in data}
        _cts = {str(d.get("content", "")) for d in data}
        head = [{"id": str(c.get("id", "")), "content": str(c.get("content", "")),
                 "created_at": str(c.get("created_at", "") or "")}
                for c in cards
                if str(c.get("id", "")) and str(c.get("id", "")) not in _ids
                and str(c.get("content", "")) not in _cts]
        return head + data if head else data
    except Exception as exc:  # noqa: BLE001
        log.warning("recall_union FAILED err=%s (original window kept)", type(exc).__name__)
        return data


# ── ④ 轨道D重排器接线(rerank_client·env BRAINOS_RERANK总闸缺省0=不调) ─────────
def RERANK_ON() -> bool:
    return os.environ.get("BRAINOS_RERANK", "0") == "1"


def rerank_hook_scored(data: list[dict[str, Any]], query: str
                       ) -> tuple[list[dict[str, Any]], dict[str, float] | None]:
    """[U2] rerank_hook 分数透出版: 排序语义与原 rerank_hook 逐位一致(同TOPN/同降级/
    同长度守恒), 额外返回 {id: score}(P(yes)·8098口径) 供尾截断(win_density.tail_cut)。
    分数=None ⇔ RERANK门关/空表/降级原序/长度守恒失败——尾截断收到 None 必须直通。"""
    if not RERANK_ON() or not data:
        return data, None
    try:
        import rerank_client as rr  # noqa: PLC0415 — 轨道D交付件(纯stdlib)
        pairs = [{"id": str(d.get("id", "")),
                  "text": str(d.get("content", ""))} for d in data]
        rows = rr.rerank(query, pairs)  # 客户端内置超时/死端口降级原序+计数
        by_id: dict[str, dict[str, Any]] = {}
        for d in data:
            by_id.setdefault(str(d.get("id", "")), d)  # 同id保首(契约同源)
        head: list[dict[str, Any]] = []
        seen: set[int] = set()
        for r in rows:
            d = by_id.get(str(r.get("id", "")))
            if d is not None and id(d) not in seen:
                head.append(d)
                seen.add(id(d))
        out = head + [d for d in data if id(d) not in seen]  # 缺者/超窗原序沉尾
        if len(out) == len(data):  # 长度守恒才换序(异常形回退原序)
            scores = {str(r.get("id", "")): float(r.get("score", 0.0)) for r in rows
                      if isinstance(r, dict)}
            log.info("rerank hook ok n=%d stats=%s", len(pairs), rr.stats())
            return out, scores
        return data, None
    except Exception as exc:  # noqa: BLE001 — fail-soft: 原序直通
        log.warning("rerank hook FAILED err=%s (original order kept)", type(exc).__name__)
        return data, None


def rerank_hook(data: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """轨道D Qwen3-Reranker调用点(只作用于证据卡·英雄卡在本函数之后prepend):
    门关=零开销直通; 开=前TOPN(缺省30)卡经rerank_client送127.0.0.1:8098 P(yes)降序
    回排·超窗候选原相对序沉尾·长度守恒校验后才换序。三层fail-soft(门关/客户端降级/
    本层try)任何失败→原序直通·绝不阻search。
    [U2] 委托 rerank_hook_scored 只取排序(兼容入口·逐位等价·分数弃)。"""
    return rerank_hook_scored(data, query)[0]


# ── 指纹支撑计数(自适应k的扩张信号) ──────────────────────────────────────────
def _sig_count(evid: list[dict[str, Any]], fp: dict[str, list[str]]) -> int:
    """窗内证据卡与query指纹的词面支撑数: 卡content含任一fp值(substring·小写)。
    值过滤: len≥4防短符号误报("os"/"re"); cmds按"工具|实参"拆两半各自匹配。"""
    vals: list[str] = []
    for k in ("paths", "errsig", "tests", "symbols"):
        for v in (fp.get(k) or [])[:24]:
            v = str(v).lower()
            if len(v) >= 4:
                vals.append(v)
    for v in (fp.get("cmds") or [])[:16]:
        for part in str(v).lower().split("|"):
            part = part.strip()
            if len(part) >= 4:
                vals.append(part)
    if not vals:
        return 0
    n = 0
    for d in evid:
        c = str(d.get("content", "")).lower()
        if any(v in c for v in vals):
            n += 1
    return n


# ── 主入口(M5终窗编排) ───────────────────────────────────────────────────────
def apply(data: list[dict[str, Any]], query: str, tenant: str,
          top_k: int) -> list[dict[str, Any]]:
    """编程模式终窗编排(aml_service单挂接点·k13b后): 非编程/门关→data原样返回。"""
    if not ON() or not isinstance(data, list) or not data:
        return data
    if not is_coding(query):
        return data  # 零改动路径(文本query·字节级等价)
    # [R2·F1叙事卡禁入编程窗] env BRAINOS_CW_NODIGEST 缺省0=关=逐字节等价。
    # is_coding=True 的 query: digest 家族卡在组窗入口整段剥除(一律不入窗·覆盖
    # organ遗留窗/ens融合/未来注入面); 文本query已在上方早退=文本弹药照常。
    if NODIGEST_ON():
        _dg_pre = len(data)
        data = strip_digest(data)
        if len(data) != _dg_pre:
            log.info("cw digest strip tenant=%s cards %d->%d",
                     str(tenant), _dg_pre, len(data))
    try:
        top_k = max(1, int(top_k))
        fp = _query_fingerprint(query)
        # ① 英雄卡(FIX/ANTI/COMPR·≤3·fail-soft) + 互去重(id+content)
        # [M10/K·英雄指纹门控] BRAINOS_CW_HERO_FP=1时: 纯散文query(query指纹空=无路径/
        # 报错/符号/命令信号)不上英雄卡——错题经验卡对纯描述query是负资产(K实测同窗对位
        # MRR 0.312→0.145·英雄置顶让位=与I expand的正确分工: 指纹富集才上英雄)。缺省0=关。
        heroes: list[dict[str, Any]] = []
        try:
            import deep_expdistill as expd  # noqa: PLC0415
            raw_heroes = ([] if (os.environ.get("BRAINOS_CW_HERO_FP", "0") == "1" and not fp)
                           else expd.hero_cards(query, str(tenant), fingerprint=fp) or [])
        except Exception as exc:  # noqa: BLE001 — 英雄卡失败=纯证据窗(服务不死)
            log.warning("hero_cards FAILED err=%s (evidence-only window)", type(exc).__name__)
            raw_heroes = []
        h_seen: set[str] = set()
        for h in raw_heroes:
            if not isinstance(h, dict):
                continue
            k_id, k_ct = str(h.get("id", "")), str(h.get("content", ""))
            if not k_id or not _k_content_ok(k_ct) or k_id in h_seen or k_ct in h_seen:
                continue
            h_seen.add(k_id)
            h_seen.add(k_ct)
            heroes.append({"id": k_id, "content": k_ct,
                           "created_at": str(h.get("created_at", "") or "")})
        heroes = heroes[:min(HERO_MAX(), top_k)]
        # [M9·repo_map] 地图卡第4英雄位(轨道J·≤1张·FIX/ANTI/COMPR之后证据卡之前):
        #  在HERO_MAX截断之后追加→计入窗编排(len(heroes)入k_t扩张·keep数=KMIN+min(n_sig,…)
        #  与len(heroes)消去=证据位不被挤占·仅窗顶收缩)但不挤占前3英雄位; 去重闸面沿用
        #  h_seen(id+content)。env总闸BRAINOS_REPO_MAP缺省0=关(零改动路径)。
        try:
            import repo_map as rmap  # noqa: PLC0415
            if len(heroes) < top_k:
                for _m in (rmap.map_card(str(tenant), str(query)) or [])[:1]:
                    _mid, _mct = str(_m.get("id", "")), str(_m.get("content", ""))
                    if _mid and _mct and _mid not in h_seen and _mct not in h_seen:
                        h_seen.add(_mid)
                        h_seen.add(_mct)
                        heroes.append({"id": _mid, "content": _mct, "created_at": ""})
        except Exception as _rm_exc:  # noqa: BLE001 — 地图失败=无此卡(服务不死)
            log.warning("repo_map map_card FAILED err=%s", type(_rm_exc).__name__)
        # 窗内证据与英雄卡按 id+content 去重(英雄胜出·置顶)
        evid = [d for d in data
                if str(d.get("id", "")) not in h_seen
                and str(d.get("content", "")) not in h_seen]
        # ④ 轨道D重排器(只重排证据卡·env门缺省关·失败原序)
        #    [U2] 分数透出版: 排序逐位等价·score表交给尾截断(门关=弃分·零行为差)
        try:
            evid, rr_scores = rerank_hook_scored(evid, query)
        except Exception as exc:  # noqa: BLE001
            rr_scores = None
            log.warning("rerank_hook wrapper err=%s", type(exc).__name__)
        # [U2·尾截断] rerank P(yes) < BRAINOS_RR_TAIL(缺省0.15·8098实测标定)的证据卡
        # 淘汰——junk 不占窗也不再经 n_sig 抬 k(位于③自适应k选择之前·故意)。
        # 直通域: BRAINOS_RR_TAILCUT缺省0 / scores=None(门关或降级) / 全零分(降级指纹);
        # 保底=最高分1张(重排器全体低分也不交空证据窗)。fail-soft: 异常→evid原样。
        try:
            import win_density  # noqa: PLC0415 — U2密度包(门关=直通)
            evid = win_density.tail_cut(evid, rr_scores)
        except Exception as _tc_exc:  # noqa: BLE001 — fail-soft: 证据原样
            log.warning("win_density.tail_cut FAILED err=%s (evidence kept)",
                        type(_tc_exc).__name__)
        # ③ 自适应目标k: base=KMIN·英雄占位+指纹支撑卡扩张·钳[KMIN, min(KMAX,top_k)]
        kmin_e = min(KMIN(), top_k)
        kmax_e = max(kmin_e, min(KMAX_SPAN(), top_k))
        n_sig = _sig_count(evid, fp)
        k_t = kmin_e + len(heroes) + min(n_sig, kmax_e - kmin_e)
        k_t = max(kmin_e, min(k_t, kmax_e))
        keep = evid[:max(0, k_t - len(heroes))]
        win = heroes + keep
        # [U2·近重卡去重] 窗内 content 词级8-gram Jaccard ≥ BRAINOS_CW_DEDUP_TH(缺省
        # 0.85)的卡对坍缩为信息量大的一张(长者>含verdict>先现)·胜者放组首现位——
        # P2病灶: 同repo邻域重复卡/干扰卡稀释注意力。英雄卡(位次+卡面)豁免。
        # 位于 span_cut 之前(被丢卡的行不值得裁)·预算淘汰之前(坍缩省出的预算留白)。
        # 门 BRAINOS_CW_DEDUP 缺省0=关·关=win原对象直通; fail-soft: 异常→win原样。
        try:
            import win_density  # noqa: PLC0415 — U2密度包(门关=直通)
            _dd_pre = len(win)
            win = win_density.collapse_near_dups(win, hero_n=len(heroes))
            if len(win) != _dd_pre:
                log.info("win_density dedup tenant=%s cards %d->%d",
                         str(tenant), _dd_pre, len(win))
        except Exception as _dd_exc:  # noqa: BLE001 — fail-soft: 原窗直通
            log.warning("win_density dedup FAILED err=%s (window kept)",
                        type(_dd_exc).__name__)
        # [L轨·L1 SPAN] 行级窗口裁剪(env BRAINOS_SPAN缺省0=win原对象直通·逐字节等价):
        # 先裁行再数预算——非英雄卡裁到BRAINOS_SPAN_CARD(缺省1400)字符·近重复行坍缩·
        # 证据行(报错/diff/测试名/verdict/路径)保留; 英雄卡(FIX/ANTI/COMPR·位<hero_n
        # 或卡面识别)豁免; 单卡异常→整卡原文直通; 本层异常→win原样(下方预算淘汰照旧)。
        try:
            import span_cut  # noqa: PLC0415 — L轨行级裁剪(门关=直通)
            _sp_pre = sum(len(str(d.get("content", ""))) for d in win)
            win = span_cut.apply_window(win, query, fp, hero_n=len(heroes))
            _sp_post = sum(len(str(d.get("content", ""))) for d in win)
            if _sp_post != _sp_pre:
                log.info("span cut tenant=%s cards=%d chars %d->%d",
                         str(tenant), len(win), _sp_pre, _sp_post)
        except Exception as _sc_exc:  # noqa: BLE001 — fail-soft: 原卡直通
            log.warning("span_cut FAILED err=%s (original cards kept)",
                        type(_sc_exc).__name__)
        # ③b 字符预算: 尾部淘汰非英雄卡(英雄豁免·除非总量超top_k)
        budget = BUDGET()
        total = sum(len(str(d.get("content", ""))) for d in win)
        while (total > budget or len(win) > top_k) and len(win) > len(heroes):
            drop = win.pop()
            total -= len(str(drop.get("content", "")))
        # 硬契约: id/content非空·len≤top_k
        win = [d for d in win if str(d.get("id", "")) and str(d.get("content", ""))]
        win = win[:top_k]
        log.info("coding window applied tenant=%s heroes=%d evid=%d k=%d chars=%d",
                 str(tenant), len(heroes), len(evid), len(win),
                 sum(len(str(d.get("content", ""))) for d in win))
        # [R3·窗遥测] env BRAINOS_CW_TELEM 缺省0=关=无此行(日志面与现版逐字节等价);
        # 开=追加一行 CWTELEM(k/chars/heroes/k_t/n_sig/kmin/kmax/budget)供冒烟后分析。
        if TELEM_ON():
            _telem(str(tenant), str(query), win, len(heroes), len(evid),
                   k_t, n_sig, kmin_e, kmax_e, budget)
        return win
    except Exception as exc:  # noqa: BLE001 — fail-soft: 任何异常=原窗直通
        log.error("coding_window apply FAILED err=%s (original window kept)",
                  type(exc).__name__)
        return data
