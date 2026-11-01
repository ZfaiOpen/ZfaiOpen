# win_density.py — U2轨·窗口密度包(2026-10-26·治P2: 干扰卡稀释agent注意力)
# ═══════════════════════════════════════════════════════════════════════════════
# 病灶(V验证官归因·CODING_PLAN §7): 窗口里 ~50% 干扰卡(跨库噪声+同repo邻域非gold)
#   稀释 agent 注意力。密度包在 KMAX 钉死 30·总预算 48K 不变的前提下, 让同预算装
#   更少字节更多信息——arm-v2 教训: 放宽窗宽是错误方向, 纯密度才是。
#
# 本模块两件(SPAN 行级裁剪=第三件·已在 span_cut.py 集成·本件不重复):
#   ② 近重卡去重 collapse_near_dups(win, hero_n):
#      窗内卡 content 词级8-gram shingle 相似度 ≥ BRAINOS_CW_DEDUP_TH(缺省0.85)的
#      卡对坍缩为信息量大的一张(字符长者优先·含verdict行者次之·先出现保底), 胜者卡
#      放组内最早出现位(位次=最高排名槽·序语义保持)。相似度=max(Jaccard, 包含度):
#      Jaccard 抓互有出入的近拷贝·包含度抓"短卡⊂长卡"的子集冗余(长者胜=信息无损)。
#      英雄卡(位次<hero_n或 FIX[/ANTI[/ISSUE-MAP: 卡面)豁免——既不被丢也不被顶替。
#      归属守卫: 双卡 [SBCTX task=] 锚均可解析且不同→禁止合并(跨任务坍缩会把早位
#      gold 卡顶成他任务卡=R@5 回退·首轮记分板 4 题实证后加); 任一方无锚=允许
#      (生产轨迹无 SBCTX 标记时行为退回纯相似度)。
#      8-gram 词级粒度与 span_cut 行坍缩同源: 字符级会误杀共享短token的不同证据行。
#      Jaccard 对 <8 词小卡退化: 归一化后精确相等=1.0(重复), 否则 0.0(不合并)。
#   ③ 重排尾截断 tail_cut(evid, scores):
#      rerank P(yes) < BRAINOS_RR_TAIL(缺省0.15·8098实测: 无关对0.003/相关对0.999)
#      的证据卡淘汰——junk 不占窗也不再经 n_sig 抬 k。scores=None(RERANK门关/降级
#      原序/任何失败)→直通; 全零分=降级指纹→直通(fail-soft); 保底保留最高分1张
#      (重排器全体<阈时不交空证据窗——失败经验也强于无证据)。
#
# 挂接(全部在 coding_window.apply 内·aml_service.py 零改动):
#   evid级: rerank_hook_scored(排序语义与rerank_hook逐位一致+分数透出) → tail_cut
#           (位于 n_sig/k_t 选择之前——junk 不参与窗扩张)
#   win级:  win = heroes + keep 之后、span_cut 之前 → collapse_near_dups
#           (先坍缩再裁行: 被丢卡的行不值得裁)
#
# 纪律: ①纯规则零LLM零DB零外部调用 ②确定性(同输入恒同输出·tie=先出现者优先)
#   ③env门缺省全关(BRAINOS_CW_DEDUP / BRAINOS_RR_TAILCUT)=逐位直通 ④fail-soft:
#   单卡异常跳过该卡比较·总异常原窗直通·绝不阻search ⑤KMAX/预算零触碰。
#   回滚 = 两处hunk摘除+删本件(无状态)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger("win_density")

_NGRAM = 8  # 近重卡 shingle n(词级·与 span_cut 行坍缩同粒度)

_RX_WS = re.compile(r"\s+")
# 归属锚(sbctx_lib.MARKER_RE 同款·本地定义=零跨树依赖): [SBCTX task=X]
_RX_ANCHOR = re.compile(r"\[SBCTX task=([^\]\s]+)\]")
# verdict 行(信息量判据·span_cut._RX_EXIT/_RX_PASS 同语义子集·本地定义=模块自洽)
_RX_VERDICT = re.compile(
    r"\b(?:exit code|exit status)\s+\d+\b|^\s*exit\s+\d+\b|\bpassed\b|\bpassing\b"
    r"|\bfailed\b|\bfailing\b|\bfailure\b|^\s*(?:OK|ok)\b"
    r"|\ball tests (?:passed|pass)\b", re.I | re.M)

_HERO_PREFIXES = ("FIX[", "ANTI[", "ISSUE-MAP:")  # 与 span_cut 同表(英雄卡面)


def _on(name: str) -> bool:
    """env门(缺省0=关)。"""
    try:
        return str(os.environ.get(name, "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _f(name: str, dft: str) -> float:
    try:
        v = float(os.environ.get(name, dft) or dft)
        return v if v == v else float(dft)  # NaN 防御(非法值=回缺省)
    except (TypeError, ValueError):
        return float(dft)


def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)


def DEDUP_ON() -> bool:
    """近重卡去重门 BRAINOS_CW_DEDUP(缺省0=关·关=collapse_near_dups原对象直通)。"""
    return _on("BRAINOS_CW_DEDUP")


def TAIL_ON() -> bool:
    """重排尾截断门 BRAINOS_RR_TAILCUT(缺省0=关·关=tail_cut原列表直通)。"""
    return _on("BRAINOS_RR_TAILCUT")


def DEDUP_TH() -> float:
    """去重阈值 BRAINOS_CW_DEDUP_TH(缺省0.85·Jaccard)。钳[0.5,1.0]防误配。"""
    return min(1.0, max(0.5, _f("BRAINOS_CW_DEDUP_TH", "0.85")))


def RR_TAIL() -> float:
    """尾截阈值 BRAINOS_RR_TAIL(缺省0.15·P(yes)口径·8098 Qwen3-Reranker实测标定:
    无关对≈0.003 / 相关对≈0.999)。"""
    return _f("BRAINOS_RR_TAIL", "0.15")


def RR_KEEP_MIN() -> int:
    """尾截深度地板 BRAINOS_RR_KEEP_MIN(缺省30=全窗深保护): rerank 序前 KEEP_MIN 名
    恒保留(只砍其后的低分)——绝对阈值 0.15 单独使用会把 rank13-30 的弱分 gold 一并砍掉
    (首轮 U3-Q1 实测 R@30 −10pp·38 题丢窗), 地板=闸兼容档; 调小=激进档(实验用)。"""
    return max(1, _i("BRAINOS_RR_KEEP_MIN", "30"))


# ── shingle 工具(与 span_cut 同粒度·卡级而非行级) ────────────────────────────
def _norm_text(text: str) -> str:
    return _RX_WS.sub(" ", str(text or "").strip().lower())


def _shingles(text: str, n: int = _NGRAM) -> frozenset[str]:
    """归一化文本→词级n-gram shingle集; 词数<n→空集(小卡只走精确等值合并)。"""
    toks = _norm_text(text).split(" ")
    if len(toks) < n:
        return frozenset()
    return frozenset(" ".join(toks[i:i + n]) for i in range(len(toks) - n + 1))


def _sim(a_sh: frozenset[str], b_sh: frozenset[str],
         a_norm: str, b_norm: str) -> float:
    """确定性相似度 = max(Jaccard, 包含度)。Jaccard=|A∩B|/|A∪B| 抓"互有出入的近拷贝";
    包含度=|A∩B|/min(|A|,|B|) 抓"短卡内容完全在长卡内"的子集冗余(胜者=长者→信息
    无损·brief"保留信息量大的(长者)"语义的必要条件: 纯Jaccard下+15%尾巴即漏并)。
    双方无shingle小卡→归一化精确等值(1/0); 单边无shingle→0.0(短卡与长卡永不合并)。"""
    if a_sh and b_sh:
        inter = len(a_sh & b_sh)
        if not inter:
            return 0.0
        j = inter / (len(a_sh) + len(b_sh) - inter)
        c = inter / min(len(a_sh), len(b_sh))
        return j if j >= c else c
    if not a_sh and not b_sh:
        return 1.0 if a_norm == b_norm and bool(a_norm) else 0.0
    return 0.0


def _is_hero(d: dict[str, Any]) -> bool:
    c = str(d.get("content", "")).lstrip()
    return c.startswith(_HERO_PREFIXES)


def _info_key(d: dict[str, Any], idx: int) -> tuple[int, int, int]:
    """信息量排序键(大者胜): (字符长, 含verdict, 先出现)。确定性全序。"""
    ct = str(d.get("content", ""))
    return (len(ct), 1 if _RX_VERDICT.search(ct) else 0, -idx)


def _anchor(content: str) -> str | None:
    """卡内容归属锚([SBCTX task=X]·无标记=None)。"""
    m = _RX_ANCHOR.search(str(content or ""))
    return m.group(1) if m else None


def _merge_allowed(a_ct: str, b_ct: str) -> bool:
    """归属守卫(闸2实测驱动): 双方锚均可解析且不同→禁止合并(跨任务坍缩会把早位
    gold 卡顶成他任务卡=R@5回·U2首轮4题实证)。任一方无锚(生产轨迹无SBCTX标记)或
    同锚→允许。确定性·纯函数。"""
    aa, bb = _anchor(a_ct), _anchor(b_ct)
    if aa is None or bb is None:
        return True
    return aa == bb


# ── ② 近重卡去重(窗级·坍缩) ──────────────────────────────────────────────────
def collapse_near_dups(win: list[dict[str, Any]],
                       hero_n: int = 0) -> list[dict[str, Any]]:
    """窗内近重卡坍缩(纯函数·确定性·不改输入列表/字典):
    顺序单遍聚类——每卡与已保留代表比相似度, ≥阈→并入该组(不新开槽);
    组内胜者=info_key最大者, 放在组首现位。英雄卡(位次<hero_n或卡面)恒保留。
    门关→win原对象直通; 总异常→win原对象直通。"""
    if not DEDUP_ON():
        return win  # 门关=逐位直通(逐字节等价)
    if not isinstance(win, list) or len(win) < 2:
        return win
    try:
        th = DEDUP_TH()
        hero_n = max(0, int(hero_n))
        reps: list[dict[str, Any]] = []          # 代表卡(按现槽位序)
        rep_sh: list[frozenset[str]] = []
        rep_norm: list[str] = []
        rep_anch: list[str | None] = []          # 代表归属锚(守卫用)
        rep_best: list[tuple[int, int, int]] = []  # 组内当前胜者 info_key
        out: list[dict[str, Any]] = []
        for i, d in enumerate(win):
            if not isinstance(d, dict):
                out.append(d)
                reps.append(d)  # 非dict占位=不参与比较(类型异常防御)
                rep_sh.append(frozenset())
                rep_norm.append("")
                rep_anch.append(None)
                rep_best.append((0, 0, -i))
                continue
            # 位次豁免只看原始位<hero_n; 卡面豁免独立判定(与 span_cut 同口径)
            if i < hero_n or _is_hero(d):
                out.append(d)
                reps.append(d)
                rep_sh.append(frozenset())
                rep_norm.append("")
                rep_anch.append(None)
                rep_best.append((0, 0, -i))
                continue
            ct = str(d.get("content", ""))
            sh = _shingles(ct)
            nm = _norm_text(ct)
            anch = _anchor(ct)
            merged = False
            for ri in range(len(reps)):
                try:
                    s = _sim(sh, rep_sh[ri], nm, rep_norm[ri])
                except Exception:  # noqa: BLE001 — 单比较异常=不合并(fail-soft)
                    continue
                if s >= th:
                    # [归属守卫] 跨任务近重不并(防gold卡被顶出早位·闸2首轮实证)
                    aa = rep_anch[ri]
                    if anch is not None and aa is not None and anch != aa:
                        continue
                    # 并入组ri: 胜者若为本卡→顶替该槽内容(位次不变·序语义保持)
                    if _info_key(d, i) > rep_best[ri]:
                        out[ri] = d
                        rep_best[ri] = _info_key(d, i)
                        reps[ri] = d
                        rep_sh[ri] = sh
                        rep_norm[ri] = nm
                        rep_anch[ri] = anch
                    merged = True
                    break
            if not merged:
                out.append(d)
                reps.append(d)
                rep_sh.append(sh)
                rep_norm.append(nm)
                rep_anch.append(anch)
                rep_best.append(_info_key(d, i))
        n_drop = len(win) - len(out)
        if n_drop:
            log.info("win_density dedup ok cards %d->%d collapsed=%d th=%.2f",
                     len(win), len(out), n_drop, th)
        return out
    except Exception as exc:  # noqa: BLE001 — 总fail-soft: 原窗直通
        log.warning("win_density collapse_near_dups FAILED err=%s (window kept)",
                    type(exc).__name__)
        return win


# ── ③ 重排尾截断(evid级·淘汰) ────────────────────────────────────────────────
def tail_cut(evid: list[dict[str, Any]],
             scores: dict[str, float] | None) -> list[dict[str, Any]]:
    """rerank 低分证据卡淘汰(纯函数·确定性·不改输入):
    score < RR_TAIL 且 rerank 序位 ≥ RR_KEEP_MIN → 丢(深度地板保护窗深内 gold——
    首轮 U3-Q1 实测: 无地板时 0.15 绝对阈把 rank13-30 弱分 gold 砍掉 R@30 −10pp)。
    保底=保留最高分1张(并列取rerank序先者); 英雄卡面恒保留。
    直通域: 门关 / scores=None(门关或rerank降级) / 全零分(降级指纹) / 空表。
    总异常→原列表直通。"""
    if not TAIL_ON():
        return evid
    if not isinstance(evid, list) or not evid or not isinstance(scores, dict):
        return evid
    try:
        vals = [float(scores.get(str(d.get("id", "")), 0.0)) for d in evid]
        if not any(vals):  # 全零=降级指纹(rerank_client失败回填0.0)→fail-soft直通
            return evid
        th = RR_TAIL()
        keep_min = RR_KEEP_MIN()
        keep_idx: list[int] = []
        best_i = 0
        for i, d in enumerate(evid):
            if isinstance(d, dict) and _is_hero(d):
                keep_idx.append(i)  # 防御: 英雄卡面恒保留(正常不在evid)
                continue
            if vals[i] >= th or i < keep_min:  # 过阈 或 窗深地板内→保留
                keep_idx.append(i)
            if vals[i] > vals[best_i]:
                best_i = i
        if not keep_idx:
            keep_idx = [best_i]  # 保底1张(最高分·rerank序先者)
        out = [evid[i] for i in keep_idx]
        n_cut = len(evid) - len(out)
        if n_cut:
            log.info("win_density tail_cut ok evid %d->%d cut=%d th=%.2f keep_min=%d",
                     len(evid), len(out), n_cut, th, keep_min)
        return out
    except Exception as exc:  # noqa: BLE001 — 总fail-soft: 原列表直通
        log.warning("win_density tail_cut FAILED err=%s (evidence kept)",
                    type(exc).__name__)
        return evid
