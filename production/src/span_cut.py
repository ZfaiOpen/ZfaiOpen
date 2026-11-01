# span_cut.py — L轨·L1 行级窗口裁剪(SPAN CUT·2026-10-25)
# ═══════════════════════════════════════════════════════════════════════════════
# 战术依据(第二波攻势·L1): 官方 answer 模型拿 117K token 预算, 检索窗的有效密度决定
#   agent 能否用上证据。轨迹卡大量行是废话(空行/重复报错/过程叙述)——query 条件化的
#   行级裁剪保住证据行(报错/diff hunk/测试名/verdict/文件路径)砍掉废行, 同样字符预算
#   能装 2-3 倍卡数 → R@100 层面 gold 更多元进窗(宽窗+高密度)。
#
# 纪律: ①零LLM纯规则·确定性(同输入恒同输出) ②env总门 BRAINOS_SPAN 缺省0=关,
#   关=apply_window 返回原对象逐字节等价 ③fail-soft: 任何异常→整卡原文直通(绝不阻
#   search, 单卡异常只牺牲该卡) ④不碰 data/生产树, 无DB零状态, 回滚=摘 coding_window
#   挂接hunk+删本件。
#
# 算法(单卡 cut_card):
#   1) 行打分(确定性级联·每行取首个命中):
#        指纹命中行(query指纹 paths/errsig/symbols/tests 词面·len≥4·substring) 5.0
#        > query词面重叠(行内命中≥2个query词)                    3.0
#        > diff hunk 行(@@开头; 或卡含diff标记时 +/- 开头·markdown弹点防误伤) 2.5
#        > verdict 行(Exit code N / Exit N / passed / FAILED / OK)          2.5
#        > 时间戳/会话头行(ISO时间戳 / [SESSION / session= / Traceback头)    1.0
#        > 其余                                                              0.3
#        空行(纯空白)                                                        0.0
#   2) 近重复行坍缩: 归一化行(小写+压空白)精确重复→丢; 词级8-gram shingle 与本卡
#      已保留行集合相交→近重复丢(保留首现·重复报错洪水坍缩·共 line<8 词不参与
#      shingle 只走精确重复——词级8连词=经典近重复粒度, 字符级8连字会误杀
#      共享短 token 的不同证据行, 单测 T7 实证后定版)。
#   3) 卡预算 BRAINOS_SPAN_CARD(缺省1400字符): 存活行按(-分, 原序号)排序装入
#      (确定性tie=先出现者优先), 再按原序输出(顺序保持=叙事可读)。整卡≤预算→
#      原文逐字节直通(小卡零风险)。装不进任何行时保底取最高分行(内容非空硬契约)。
#   窗级 apply_window(win, query, fp, hero_n):
#      位次<hero_n 或 内容以 FIX[/ANTI[/ISSUE-MAP: 开头(=FIX/ANTI/COMPR英雄卡,
#      已是压缩形态)→豁免; 其余卡逐卡 cut_card; 单卡异常→该卡原文直通; 总异常→
#      原窗直通。门关→win 原对象直通。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger("span_cut")

_NGRAM = 8  # 近重复 shingle n(字符级·归一化行)


def ON() -> bool:
    """env总闸BRAINOS_SPAN(缺省0=关): 关=apply_window原对象直通(逐字节等价)。"""
    try:
        return str(os.environ.get("BRAINOS_SPAN", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)


def CARD_BUDGET() -> int:
    """单卡裁剪后字符预算(BRAINOS_SPAN_CARD·缺省1400)。"""
    return max(200, _i("BRAINOS_SPAN_CARD", "1400"))


# ── 行分类正则(纯函数·确定性) ────────────────────────────────────────────────
_RX_EXIT = re.compile(r"\b(?:exit code|exit status)\s+\d+\b", re.I)
_RX_EXIT2 = re.compile(r"^\s*exit\s+\d+\b", re.I)
_RX_PASS = re.compile(r"\bpassed\b|\bpassing\b", re.I)
_RX_FAIL = re.compile(r"\bfailed\b|\bfailing\b|\bfailure\b", re.I)
_RX_OK = re.compile(r"^\s*(?:OK|ok)\b|\ball tests (?:passed|pass)\b", re.I)
_RX_TS = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?")
_RX_SESS = re.compile(r"\[SESSION\b|^\s*session[=:\s]|^\s*\[\d{4}-\d{2}-\d{2}", re.I)
_RX_WS = re.compile(r"\s+")
_RX_WORD = re.compile(r"[a-z0-9_][a-z0-9_\-.]{3,}")

_HERO_PREFIXES = ("FIX[", "ANTI[", "ISSUE-MAP:")  # deep_expdistill FIX/ANTI/COMPR卡面


def is_hero_content(content: str) -> bool:
    """英雄卡内容识别(FIX/ANTI/COMPR·已是压缩形态→豁免裁剪)。"""
    c = str(content or "").lstrip()
    return c.startswith(_HERO_PREFIXES)


# ── 词面集构造(与 coding_window._sig_count 同口径·确定性) ────────────────────
def _fp_values(fp: dict[str, list[str]] | None) -> list[str]:
    """query指纹→匹配值表(小写·len≥4·paths/errsig/symbols/tests+cmds拆半)。
    errsig("cls:msg")拆两半各自匹配(_norm_msg冒号无空格·原行带空格)。"""
    vals: list[str] = []
    if not isinstance(fp, dict):
        return vals
    for k in ("paths", "errsig", "tests", "symbols"):
        for v in (fp.get(k) or [])[:24]:
            v = str(v).lower()
            if len(v) >= 4:
                vals.append(v)
                if k == "errsig" and ":" in v:
                    for part in v.split(":", 1):
                        part = part.strip()
                        if len(part) >= 4:
                            vals.append(part)
    for v in (fp.get("cmds") or [])[:16]:
        for part in str(v).lower().split("|"):
            part = part.strip()
            if len(part) >= 4:
                vals.append(part)
    return vals


def _query_words(query: str) -> list[str]:
    """query→小写词面(len≥4·去重·截80·确定性保序)。"""
    seen: set[str] = set()
    out: list[str] = []
    for w in _RX_WORD.findall(str(query or "").lower()):
        if w not in seen:
            seen.add(w)
            out.append(w)
            if len(out) >= 80:
                break
    return out


def _norm_line(line: str) -> str:
    return _RX_WS.sub(" ", str(line or "").strip().lower())


def _shingles(nline: str, n: int = _NGRAM) -> set[str]:
    """归一化行→词级n-gram shingle集(词数<n→空集=只走精确重复合并)。"""
    toks = nline.split(" ")
    return {" ".join(toks[i:i + n]) for i in range(len(toks) - n + 1)} \
        if len(toks) >= n else set()


def _line_score(line: str, fp_vals: list[str], q_words: list[str],
                card_has_diff: bool) -> float:
    """行打分(确定性级联·首个命中即返回)。空行0。
    [SBCTX task=]归属锚行=5.5(身份行·裁掉=卡不可归属·评测与溯源双输——高于一切证据行)。"""
    if not line.strip():
        return 0.0
    low = line.lower()
    if "[sbctx task=" in low:
        return 5.5
    # ① 指纹命中(query指纹词面·权重5.0)
    for v in fp_vals:
        if v in low:
            return 5.0
    # ② query词面重叠≥2(权重3.0)
    if sum(1 for w in q_words if w in low) >= 2:
        return 3.0
    # ③ diff hunk(权重2.5): @@恒hunk; +/-开头仅在卡确含diff标记时
    #    (markdown弹点"- item"防误伤·确定性: 卡级布尔非行级上下文)
    if line.startswith("@@") or line.startswith("diff --git"):
        return 2.5
    if card_has_diff and (line.startswith("+") or line.startswith("-")):
        return 2.5
    # ④ verdict 行(权重2.5)
    if (_RX_EXIT.search(line) or _RX_EXIT2.match(line) or _RX_PASS.search(line)
            or _RX_FAIL.search(line) or _RX_OK.search(line)):
        return 2.5
    # ⑤ 时间戳/会话头(权重1.0)
    if _RX_TS.search(line) or _RX_SESS.search(line):
        return 1.0
    return 0.3


def cut_card(content: str, fp_vals: list[str], q_words: list[str],
             budget: int) -> str:
    """单卡行级裁剪(纯函数·确定性): ≤预算→原文直通; 否则 打分→坍缩→按分装箱→原序输出。"""
    text = str(content or "")
    lines = text.split("\n")
    if len(text) <= budget:
        return text  # 整卡≤预算=原样直通(小卡零风险·含空行原貌)
    card_has_diff = "diff --git" in text or "@@" in text
    # ① 打分 + ② 近重复坍缩(保留首现·与分数无关的前置坍缩)
    scored: list[tuple[float, int, str]] = []
    seen_exact: set[str] = set()
    seen_sh: set[str] = set()
    for idx, ln in enumerate(lines):
        nl = _norm_line(ln)
        if not nl:
            continue  # 空行不参与(分0·裁剪路径自然淘汰)
        if nl in seen_exact:
            continue  # 精确重复
        sh = _shingles(nl)
        if sh & seen_sh:
            continue  # 近重复(8-gram相交·保留首现)
        seen_exact.add(nl)
        seen_sh |= sh
        scored.append((_line_score(ln, fp_vals, q_words, card_has_diff), idx, ln))
    if not scored:
        return text  # 防御: 全空卡=原文直通
    # ③ 装箱: (-分, 原序号)升序=分高先装·同分先出现先装(确定性)
    scored.sort(key=lambda t: (-t[0], t[1]))
    kept_idx: list[int] = []
    used = 0
    for sc, idx, ln in scored:
        cost = len(ln) + 1  # 换行
        if used + cost <= budget:
            kept_idx.append(idx)
            used += cost
    if not kept_idx:  # 预算极小→保底最高分行(内容非空硬契约)
        kept_idx = [scored[0][1]]
    kept_idx.sort()  # 原序输出(顺序保持=叙事可读)
    return "\n".join(lines[i] for i in kept_idx)


def apply_window(win: list[dict[str, Any]], query: str, fp: dict[str, list[str]] | None,
                 hero_n: int) -> list[dict[str, Any]]:
    """窗级行裁剪(coding_window.apply 预算淘汰前调用·门关=原对象直通):
    英雄卡(位次<hero_n 或 FIX/ANTI/COMPR卡面)豁免; 其余卡 cut_card;
    单卡异常→该卡原文直通; 本函数级异常→原窗直通。"""
    if not ON() or not isinstance(win, list) or not win:
        return win
    try:
        vals = _fp_values(fp if isinstance(fp, dict) else {})
        qw = _query_words(query)
        budget = CARD_BUDGET()
        out: list[dict[str, Any]] = []
        n_cut = n_char = 0
        for i, d in enumerate(win):
            try:
                if not isinstance(d, dict):
                    out.append(d)
                    continue
                ct = str(d.get("content", ""))
                if i < max(0, hero_n) or is_hero_content(ct) or len(ct) <= budget:
                    out.append(d)
                    continue
                nc = cut_card(ct, vals, qw, budget)
                if nc and nc != ct:
                    nd = dict(d)  # 不改输入卡(调用方持有原列表)
                    nd["content"] = nc
                    out.append(nd)
                    n_cut += 1
                    n_char += len(ct) - len(nc)
                else:
                    out.append(d)
            except Exception:  # noqa: BLE001 — 单卡异常=整卡原文直通
                out.append(d)
        if n_cut:
            log.info("span_cut ok cards=%d cut=%d chars_saved=%d budget=%d",
                     len(win), n_cut, n_char, budget)
        return out
    except Exception as exc:  # noqa: BLE001 — 总fail-soft: 原窗直通
        log.warning("span_cut apply_window FAILED err=%s (original window kept)",
                    type(exc).__name__)
        return win
