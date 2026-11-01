#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""evidence_rerank.py v3 — 代码语境证据重排 (20261013 erank3·冒烟0分复盘手术)
真证据判定=平台自带标签: diff --git/```/[tool_result]/[tool_use (叙述里提及代码不算证据)
权重: 真证据2.0 > 代码事实1.3 > 普通1.0 > 结论叙述0.55 > [thinking]0.5 > 文本派生0.4
v3新增(冒烟0分根因手术): 历史会话"中途结论卡"(Good progress!/All tests pass now类)
以organ高分霸占top1给agent虚假信心带偏补丁→降权0.55; 源码摘录/报错事实卡→升权1.3。
条件触发: 窗口代码卡占比>=0.25才启用·否则原样返回(文本赛道零回归)
铁律: 只动序不增不减不改内容·稳定排序确定性·零LLM·调用方try兜底
"""
from __future__ import annotations
import logging
import re

_LOG = logging.getLogger("erank")

_DIFF_RE = re.compile(r"diff --git|```|\[tool_result\]|\[tool_use")
_DERIVED_PREFIXES = ("ATTRIBUTE TIMELINE", "MEMORY STATUS", "RULE:", "AS-OF", "STEP ",
                     "API TIMELINE", "API DEPRECATED", "MIGRATION:", "CHAIN:", "推导",
                     "TASK BRIEF", "CONFIDENTIAL", "BUGFIX[", "SIMILAR BUG", "PATTERN[",
                     "ANTI:", "EXC STRATEGY", "CHANGE SUMMARY", "CHANGE ORDER", "FUNC ",
                     "GRAPH ", "INHERITS:", "IMPACT:", "CYCLE:", "CODE STYLE", "SENSITIVE:")
_THINK_RE = re.compile(r"^\s*\[thinking\]")
_CONCL_RE = re.compile(
    r"^\s*(Good progress|Great progress|All \w+ ?tests? (pass|passed)|"
    r"Now I (can see|see|understand)|There (are|is) (two|three|a|an|several) (main |possible )?(issues?|problems?|bugs?)|"
    r"So after (step|creating|that)|I (see|think|found|believe|notice)|"
    r"Let me|The remaining issues?|Perfect[.!]|Wait[,.]|Hmm|Aha|Looks (good|like)|"
    r"That('| i)s (the|a) (issue|problem|bug))", re.I)
_SUMMARY_RE = re.compile(r"^\s*(Here's a summary|Here is a summary|##\s*(Problem|Changes Summary|Fix Summary))", re.I)
_FAILTEST_RE = re.compile(r"^\s*Exit code [1-9]\d*\b")
_CODEISH_RE = re.compile(
    r"^\s*\d+\t|^\s*(def |class |import |from )|Exit code \d|Traceback|"
    r"(PASSED|FAILED|ERROR) |assert |raise |Exception|ValueError|TypeError")

def _is_evidence(t: str) -> bool:
    return bool(_DIFF_RE.search(t))

def rerank(data, query):
    try:
        if not data or len(data) < 4:
            return data
        contents = [str(c.get("content", "")) for c in data]
        ev_n = sum(1 for t in contents if _is_evidence(t))
        _LOG.info("erank probe n=%d ev_n=%d heads=%r q=%.30s", len(data), ev_n, [contents[i][:44] for i in range(min(3, len(contents)))], str(query))
        if ev_n < 2:
            return data  # v3.3: 守卫降至2+probe日志(定位叙述霸榜:窗口ev_n实测)
        n = len(data)
        keyed = []
        for i, (card, t) in enumerate(zip(data, contents)):
            base = float(n - i)
            if _THINK_RE.match(t):
                w = 0.5  # v4: [thinking]叙述卡优先判定(csan前视角)——v2本意修复:此前被_DIFF_RE先判,叙述卡内嵌```伪装成2.0真证据=冒烟0分霸榜真根因
            elif _CONCL_RE.match(t):
                w = 0.55
            elif _SUMMARY_RE.match(t):
                w = 0.55  # v5: "Here's a summary of the fix"类历史修复总结=结论叙述降权(防与gold修法冲突带偏)
            elif _is_evidence(t):
                w = 2.0
            elif _FAILTEST_RE.match(t):
                w = 0.8  # v5: Exit code N失败测试输出=历史失败痕迹,低于普通卡
            elif _CODEISH_RE.search(t):
                w = 1.3
            elif t.lstrip().startswith(_DERIVED_PREFIXES):
                w = 0.4
            else:
                w = 1.0
            keyed.append((base * w, i, card))
        keyed.sort(key=lambda x: (-x[0], x[1]))
        return [c for _, _, c in keyed]
    except Exception:
        return data
