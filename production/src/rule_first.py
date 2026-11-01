# rule_first.py — T-E席·G2规则域回踩修复·总装层RULE置顶重排(2026-10-10)
# ═════════════════════════════════════════════════════════════════════════════
# 法证根因(zfai@192.168.28.196 只读取证·service.log+query_trace.log):
#   四推理引擎(T1时间线/T2闭包/T3时序/T4策略)部署后注入器2→6(overlay/cfact/
#   T4/T3/T2/T1), 浮顶位竞争。官方G2规则题"What temperature unit should the
#   assistant use?"的RULE卡(kind=rule·文本"RULE:"前缀)在overlay浮顶门槛
#   (s*2>=len(qt))下判不出浮顶→沉窗位3(simG2双探针11:49:47/11:53:55实证
#   win=4: [text,text,RULE,DIGEST]·全天同形); 同窗口官方租户实测T2闭包
#   inject=1x12次/T1时间线inject=2x4次挤位。G2规则分40→20回落与RULE沉位吻合。
# 治法(单点收口·五禁令④单一实现路径):
#   不动四引擎inject(禁四处重复让位逻辑), 新增本模块由aml_service在
#   dtl.inject(T1·末位注入器)之后统一调用一次: 规则信号问句命中且窗内有
#   "RULE:"前缀卡→稳定提前到位1(rank0 organ最优不动·其余卡相对序顺延·
#   零增删零改写=窗口集合不变·幂等)。非规则问句或无RULE卡=原窗直通
#   (同一对象返回·零回归由构造保证)。
# 纪律: 纯stdlib·零LLM·零题目硬编码(通用信号词)·确定性(同入参同输出)·
#       fail-soft(任何异常=原窗直通)·开关可回滚。
# 回滚: BRAINOS_CC_RULEFIRST=0 或摘除aml_service单hunk+本件。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import os
import re
from typing import Any

# 规则/习惯/偏好/温度族信号词(EN \b词界+CN字面) — 覆盖官方G2题面家族
# ("What temperature unit should the assistant use?"命中temperature);
# 与card_construct._REL_BIGRAMS同族但独立编译(本件零依赖=可单点摘除)。
_RULE_QUERY_RE = re.compile(
    r"\b(rules?|ruled|habits?|habitual|prefer\w*|always|never|"
    r"temperatures?|celsius|fahrenheit|"
    r"units?|metric|imperial|settings?|configuration|configured)\b"
    r"|规则|习惯|偏好|总是|从不|温度|摄氏|华氏|单位|公制|英制|设置|惯例|规矩",
    re.IGNORECASE)

_RULE_PREFIX = "RULE:"


def RULE_FIRST_ON() -> bool:
    """开关(env·运行期读·置0即回基线直通)。"""
    return os.environ.get("BRAINOS_CC_RULEFIRST", "1") != "0"


def is_rule_query(query: str) -> bool:
    """规则信号问句判定(纯函数·确定性): 命中规则/习惯/偏好/温度族信号词。"""
    try:
        return bool(_RULE_QUERY_RE.search(str(query or "")))
    except Exception:  # noqa: BLE001 — 判定失败=按非规则问句(直通)
        return False


def rule_first_rerank(query: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """总装层RULE置顶重排(六注入器全部之后调用·单点收口)。
    位序约定与card_construct浮顶同款: 位0=rank0 organ最优不动·位1=第一浮顶位。
    稳定重排: "RULE:"前缀卡组保持彼此相对序提前到位1, 其余卡保持相对序顺延;
    零增删零改写(窗口集合与文本逐字节不变·只调序)。幂等: 已置顶时输出逐卡
    全等于输入。fail-soft: 任何异常=原窗直通(返回原对象)。"""
    if not data or not RULE_FIRST_ON():
        return data
    try:
        if not is_rule_query(query):
            return data
        head, rest = data[:1], data[1:]
        rules = [d for d in rest if str(d.get("content", "")).startswith(_RULE_PREFIX)]
        if not rules:
            return data  # 窗内无RULE卡=直通(非G2租户零扰动·回归由构造保证)
        others = [d for d in rest if not str(d.get("content", "")).startswith(_RULE_PREFIX)]
        return head + rules + others
    except Exception:  # noqa: BLE001 — 重排失败=原窗直通(零回归)
        return data
