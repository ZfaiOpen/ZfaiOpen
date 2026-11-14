# brainos/memory/recall_salience.py — 影子重建件(披露: 196冻结版真身不在本机, 此为API同形的
# 影子替代实现) — 噪声语料提取竞争门控(通用语料统计启发·零题目先验·零LLM)。
# ═══════════════════════════════════════════════════════════════════════════════
# 影子保真度声明(诚实交付):
#   - extract_query_speakers / name_in_text: 按organ调用点语义忠实实现(词边界大小写不敏感
#     匹配), 行为与真身应当一致——这两件是纯匹配器, 无统计状态。
#   - ConvSalience / find_duplicates: 影子中性实现(显著度恒1.0·零重复标记)——真身是
#     "四重门控"统计启发(公式不在本机任何归档), 影子以确定性中性值替代。影响面:
#     spk_boost加权取sal=1.0(满加成形态)·dream清洗无合并。**影子床分绝对值与196不可互换,
#     仅"同栈前后差值"有效** — 判例: 分数宣称必须带口径三元组。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import re
from typing import Iterable


def name_in_text(name: str, text: str) -> bool:
    """人名是否在文本出现(词边界·大小写不敏感·CJK直接子串)。"""
    n = (name or "").strip()
    t = text or ""
    if not n:
        return False
    if re.search(r"[\u4e00-\u9fff]", n):
        return n in t
    try:
        return re.search(r"(?<![\w'])" + re.escape(n) + r"(?![\w'])", t, re.IGNORECASE) is not None
    except re.error:
        return n.lower() in t.lower()


def extract_query_speakers(query: str, speakers: Iterable[str]) -> list[str]:
    """query中命中的已知说话人名(按词边界匹配·返回输入序)。"""
    q = query or ""
    hits: list[str] = []
    for spk in speakers:
        s = (spk or "").strip()
        if s and name_in_text(s, q) and s.lower() not in [h.lower() for h in hits]:
            hits.append(s)
    return hits


class ConvSalience:
    """会话显著度(影子中性: fit记语料量·salience恒1.0)。"""

    def __init__(self) -> None:
        self._n = 0

    def fit(self, texts: list[str]) -> None:
        self._n = len(texts or [])

    def salience(self, text: str) -> float:
        return 1.0


def find_duplicates(texts: list[str], salience: ConvSalience | None = None) -> set[int]:
    """重复卡索引集(影子中性: 恒空集=零合并)。"""
    return set()
