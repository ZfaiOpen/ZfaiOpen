#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""content_sanitizer.py — 出口content消毒层 (2026-10-12 · mm同款病修复)
根因: BRAIOS内部标记([D1:x]刀谓词注记等)+原始数据形态原样出库→平台EVAL parser毒杀(EVAL_RUNTIME_ERROR)
方案: mm战场已验证的sanitize_content移植——剥内部标记链·空content兜底
铁律: 只剥标记不动正文·确定性·零LLM·fail-soft·env BRAIOS_SANITIZE=0全局关门
"""
from __future__ import annotations
import os
import re

# BRAIOS自家刀谓词注记: [D1:2] [D2:15] ... (任何位置)
_KNIFE_MARK = re.compile(r"\s*\[D[1-9]:\d+\]")
# 分片标记: [part 1/2]
_PART_MARK = re.compile(r"\s*\[part \d+/\d+\]")
# content开头的连续方括号元数据链: [clbench_16_32k][session_1][text] ... (无内空格的短标记·连续≥1组)
_HEADER_CHAIN = re.compile(r"^((?:\[[^\]\s]{1,32}\])+)\s*")
# 已知数据集/会话标记(任何位置·保守锚定已知名)
_KNOWN_TAGS = re.compile(r"\s*\[(?:clbench|memtrapbench|locomo|longmemeval|personamem|scriptmem|beam|swe[a-z]*|session)_?[^\]\s]{0,24}\]")

_ENABLED = os.environ.get("BRAIOS_SANITIZE", "1") != "0"

def sanitize_text(text: str) -> str:
    if not _ENABLED or not text:
        return text
    # 1. content开头元数据链整链剥除(逐段剥到首段非标记为止)
    prev = None
    out = text
    while prev != out:
        prev = out
        out = _HEADER_CHAIN.sub("", out, count=1)
    # 2. 任何位置的内部分散标记
    out = _KNIFE_MARK.sub("", out)
    out = _PART_MARK.sub("", out)
    out = _KNOWN_TAGS.sub("", out)
    return out.lstrip() if out != text else text

def sanitize(data):
    """对search响应卡列表就地消毒(返回新列表)·空content兜底保留原文。"""
    try:
        if not _ENABLED or not data:
            return data
        out = []
        for card in data:
            try:
                raw = str(card.get("content", ""))
                clean = sanitize_text(raw)
                if not clean.strip():
                    clean = raw  # 空兜底: 剥成空=保留原文(宁毒勿空)
                if clean is not raw:
                    card = dict(card)
                    card["content"] = clean
                out.append(card)
            except Exception:
                out.append(card)
        return out
    except Exception:
        return data
