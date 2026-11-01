# knife9_g4.py — [刀8.7·G4经验数据发现20261017·68.28战报G4=0钉因: 数字/频率类query
# 无枚举形证据卡] G4形query(数量/频率/惯例)→窗内数字事实线按时间枚举成卡位1注入。
# 纯摘录窗口已有卡(零新增检索·禁4✓摘录原文)·条件触发(非G4形字节直通)·env可关。
from __future__ import annotations
import hashlib, os, re
from typing import Any

_G4_Q_RE = re.compile(
    r"\bhow\s+(?:many|often|much|frequently)\b|\b(?:pattern|habitually|usually|typically|regularly|frequency)\b"
    r"|\b(?:rate|average|trend|likely|predict\w*|simulat\w*|projection|uptake|estimate\w*)\b"
    r"|几次|多少|频率|通常|惯例|规律|概率|趋势", re.I)  # [刀8.7c·v0.3] 对齐平台真实题面(uptake rate/likely to pursue族)
_NUM_LINE_RE = re.compile(r"\d")
_MAX_ENUM = 10


def g4_enum_card(query: str, data: list[dict[str, Any]], top_k: int = 100,
                 force: bool = False) -> list[dict[str, Any]]:
    """G4枚举卡: 数字事实线按created_at升序摘录·位1注入·非G4形/无数字线=原窗直通。"""
    if not data or os.environ.get("BRAINOS_G4_ENUM", "1") == "0":
        return data
    try:
        if not force and not _G4_Q_RE.search(str(query or "")):
            return data  # [v0.4.1] force=意图分类G4命中(隐式考题无how-many词面)
        lines: list[tuple[str, str]] = []
        for d in data:
            c = str(d.get("content", ""))
            if c.startswith(("[digest", "[全局摘要", "[数据枚举", "注意:")):
                continue  # 仅排除digest/枚举/注记类(fact-time/timeline行含原文事实·保留摘录)
            if _NUM_LINE_RE.search(c):
                lines.append((str(d.get("created_at", ""))[:10], c[:110]))
        import logging
        valid_raw = sum(1 for _dt, t in lines if not t.startswith("["))
        logging.getLogger("k9").info("g4_valid_sample_count q=%.60s raw=%d total=%d",
                                     str(query)[:60], valid_raw, len(lines))
        if len(lines) < 2:
            # [刀8.7c·v0.3②1] 样本不足不再空手而回: 保守事实汇总(窗内前3条原文·零编造)
            import logging as _lg
            _lg.getLogger("k9").warning("g4_output_empty q=%.60s (样本<2·降级保守汇总)", str(query)[:60])
            keep = [d for d in data if not str(d.get("content","")).startswith(("[digest","[全局摘要","[数据枚举","注意:"))][:3]
            if len(keep) >= 1:
                summ = " ".join(f"«{str(d.get('content',''))[:80]}»" for d in keep)
                nid0 = hashlib.sha1(f"g4c|{query[:30]}|{keep[0].get('id','')}".encode()).hexdigest()[:16]
                return [{"id": nid0,
                         "content": f"[PatternInference] 保守事实汇总(样本不足·仅列原文): {summ}",
                         "created_at": str(keep[0].get("created_at","")) or ""}] + data
            return data
        lines.sort(key=lambda x: (x[1].startswith("["), x[0]))  # [刀8.7b] 原句优先·派生垫后
        body = " ".join(f"«{t}»({dt})" for dt, t in lines[:_MAX_ENUM])
        # 规律线索: 时间词跨线复现≥2次→纯统计附注(禁4✓零推演·只报复现事实)
        import collections
        tc = collections.Counter()
        for _dt, t in lines[:_MAX_ENUM]:
            if t.startswith("["):
                continue  # 派生卡(timeline/fact-time)的时间词=伪造频次源·规律只数非派生原句
            for m in re.finditer(r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|weekly|monthly|yearly|every\s+\w+)\b", t, re.I):
                tc[m.group(1).lower()] += 1
        patt = [f"{w}×{n}" for w, n in tc.most_common(3) if n >= 2]
        if patt:
            body += " ‖ 规律线索(复现统计): " + ", ".join(patt)
        nid = hashlib.sha1(f"g4|{query[:40]}|{lines[0][1][:30]}".encode()).hexdigest()[:16]
        card = {"id": nid,
                "content": f"[PatternInference] 数据枚举与规律(纯原文摘录·证据{len(lines[:_MAX_ENUM])}条): {body}",
                "created_at": lines[-1][0] or ""}
        if any(d.get("id") == nid for d in data):
            return data
        if len(data) >= max(int(top_k), 1):
            return data  # top_k契约: 满窗弃注记(实际top_k·硬编码100已除)
        return [card] + data
    except Exception:  # noqa: BLE001 — fail-soft原窗直通
        return data
