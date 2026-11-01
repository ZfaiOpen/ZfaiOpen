"""深度证据处理器v2 — ADD时把对话原文变成结构化知识卡
这是从"检索系统"到"理解系统"的跨越——不是更多检索优化·是存储时就变成知识。
零LLM·纯规则+模式匹配·2026-10-09 B-0-B席"""

import re, json, hashlib
from datetime import datetime
from collections import defaultdict

# ── 代词解引 ──
_PRONOUN = {"he":"male","she":"female","his":"male","her":"female","him":"male"}
_NAME_POOL = set()  # 从上下文收集的人名

# ── 时间归一化(相对→绝对·以消息timestamp为锚) ──
_REL_PAT = [
    (re.compile(r"yesterday|昨天", re.I), -1, "day"),
    (re.compile(r"last week|上周", re.I), -7, "day"),
    (re.compile(r"two weeks ago|两周前", re.I), -14, "day"),
    (re.compile(r"last month|上个月", re.I), -30, "day"),
    (re.compile(r"today|今天", re.I), 0, "day"),
    (re.compile(r"tomorrow|明天", re.I), 1, "day"),
    (re.compile(r"next week|下周", re.I), 7, "day"),
    (re.compile(r"three days ago|三天前", re.I), -3, "day"),
    (re.compile(r"last year|去年", re.I), -365, "day"),
]

def _abs_date(ts_str, offset_days):
    try:
        base = datetime.fromisoformat(ts_str.replace("Z","+00:00"))
        from datetime import timedelta
        return (base + timedelta(days=offset_days)).strftime("%Y-%m-%d")
    except:
        return ts_str[:10] if ts_str else ""

def resolve_relative(text, ts):
    """相对时间→绝对日期"""
    from datetime import timedelta, datetime
    resolved = text
    for pat, offset, _ in _REL_PAT:
        m = pat.search(resolved)
        if m:
            abs_date = _abs_date(ts, offset)
            resolved = resolved[:m.start()] + abs_date + resolved[m.end():]
    return resolved

# ── 人名抽取(零LLM·大写词+称谓模式) ──
_NAME_PAT = re.compile(r"(?:^|\.\s+|\?\s+|!\s+)([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)[:\s]")
def extract_names(text):
    names = set()
    for m in _NAME_PAT.finditer(text):
        names.add(m.group(1))
    # 显式说话人
    for m in re.finditer(r"(\w+):\s", text):
        names.add(m.group(1))
    return names

# ── 三元组抽取(泛式·非域词典) ──
_TRIPLE_PATS = [
    # X is/lives/works in/at Y
    (re.compile(r"([A-Z]\w+)\s+(?:lives?|works?|stays?|is)\s+(?:in|at|from)\s+([A-Z][\w\s]{2,30}?)(?:[.,!\n]|$)", re.I), "reside"),
    (re.compile(r"([\u4e00-\u9fa5]\w*)\s*(?:住在|在|工作在)\s*([\u4e00-\u9fa5A-Za-z]+)", re.I), "reside_cn"),
    # X moved/went to Y
    (re.compile(r"(\w+)\s+(?:moved|went|came|travelled?)\s+to\s+([A-Z][\w\s]{2,30}?)(?:[.,!\n]|$)", re.I), "move_to"),
    # X likes/loves/hates Y
    (re.compile(r"(\w+)\s+(?:likes?|loves?|enjoys?|hates?|prefers?)\s+([\w\s]{2,30}?)(?:[.,!\n]|$)", re.I), "preference"),
    # X works as/is a Y
    (re.compile(r"(\w+)\s+(?:works?\s+as|is)\s+(?:a|an)?\s*([\w\s]{2,30}?)(?:[.,!\n]|$)", re.I), "occupation"),
    # X got/received Y
    (re.compile(r"(\w+)\s+(?:got|received|obtained|won)\s+([\w\s]{2,30}?)(?:[.,!\n]|$)", re.I), "acquisition"),
]

def extract_facts(text, speaker, ts):
    """零LLM事实抽取(泛式)"""
    facts = []
    text_resolved = resolve_relative(text, ts)
    names = extract_names(text_resolved)
    
    for pat, rel_type in _TRIPLE_PATS:
        for m in pat.finditer(text_resolved):
            subj = m.group(1).strip()
            obj = m.group(2).strip().rstrip(".,!?")
            if not subj or not obj or len(obj) < 2:
                continue
            facts.append({
                "subject": subj, "relation": rel_type, "object": obj,
                "date": _abs_date(ts, 0), "source_text": text_resolved[:80],
                "speaker": speaker,
            })
    
    # 中文事实
    for m in re.finditer(r"([\u4e00-\u9fa5]{2,4})(?:的|在|住在)([\u4e00-\u9fa5A-Za-z]{2,15})", text_resolved):
        facts.append({
            "subject": m.group(1), "relation": "cn_attr",
            "object": m.group(2), "date": _abs_date(ts, 0), "speaker": speaker,
        })
    
    return facts

# ── 深度处理器主函数 ──
def deep_process(messages, tenant, request_id):
    """深度处理：对话原文→结构化知识卡
    返回: (原文卡列表·知识卡列表·supersede链)"""
    
    # ① 代词解引准备
    all_names = set()
    for m in messages:
        all_names |= extract_names(m.get("content", ""))
    
    # ② 逐消息处理
    original_cards = []
    knowledge_cards = []
    fact_index = defaultdict(list)  # (entity_lower, relation) -> [facts]
    
    for i, m in enumerate(messages):
        text = m.get("content", "")
        ts = m.get("timestamp", "")
        speaker = m.get("role", "user")
        dia = f"{request_id}:{i}"
        
        # 时间归一(相对→绝对)
        text_resolved = resolve_relative(text, ts)
        
        # 原文卡(不变·合规)
        original_cards.append({
            "conv": tenant, "sess": f"{request_id}:{i}",
            "dia": dia, "speaker": speaker,
            "text": text_resolved,
            "dt": _safe_dt(ts),
        })
        
        # 事实抽取
        facts = extract_facts(text_resolved, speaker, ts)
        for f in facts:
            ent_key = f["subject"].lower()
            rel_key = f["relation"]
            
            # supersede检测: 同实体同关系·值不同→新值覆盖旧值
            prev_list = fact_index.get((ent_key, rel_key), [])
            if prev_list:
                prev = prev_list[-1]
                if prev["object"].lower() != f["object"].lower():
                    # 新值覆盖旧值·标记supersede
                    knowledge_cards.append({
                        "key": f"sup|{hashlib.md5((ent_key + rel_key + str(f.get('object',''))).encode()).hexdigest()[:10]}",
                        "conv": tenant, "sess": f"{request_id}:{i}",
                        "dia": dia, "speaker": speaker,
                        "text": f"[SUPERSEDES] {prev['subject']} {prev['relation']} {prev['object']} → NOW {f['object']} (as of {f['date']})",
                        "dt": _safe_dt(ts), "kind": "supersede",
                        "entity": ent_key, "supersedes": prev["key"],
                    })
            
            fact_index[(ent_key, rel_key)].append(f)
            
            # 知识卡(原子事实·结构化)
            knowledge_cards.append({
                "key": "fact|" + hashlib.md5((ent_key + rel_key + str(f.get("object","")) + str(i)).encode()).hexdigest()[:10],
                "conv": tenant, "sess": f"{request_id}:{i}",
                "dia": dia, "speaker": speaker,
                "text": f"{f['subject']} | {f['relation']} | {f['object']} | date:{f['date']}",
                "dt": _safe_dt(ts), "kind": "atomic_fact",
                "entity": ent_key, "relation": rel_key, "object": f["object"],
            })
        
        # 规则检测
        for r in re.finditer(r"(?:always|never|must|should|需要|必须|记得|永远)\s+([\w\s,]{5,50})", text, re.I):
            knowledge_cards.append({
                "key": f"rule|{hashlib.md5(r.group(0).encode()).hexdigest()[:10]}",
                "conv": tenant, "sess": f"{request_id}:{i}",
                "dia": dia, "speaker": speaker,
                "text": f"RULE: {r.group(0).strip()}",
                "dt": _safe_dt(ts), "kind": "rule",
            })
    
    return original_cards, knowledge_cards


def _safe_dt(ts):
    try:
        datetime.fromisoformat(ts.replace("Z","+00:00"))
        return ts
    except:
        return ts[:19] if ts else "1970-01-01T00:00:00"

from datetime import datetime
