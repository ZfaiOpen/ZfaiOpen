"""深度证据构造器v2 — ADD时零LLM深度处理
从对话消息提取原子事实卡/因果对/归一化日期/规则卡/趋势卡
全部在ADD时完成·SEARCH时只做选择排序·合规=只改卡片内容不改响应格式
"""
import re, json, hashlib
from datetime import datetime, timezone
from collections import defaultdict

# ── 封闭类文法：跨语言三元组抽取 ──
# BE类: X is/was/will be Y · X lives/works/stands in Y · X变成Y
# HAVE类: X has/owns/uses Y · X有/用/戴着Y
# 变态类: X moved/switched/changed from A to B · X从A换成了B
_BE_PAT = re.compile(
    r"(\w+)\\s+(?:is|was|will be|lives?|lived|works?|worked|stands?|stayed|remains?)\\s+"
    r"(?:in|at|from|with|as)?\\s*([A-Z][\\w\\s]{1,40}|\\w+)", re.I)
_HAVE_PAT = re.compile(
    r"(\\w+)\\s+(?:has|had|owns?|owned|uses?|used|wears?|wore|got)\\s+"
    r"(?:a|an|the)?\\s*([\\w\\s]{2,40})", re.I)
_BECOME_PAT = re.compile(
    r"(\\w+)\\s+(?:moved?|switched?|changed?|transformed?)\\s+"
    r"(?:from\\s+)?(?:being\\s+)?(?:a|an)?\\s*([\\w\\s]{2,40}?)\\s+"
    r"(?:to|into|成为|换成了|变成了)\\s+([\\w\\s]{2,40})", re.I)
_CN_BE = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9]+)(?:是|在|住在|工作在|现在)([\u4e00-\u9fa5A-Za-z0-9]+)")
_CN_HAVE = re.compile(r"([\u4e00-\u9fa5A-Za-z0-9]+)(?:有|戴着|拿着|在吃|在用)([\u4e00-\u9fa5A-Za-z0-9]+)")

# ── 日期归一化 ──
_DATE_PATS = [
    (re.compile(r"(\\d{4})-(\\d{2})-(\\d{2})"), "{y}-{m:02d}-{d:02d}"),
    (re.compile(r"(\\d{4})年(\\d{1,2})月(\\d{1,2})日"), "{y}-{m:02d}-{d:02d}"),
    (re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\\s+(\\d{1,2}),?\\s+(\\d{4})"),
     lambda m: f"{m.group(3)}-{['','jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'].index(m.group(1).lower()):02d}-{int(m.group(2)):02d}"),
]

def extract_dates(text):
    dates = []
    for pat, fmt in _DATE_PATS:
        for m in pat.finditer(text):
            try:
                if callable(fmt):
                    dates.append(fmt(m))
                else:
                    dates.append(fmt.format(y=int(m.group(1)), m=int(m.group(2)), d=int(m.group(3))))
            except: pass
    return dates

def extract_triples(text, speaker):
    """零LLM三元组抽取(封闭类文法)"""
    triples = []
    for pat in [_BE_PAT, _HAVE_PAT, _BECOME_PAT, _CN_BE, _CN_HAVE]:
        for m in pat.finditer(text):
            subj = m.group(1).strip()
            pred = "state" if pat in (_BE_PAT, _CN_BE) else ("have" if pat in (_HAVE_PAT, _CN_HAVE) else "become")
            obj = (m.group(2) or m.group(3) if m.lastindex >= 3 else m.group(2)).strip()
            if subj and obj and subj.lower() not in ("i", "you", "it", "this", "that"):
                triples.append({"subject": subj, "predicate": pred, "object": obj, "speaker": speaker})
    return triples

def extract_rules(text):
    rules = []
    for m in re.finditer(r"(?:always|never|must|should|have to|需要|必须|记得|永远|每次)\\s+([\\w\\s,]{5,60})", text, re.I):
        rules.append(m.group(0).strip())
    return rules

def deep_construct(messages, tenant, request_id):
    """深度证据构造器：ADD时对每条消息做深度处理
    返回: (原文卡列表·派生原子卡列表·冲突对列表)"""
    original_cards = []
    atomic_cards = []
    date_annotations = []
    supersede_pairs = []  # (entity, attr, old_val, new_val, old_key, new_key)
    fact_registry = defaultdict(list)  # (entity_normalized, attr_category) -> [(value, card_key, dt)]

    for i, m in enumerate(messages):
        text = m.get("content", "")
        ts = m.get("timestamp", "")
        speaker = m.get("role", "user")
        dia = f"{request_id}:{i}"

        # 原文卡(不改)
        orig = {
            "conv": f"{tenant}:{sess_of(request_id)}",
            "sess": f"{request_id}:{i}",
            "dia": dia,
            "speaker": speaker,
            "text": text,
            "dt": honest_dt(text, ts),
        }
        original_cards.append(orig)

        # 日期归一化
        dates = extract_dates(text)
        date_str = ",".join(dates) if dates else ""

        # 三元组抽取
        triples = extract_triples(text, speaker)
        for t in triples:
            ent = re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", t["subject"].lower())
            attr = re.sub(r"[^a-z]", "", t["predicate"])
            val = t["object"]
            key = f"{ent}|{attr}|{hashlib.md5(val.encode()).hexdigest()[:8]}"
            atomic_cards.append({
                "key": key, "conv": orig["conv"], "sess": orig["sess"], "dia": dia,
                "speaker": speaker, "text": f"{t['subject']} {t['predicate']} {t['object']}"
                + (f" (date: {date_str})" if date_str else ""),
                "dt": orig["dt"], "entity": ent, "attr": attr, "value": val,
                "dates": date_str,
            })
            # supersede检测: 同(ent,attr)但val不同
            prev = fact_registry.get((ent, attr), [])
            if prev and prev[-1]["value"].lower() != val.lower():
                supersede_pairs.append({
                    "entity": ent, "attr": attr,
                    "old": prev[-1]["value"], "new": val,
                    "old_key": prev[-1]["key"], "new_key": key,
                })
            fact_registry[(ent, attr)].append({"value": val, "key": key})

        # 规则卡
        for r in extract_rules(text):
            atomic_cards.append({
                "key": f"rule|{hashlib.md5(r.encode()).hexdigest()[:8]}",
                "conv": orig["conv"], "sess": orig["sess"], "dia": dia,
                "speaker": speaker, "text": f"RULE: {r}",
                "dt": orig["dt"], "kind": "rule",
            })

        # 日期注记追加到原文卡
        if dates:
            original_cards[-1]["text"] += f" [dates: {','.join(dates)}]"

    return original_cards, atomic_cards, supersede_pairs


def sess_of(request_id):
    return request_id.split("_")[0] if "_" in request_id else request_id

def honest_dt(text, ts):
    """诚实dt：有时间源→归一化ISO·无→'unknown'(排序用obs_seq不依赖此)"""
    from card_construct import _parse_dt
    r = _parse_dt(ts)
    return r or "unknown"
