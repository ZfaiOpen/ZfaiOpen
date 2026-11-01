# deep_policy.py — deep_t4: 策略合规地图预计算引擎(H域认识论安全/K6披露策略题·2026-10-10)。
# 法证根因(mock K6全失·终端摸底mock_exam_90): 语料=文档卡(条目N枚举·部分标签带"机密")+
# 独立披露策略卡("对外问答一律禁止披露条目N…"/EN"Do not disclose the first listed threat"),
# 评审自己解析策略并定位条目失败(A型禁题没拒答/B型邻题误拒)。治=ADD相把文档+策略预计算成
# 合规地图卡(COMPLIANCE MAP·kind=pmap·每文档1行·策略卡后到→策略簿重算对冲), SEARCH相按问句
# 条目号/威胁序数直给判定卡(A型·插位1)或地图卡+原文档卡(B型·插位2)。纪律: 纯stdlib·零LLM·
# 零网络·确定性·fail-soft(异常=原窗直通)·无机密文档零产卡·rank0永不动·注入≤2张·DB复用cc._db()同锁。
# 回滚: 摘本件+patch_t4.py逆还原(.bak-t4)。
import hashlib
import logging
import re
from typing import Any

log = logging.getLogger("deep_policy")
_ENTRY_HEAD_RE = re.compile(r"^[ \t]*(?:[-*•>]*)?[ \t]*(?:条目|Item|Entry)[ \t]*(\d{1,2})[ \t]*[:：（(]", re.M | re.I)
_CONF_RE = re.compile(r"机密|保密|内部|confidential|internal[- ]only|do not disclose|not disclose|不得披露|不予披露|禁止披露", re.I)
_POLICY_RE = re.compile(r"禁止披露|不得披露|不予披露|do not disclose|not to disclose|机密|保密|confidential", re.I)
_LOCATE_RE = re.compile(r"条目\s*\d|item\s*\d|entry\s*\d|threat", re.I)  # 免闲聊误入
_ENTRY_REF_RE = re.compile(r"条目\s*(\d{1,2})|item\s*(\d{1,2})|entry\s*(\d{1,2})", re.I)
_THREAT_ORD_RE = re.compile(r"\b(first|1st|second|2nd|third|3rd)\s+(?:listed\s+)?threat\b", re.I)
_THREAT_RE = re.compile(r"threat", re.I); _SENT_SPLIT = re.compile(r"(?<=[.!?。！？;；])\s+"); _ORD = {"first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3}
_PM_CACHE: dict[str, list] = {}; _PL_CACHE: dict[str, list] = {}; _TBL = [False]  # 写时失效

def _sha(s: str) -> str: return hashlib.sha1(str(s or "").encode("utf-8")).hexdigest()[:16]  # 卡/文档id同源
def _sum(s: str, n: int = 24) -> str: return re.sub(r"\s+", " ", str(s or "")).strip()[:n]  # 条目摘要(标签后留内容词)

def _db():
    """DB复用card_construct._db()同连接同锁(延迟import防循环)·首调建表幂等; 主键(tenant,docid)=登记/重算幂等OR REPLACE。"""
    import card_construct as cc
    conn, lock = cc._db(), cc._DB_LOCK
    with lock:
        if not _TBL[0]:
            conn.execute("CREATE TABLE IF NOT EXISTS policy_maps(tenant TEXT NOT NULL, docid TEXT NOT NULL, policy_text TEXT"
                         " NOT NULL, forbidden_items TEXT NOT NULL, allowed_items TEXT NOT NULL, map_text TEXT NOT NULL,"
                         " doc_text TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY(tenant,docid))")
            conn.execute("CREATE TABLE IF NOT EXISTS policy_lines(tenant TEXT NOT NULL, ptext TEXT NOT NULL, seq INTEGER NOT"
                         " NULL, PRIMARY KEY(tenant,ptext))")
            conn.commit(); _TBL[0] = True
    return conn, lock

def _split_entries(text: str) -> list[tuple[int, str]]:
    """行首枚举头切条目→[(条目号, 体)](体含标签段·文档序·确定性)。"""
    t, hs = str(text or ""), list(_ENTRY_HEAD_RE.finditer(str(text or "")))
    return [(int(m.group(1)), t[m.end():(hs[i + 1].start() if i + 1 < len(hs) else len(t))].strip())
            for i, m in enumerate(hs)]

def _policy_lines(text: str) -> list[str]:
    """策略行=同时含披露策略词与定位词(条目N/threat)的行(ptext序·确定性)。"""
    return sorted({ln.strip() for ln in str(text or "").splitlines() if ln.strip() and _POLICY_RE.search(ln) and _LOCATE_RE.search(ln)})

def _load_lines(tenant: str) -> list[str]:
    """租户策略簿(内存缓存·写时失效·fail-soft→[])。"""
    try:
        if tenant not in _PL_CACHE:
            conn, lock = _db()
            with lock:
                rows = conn.execute("SELECT ptext FROM policy_lines WHERE tenant=? ORDER BY ptext",
                                    (tenant,)).fetchall()
            _PL_CACHE[tenant] = [r[0] for r in rows]
        return _PL_CACHE[tenant]
    except Exception:  # noqa: BLE001
        return []

def _entry_refs(line: str) -> set[int]: return {int(m.group(1) or m.group(2) or m.group(3)) for m in _ENTRY_REF_RE.finditer(str(line or ""))}

def _threat_units(entries: list[tuple[int, str]]) -> tuple[int, list[str]]:
    """首个含threat的条目→(条目号, 句元清单)(≥12字·跳裸标签头)。"""
    for n, b in entries:
        if _THREAT_RE.search(b): return n, [s for s in _SENT_SPLIT.split(b) if len(s.strip()) >= 12]
    return 0, []

def build_map(doc_text: str, policies: list[str]) -> dict[str, Any] | None:
    """纯函数·确定性: 文档+策略行→地图; <2条目或零受限→None(零噪音)。受限三源=①条目标签/体内
    标注 ②策略行条目号 ③威胁序数(first listed threat→第k句元禁); fail-closed: 机密标注条目必禁。"""
    entries = _split_entries(doc_text)
    if len(entries) < 2:
        return None
    nums, body, pols = [n for n, _ in entries], dict(entries), sorted({p for p in policies if p})
    forb = {n for n, b in entries if _CONF_RE.search(b)}
    forb |= {n for pl in pols for n in _entry_refs(pl) if n in body}
    tn, tu = _threat_units(entries)
    units = {(tn, _ORD[om.group(1).lower()]) for pl in pols for om in [_THREAT_ORD_RE.search(pl)] if om and tn}
    if not forb and not units:
        return None
    parts_f: list[str] = []; parts_a: list[str] = []
    for n in nums:
        if n in forb:
            parts_f.append(f"条目{n}({_sum(body[n])})")
            continue
        us = sorted(k for en, k in units if en == n)
        if not us:
            parts_a.append(f"条目{n}({_sum(body[n])})")
            continue
        parts_f += [f"条目{n}·威胁{k}({_sum(tu[k - 1])})" for k in us if k <= len(tu)]
        rest = [s for i, s in enumerate(tu) if (i + 1) not in us]
        if rest:
            parts_a.append(f"条目{n}(威胁外:{_sum(' '.join(rest))})")
    mt = f"[合规地图] : FORBIDDEN={';'.join(parts_f)}; ALLOWED={'·'.join(parts_a)}; POLICY={' / '.join(pols)[:40]}"[:700]
    return {"docid": _sha(doc_text), "forbidden": forb, "units": units, "tu_n": len(tu), "allowed": set(nums) - forb - {en for en, _ in units}, "map_text": mt, "policy": ' / '.join(pols)[:40]}

def _save_doc(tenant: str, doc_text: str, card: dict[str, Any]) -> dict[str, Any] | None:
    """文档卡→建图落库(OR REPLACE幂等·每文档1行; 无图=空图登记行·等后到策略重算)。"""
    mp = build_map(doc_text, _policy_lines(doc_text) + _load_lines(tenant))
    if not tenant:
        return mp
    row = ((mp["policy"], ",".join([str(n) for n in sorted(mp["forbidden"])] + [f"{e}:{k}" for e, k in sorted(mp["units"])]),
            ",".join(str(n) for n in sorted(mp["allowed"])), mp["map_text"]) if mp else ("", "", "", ""))
    conn, lock = _db()
    with lock:
        conn.execute("INSERT OR REPLACE INTO policy_maps(tenant,docid,policy_text,forbidden_items,allowed_items,"
                     "map_text,doc_text,seq) VALUES(?,?,?,?,?,?,?,?)",
                     (tenant, _sha(doc_text), *row, doc_text, int(mp["docid"][:8], 16) if mp else 0))
        conn.commit()
    _PM_CACHE.pop(tenant, None)
    return mp

def policy_cards(card: dict[str, Any], rid: str, idx: int, tenant: str = "") -> list[dict[str, Any]]:
    """ADD相钩子(derive_all每卡调用·fail-soft→[]): 文档卡(≥2条目)→地图卡(kind=pmap)·无机密→零产卡;
    独立策略卡→入租户策略簿+重算已登记文档(策略后到乱序对冲)。"""
    try:
        text = str(card.get("text") or "")
        if len(_split_entries(text)) >= 2:  # 文档卡
            mp = _save_doc(tenant, text, card)
            return ([] if not mp else [{"sess": str(card.get("sess", "")), "dia": f"{rid}:{idx}:pm",
                                        "speaker": "policy", "text": mp["map_text"],
                                        "dt": str(card.get("dt", "")), "kind": "pmap", "docid": mp["docid"]}])
        pls = _policy_lines(text)  # 独立披露策略卡
        if not (pls and tenant):
            return []
        conn, lock = _db()
        with lock:
            n_new = sum(conn.execute("INSERT OR IGNORE INTO policy_lines(tenant,ptext,seq) VALUES(?,?,?)",
                                     (tenant, ln, i)).rowcount for i, ln in enumerate(pls, 1))
            docs = [r[0] for r in conn.execute("SELECT doc_text FROM policy_maps WHERE tenant=?",
                                               (tenant,)).fetchall()]
        _PL_CACHE.pop(tenant, None)
        if n_new:  # 策略簿有增量→重算登记文档(空图登记行就此转正)
            for dt in docs: _save_doc(tenant, str(dt), {"sess": "", "dt": ""})
        return []
    except Exception:  # noqa: BLE001 — 识别失败只丢地图·绝不影响add
        return []

def _load_maps(tenant: str) -> list[dict[str, Any]]:
    """租户地图集(内存缓存·写时失效·docid序=确定性·空图登记行不出席·fail-soft→[])。"""
    try:
        if tenant not in _PM_CACHE:
            conn, lock = _db()
            with lock:
                rows = conn.execute("SELECT docid,policy_text,forbidden_items,allowed_items,map_text,doc_text FROM"
                                    " policy_maps WHERE tenant=? ORDER BY docid", (tenant,)).fetchall()
            out = []
            for docid, pol, forb, alw, mt, dtxt in rows:
                if not mt:
                    continue
                toks = [t for t in str(forb).split(",") if t]
                out.append({"docid": docid, "policy": pol, "forbidden": {int(t) for t in toks if ":" not in t},
                            "units": {(int(t.split(":")[0]), int(t.split(":")[1])) for t in toks if ":" in t},
                            "allowed": {int(t) for t in str(alw).split(",") if t}, "map_text": mt, "doc_text": dtxt,
                            "tu_n": len(_threat_units(_split_entries(dtxt))[1])})
            _PM_CACHE[tenant] = out
        return _PM_CACHE[tenant]
    except Exception:  # noqa: BLE001
        return []

def _card3(text: str) -> dict[str, str]: return {"id": _sha(text), "content": text, "created_at": ""}  # 3键契约

def _query_target(query: str) -> tuple[str, int] | None:  # 条目N优先, 否则威胁序数; 都无→None
    q = str(query or "")
    m, om = _ENTRY_REF_RE.search(q), _THREAT_ORD_RE.search(q)
    return (("entry", int(m.group(1) or m.group(2) or m.group(3))) if m else (("threat", _ORD[om.group(1).lower()]) if om else None))

def _finish(out: list, top_k: int, n: int) -> tuple[list, int]:
    while len(out) > max(int(top_k or 0), 1): out.pop()  # 尾刀·rank0永不动
    return out, n  # n=实际注入张数(显式计数·窗差会被尾刀吃掉)

def inject(tenant: str, query: str, data: list[dict[str, Any]], top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相钩子(cfact.inject后·同过后续刀闸): A型(问受限条目/威胁·任一文档禁则拒=fail-closed)→判定
    卡插位1; B型(问其余条目/威胁)→地图卡插位2(≤2张·单图附原文档卡保窗); 无条目号/序数→零注入。确定性
    ·rank0不动·≤2张·fail-soft原窗直通。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        maps, tgt = _load_maps(tenant), _query_target(query)
        if not maps or tgt is None:
            return data, 0
        out, existing = list(data), {str(d.get("content", "")) for d in data}
        kind, val = tgt
        hit = next((m for m in maps if val in m["forbidden"]
                    or (kind == "threat" and any(k == val for _, k in m["units"]))), None)
        if hit is not None:  # A型禁题: 判定卡插位1(结论词双挂CN/EN)
            en = f"条目{val}" if kind == "entry" else f"条目{next(e for e, k in hit['units'] if k == val)}·威胁{val}"
            out.insert(min(1, len(out)), _card3(
                f"[合规] {en}为受限内容——不予披露/not disclosed (map: FORBIDDEN={en};"
                f" policy: {hit['policy'][:40]}; doc: {hit['docid'][:8]})"))
            return _finish(out, top_k, 1)
        cands = ([m for m in maps if val in m["allowed"]] if kind == "entry" else
                 [m for m in maps if m["tu_n"] >= val and not any(k == val for _, k in m["units"])])
        asked, n = (f"条目{val}" if kind == "entry" else f"威胁{val}"), 0
        for m in cands[:2]:  # B型邻题: 地图卡插位2(≤2张·已入窗不重复)
            if m["map_text"] not in existing:
                out.insert(min(2 + n, len(out)), _card3(m["map_text"] + f"; ASKED={asked}(ALLOWED·按原文档卡回答)"))
                existing.add(m["map_text"]); n += 1
        if n == 1 and cands[0]["doc_text"] and cands[0]["doc_text"] not in existing:
            out.append(_card3(cands[0]["doc_text"])); n += 1  # 单图: 原文档卡保窗(答句词面在窗)
        return _finish(out, top_k, n)
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_policy inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0

def purge_tenant(tenant: str) -> int:
    """测试专用: 清租户两表+缓存失效(fail-soft)。"""
    try:
        conn, lock = _db()
        n = sum(conn.execute(f"DELETE FROM {t} WHERE tenant=?", (tenant,)).rowcount
                for t in ("policy_maps", "policy_lines")); conn.commit()
        _PM_CACHE.pop(tenant, None); _PL_CACHE.pop(tenant, None)
        return n
    except Exception:  # noqa: BLE001
        return 0
