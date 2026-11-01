# corpus_fact.py — deep_jf: 官方JSON事实方言(Corpus/UPDATE)边车观察+搜索注入(2026-10-09)
# 法证根因(AML 48.38): 官方D1/B2/D2域用JSON事实方言(基础卡"Corpus: {subject,relation,object}",
# 更新卡"Corpus: UPDATE: replace the prior value ..."官方原话明示覆盖, 前缀形连续多[tag])。
# 病: deep_fact只讲NL文法→JSON卡零事实卡→新旧值混窗评审拿旧值(D1=0); 二跳中间实体
#     不在query词面→链卡永不进窗(B2=0)。
# 双钩子(card_construct边车同纪律: 零organ扰动·同过k2-k7刀闸·fail-soft·确定性):
#   observe=ADD相(derive_all尾): parse_corpus→jfacts; UPDATE无条件覆盖同键旧值(同值重述不覆盖);
#   inject=SEARCH相(overlay_inject后): current值卡词面匹配浮顶位1/2 + B2二跳链窗尾·总注入≤4。
# 回滚: 摘本件 + patch_jf.py四hunk逆还原(.bak-jf)。
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

log = logging.getLogger("corpus_fact")
_TAG_RUN_RE = re.compile(r"^(?:\[[^\]]{1,40}\]\s*)+")   # 连续多[tag]前缀(官方4连实证)
_UPDATE_RE = re.compile(r"UPDATE:\s*replace the prior value")
_DDL = ("CREATE TABLE IF NOT EXISTS jfacts(tenant TEXT NOT NULL, cid TEXT NOT NULL, sess TEXT,"
        " dia TEXT, dt TEXT, subject TEXT NOT NULL, relation TEXT NOT NULL, object TEXT NOT NULL,"
        " is_update INTEGER DEFAULT 0, superseded_by TEXT, seq INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jf_tcid ON jfacts(tenant, cid)")
_JF_CACHE: dict[str, list[dict[str, Any]]] = {}   # tenant→rows(写时失效)


def _balanced_json(s: str, start: int) -> int | None:
    """s[start:]起首个平衡{...}块的尾界(exclusive·串内花括号/转义免疫); 未闭合=None。"""
    depth, in_str, esc = 0, False, False
    for k in range(start, len(s)):
        ch = s[k]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return k + 1
    return None


def parse_corpus(text: Any) -> dict[str, Any] | None:
    """官方JSON事实方言解析: 剥连续[tag]→找"Corpus:"→UPDATE标记→平衡{}JSON块。
    →{"subject","relation","object","is_update"}; 缺任一字段/坏JSON/非方言=None。"""
    try:
        t = _TAG_RUN_RE.sub("", str(text or "").lstrip(), count=1)
        i = t.find("Corpus:")
        if i < 0:
            return None
        seg = t[i + 7:]
        b = seg.find("{")
        if b < 0:
            return None
        end = _balanced_json(seg, b)
        if end is None:
            return None
        obj = json.loads(seg[b:end])
        if not isinstance(obj, dict):
            return None
        subject = str(obj.get("subject", "")).strip()
        relation = str(obj.get("relation", "")).strip()
        obj_val = str(obj.get("object", "")).strip()
        if not (subject and relation and obj_val):
            return None
        return {"subject": subject, "relation": relation, "object": obj_val,
                "is_update": bool(_UPDATE_RE.search(seg[:b]))}
    except Exception:  # noqa: BLE001 — 方言解析fail-soft: 脏卡=非方言
        return None


def _ensure(conn: Any) -> None:
    for stmt in _DDL:
        conn.execute(stmt)


def _cid_of(tenant: str, subject: str, relation: str, obj: str, dt: str) -> str:
    return hashlib.sha1(
        f"{tenant}|{subject.lower()}|{relation.lower()}|{obj}|{dt}".encode("utf-8")
    ).hexdigest()[:16]


def observe(tenant: str, cards: list[dict[str, Any]], rid: str) -> int:
    """ADD相钩子(derive_all尾·fail-soft): 原文卡→parse_corpus→jfacts落库(幂等)。
    UPDATE语义(官方原话"replace the prior value")=无条件覆盖同键现值; 同值=重述不覆盖。
    DB复用card_construct._db()同连接同锁(延迟import防循环)。"""
    if not tenant or not cards:
        return 0
    rows = []
    for c in cards:
        if not isinstance(c, dict):
            continue
        p = parse_corpus(c.get("text", ""))
        if not p:
            continue
        dt = str(c.get("dt", "") or "")
        rows.append((tenant, _cid_of(tenant, p["subject"], p["relation"], p["object"], dt),
                     str(c.get("sess", "") or ""), str(c.get("dia", "") or ""), dt,
                     p["subject"], p["relation"], p["object"], 1 if p["is_update"] else 0))
    if not rows:
        return 0
    try:
        import card_construct as cc  # 延迟import防循环(复用_db连接/_DB_LOCK)
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            for r in rows:
                conn.execute(
                    "INSERT OR IGNORE INTO jfacts"
                    "(tenant,cid,sess,dia,dt,subject,relation,object,is_update,superseded_by)"
                    " VALUES(?,?,?,?,?,?,?,?,?,NULL)", r)
                if r[8]:  # UPDATE行: 同键现值全部让位(唯一键OR IGNORE=重放幂等·不复活旧值)
                    conn.execute(
                        "UPDATE jfacts SET superseded_by=? WHERE tenant=? AND lower(subject)=?"
                        " AND lower(relation)=? AND cid!=? AND superseded_by IS NULL"
                        " AND lower(object)!=lower(?)",
                        (r[1], tenant, r[5].lower(), r[6].lower(), r[1], r[7]))
            conn.commit()
        _JF_CACHE.pop(tenant, None)
    except Exception as exc:  # noqa: BLE001 — 观察失败只丢方言卡·绝不影响add
        log.error("corpus_fact observe FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0
    return len(rows)


def _load(tenant: str) -> list[dict[str, Any]]:
    """读租户jfacts(内存缓存·写时失效·fail-soft→[])。"""
    cached = _JF_CACHE.get(tenant)
    if cached is not None:
        return cached
    import card_construct as cc  # 延迟import防循环
    with cc._DB_LOCK:
        conn = cc._db()
        _ensure(conn)
        raw = conn.execute(
            "SELECT cid,subject,relation,object,is_update,superseded_by,dt,seq"
            " FROM jfacts WHERE tenant=? ORDER BY seq", (tenant,)).fetchall()
    rows = [{"cid": r[0], "subject": r[1], "relation": r[2], "object": r[3],
             "is_update": r[4], "superseded_by": r[5], "dt": r[6] or "", "seq": r[7]}
            for r in raw]
    _JF_CACHE[tenant] = rows
    return rows


def _card3(text: str, dt: str) -> dict[str, str]:
    return {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
            "content": text, "created_at": dt or ""}


def _chain_card(cur: list[dict[str, Any]], out: list[dict[str, Any]],
                cards: list[dict[str, Any]],
                existing: set[str]) -> tuple[str, str] | None:
    """B2二跳链(最多1张·确定性): 窗内jfact(注入卡带row/organ原窗JSON卡content含
    "subject"→parse_corpus)的object→subject=该object的current jfact→链卡
    "FACT: {obj} | {rel2} = {obj2} (2-hop via {首跳subject})"。"""
    row_by_text = {c["text"]: c["row"] for c in cards}
    for d in out:
        content = str(d.get("content", ""))
        r = row_by_text.get(content)
        if r is not None:
            src_s, src_o, src_cid = r["subject"], r["object"], r["cid"]
        elif '"subject"' in content:
            p = parse_corpus(content)
            if not p:
                continue
            src_s, src_o, src_cid = p["subject"], p["object"], None
        else:
            continue
        low = src_o.lower()
        for t in cur:
            if t["subject"].lower() != low or (src_cid is not None and t["cid"] == src_cid):
                continue
            ctext = f"FACT: {src_o} | {t['relation']} = {t['object']} (2-hop via {src_s})"
            # chain3优先: 3跳可用且未在窗→出3跳; 否则回落2跳
            low3 = t["object"].lower()
            for t3 in cur:
                if t3["subject"].lower() != low3 or t3["cid"] in (t["cid"], src_cid):
                    continue
                ctext3 = (f"FACT: {t3['subject']} | {t3['relation']} = {t3['object']} "
                          f"(3-hop via {src_s}->{t['subject']})")
                if ctext3 not in existing:
                    return ctext3, t3["dt"]
            if ctext not in existing:
                return ctext, t["dt"]
    return None


def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相钩子(overlay_inject之后·同过k2-k7刀闸): current值卡词面匹配浮顶
    (最强插位1·次强位2·rank0不动·updated优先) + B2二跳链卡窗尾。去重(已在窗跳过)
    ·总注入≤4·确定性(同入参同输出)·fail-soft(异常=原窗直通)。不裁窗: ≤top_k契约
    由aml_service尾刀data[:top_k]执法(浮顶保位·溢出先切链卡)。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        import card_construct as cc  # 延迟import防循环(复用_tokens)
        rows = _load(tenant)
        cur = [r for r in rows if not r["superseded_by"]]
        if not cur:
            return data, 0
        sup_old: dict[str, str] = {}
        for r in rows:  # seq升序→后写胜: 被覆盖旧值取最新一条
            if r["superseded_by"]:
                sup_old[r["superseded_by"]] = r["object"]
        cards = []
        for r in cur:
            text = f"FACT: {r['subject']} | {r['relation']} = {r['object']}"
            old = sup_old.get(r["cid"])
            if old and old != r["object"]:
                text += f" (UPDATED: replaces {old})"
            if r["is_update"]:
                text += " [official update directive]"
            cards.append({"row": r, "text": text})
        # deep_jf-fix(P11d): organ窗内过期raw Corpus卡剔除 — 其(subject,relation)键
        # 已被current值覆盖且object=旧值→默认(current)意图下移出窗(history意图保留)。
        # 官方评审对rank0偏重, 旧值占rank0=D1翻车源。
        try:
            _q = str(query or "").lower()
            import re as _re
            _hist = any(w in _q for w in ("ever", "used to", "before", "history", "曾经", "以前", "之前")) or bool(
                _re.search(r"(?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}|as\s+of|(?:截至|在)\s*\d{4}年", _q, _re.I))
            if not _hist and cur:
                _curk = {}
                for r in cur:
                    _curk[(r["subject"].lower(), r["relation"].lower())] = r["object"]
                _stale = []
                for _di, _d in enumerate(data):
                    _ct = str(_d.get("content", ""))
                    if '"subject"' not in _ct:
                        continue
                    _p = parse_corpus(_ct)
                    if not _p or _p["is_update"]:
                        continue
                    _ko = (_p["subject"].lower(), _p["relation"].lower())
                    if _ko in _curk and _p["object"].lower() != _curk[_ko].lower():
                        _stale.append(_di)
                for _di in reversed(_stale):
                    data.pop(_di)
                if _stale:
                    log.info("corpus_fact stale-drop tenant=%s dropped=%d", tenant, len(_stale))
        except Exception:
            pass
        existing = {str(d.get("content", "")) for d in data}
        picks: list[tuple[int, int, int, dict[str, Any]]] = []
        qt = cc._tokens(query)
        if qt and len(qt) <= 12:  # 词面匹配: subject×2+relation+object·命中≥1·取前2
            for c in cards:
                if c["text"] in existing:
                    continue
                r = c["row"]
                hits = (2 * len(qt & cc._tokens(r["subject"]))
                        + len(qt & cc._tokens(r["relation"]))
                        + len(qt & cc._tokens(r["object"])))
                if hits >= 1:
                    picks.append((hits, r["is_update"], r["seq"], c))
            picks.sort(key=lambda x: (-x[0], -x[1], x[2]))
            picks = picks[:2]
        out = list(data)
        n = 0
        pos = 1
        for _, _, _, c in sorted(picks, key=lambda x: (-x[1], -x[0], x[2])):  # updated优先
            out.insert(min(pos, len(out)), _card3(c["text"], c["row"]["dt"]))
            existing.add(c["text"])
            pos += 1
            n += 1
        if n < 4:
            chain = _chain_card(cur, out, cards, existing)
            if chain:
                out.insert(min(3, len(out)), _card3(chain[0], chain[1]))
                n += 1
        return out, n
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("corpus_fact inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0


def purge_tenant(tenant: str) -> int:
    """测试专用: 清租户jfacts+缓存失效。"""
    try:
        import card_construct as cc  # 延迟import防循环
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            cur = conn.execute("DELETE FROM jfacts WHERE tenant=?", (tenant,))
            conn.commit()
        _JF_CACHE.pop(tenant, None)
        return cur.rowcount
    except Exception:  # noqa: BLE001
        return 0
