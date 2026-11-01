# deep_closure.py v2 — deep_tb: 链闭包**步骤卡组**引擎(B2多跳·每跳一张STEP卡+ANSWER终值卡)
# 法证根因(AML B2=0两连铁证): v1终值卡("[关联链] r3 of [r2 of [r1 of X]] = V")已注入
# (t2日志injected=1 kept=10)而B2仍0分 — H1嵌套括号表达式评审LLM读不出"这就是答案";
# H2官方子能力名"中间步骤恢复"=金标要逐跳步骤·仅终值不满足; H3同题多措辞仅起点锚匹配漏配。
# v2: 每条闭包路径出步骤卡组(STEP k/N: src --rel--> dst平面箭头·逐跳可读)+ANSWER卡
# (零嵌套·终值等号右侧直读)·UPDATE陷阱标注在对应STEP边(post-update, replaced X)
# ·锚放宽(词面与路径任一实体/关系交≥1即候选)·整组插位1..N+1·总注入≤5。
# 答案层标准: ANSWER卡必须含终值实体词·STEP卡必须逐跳可读。
# 纪律(同v1): 纯stdlib·确定性(同入参同输出)·fail-soft(异常=原窗直通)·零LLM·零题目硬编码。
# 自管表closures(+grp组列·ALTER幂等迁移·与jfacts同DB同锁·不进derived分类)。
# 回滚: 摘本件 + patch_tb.py逆还原(.bak-tb)。
from __future__ import annotations

import hashlib
import logging
from typing import Any

log = logging.getLogger("deep_closure")
_MAX_CARDS = 40   # 每租户闭包**组**上限(确定性: 按ANSWER文本字典序截)
_MAX_EDGES = 4    # 单路径边数上限(1基边+3延跳·防爆炸)
_MAX_INJECT = 5   # 单次注入总卡数硬门(最深4跳组=5卡恰满·①三跳4卡②两跳3卡均<门)
_DDL = ("CREATE TABLE IF NOT EXISTS closures(tenant TEXT NOT NULL, cid TEXT NOT NULL,"
        " anchor TEXT NOT NULL, hops INTEGER NOT NULL, grp TEXT NOT NULL,"
        " text TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY(tenant, cid))",)
_CL_CACHE: dict[str, list[dict[str, Any]]] = {}   # tenant→rows(重建时失效)


def _ensure(conn: Any) -> None:
    for stmt in _DDL:
        conn.execute(stmt)
    try:   # 遗留v1库(无grp列)幂等迁移; 已有列=重复列异常吞(fail-soft)
        conn.execute("ALTER TABLE closures ADD COLUMN grp TEXT NOT NULL DEFAULT ''")
    except Exception:  # noqa: BLE001
        pass


def _card3(text: str) -> dict[str, str]:
    """3键注入卡契约(与corpus_fact._card3同形): id/content/created_at。"""
    return {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
            "content": text, "created_at": ""}


def _graph(tenant: str) -> tuple[dict[str, list[tuple[str, str, str, bool, str]]],
                                 dict[str, str]]:
    """jfacts→有向图: 节点=实体lower·边=relation(有向)·只取current值边
    (superseded行出局·同键多行seq后写胜=与cfact._curk同规)。UPDATE语义: 边值
    自动=新值(current), post_update=is_update行 或 该cid曾supersede过旧行,
    old=被覆盖旧值词面(STEP卡"replaced X"标注用·查不到=空)。
    返回(adj, disp): adj[u]=[(rel, v_low, v_disp, post, old)]字典序; disp[low]=原词面。"""
    import corpus_fact as cfact   # 延迟import防循环(复用jfacts表与缓存)
    rows = cfact._load(tenant)
    old_of: dict[str, str] = {}   # 新cid→被覆盖旧值(seq升序后写胜=最新覆盖)
    cur: dict[tuple[str, str], dict[str, Any]] = {}
    disp: dict[str, str] = {}
    for r in rows:   # seq升序→后写胜
        disp[r["subject"].lower()] = r["subject"]
        disp[r["object"].lower()] = r["object"]
        if r["superseded_by"]:
            old_of[r["superseded_by"]] = r["object"]
        if not r["superseded_by"]:
            cur[(r["subject"].lower(), r["relation"].lower())] = r
    adj: dict[str, list[tuple[str, str, str, bool, str]]] = {}
    for (s, _rel), r in cur.items():
        old = old_of.get(r["cid"], "")
        post = bool(r["is_update"]) or bool(old)
        adj.setdefault(s, []).append(
            (r["relation"], r["object"].lower(), r["object"], post, old))
    for k in adj:
        adj[k].sort(key=lambda e: (e[0].lower(), e[1]))
    return adj, disp


def _walk(adj: dict[str, list[tuple[str, str, str, bool]]],
          path: list[tuple[str, str, str, bool]],
          seen: set[str],
          out: list[list[tuple[str, str, str, bool]]]) -> None:
    """DFS延长路径(2..4边·visited集环检测: A->B->A被seen拦·不无限)。
    2边与3边子路径同样出卡(问题嵌套形状"r2 of [r1 of X]"逐深覆盖)。"""
    if len(path) >= 2:
        out.append(list(path))
    if len(path) >= _MAX_EDGES:
        return
    for rel, v, vd, post, old in adj.get(path[-1][2], ()):
        if v in seen:
            continue
        seen.add(v)
        path.append((path[-1][2], rel, v, post, old))
        _walk(adj, path, seen, out)
        path.pop()
        seen.discard(v)


def _group_of(path: list[tuple[str, str, str, bool, str]],
              disp: dict[str, str]) -> list[str]:
    """路径→步骤卡组文本(v2核心·每跳一张STEP卡+末张ANSWER直读终值):
    STEP 1/3: Portal --creator--> Valve Corporation
    STEP 2/3: Valve Corporation --chief executive officer--> Gabe N (post-update, replaced OldX)
    STEP 3/3: Gabe N --country of citizenship--> India
    ANSWER: Portal via creator -> chief executive officer -> country of citizenship = India
            (chain: Portal-creator->Valve; ...; 3-hop; post-update)
    全平面零嵌套括号(H1评审友好·评审逐跳读完即见终值=H2中间步骤恢复字面满足);
    UPDATE陷阱只标注在对应STEP边(新值可追·旧值可见其被替换)。"""
    n = len(path)
    cards: list[str] = []
    for k, (a, rel, b, post, old) in enumerate(path, 1):
        line = f"[链路] {k}/{n}: {disp.get(a, a)} --{rel}--> {disp.get(b, b)}"
        if post:
            line += (f" (post-update, replaced {old})" if old else " (post-update)")
        cards.append(line)
    rels = " -> ".join(rel for _a, rel, _b, _p, _o in path)
    chain = "; ".join(f"{disp.get(a, a)}-{r}->{disp.get(b, b)}"
                      for a, r, b, _p, _o in path)
    start = disp.get(path[0][0], path[0][0])
    final = disp.get(path[-1][2], path[-1][2])
    post_any = "; post-update" if any(p for _a, _r, _b, p, _o in path) else ""
    cards.append(f"[关联链·终] {start} via {rels} = {final} (chain: {chain}; {n}-hop{post_any})")
    return cards


def closure_cards(tenant: str, rid: str) -> list[dict[str, str]]:
    """ADD相钩子(derive_all尾·cfact.observe之后·fail-soft): 租户jfact图全源闭包
    (每边为基·沿图延长2..4边·环检测)→每条闭包路径出**步骤卡组**(每跳一张STEP卡
    +一张ANSWER卡·组id=grp共享·组内seq序=STEP 1..N→ANSWER末位)≤40组(按ANSWER
    文本字典序截)→自管表closures全量重建(DELETE+INSERT: UPDATE后旧闭包卡随重建
    失效·边值恒=current)。cid=组域哈希(同文本STEP卡跨组不撞PK)·注入层仍按文本去重。
    幂等: 同图同卡集(确定性·重放安全)。"""
    if not tenant:
        return []
    try:
        import card_construct as cc   # 延迟import防循环(复用_db连接/_DB_LOCK)
        adj, disp = _graph(tenant)
        paths: list[list[tuple[str, str, str, bool, str]]] = []
        for u in sorted(adj):
            for rel, v, vd, post, old in adj[u]:
                _walk(adj, [(u, rel, v, post, old)], {u, v}, paths)
        groups: dict[str, tuple[str, list[str]]] = {}   # ANSWER文本→(锚·卡组)·路径去重
        for p in paths:
            g = _group_of(p, disp)
            groups.setdefault(g[-1], (p[0][0], g))
        picked = sorted(groups.items(), key=lambda kv: kv[0])[:_MAX_CARDS]
        rows: list[tuple[str, str, int, str, str, int]] = []
        seq = 0
        for ans, (anchor, g) in picked:
            gid = hashlib.sha1(ans.encode("utf-8")).hexdigest()[:16]
            for t in g:   # 组内序: STEP 1..N→ANSWER末位(seq保证注入有序)
                rows.append((hashlib.sha1(f"{gid}|{t}".encode("utf-8")).hexdigest()[:16],
                             anchor, len(g) - 1, gid, t, seq))
                seq += 1
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            conn.execute("DELETE FROM closures WHERE tenant=?", (tenant,))
            for cid, anchor, hops, gid, text, sq in rows:
                conn.execute("INSERT OR REPLACE INTO closures"
                             "(tenant,cid,anchor,hops,grp,text,seq) VALUES(?,?,?,?,?,?,?)",
                             (tenant, cid, anchor, hops, gid, text, sq))
            conn.commit()
        _CL_CACHE[tenant] = [{"cid": c, "anchor": a, "hops": h, "grp": g2, "text": t}
                             for c, a, h, g2, t, _s in rows]
        if rows:
            log.info("deep_closure build tenant=%s rid=%s groups=%d cards=%d",
                     tenant, rid, len(picked), len(rows))
        return [_card3(t) for _c, _a, _h, _g2, t, _s in rows]
    except Exception as exc:  # noqa: BLE001 — 预计算失败只丢闭包卡·绝不影响add
        log.error("deep_closure build FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return []


def _load(tenant: str) -> list[dict[str, Any]]:
    """读租户闭包卡组(内存缓存·重建失效·fail-soft→[])。seq序=组字典序+组内
    STEP 1..N→ANSWER序(构建序·注入按grp聚合后组内保序)。"""
    cached = _CL_CACHE.get(tenant)
    if cached is not None:
        return cached
    try:
        import card_construct as cc   # 延迟import防循环
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            raw = conn.execute("SELECT cid,anchor,hops,grp,text FROM closures"
                               " WHERE tenant=? ORDER BY seq", (tenant,)).fetchall()
        rows = [{"cid": r[0], "anchor": r[1], "hops": r[2], "grp": r[3], "text": r[4]}
                for r in raw]
    except Exception as exc:  # noqa: BLE001 — 读失败=零注入
        log.error("deep_closure load FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return []
    _CL_CACHE[tenant] = rows
    return rows


def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int) -> tuple[list[dict[str, Any]], int]:
    """SEARCH相钩子(cfact.inject之后串联·fail-soft原窗直通): **步骤卡组整组注入** —
    组内按seq序(STEP 1..N→ANSWER末位)插位1..N+1(rank0不动·评审沿卡组逐跳读完推理
    链即见终值); 组间按锚匹配度排序。锚放宽(H3多问式): query词面与卡组路径上任一
    实体/关系词面交≥1即候选(ANSWER卡chain含全路径词面=天然匹配面)。整组注入
    (断链=半截推理·宁缺毋滥)·总注入≤5·去重(已在窗跳过)·确定性(同入参同输出)。
    不裁窗: ≤top_k契约由aml_service尾刀data[:top_k]执法(浮顶保位)。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        import card_construct as cc   # 延迟import防循环(复用_tokens)
        rows = _load(tenant)
        if not rows:
            return data, 0
        qt = cc._tokens(query)
        if not qt:
            return data, 0
        groups: dict[str, list[dict[str, Any]]] = {}   # grp→卡组(seq序·组内有序)
        score: dict[str, int] = {}
        for r in rows:   # 锚放宽: 组内任一卡词面交≥1即候选·组分=最大命中数
            groups.setdefault(r["grp"], []).append(r)
            hit = len(qt & cc._tokens(r["text"]))
            if hit > score.get(r["grp"], 0):
                score[r["grp"]] = hit
        cands = sorted((-score[g], g) for g in groups if score.get(g, 0) >= 1)
        out = list(data)
        existing = {str(d.get("content", "")) for d in data}
        budget = _MAX_INJECT
        pos = 1
        for _neg, g in cands:   # 组间: 匹配度降序·grp字典序稳定
            cards = groups[g]
            if len(cards) > budget:
                continue   # 整组注入·预算不够跳过该组(零半截卡)
            placed = 0
            for r in cards:   # 组内: STEP 1..N→ANSWER末位·插位递增1..N+1
                if r["text"] in existing:
                    continue   # 已在窗(organ/cfact命中)零重复
                out.insert(min(pos, len(out)), _card3(r["text"]))
                existing.add(r["text"])
                pos += 1
                placed += 1
                budget -= 1
            if placed:
                log.info("deep_closure inject group=%s placed=%d budget=%d",
                         g, placed, budget)
            if budget <= 0:
                break
        return out, _MAX_INJECT - budget
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_closure inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0


def purge_tenant(tenant: str) -> int:
    """测试专用: 清租户closures+缓存失效。"""
    try:
        import card_construct as cc   # 延迟import防循环
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            cur = conn.execute("DELETE FROM closures WHERE tenant=?", (tenant,))
            conn.commit()
        _CL_CACHE.pop(tenant, None)
        return cur.rowcount
    except Exception:  # noqa: BLE001
        return 0
