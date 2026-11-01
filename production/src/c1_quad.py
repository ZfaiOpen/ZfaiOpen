# c1_quad.py — C1四路混合检索最小版(梯队二C1执行席·20261014)
# ═════════════════════════════════════════════════════════════════════════════
# 战役依据: 梯队二C1任务书 — Graphiti全KG是重炮, 本件是轻机枪: 语义(现有embed)+
# 词面(现有锚) + 实体词典(新) + 时间索引(新), 线性融合重排, 全SQLite零新依赖。
# W2-1床尺: 证据进窗率84%(armC2)·temporal M2 10/11·M1 5/90 — M1卡"题面↔原文词面
# 检索缺口"(W2-1定谳刀4靶面), 本件即该靶面的最小实现: 实体/时间两路新召回源把
# organ语义窗外的gold原文卡与含时间词面卡拉回截断线前竞争。
#
# 四路定义(线性融合):
#   路1 语义 = 现有embed序(organ+k1..k8排序结果的位次先验 rank_prior=(N-i)/N)
#   路2 词面 = query实词轻词干与卡面重叠率(含W2-1 fact-time面/k3锚注的日期词面)
#   路3 实体 = 实体词典命中(query实体→卡文出现; 词典=ingestion/启动回填自消息构建:
#          行首说话人名/大写词序列(人名地名)/称呼别名(call me X等)/W2-3事件名词面)
#   路4 时间 = query时间表达(time_norm复用解析+w2tl显式日期)区间命中; 无区间时
#          退化为租户日内新近分位(bisect·dt来自W2-1透传修复后的卡dt字段)
#   score = W1·语义 + W2·词面 + W3·实体 + W4·时间 — env权重可调, 总闸缺省关。
#
# 纪律(对表w2_timeline/knife家规):
#   总闸 ZFAIOS_FUSION(缺省0=关): 关=observe零写库·backfill跳过·rerank字节级直通。
#   确定性: 同输入同输出(无now()/随机; time_norm锚=租户max day非墙钟); 稳定排序
#   (同分保原位次)。rank0不动(ZFAIOS_FUSION_KEEP_TOP1缺省1·对表w2tl/k8纪律)。
#   合规: 实体/时间两路新召回卡同过k2.filter_gate同口径隐私闸(BRAINOS_K2GATE=1时)
#   =零旁路; 卡身份=sha1(sess|dt|text)[:16]与_evidence_id/k2.card_id_of同源同值。
#   fail-soft: 单实体/单会话/单查询失败只丢该路, 绝不影响add/search成功语义。
#   存储: SQLite WAL单件(ent_occ/tm_idx/ent_alias/meta四表·env ZFAIOS_C1_DB可指),
#   索引列齐(tenant,ent)/(tenant,day), search相零全表扫(词典posting+区间查询)。
# 回滚: env ZFAIOS_FUSION=0(即时软关·零行为) + 摘aml_service三处hunk与import
#       + 删本件与time_norm.py副本(逐字节拷贝自/www/locomo50/time_norm.py·md5
#       bcb0212a87918beb0b01324605c9a720) + 删c1_quad.db(可留档)。测试副本专用件,
#       生产8090零触碰。
# ═════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import bisect
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path as _P
from typing import Any

import card_construct as _cc
import time_norm as _tn  # W2-3归一器副本(实体面=事件名词表·相对时间解析复用)

log = logging.getLogger("c1_quad")

_DAYS_IN_MONTH = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
_MONTHS_SET = {"january", "february", "march", "april", "may", "june", "july",
               "august", "september", "october", "november", "december"}
_WEEKDAYS_SET = {"monday", "tuesday", "wednesday", "thursday", "friday",
                 "saturday", "sunday"}
# query/抽取侧泛用大写词黑名单(称谓/疑问词/通用开场——防词典噪声·零题目先验)
_GENERIC_CAPS = {"the", "and", "but", "hey", "hi", "oh", "well", "yes", "no",
                 "okay", "wow", "so", "or", "if", "it", "in", "on", "at", "my",
                 "we", "he", "she", "you", "they", "this", "that", "what",
                 "when", "where", "who", "how", "why", "can", "will", "did",
                 "do", "does", "is", "was", "are", "were", "have", "has", "had",
                 "let", "please", "thanks", "thank", "good", "great", "yeah",
                 "yep", "nope", "not", "just", "really", "maybe", "sure", "ok",
                 "there", "then", "also", "about", "with", "for", "from"}
# W2-3事件名词面(实体词典的event类·源自time_norm._EVENT_NOUNS同族)
_EVENT_NOUNS = ("birthday", "wedding", "anniversary", "graduation", "funeral",
                "engagement", "housewarming")
_ALIAS_RES = (re.compile(r"\bcall(?:s|ed)?\s+(?:me|myself|herself|himself)\s+"
                         r"([A-Z][a-z]{2,})"),
              re.compile(r"\b(?:aka|a\.k\.a\.|nickname|nicknamed|goes by)\s+"
                         r"([A-Z][a-z]{2,})"))
_STOP = set("""a an the of in on at to for with and or is are was were be been
being do does did have has had i you he she it we they me him her them my your
his their its this that these those about from as by into up down out over
under again then once here there when where why how all any both each few more
most other some such no nor not only own same so than too very can will just
dont should now what which who whom whose did doing get got go went come came""".split())


def ON() -> bool:
    """总闸: env ZFAIOS_FUSION(缺省0=关·关=observe零写·rerank字节直通)。"""
    return os.environ.get("ZFAIOS_FUSION", "0") == "1"


def _cap(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(f"ZFAIOS_FUSION_{name}", str(default))))
    except (TypeError, ValueError):
        return default


def _w(name: str, default: float) -> float:
    try:
        return float(os.environ.get(f"ZFAIOS_FUSION_{name}", str(default)))
    except (TypeError, ValueError):
        return default


def _stem(w: str) -> str:
    """轻词干(与locomo50_measure同法·词面路)"""
    for suf in ("ing", "ies", "es", "ed", "s"):
        if len(w) > 3 and w.endswith(suf):
            return w[: -len(suf)] + ("y" if suf == "ies" else "")
    return w


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]", " ", str(s or "").lower()).strip()


def _day_of(dt: str) -> str:
    d = str(dt or "")
    return d[:10] if len(d) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", d) else ""


def _month_end(ym: str) -> str:
    try:
        y, m = int(ym[:4]), int(ym[5:7])
        if m == 2 and (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)):
            return f"{ym}-29"
        return f"{ym}-{_DAYS_IN_MONTH[m]:02d}"
    except (ValueError, IndexError):
        return ym


# ── 存储(SQLite WAL·单例·线程锁·与k2/card_construct同款纪律) ────────────────
_DB_LOCK = threading.Lock()
_DB: sqlite3.Connection | None = None
_GEN = 0                 # 进程内写代数(词典内存缓存失效用)
_GEN_LOCK = threading.Lock()


def _db_path() -> str:
    return os.environ.get("ZFAIOS_C1_DB", str(_P(__file__).parent / "c1_quad.db"))


def _db() -> sqlite3.Connection:
    global _DB
    if _DB is None:
        conn = sqlite3.connect(_db_path(), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("""CREATE TABLE IF NOT EXISTS ent_occ(
            tenant TEXT NOT NULL, ent TEXT NOT NULL, kind TEXT NOT NULL,
            sess TEXT NOT NULL, dia TEXT NOT NULL, dt TEXT NOT NULL,
            PRIMARY KEY(tenant, ent, sess, dia))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_c1_ent ON ent_occ(tenant, ent)")
        conn.execute("""CREATE TABLE IF NOT EXISTS tm_idx(
            tenant TEXT NOT NULL, day TEXT NOT NULL, sess TEXT NOT NULL,
            dia TEXT NOT NULL, PRIMARY KEY(tenant, day, sess, dia))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_c1_tm ON tm_idx(tenant, day)")
        conn.execute("""CREATE TABLE IF NOT EXISTS ent_alias(
            tenant TEXT NOT NULL, alias TEXT NOT NULL, canon TEXT NOT NULL,
            PRIMARY KEY(tenant, alias))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS meta(
            tenant TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL,
            PRIMARY KEY(tenant, k))""")
        conn.commit()
        _DB = conn
    return _DB


def _bump_gen() -> None:
    global _GEN
    with _GEN_LOCK:
        _GEN += 1


def _gen() -> int:
    with _GEN_LOCK:
        return _GEN


# ── ①实体词典: 消息→实体抽取(确定性·零LLM·通用句法启发) ────────────────────
_LINE_SPK_RE = re.compile(r"(?:^|\n)\s*([A-Z][a-z]{2,})\s*:")
_CAPSEQ_RE = re.compile(r"[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*")


def _is_cal_word(tok: str) -> bool:
    """月/星期词(单token实体免索引/免query实体化 — 时间路面专管)。"""
    return tok in _MONTHS_SET or tok in _WEEKDAYS_SET


def extract_entities(text: str) -> list[tuple[str, str]]:
    """卡文本→[(实体norm, kind)] kind: person/name/event/alias。确定性。
    person=行首"Name:"说话人面(LoCoMo真名主源); name=句中大写词序列(人名地名);
    event=W2-3事件名词面; alias=称呼别名句式。月/星期单词除外(时间路面管)。"""
    t = str(text or "")
    if not t:
        return []
    out: list[tuple[str, str]] = []
    for m in _LINE_SPK_RE.finditer(t):
        out.append((_norm(m.group(1)), "person"))
    for m in _CAPSEQ_RE.finditer(t):
        seq = m.group(0)
        k = m.start() - 1
        while k >= 0 and t[k] in " \t":     # 回扫空白找真前字符(句边界判定)
            k -= 1
        prev = t[k] if k >= 0 else ""
        if k < 0 or prev in ".!?\n\":":     # 句首/说话人冒号后=非句中引用
            continue
        if len(seq) > 40:
            continue
        toks = _norm(seq).split()
        if not toks:
            continue
        if len(toks) == 1 and (toks[0] in _GENERIC_CAPS or _is_cal_word(toks[0])
                               or len(toks[0]) < 3):
            continue
        if all((x in _GENERIC_CAPS or _is_cal_word(x)) for x in toks):
            continue
        out.append((" ".join(toks), "name"))
    low = " " + _norm(t) + " "
    for ev in _EVENT_NOUNS:
        if f" {ev} " in low or f" {ev}s " in low:
            out.append((ev, "event"))
    for rx in _ALIAS_RES:
        for m in rx.finditer(t):
            a = _norm(m.group(1))
            if len(a) >= 3:
                out.append((a, "alias"))
    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for ent, kind in out:
        if not ent or len(ent) < 3 or ent in seen:
            continue
        if " " not in ent and ent in _GENERIC_CAPS:
            continue
        seen.add(ent)
        uniq.append((ent, kind))
    return uniq


def _card_role(dia: str) -> str:
    """卡资格: msg=原文消息卡(dia="{rid}:{i}"单冒号)·tl=W2-1时间线卡·其余派生卡不索引。"""
    d = str(dia or "")
    if d.startswith("W2TL:"):
        return "tl"
    if d.count(":") == 1 and d.rsplit(":", 1)[1].isdigit():
        return "msg"
    return ""


def observe(tenant: str, cards: list[dict[str, Any]]) -> int:
    """add相观察: 原文卡+时间线卡→实体词典/时间索引增量落盘(幂等OR IGNORE·批事务)。
    总闸关=零写零成本。返回新落行数。异常上抛由调用方log(fail-soft·不涉add语义)。"""
    if not ON() or not cards:
        return 0
    ent_rows: list[tuple[Any, ...]] = []
    tm_rows: list[tuple[Any, ...]] = []
    for c in cards:
        if not isinstance(c, dict):
            continue
        role = _card_role(str(c.get("dia", "")))
        if not role:
            continue
        sess = str(c.get("sess", ""))
        dia = str(c.get("dia", ""))
        dt = str(c.get("dt", ""))
        day = _day_of(dt)
        if role == "msg" and day:
            tm_rows.append((tenant, day, sess, dia))
        for ent, kind in extract_entities(c.get("text", "")):
            ent_rows.append((tenant, ent, kind, sess, dia, dt))
    if not ent_rows and not tm_rows:
        return 0
    conn = _db()
    with _DB_LOCK, conn:
        if ent_rows:
            conn.executemany("INSERT OR IGNORE INTO ent_occ VALUES(?,?,?,?,?,?)", ent_rows)
        if tm_rows:
            conn.executemany("INSERT OR IGNORE INTO tm_idx VALUES(?,?,?,?)", tm_rows)
    _bump_gen()
    return len(ent_rows) + len(tm_rows)


def backfill(cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    """启动相存量回填(对表k3/k6家规): organ全卡→实体词典+时间索引全量重建。
    幂等: 租户meta indexed==应索引卡数且已有行则跳过; 否则DELETE+重建。
    总闸关=跳过零写。"""
    t0 = time.time()
    if not ON():
        return (0, 0, 0.0)
    by_tenant: dict[str, list[dict[str, Any]]] = {}
    for c in cards or []:
        if not isinstance(c, dict):
            continue
        if not _card_role(str(c.get("dia", ""))):
            continue
        conv = str(c.get("conv", ""))
        tenant = conv.split(":", 1)[0] if ":" in conv else "default"
        by_tenant.setdefault(tenant, []).append(c)
    conn = _db()
    n_cards = n_rows = 0
    for tenant, cs in by_tenant.items():
        want = len(cs)
        try:
            with _DB_LOCK:
                cur = conn.execute("SELECT COUNT(*) FROM ent_occ WHERE tenant=?",
                                   (tenant,)).fetchone()
                have_rows = int(cur[0]) if cur else 0
                cur2 = conn.execute("SELECT v FROM meta WHERE tenant=? AND k='indexed'",
                                    (tenant,)).fetchone()
                have_cards = int(cur2[0]) if cur2 else -1
            if have_cards == want and have_rows > 0:
                n_cards += want
                continue  # 幂等跳过(重启回放零重建)
            ent_rows, tm_rows = [], []
            for c in cs:
                role = _card_role(str(c.get("dia", "")))
                sess = str(c.get("sess", ""))
                dia = str(c.get("dia", ""))
                dt = str(c.get("dt", ""))
                day = _day_of(dt)
                if role == "msg" and day:
                    tm_rows.append((tenant, day, sess, dia))
                for ent, kind in extract_entities(c.get("text", "")):
                    ent_rows.append((tenant, ent, kind, sess, dia, dt))
            with _DB_LOCK, conn:
                conn.execute("DELETE FROM ent_occ WHERE tenant=?", (tenant,))
                conn.execute("DELETE FROM tm_idx WHERE tenant=?", (tenant,))
                if ent_rows:
                    conn.executemany("INSERT OR IGNORE INTO ent_occ VALUES(?,?,?,?,?,?)",
                                     ent_rows)
                if tm_rows:
                    conn.executemany("INSERT OR IGNORE INTO tm_idx VALUES(?,?,?,?)", tm_rows)
                conn.execute("INSERT OR REPLACE INTO meta VALUES(?,?,?)",
                             (tenant, "indexed", str(want)))
            n_cards += want
            n_rows += len(ent_rows) + len(tm_rows)
            log.info("c1 backfill tenant=%s cards=%d ent_rows=%d tm_rows=%d",
                     tenant, want, len(ent_rows), len(tm_rows))
        except Exception as exc:  # noqa: BLE001 — 单租户失败只丢该租户
            log.error("c1 backfill tenant=%s FAILED err=%s", tenant, type(exc).__name__)
    _bump_gen()
    return (n_cards, n_rows, (time.time() - t0) * 1000)


# ── 检索相只读面: 词典内存缓存/posting/时间索引区间 ─────────────────────────
_ENT_CACHE: dict[str, tuple[int, set[str]]] = {}
_DAYS_CACHE: dict[str, tuple[int, list[str]]] = {}


def _tenant_ents(tenant: str) -> set[str]:
    g = _gen()
    hit = _ENT_CACHE.get(tenant)
    if hit and hit[0] == g:
        return hit[1]
    try:
        with _DB_LOCK:
            rows = _db().execute("SELECT DISTINCT ent FROM ent_occ WHERE tenant=?",
                                 (tenant,)).fetchall()
        ents = {str(r[0]) for r in rows}
    except Exception:  # noqa: BLE001
        ents = set()
    _ENT_CACHE[tenant] = (g, ents)
    return ents


def _tenant_days(tenant: str) -> list[str]:
    """租户dt排序日列表(时间索引读面·路4新近分位用)。"""
    g = _gen()
    hit = _DAYS_CACHE.get(tenant)
    if hit and hit[0] == g:
        return hit[1]
    try:
        with _DB_LOCK:
            rows = _db().execute(
                "SELECT DISTINCT day FROM tm_idx WHERE tenant=? ORDER BY day",
                (tenant,)).fetchall()
        days = [str(r[0]) for r in rows]
    except Exception:  # noqa: BLE001
        days = []
    _DAYS_CACHE[tenant] = (g, days)
    return days


def query_entities(tenant: str, query: str) -> list[str]:
    """query→词典命中实体norm列表(1~3gram最长优先·月/星期单词除外=时间路面管)。
    别名命中→展开canonical(ent_alias预留·现行alias实体直入ent_occ)。确定性·封顶8。"""
    ents = _tenant_ents(tenant)
    if not ents:
        return []
    toks = [t for t in _norm(query).split() if t]
    found: list[str] = []
    seen: set[str] = set()
    n = len(toks)
    i = 0
    while i < n:
        matched = ""
        for ln in (3, 2, 1):
            if i + ln <= n:
                ph = " ".join(toks[i:i + ln])
                if ph in ents and not (ln == 1 and _is_cal_word(ph)):
                    matched = ph
                    break
        if matched:
            if matched not in seen:
                seen.add(matched)
                found.append(matched)
            i += (matched.count(" ") + 1)
        else:
            i += 1
    try:
        if found:
            with _DB_LOCK:
                for ent in list(found):
                    for (canon,) in _db().execute(
                            "SELECT canon FROM ent_alias WHERE tenant=? AND alias=?",
                            (tenant, ent)).fetchall():
                        c = str(canon)
                        if c and c not in seen:
                            seen.add(c)
                            found.append(c)
    except Exception:  # noqa: BLE001
        pass
    return found[:8]


def query_intervals(svc: Any, tenant: str, query: str) -> list[tuple[str, str]]:
    """query→[(day_from, day_to)]时间区间。显式日期=W2-1注入器同款解析(query_days);
    相对表达=time_norm复用解析(锚=租户max day·非墙钟·确定性·conf>=0.6才收)。
    区间编码: 正常=(YYYY-MM-DD,YYYY-MM-DD); 裸月=(0000-MM-00,9999-MM-99);
    裸月日=(0000-MM-DD,9999-MM-DD) — _tm_fetch按substr匹配。封顶4。"""
    ivs: list[tuple[str, str]] = []
    try:
        import w2_timeline as _w2tl
        kind, vals = _w2tl.query_days(query)
        for v in vals:
            if kind == 0 and len(v) == 10:
                ivs.append((v, v))
            elif kind == 1 and len(v) == 7:      # YYYY-MM
                ivs.append((f"{v}-01", _month_end(v)))
            elif kind == 1 and len(v) == 5:      # MM-DD(无年)
                ivs.append((f"0000-{v}-00", f"9999-{v}-99"))
            elif kind == 1 and len(v) <= 2:      # 裸月MM
                ivs.append((f"0000-{int(v):02d}-00", f"9999-{int(v):02d}-99"))
            elif kind == 2 and len(v) == 4:      # YYYY
                ivs.append((f"{v}-01-01", f"{v}-12-31"))
    except Exception:  # noqa: BLE001
        pass
    if len(ivs) < 4:
        try:
            days = _tenant_days(tenant)
            anchor = f"{days[-1]}T12:00:00" if days else None
            for r in _tn.normalize(str(query or ""), anchor):
                if r.confidence < 0.6 or not r.value:
                    continue
                rng = r.value.get("range")
                if rng and len(rng) == 2:
                    a, b = _day_of(str(rng[0])), _day_of(str(rng[1]))
                    if a and b:
                        ivs.append((min(a, b), max(a, b)))
                else:
                    d = _day_of(str(r.value.get("iso", "")))
                    if d:
                        ivs.append((d, d))
                if len(ivs) >= 4:
                    break
        except Exception:  # noqa: BLE001
            pass
    return sorted(set(ivs))[:4]


def _iv_kind(iv: tuple[str, str]) -> int:
    a, b = iv
    if a.endswith("-00") or b.endswith("-99"):
        return 1  # 裸月/月日(任意年)
    return 0


def _tm_fetch(tenant: str, ivs: list[tuple[str, str]], limit: int) -> list[tuple[str, str, str]]:
    """时间索引区间→[(sess,dia,day)]候选(裸区间走substr·正常走BETWEEN·确定性)。"""
    out: list[tuple[str, str, str]] = []
    try:
        conn = _db()
        with _DB_LOCK:
            for iv in ivs:
                a, b = iv
                if _iv_kind(iv):
                    key = a[5:10]        # "MM-DD"(裸月时为"MM-00"→取MM)
                    col, ln = ("substr(day,6,2)", 2) if key.endswith("-00") \
                        else ("substr(day,6,5)", 5)
                    key = key[:ln]
                    rows = conn.execute(
                        f"SELECT sess, dia, day FROM tm_idx WHERE tenant=? AND "
                        f"{col}=? ORDER BY day DESC, sess, dia LIMIT ?",
                        (tenant, key, limit)).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT sess, dia, day FROM tm_idx WHERE tenant=? AND "
                        "day BETWEEN ? AND ? ORDER BY day DESC, sess, dia LIMIT ?",
                        (tenant, a, b, limit)).fetchall()
                for sess, dia, day in rows:
                    out.append((str(sess), str(dia), str(day)))
    except Exception:  # noqa: BLE001
        pass
    return out


def _ent_fetch(tenant: str, qents: list[str], limit: int) -> list[tuple[str, str]]:
    """词典posting→[(sess,dia)]候选(按dt新→旧·确定性)。"""
    out: list[tuple[str, str]] = []
    try:
        conn = _db()
        with _DB_LOCK:
            for ent in qents:
                rows = conn.execute(
                    "SELECT sess, dia FROM ent_occ WHERE tenant=? AND ent=? "
                    "ORDER BY dt DESC, sess, dia LIMIT ?",
                    (tenant, ent, limit)).fetchall()
                for sess, dia in rows:
                    out.append((str(sess), str(dia)))
    except Exception:  # noqa: BLE001
        pass
    return out


# ── ③线性融合重排(search相·截断前最后排序位) ────────────────────────────────
def fusion_rerank(svc: Any, tenant: str, query: str, data: list[dict[str, Any]],
                  top_k: int) -> list[dict[str, Any]]:
    """四路线性融合: score = W1·语义位次 + W2·词面重叠 + W3·实体命中 + W4·时间命中。
    总闸关/异常→原样直通。rank0不动(KEEP_TOP1)。实体/时间两路新召回卡先过k2同口径
    隐私闸再入竞争(K2GATE=1时)·入窗封顶FETCH_MAX(评分后截取)。返回新列表(原dict
    零拷贝·metadata随行·响应3键由cfix2末位重建保证)。"""
    if not ON() or not data:
        return data
    t0 = time.perf_counter()
    w1 = _w("W1", 1.0)
    w2 = _w("W2", 0.4)
    w3 = _w("W3", 0.5)
    w4 = _w("W4", 0.3)
    qents = query_entities(tenant, query) if w3 > 0 else []
    ivs = query_intervals(svc, tenant, query) if w4 > 0 else []
    fetched: list[dict[str, Any]] = []
    fetch_on = os.environ.get("ZFAIOS_FUSION_FETCH", "1") != "0"
    if fetch_on and (qents or ivs):
        organ = getattr(svc, "organ", None)
        cards_map = getattr(organ, "_cards", None)
        if cards_map:
            have_txt = {str(d.get("content", "")) for d in data}
            cand: dict[str, tuple[str, str]] = {}   # text -> (sess, dt)
            tprefix = f"{tenant}:"
            tl_cnt = 0
            tl_max = _cap("TL_MAX", 2)
            for sess, dia in _ent_fetch(tenant, qents, _cap("ENT_FETCH", 120)):
                c = cards_map.get(f"{tprefix}{sess}|{dia}")
                if not isinstance(c, dict):
                    continue
                text = str(c.get("text", ""))
                if not text or text in have_txt or text in cand:
                    continue
                if str(dia).startswith("W2TL:"):
                    tl_cnt += 1
                    if tl_cnt > tl_max:   # 时间线卡封顶(与w2tl注入器同款防挤窗)
                        continue
                cand[text] = (sess, str(c.get("dt", "")))
            for sess, dia, _day in (_tm_fetch(tenant, ivs, _cap("TM_FETCH", 240))
                                    if ivs else []):
                c = cards_map.get(f"{tprefix}{sess}|{dia}")
                if not isinstance(c, dict):
                    continue
                text = str(c.get("text", ""))
                if not text or text in have_txt or text in cand:
                    continue
                cand[text] = (sess, str(c.get("dt", "")))
            if cand:
                fetched = [{"id": _cc._cid_of(sess, dt, text), "content": text,
                            "created_at": dt}
                           for text, (sess, dt) in sorted(cand.items())]
                if os.environ.get("BRAINOS_K2GATE", "0") == "1" and fetched:
                    try:
                        import knife2_sensitive as _k2
                        fetched, _ndrop = _k2.filter_gate(tenant, query, fetched)
                    except Exception:  # noqa: BLE001 — 闸失败=宁弃注入(fail-closed)
                        fetched = []
    # 评分(既有窗卡=路1位次先验; 新召回卡rank先验=0须凭实体/时间/词面挣位)
    n_all = len(data)
    qstem_set = list(dict.fromkeys(
        _stem(t) for t in _norm(query).split()
        if len(t) > 2 and t not in _STOP and not _is_cal_word(t)))[:16]
    days = _tenant_days(tenant) if (w4 > 0 and not ivs) else []
    span = max(1, len(days))
    ents_list = [(i, d, (n_all - i) / n_all) for i, d in enumerate(data)]
    ents_list += [(n_all + j, d, 0.0) for j, d in enumerate(fetched)]
    scores: list[float] = []
    for _i, d, rank in ents_list:
        tlow = str(d.get("content", "")).lower()
        s = w1 * rank
        if w2 > 0 and qstem_set:
            s += w2 * (sum(1 for st in qstem_set if st in tlow) / len(qstem_set))
        if w3 > 0 and qents:
            s += w3 * (sum(1 for e in qents if e in tlow) / len(qents))
        if w4 > 0:
            day = _day_of(d.get("created_at", ""))
            if ivs:
                if any(a <= day <= b for a, b in ivs):
                    s += w4
            elif day:
                s += w4 * (bisect.bisect_right(days, day) / span)
        scores.append(s)
    order = sorted(range(len(ents_list)), key=lambda j: (-scores[j], ents_list[j][0]))
    fmax = _cap("FETCH_MAX", 6)
    f_kept = 0
    out: list[dict[str, Any]] = []
    for j in order:
        d = ents_list[j][1]
        if j >= n_all:                      # 新召回卡评分后封顶
            if f_kept >= fmax:
                continue
            f_kept += 1
        out.append(d)
    if os.environ.get("ZFAIOS_FUSION_KEEP_TOP1", "1") != "0" and data:
        first = str(data[0].get("id", ""))
        out = [data[0]] + [d for d in out if str(d.get("id", "")) != first]
    try:
        log.info("c1 fusion tenant=%s qents=%d ivs=%d cand=%d fetched=%d kept_f=%d "
                 "n=%d dt=%.1fms", tenant, len(qents), len(ivs), len(ents_list),
                 len(fetched), f_kept, len(out), (time.perf_counter() - t0) * 1000)
    except Exception:  # noqa: BLE001
        pass
    return out


def stats() -> dict[str, Any]:
    """量规(零行为·诊断用): 表行数+词典规模。"""
    try:
        conn = _db()
        with _DB_LOCK:
            def g(q: str) -> int:
                return int(conn.execute(q).fetchone()[0])
        return {"db": _db_path(),
                "ent_rows": g("SELECT COUNT(*) FROM ent_occ"),
                "ent_distinct": g("SELECT COUNT(DISTINCT ent) FROM ent_occ"),
                "tm_rows": g("SELECT COUNT(*) FROM tm_idx"),
                "alias_rows": g("SELECT COUNT(*) FROM ent_alias")}
    except Exception as exc:  # noqa: BLE001
        return {"err": type(exc).__name__}
