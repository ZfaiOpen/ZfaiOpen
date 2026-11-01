# knife6_cards.py — 部署线S7刀·G2规则卡 + G4经验卡/趋势卡(零LLM·纯结构化·app层overlay·20261009)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方冒烟G2规则应用=20·G4经验发现=0 — 规则句("以后每周都去健身房"/"no food near
#       the lathes")与经验句("Hard-won lesson/booking tip")未被结构化利用: 规则/经验类query
#       的命中原句卡沉在top10外(校准床G3-01实证: 锚在top100窗内·top10外=检索缺分辨)。
# 法典纪律(任务单S7): ①规则卡/经验卡=一等公民(原句+主体+对象元数据) ②检索时规则/经验/
#       聚合意图query→置顶加成(有限加成·每类≤4·总计≤6·不淹没事实卡) ③清单加项即死护栏:
#       非意图query字节级直通——具体事实query证据中零规则卡/零趋势卡(命中数守卫=0)
#       ④与四刀共存: 不动supersede/隐私闸/时间索引/延迟memo任何路径(k3时间档query与
#       强时间线意图让位=零注入; k1 history弱代理(\bdid\b类)不劫持G3经验类query;
#       敏感卡过k2放行门才可注入——只consult其公开纯函数)。
# 做法:
#   ADD相    observe(): 词典启发式逐句检测 — 规则句(rule_signals)→规则卡·经验句(exp_signals)
#            →经验卡(均存原句verbatim+subject+object元数据落盘knife6_cards.db);
#            数值事实抽取(metrics正则: 数字+单位+实体)→聚簇点; 同实体同度量≥3点→
#            生成趋势卡("X的Y n readings ... range [a,b] unit — trend rising/falling/stable"·纯算术)。
#   STARTUP  backfill(): organ存量卡全量回填(幂等OR REPLACE·单事务·毫秒级·knife3同法) —
#            修前已入库卡亦被S7覆盖(校准床租户=账本重放零新add·全靠回填建卡)。
#   SEARCH相 boost(): query三意图检测(rule_intent/exp_intent/agg_intent·中英·名词锚定禁高频词) —
#            命中意图才置顶加成: 意图类卡按词面重叠分排σ前插(去重: 原卡已在证据=移动置顶
#            非重复注入; 不在=合成注入·id/content/created_at同源organ语义), 上限硬闸;
#            敏感卡(k2类目)仅当query显式问到该域(release)才可注入(H2零回退);
#            零意图=字节级直通(零拷贝零改动) — 护栏由构造保证+selftest机械断言。
#   持久化   knife6_cards.db (sqlite WAL + synchronous=NORMAL — knife1/2/3同款纪律)。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与aml_service._evidence_id同源同值
#            (knife1/k2/k3同法); 趋势卡id="k6t_"+sha1(tenant|entity|metric)[:12]。
# 词典维护: knife6_rules.json(同目录) — 改词典+重启即生效; env KNIFE6_DICT/KNIFE6_DB可换路径。
# 合规: 日志零内容 — 只记kind/计数/卡id哈希; 不落query/文本/主体/值。
# env: BRAINOS_K6(缺省开·=0一键关门回旧行为) / KNIFE6_DICT / KNIFE6_DB / BRAINOS_K6_MAX_PIN
# 回滚 = 摘除本文件/knife6_rules.json + 还原k6_service.py三处hunk(knife6_cards.db可留档)。
# ═══════════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("knife6")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE6_DICT", str(APP_DIR / "knife6_rules.json")))
DB_PATH = Path(os.environ.get("KNIFE6_DB", str(APP_DIR.parent / "data" / "knife6_cards.db")))

_SENT_SPLIT = re.compile(r"(?<=[.!?;。！？；\n])\s*")
# 趋势卡数值单位缺省口径(词典metrics无unit字段的兜底·只进趋势卡文案元数据)
_UNIT_OF = {"weight": "kg", "run_distance": "km", "sleep_hours": "hours",
            "spend": "money", "rating": "/10", "duration_booking": "ahead"}

with open(DICT_PATH, encoding="utf-8") as _f:
    _DICT = json.load(_f)
_ADD = _DICT.get("add_side", {})
_QUERY = _DICT.get("query_side", {})
_LIMITS = _DICT.get("limits", {})
MAX_PIN_PER_KIND = int(os.environ.get("BRAINOS_K6_MAX_PIN",
                                      str(_LIMITS.get("max_pin_per_kind", 4))))
MAX_PIN_TOTAL = int(_LIMITS.get("max_pin_total", 6))
TREND_MIN_POINTS = int(_LIMITS.get("trend_min_points", 3))

_RULE_RES = [re.compile(rx, re.IGNORECASE) for rx in _ADD.get("rule_signals", [])]
_EXP_RES = [re.compile(rx, re.IGNORECASE) for rx in _ADD.get("exp_signals", [])]
_NAME_PREFIX = re.compile(_ADD.get("subjects", {}).get("name_prefix",
                                                      "^(?:from\\s+)?([A-Z][\\w'-]{1,20})\\s*:\\s"))
_FIRST_PERSON = re.compile(_ADD.get("subjects", {}).get("first_person", "^\\s*(?:I|we|my|our)\\b"),
                           re.IGNORECASE)
_RULE_Q_RES = [re.compile(rx, re.IGNORECASE) for rx in _QUERY.get("rule_intent", [])]
_EXP_Q_RES = [re.compile(rx, re.IGNORECASE) for rx in _QUERY.get("exp_intent", [])]
_AGG_Q_RES = [re.compile(rx, re.IGNORECASE) for rx in _QUERY.get("agg_intent", [])]
_STRONG_RES = [re.compile(rx, re.IGNORECASE) for rx in _QUERY.get("strong_timeline", [])]

# knife4_1015(60.34战报§四刀4·RULE卡保底进窗): rule-need query结构信号(通用文档形·零题面词)。
# ①选项列表形: 同query出现≥3个[A-F]选项标记(MCQ结构形·中英卷通用); ②建议/应当形:
# 规范问法词(should/建议/应当类·通用词类)。触发=结构信号非具体措辞(禁2✓零序列预测)。
_OPT_MARK = re.compile(r"(?:^|\s)[A-F][.、:：)）]\s*\S", re.IGNORECASE)
_ADVICE_Q = re.compile(
    r"\b(?:should|shall|ought\s+to|recommended?|suggest(?:ed|s|ion)?|advise?|advisable|"
    r"best\s+(?:way|practice|option)|which\s+(?:one|option|choice))\b"
    r"|应当|应该|应如何|该怎么办|该怎么处理|如何处理|怎么处理|建议|需要注意|注意事项|哪个更(?:好|合适)"
    r"|最(?:好|佳)(?:做法|方案|选择)", re.IGNORECASE)
_RW_MAX = max(1, int(os.environ.get("BRAINOS_K6_WIN_MAX", "2")))
_METRICS = [(m["key"], m.get("label", m["key"]), re.compile(m["rx"], re.IGNORECASE))
            for m in _DICT.get("metrics", [])]

_STOP = {"what", "does", "did", "do", "is", "are", "was", "were", "will", "the", "of", "for",
         "about", "and", "to", "in", "on", "at", "a", "an", "his", "her", "their", "its",
         "with", "she", "he", "they", "we", "you", "i", "s", "how", "when", "why", "which",
         "who", "where", "that", "this", "it"}
_WORD_RE = re.compile(r"[a-z0-9']+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]+")


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16]·knife1/2/3同法)。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def _tokens(text: str) -> set[str]:
    """词面token: latin词(去停用词) + 中文二字组(零分词依赖)。"""
    t = (text or "").lower()
    out = {w for w in _WORD_RE.findall(t) if len(w) > 1 and w not in _STOP}
    for run in _CJK_RE.findall(t):
        if len(run) >= 2:
            out.update(run[i:i + 2] for i in range(len(run) - 1))
        else:
            out.add(run)
    return out


def _classify_sent(sent: str) -> tuple[bool, bool]:
    """句子→(是规则句, 是经验句)·零LLM词典启发式(宁缺勿错: 命中任一信号即算)。"""
    rule = any(rx.search(sent) for rx in _RULE_RES)
    exp = any(rx.search(sent) for rx in _EXP_RES)
    return rule, exp


def _subject_of(sent: str, speaker: str) -> str:
    m = _NAME_PREFIX.match(sent.strip())
    if m:
        return m.group(1)
    if _FIRST_PERSON.match(sent):
        return speaker or ""
    return speaker or ""


def _object_of(sent: str) -> str:
    """对象元数据(仅落盘供检索调试·不参与命中判定): 规则/经验信号词后的前≤6词。"""
    cut = re.split(r"(?::\s|\s(?:is|are|:)\s)", sent, maxsplit=1)
    seg = cut[-1] if len(cut) > 1 else sent
    words = _WORD_RE.findall(seg.lower())
    return " ".join(words[:6])


def extract_points(sent: str) -> list[tuple[str, float, str]]:
    """句子→[(metric_key, value, unit)]·每句每度量取首个匹配(正则·零LLM)。"""
    out: list[tuple[str, float, str]] = []
    for key, _label, rx in _METRICS:
        m = rx.search(sent)
        if not m:
            continue
        raw = next((g for g in m.groups() if g), None)
        if raw is None:
            continue
        try:
            val = float(raw)
        except ValueError:
            continue
        out.append((key, val, _UNIT_OF.get(key, "")))
    return out


def _fmt(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else f"{v:.2f}".rstrip("0").rstrip(".")


def query_kinds(query: str) -> list[str]:
    """query→意图类列表(零LLM·词典检测·可独立单测=零扰动机械门)。"""
    if not query:
        return []
    kinds: list[str] = []
    if any(rx.search(query) for rx in _RULE_Q_RES):
        kinds.append("rule")
    if any(rx.search(query) for rx in _EXP_Q_RES):
        kinds.append("exp")
    if any(rx.search(query) for rx in _AGG_Q_RES):
        kinds.append("trend")
    return kinds


class Knife6:
    def __init__(self, db_path: Path, dict_path: Path = DICT_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS cards(
            tenant TEXT NOT NULL, eid TEXT NOT NULL, kind TEXT NOT NULL,
            sh TEXT NOT NULL,
            sent TEXT NOT NULL, subject TEXT NOT NULL DEFAULT '',
            obj TEXT NOT NULL DEFAULT '', dt TEXT NOT NULL DEFAULT '',
            cats TEXT NOT NULL DEFAULT '', seq INTEGER NOT NULL,
            PRIMARY KEY(tenant, eid, kind, sh))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS points(
            tenant TEXT NOT NULL, eid TEXT NOT NULL, mkey TEXT NOT NULL,
            entity TEXT NOT NULL, val REAL NOT NULL, unit TEXT NOT NULL DEFAULT '',
            dt TEXT NOT NULL DEFAULT '', cats TEXT NOT NULL DEFAULT '', seq INTEGER NOT NULL,
            PRIMARY KEY(tenant, eid, mkey))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS trends(
            tenant TEXT NOT NULL, entity TEXT NOT NULL, mkey TEXT NOT NULL,
            content TEXT NOT NULL, n INTEGER NOT NULL, vmin REAL NOT NULL,
            vmax REAL NOT NULL, direction TEXT NOT NULL, dt TEXT NOT NULL DEFAULT '',
            cats TEXT NOT NULL DEFAULT '', updated_ms INTEGER NOT NULL,
            PRIMARY KEY(tenant, entity, mkey))""")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0

    # ── ADD相: 卡生成 ──────────────────────────────────────────────────────────
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        n_sent = 0
        with self._lock:
            seq = time.time_ns()
            for c in cards:
                try:
                    text = str(c.get("text") or "")
                    sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
                    speaker = str(c.get("speaker") or "")
                    if not text:
                        continue
                    eid = card_id_of(sess, dt, text)
                    for sent in _SENT_SPLIT.split(text):
                        sent = sent.strip()
                        if len(sent) < 4:
                            continue
                        is_rule, is_exp = _classify_sent(sent)
                        if not (is_rule or is_exp):
                            self._observe_points(tenant, eid, sent, speaker, dt, seq)
                            continue
                        kind = "rule" if is_rule else "exp"
                        subject = _subject_of(sent, speaker)
                        cats = self._k2_cats(sent)
                        sh = hashlib.sha1(sent.encode("utf-8")).hexdigest()[:12]
                        self._conn.execute(
                            "INSERT OR REPLACE INTO cards VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (tenant, eid, kind, sh, sent, subject, _object_of(sent), dt,
                             json.dumps(sorted(cats)), seq))
                        n_sent += 1
                        self._observe_points(tenant, eid, sent, subject or speaker, dt, seq, cats)
                except Exception:  # noqa: BLE001 — 单卡脏数据跳过(观察fail-soft)
                    continue
            self._conn.commit()
        self.n_obs += len(cards)
        if n_sent:
            log.info("knife6 observe tenant_hash=%04x cards=%d new_sents=%d",
                     hash(tenant) & 0xFFFF, len(cards), n_sent)

    def _observe_points(self, tenant: str, eid: str, sent: str, entity: str,
                        dt: str, seq: int, cats: set[str] | None = None) -> None:
        pts = extract_points(sent)
        if not pts:
            return
        ent = (entity or "user").strip().lower()[:64]
        cats = cats if cats is not None else self._k2_cats(sent)
        for key, val, unit in pts:
            self._conn.execute(
                "INSERT OR REPLACE INTO points VALUES(?,?,?,?,?,?,?,?,?)",
                (tenant, eid, key, ent, val, unit, dt, json.dumps(sorted(cats)), seq))
        for key, _v, _u in pts:  # 逐触达度量重建趋势(≥3点才成卡·否则清卡)
            self._rebuild_trend(tenant, ent, key, seq)

    def _rebuild_trend(self, tenant: str, ent: str, mkey: str, seq: int) -> None:
        """同实体同度量≥3点→趋势卡(纯算术: min/max/首尾方向)·幂等REPLACE。"""
        rows = self._conn.execute(
            "SELECT val, unit, dt, cats FROM points WHERE tenant=? AND entity=? AND mkey=? "
            "ORDER BY dt, eid", (tenant, ent, mkey)).fetchall()
        if len(rows) < TREND_MIN_POINTS:
            self._conn.execute("DELETE FROM trends WHERE tenant=? AND entity=? AND mkey=?",
                               (tenant, ent, mkey))
            return
        vals = [r[0] for r in rows]
        unit = next((r[1] for r in reversed(rows) if r[1]), "")
        cats: set[str] = set()
        for r in rows:
            try:
                cats |= set(json.loads(r[3] or "[]"))
            except Exception:
                pass
        vmin, vmax = min(vals), max(vals)
        first, last = vals[0], vals[-1]
        direction = "rising" if last > first else ("falling" if last < first else "stable")
        label = next((lbl for k, lbl, _ in _METRICS if k == mkey), mkey)
        series = ", ".join(_fmt(v) for v in vals)
        content = (f"Trend card: {ent}'s {label} across {len(vals)} notes: {series} — "
                   f"range [{_fmt(vmin)}, {_fmt(vmax)}] {unit} — trend {direction}.")
        self._conn.execute(
            "INSERT OR REPLACE INTO trends VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (tenant, ent, mkey, content, len(vals), vmin, vmax, direction,
             rows[-1][2], json.dumps(sorted(cats)), seq))

    def _k2_cats(self, text: str) -> set[str]:
        """敏感类目(consult knife2公开纯函数·只读不改其任何路径); k2缺席=fail-closed空集不标。"""
        try:
            import knife2_sensitive as k2  # noqa: PLC0415 — 进程内同目录consult
            return set(k2.classify_text(text))
        except Exception:
            return set()

    # ── STARTUP: 存量回填 ─────────────────────────────────────────────────────
    def backfill(self, organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
        """organ存量卡全量回填(knife3同法: conv前缀→tenant·幂等OR REPLACE·单事务)。"""
        t0 = time.time()
        n_c = n_s = 0
        with self._lock:
            seq = time.time_ns()
            for c in organ_cards:
                try:
                    text = str(c.get("text") or "")
                    conv = str(c.get("conv") or "")
                    sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
                    speaker = str(c.get("speaker") or "")
                    if not text:
                        continue
                    tenant = conv.split(":", 1)[0] or "u_default"
                    eid = card_id_of(sess, dt, text)
                    n_c += 1
                    for sent in _SENT_SPLIT.split(text):
                        sent = sent.strip()
                        if len(sent) < 4:
                            continue
                        is_rule, is_exp = _classify_sent(sent)
                        if not (is_rule or is_exp):
                            self._observe_points(tenant, eid, sent, speaker, dt, seq)
                            continue
                        kind = "rule" if is_rule else "exp"
                        subject = _subject_of(sent, speaker)
                        cats = self._k2_cats(sent)
                        sh = hashlib.sha1(sent.encode("utf-8")).hexdigest()[:12]
                        cur = self._conn.execute(
                            "INSERT OR REPLACE INTO cards VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (tenant, eid, kind, sh, sent, subject, _object_of(sent), dt,
                             json.dumps(sorted(cats)), seq))
                        n_s += cur.rowcount
                        self._observe_points(tenant, eid, sent, subject or speaker, dt, seq, cats)
                except Exception:  # noqa: BLE001 — 单卡脏数据跳过(回填fail-soft)
                    continue
            self._conn.commit()
        return n_c, n_s, (time.time() - t0) * 1000

    # ── SEARCH相: 置顶加成 ────────────────────────────────────────────────────
    def boost(self, tenant: str, query: str, data: list[dict[str, Any]],
              k1_intent: str = "current", k3_tier: str = "none") -> tuple[list[dict[str, Any]], int]:
        """意图query→意图类卡置顶(有限加成); 否则字节级直通。
        返回(加成后data, 注入数)。护栏: k3时间档query与强时间线意图(timeline/trace/
        in order/轨迹…)=零注入(四刀让位·C3时间线资产禁扰); k1 history的弱代理命中
        (如\\bdid\\b——G3经验类query常见误伤)且k6意图明确时放行置顶;
        敏感卡仅显式域放行; 每类≤MAX_PIN_PER_KIND·总计≤MAX_PIN_TOTAL。"""
        if not query or os.environ.get("BRAINOS_K6", "1") == "0":
            return data, 0
        kinds = query_kinds(query)
        if not kinds:
            return data, 0  # 零意图=字节级直通(清单加项即死护栏·构造保证)
        if (k3_tier and k3_tier != "none") or any(rx.search(query) for rx in _STRONG_RES):
            return data, 0  # C1/C2/C3时间query与强时间线意图让位(禁打架)
        try:
            import knife2_sensitive as k2  # noqa: PLC0415
            release = set(k2.release_domains(query))
        except Exception:
            release = set()
        qt = _tokens(query)
        pinned: list[dict[str, Any]] = []
        pinned_ids: set[str] = set()
        with self._lock:
            for kind in kinds:
                if kind == "trend":
                    rows = self._conn.execute(
                        "SELECT entity, mkey, content, dt, cats, updated_ms FROM trends "
                        "WHERE tenant=?", (tenant,)).fetchall()
                    cands = [(f"k6t_{hashlib.sha1(f'{tenant}|{r[0]}|{r[1]}'.encode()).hexdigest()[:12]}",
                              r[2], r[3], r[4], _tokens(r[2]), r[5]) for r in rows]
                else:
                    rows = self._conn.execute(
                        "SELECT eid, sent, dt, cats, subject FROM cards "
                        "WHERE tenant=? AND kind=?", (tenant, kind)).fetchall()
                    cands = [(r[0], r[1], r[2], r[3], _tokens(r[1]) | (_tokens(r[4]) if r[4] else set()),
                              r[0]) for r in rows]
                scored = []
                for eid, sent, dt, catsj, toks, order_key in cands:
                    try:
                        cats = set(json.loads(catsj or "[]"))
                    except Exception:
                        cats = set()
                    if cats and not (cats & release):
                        continue  # H2: 敏感卡仅显式域放行
                    scored.append((len(qt & toks), str(order_key), eid, sent, dt, kind))
                # 同源卡多句(一句一行·PK带句哈希)按分排序后每源卡只置顶最佳一句
                scored.sort(key=lambda x: (-x[0], x[1]))
                seen_eid: set[str] = set()
                for sc, _ok, eid, sent, dt, kind in scored[:MAX_PIN_PER_KIND * 3]:
                    if len(pinned) >= MAX_PIN_TOTAL:
                        break
                    if eid in pinned_ids or eid in seen_eid:
                        continue
                    seen_eid.add(eid)
                    pinned_ids.add(eid)
                    pinned.append({"id": eid, "content": sent, "created_at": dt,
                                   "metadata": {"knife6": {"kind": kind, "score": sc}}})
        if not pinned:
            return data, 0
        out = pinned + [d for d in data if str(d.get("id", "")) not in pinned_ids]
        return out, len(pinned)

    # ── SEARCH相: RULE卡保底进窗(knife4_1015·60.34战报§四刀4) ─────────────────
    def window_inject(self, tenant: str, query: str, data: list[dict[str, Any]],
                      top_k: int) -> tuple[list[dict[str, Any]], int]:
        """G2保底进窗: rule-need query(选项列表/建议形/规则意图)且租户规则卡在库→
        词面重叠top RULE卡注入位1-2(rank0不动·≤2张·fail-soft·既有三键契约)。
        病灶(60.34§二G2): 医疗窗top5零RULE卡·rule_first只重排不增删·RULE不在窗无从置顶
        — 保底进窗是其生效前置。禁超窗: 满窗时仅当尾位卡对query零词面(死重)才顶替,
        否则弃注入(零证据损失·同cc.overlay满窗纪律)。"""
        if not query or os.environ.get("BRAINOS_K6", "1") == "0":
            return data, 0
        try:
            kinds = query_kinds(query)
            opt_n = len(_OPT_MARK.findall(str(query)))
            if not kinds and opt_n < 3 and not _ADVICE_Q.search(query):
                return data, 0  # 非rule-need=字节级直通(零突变·构造保证)
            if any(rx.search(query) for rx in _STRONG_RES):
                return data, 0  # 强时间线意图让位(与k6.boost同护栏·C1/C2/C3禁打架)
            try:
                import knife2_sensitive as k2  # noqa: PLC0415
                release = set(k2.release_domains(query))
            except Exception:
                release = set()
            qt = _tokens(query)
            if not qt:
                return data, 0
            have = {str(d.get("content", "")) for d in data}
            with self._lock:
                rows = self._conn.execute(
                    "SELECT eid, sent, dt, cats FROM cards WHERE tenant=? AND kind='rule'",
                    (tenant,)).fetchall()
            scored = []
            _seen_eids = set()
            for eid, sent, dt, catsj in rows:
                if eid in _seen_eids:
                    continue
                _seen_eids.add(eid)
                if sent in have:
                    continue  # 已在窗=rule_first重排管辖·零重复注入
                try:
                    cats = set(json.loads(catsj or "[]"))
                except Exception:
                    cats = set()
                if cats and not (cats & release):
                    continue  # H2: 敏感卡仅显式域放行(k6.boost同门)
                sc = len(qt & _tokens(sent))
                if sc <= 0:
                    continue  # 词面零交=不保底(相关性下限·防无关规则卡混窗)
                scored.append((-sc, str(eid), eid, sent, dt))
            if not scored:
                return data, 0
            scored.sort(key=lambda x: (x[0], x[1]))
            out = list(data)
            n = 0
            cap = max(int(top_k), 1)
            for _nsc, _ek, eid, sent, dt in scored[:_RW_MAX]:
                if sent in have:
                    continue
                if len(out) < cap:
                    out.insert(min(1 + n, len(out)),
                               {"id": str(eid), "content": sent, "created_at": str(dt or "")})
                    have.add(sent)
                    n += 1
                    continue
                # 满窗顶替: 仅尾位对query零词面(死重)才让位·否则弃(禁超窗·零证据损失)
                qtoks = [t for t in re.findall(r"[a-z0-9']+|[\u4e00-\u9fff]", query.lower())
                         if t not in _STOP and len(t) > 1]
                placed = False
                for k_i in range(len(out) - 1, 0, -1):
                    tail = str(out[k_i].get("content", "")).lower()
                    if not any(t in tail for t in qtoks):
                        out.pop(k_i)
                        out.insert(1, {"id": str(eid), "content": sent,
                                       "created_at": str(dt or "")})
                        have.add(sent)
                        placed = True
                        n += 1
                        break
                if not placed:
                    continue
            return out, n
        except Exception:  # noqa: BLE001 — 保底失败=原窗直通(零回归)
            return data, 0

    # ── 运维 ──────────────────────────────────────────────────────────────────
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {"db": str(self.db_path), "rule_cards": g(
                "SELECT COUNT(*) FROM cards WHERE kind='rule'"),
                "exp_cards": g("SELECT COUNT(*) FROM cards WHERE kind='exp'"),
                "points": g("SELECT COUNT(*) FROM points"),
                "trends": g("SELECT COUNT(*) FROM trends"), "obs_cards_total": self.n_obs}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部knife6状态行(organ卡本体不在本模块管辖)。"""
        with self._lock:
            n = 0
            for tbl in ("cards", "points", "trends"):
                cur = self._conn.execute(f"DELETE FROM {tbl} WHERE tenant=?", (tenant,))
                n += cur.rowcount
            self._conn.commit()
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K6: Knife6 | None = None
_K6_LOCK = threading.Lock()


def get() -> Knife6:
    global _K6
    with _K6_LOCK:
        if _K6 is None:
            _K6 = Knife6(DB_PATH, DICT_PATH)
            log.info("knife6 init db=%s rule_rx=%d exp_rx=%d metrics=%d pin<=%d/%d",
                     _K6.db_path, len(_RULE_RES), len(_EXP_RES), len(_METRICS),
                     MAX_PIN_PER_KIND, MAX_PIN_TOTAL)
        return _K6


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def backfill(organ_cards: list[dict[str, Any]]) -> tuple[int, int, float]:
    return get().backfill(organ_cards)


def boost(tenant: str, query: str, data: list[dict[str, Any]],
          k1_intent: str = "current", k3_tier: str = "none") -> tuple[list[dict[str, Any]], int]:
    return get().boost(tenant, query, data, k1_intent, k3_tier)


def window_inject(tenant: str, query: str, data: list[dict[str, Any]],
                  top_k: int) -> tuple[list[dict[str, Any]], int]:
    """knife4_1015·G2: RULE卡保底进窗(选项列表/建议形query·词面重叠top·≤2张·位1-2)。"""
    return get().window_inject(tenant, query, data, top_k)


def stats() -> dict[str, Any]:
    return get().stats()


def purge_tenant(tenant: str) -> int:
    return get().purge_tenant(tenant)
