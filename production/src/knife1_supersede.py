# knife1_supersede.py — 部署线D1双时相新值覆盖 + D2矛盾检测(零LLM·纯结构化·app层overlay·20261008)
# ═══════════════════════════════════════════════════════════════════════════════════
# 血洞: 官方冒烟D1=0 — 用户说"我搬到伦敦了", 查"我住哪"还回巴黎(法典判词="没送新值卡")。
# 法典铁律: 当前态意图query必须排除superseded旧卡(新旧同现=错·清单加项即死);
#           历史/时间线意图必须照常返回全量旧卡(C3时间线100分·禁回退)。
# 做法(对冻结核心/organ/exam考场件零修改·纯overlay·可整件摘除回滚):
#   ADD相    observe(): 词典启发式抽(subject_key, pred_key, value) — 同subject同pred新值≠旧值:
#            supersede类 → 旧卡记 superseded_by=新卡id(旧卡保留不删·只在当前态检索被排除);
#            preference类 → conflict_pair 落盘(D2矛盾对·双卡共存·比较题保留)。
#   SEARCH相 filter_evidence(): history信号词 → 全量返回+按dt升序(C3时间线·禁回退);
#            其余(当前态/无信号=当前态倾向) → 排除superseded卡(排除非降权), conflict对不排除。
#            配合aml_service超采补偿(内部多取superseded数), 排除后仍按top_k填满。
#   持久化   knife1.db (sqlite WAL + synchronous=NORMAL — 与ledger.db同款纪律·重启不丢)。
#   卡身份   sha1(f"{session}|{dt}|{text}")[:16] — 与 aml_service._evidence_id 同源同值;
#            add侧输入卡片六字段(sess/dt/text), search侧输入evidence投影(session/dt/text), 恒等。
# 词典维护: knife1_predicates.json(同目录) — 改词典+重启即生效; env KNIFE1_DICT/KNIFE1_DB可换路径。
# 合规: 日志零内容 — 只记pred/class/卡id哈希/计数; 不落query/文本/主语/值。
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

log = logging.getLogger("knife1")

APP_DIR = Path(__file__).resolve().parent
DICT_PATH = Path(os.environ.get("KNIFE1_DICT", str(APP_DIR / "knife1_predicates.json")))
DB_PATH = Path(os.environ.get("KNIFE1_DB", str(APP_DIR.parent / "data" / "knife1.db")))

# ── 词典装载 ────────────────────────────────────────────────────────────────
def load_dict(path: Path = DICT_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_DICT = load_dict()
_PREDS: list[tuple[str, str, list[re.Pattern]]] = []
for _p in _DICT.get("predicates", []):
    _PREDS.append((_p["key"], _p.get("class", "supersede"),
                   [re.compile(rx, re.IGNORECASE) for rx in _p.get("patterns", [])]))
HISTORY_RES = [re.compile(rx, re.IGNORECASE) for rx in _DICT.get("history_signals", [])]
CURRENT_RES = [re.compile(rx, re.IGNORECASE) for rx in _DICT.get("current_markers", [])]

# ── 文本结构化(零LLM启发式) ─────────────────────────────────────────────────
_SENT_SPLIT = re.compile(r"(?<=[.!?;。！？；\n])\s*")
_VALUE_CUT = re.compile(
    r"\s+(?:and|but|while|because|so|also|with|for|now|currently|recently|already"
    r"|而且|但是|可是|因为|所以|然后|现在|最近|目前)\s+|[,，;；:：()（）\[\]\"“”]")
_VALUE_TAIL_FILLER = re.compile(
    r"\s*(?:(?:last|this|next|past)\s+\w+"
    r"|(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|\d+)\s*"
    r"(?:years?|months?|weeks?|days?|decades?)(?:\s+ago)?"
    r"|(?:yesterday|today|recently|now|currently|these\s+days|again|already)"
    r"|了|啦|咯|呗|呀|最近|现在|目前|去年|今年|明年|上个月|这个月|下个月|上周|这周|上周[一二三四五六日末]|昨天|今天)+\s*$")
_VALUE_LEAD_FILLER = re.compile(r"^(?:the|a|an|my|our|his|her|their|its|了|在|到|是|和|跟|与|把)\s*")
_SELF_EN = {"i", "we", "me", "us", "my", "our", "mine", "ours", "myself", "im", "ive", "ill"}
_3RD_EN = {"he", "she", "they", "him", "her", "them", "his", "their", "theirs", "it", "you", "your", "yours", "its"}
_SELF_ZH = {"我", "我们", "咱们", "俺", "咱", "自己"}
_3RD_ZH = {"他", "她", "他们", "她们", "它", "它们", "你", "你们", "您"}
_STOP_EN = {
    "and", "or", "but", "also", "then", "so", "because", "when", "while", "after", "before",
    "since", "until", "if", "the", "a", "an", "this", "that", "these", "those", "as", "at",
    "in", "on", "to", "from", "with", "by", "of", "is", "are", "am", "was", "were", "be",
    "been", "being", "has", "have", "had", "do", "does", "did", "will", "would", "can",
    "could", "just", "really", "very", "now", "currently", "recently", "today", "yesterday",
    "last", "next", "first", "finally", "note", "update", "hey", "hi", "well", "ok", "oh",
    "btw", "basically", "actually", "maybe", "probably", "think", "thought", "said", "says",
    "tell", "told", "remember", "know", "guess", "sure", "yes", "no", "not", "here", "there",
    "got", "get", "gets", "getting", "went", "come", "came", "want", "wanted", "need", "needed"}


def _sentences(text: str) -> list[str]:
    return [s for s in _SENT_SPLIT.split(text or "") if s and len(s.strip()) >= 2]


def norm_value(raw: str) -> str:
    """谓语后的原始尾串→规范值: 截断从句/时间尾饰/冠词引导/大小写归一。空串=抽不出值。"""
    v = (raw or "").strip()
    m = _VALUE_CUT.split(v)
    v = (m[0] if m else v).strip(" .;:,!?、，。；：！？\"'“”‘’()（）[]")
    prev = None
    while prev != v and v:
        prev = v
        v = _VALUE_TAIL_FILLER.sub("", v).strip(" .;:,!?、，。；：！？\"'“”‘’()（）[]")
    v = _VALUE_LEAD_FILLER.sub("", v)
    v = re.sub(r"\s+", " ", v).strip().lower()
    return v


def _subject_of(prefix: str, speaker: str) -> str | None:
    """谓语前缀→subject_key: 代词优先(我→@speaker·三人称代词=None不可解跳过) > 尾端专名。
    注意: 大写"I"是代词不是专名 — 代词判定必须先于大写名判定, 否则双说话人"I"事实互踩。"""
    p = (prefix or "").strip()
    if not p:
        return f"@{speaker or 'user'}"
    toks = [t for t in re.split(r"[\s,，、;；:：]+", p) if t]
    for i in range(len(toks) - 1, -1, -1):
        t = toks[i]
        lt = t.lower().strip(".,;:!?\"'“”‘’()（）。，；：！？·")
        if not lt or re.fullmatch(r"[\d/.\-]+", lt):
            continue
        if lt in _SELF_EN or lt.startswith(("i'", "im", "ive", "ill")):
            return f"@{speaker or 'user'}"
        if lt in _SELF_ZH:
            return f"@{speaker or 'user'}"
        if lt in _3RD_EN:
            return None
        if lt in _3RD_ZH:
            return None
        if lt in _STOP_EN:
            continue
        zm = re.search(r"([\u4e00-\u9fa5]{1,6})$", lt)
        if zm:
            z = zm.group(1)
            if z in _SELF_ZH:
                return f"@{speaker or 'user'}"
            if z in _3RD_ZH:
                return None
            # 剥时间尾饰(中文专名后常黏时间词)
            z = re.sub(r"(最近|现在|目前|去年|今年|上个月|昨天|今天|之前|以后)$", "", z)
            return z if len(z) >= 2 else None
        if t[0].isupper() and len(lt) >= 2:
            # 尾端大写专名: 向前收相邻大写词(≤2个, 复姓/双名), 停在停用词/代词/小写词
            name = [lt]
            j = i - 1
            while j >= 0 and len(name) < 3:
                pt = toks[j].strip(".,;:!?\"'“”‘’()（）。，；：！？")
                pl = pt.lower()
                if pt and pt[0].isupper() and pl not in _STOP_EN and pl not in _SELF_EN \
                        and pl not in _3RD_EN and not pl.startswith(("i'",)):
                    name.append(pl)
                    j -= 1
                else:
                    break
            return " ".join(reversed(name))
        return lt
    return None


def extract_facts(text: str, speaker: str = "") -> list[tuple[str, str, str, str]]:
    """单卡文本→[(subject_key, pred_key, class, value)]。每pred取句内首匹配; 逐句独立。"""
    out: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for sent in _sentences(text):
        hits: dict[str, tuple[str, str, str]] = {}
        for pred, klass, pats in _PREDS:
            for pat in pats:
                m = pat.search(sent)
                if not m:
                    continue
                raw = m.group("v") if "v" in (m.groupdict() or {}) and m.group("v") else sent[m.end():]
                val = norm_value(raw)
                if not val:
                    break  # 该pred本句匹配到但无值(如"got married.")→不产事实
                subj = _subject_of(sent[:m.start()], speaker)
                if subj:
                    hits[pred] = (subj, klass, val)
                break
        for pred, (subj, klass, val) in hits.items():
            k = (subj, pred, klass)
            if k not in seen:
                seen.add(k)
                out.append((subj, pred, klass, val))
    return out


def card_id_of(session: str, dt: str, text: str) -> str:
    """卡身份=aml_service._evidence_id同源同值(sha1(session|dt|text)[:16])。"""
    return hashlib.sha1(f"{session or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def detect_intent(query: str) -> tuple[str, bool]:
    """query意图: ("history"|"current", 显式当前态标记?)。history信号命中→history; 其余→current(默认当前态倾向)。"""
    q = query or ""
    hit_hist = any(rx.search(q) for rx in HISTORY_RES)
    hit_cur = any(rx.search(q) for rx in CURRENT_RES)
    return ("history" if hit_hist else "current"), hit_cur


# ── 持久化(sqlite WAL·与ledger同款纪律) ─────────────────────────────────────
class Knife1:
    def __init__(self, db_path: Path, dict_path: Path = DICT_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS fact_current(
            tenant TEXT NOT NULL, subject TEXT NOT NULL, pred TEXT NOT NULL,
            value TEXT NOT NULL, card_id TEXT NOT NULL, card_dt TEXT NOT NULL,
            obs_seq INTEGER NOT NULL,
            PRIMARY KEY(tenant, subject, pred))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS superseded(
            tenant TEXT NOT NULL, card_id TEXT NOT NULL, pred TEXT NOT NULL,
            superseded_by TEXT NOT NULL, old_value TEXT NOT NULL, new_value TEXT NOT NULL,
            obs_seq INTEGER NOT NULL, PRIMARY KEY(tenant, card_id, pred))""")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS conflicts(
            tenant TEXT NOT NULL, subject TEXT NOT NULL, pred TEXT NOT NULL,
            card_a TEXT NOT NULL, card_b TEXT NOT NULL,
            value_a TEXT NOT NULL, value_b TEXT NOT NULL, obs_seq INTEGER NOT NULL,
            PRIMARY KEY(tenant, subject, pred, card_a, card_b))""")
        self._conn.commit()
        self._lock = threading.Lock()
        self.n_obs = 0     # 观察卡数(累计·运行期)
        self.n_sup = 0     # 新supersede对数
        self.n_conf = 0    # 新conflict对数
        self.n_err = 0     # 单卡观察失败数(fail-soft计数)

    # ── ADD相 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]]) -> None:
        """build_cards产物逐卡观察。单卡失败只计数不抛(fail-soft·绝不影响add成功语义)。"""
        for c in cards or []:
            try:
                self._observe_card(str(tenant), c)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("knife1 observe card FAILED tenant=%s err=%s", tenant, type(exc).__name__)

    def _observe_card(self, tenant: str, c: dict[str, Any]) -> None:
        text = str(c.get("text", ""))
        if not text:
            return
        cid = card_id_of(str(c.get("sess", "")), str(c.get("dt", "")), text)
        speaker = str(c.get("speaker", ""))
        for subj, pred, klass, val in extract_facts(text, speaker):
            self._record(tenant, subj, pred, klass, val, cid, str(c.get("dt", "")))

    def _record(self, tenant: str, subject: str, pred: str, klass: str, value: str,
                cid: str, card_dt: str) -> None:
        with self._lock:
            seq = time.time_ns()
            row = self._conn.execute(
                "SELECT value, card_id, card_dt FROM fact_current WHERE tenant=? AND subject=? AND pred=?",
                (tenant, subject, pred)).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO fact_current VALUES(?,?,?,?,?,?,?)",
                    (tenant, subject, pred, value, cid, card_dt, seq))
                self._conn.commit()
                return
            old_val, old_cid, old_dt = str(row[0]), str(row[1]), str(row[2])
            if old_cid == cid or old_val == value:
                return  # 同卡重放/同值 → 幂等no-op
            if card_dt and old_dt and card_dt < old_dt:
                return  # 乱序防御: 迟到的旧语句/换rid重放 → 绝不回滚当前态(法典:新值覆盖单向)
            if klass == "supersede":
                # D1: 旧卡排除标记(旧卡保留不删) + 当前态推进
                self._conn.execute(
                    "INSERT OR REPLACE INTO superseded VALUES(?,?,?,?,?,?,?)",
                    (tenant, old_cid, pred, cid, old_val, value, seq))
                self._conn.execute(
                    "INSERT OR REPLACE INTO fact_current VALUES(?,?,?,?,?,?,?)",
                    (tenant, subject, pred, value, cid, card_dt, seq))
                self.n_sup += 1
                log.info("knife1 supersede tenant=%s pred=%s old=%s.. new=%s..", tenant, pred,
                         old_cid[:8], cid[:8])
            else:
                # D2: preference共存 → conflict对(双卡都不排除); 当前态仅按dt单向推进
                a, b = sorted([old_cid, cid])
                va, vb = (old_val, value) if old_cid == a else (value, old_val)
                self._conn.execute(
                    "INSERT OR IGNORE INTO conflicts VALUES(?,?,?,?,?,?,?,?)",
                    (tenant, subject, pred, a, b, va, vb, seq))
                self._conn.execute(
                    "INSERT OR REPLACE INTO fact_current VALUES(?,?,?,?,?,?,?)",
                    (tenant, subject, pred, value, cid, card_dt, seq))
                self.n_conf += 1
                log.info("knife1 conflict tenant=%s pred=%s pair=%s../%s..", tenant, pred, a[:8], b[:8])
            self._conn.commit()

    # ── SEARCH相 ──
    def superseded_ids(self, tenant: str, card_ids: list[str]) -> set[str]:
        out: set[str] = set()
        ids = [c for c in card_ids if c]
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                q = ("SELECT card_id FROM superseded WHERE tenant=? AND card_id IN ("
                     + ",".join("?" * len(chunk)) + ")")
                for r in self._conn.execute(q, (tenant, *chunk)):
                    out.add(str(r[0]))
        return out

    def extra_fetch(self, tenant: str) -> int:
        """超采补偿量=该租户superseded卡数(封顶128) — 排除后仍能按top_k填满。"""
        try:
            with self._lock:
                n = int(self._conn.execute(
                    "SELECT COUNT(*) FROM superseded WHERE tenant=?", (tenant,)).fetchone()[0])
            return min(n, 128)
        except Exception:  # noqa: BLE001
            return 0

    def filter_evidence(self, tenant: str, query: str, data: list[dict[str, Any]],
                        top_k: int) -> tuple[list[dict[str, Any]], str, int]:
        """返回(过滤后data, intent, 排除数)。history=全量+dt升序(C3禁回退); current=排除superseded。"""
        intent, _cur_mark = detect_intent(query)
        if intent == "history":
            return sorted(data, key=lambda d: str(d.get("created_at") or "9999")), intent, 0
        sup = self.superseded_ids(tenant, [str(d.get("id", "")) for d in data])
        if not sup:
            return data, intent, 0
        kept = [d for d in data if str(d.get("id", "")) not in sup]
        return kept, intent, len(data) - len(kept)

    # ── 运维 ──
    def stats(self) -> dict[str, Any]:
        with self._lock:
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "fact_current": g("SELECT COUNT(*) FROM fact_current"),
                "superseded": g("SELECT COUNT(*) FROM superseded"),
                "conflicts": g("SELECT COUNT(*) FROM conflicts"),
                "obs_cards_total": self.n_obs, "supersede_total": self.n_sup,
                "conflict_total": self.n_conf, "obs_err_total": self.n_err}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部knife1状态行(organ内卡片本体不在本模块管辖, 见交付留痕)。"""
        with self._lock:
            n = 0
            for tbl in ("superseded", "conflicts", "fact_current"):
                cur = self._conn.execute(f"DELETE FROM {tbl} WHERE tenant=?", (tenant,))
                n += cur.rowcount
            self._conn.commit()
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ──────────────────────────
_K1: Knife1 | None = None
_K1_LOCK = threading.Lock()


def get() -> Knife1:
    global _K1
    with _K1_LOCK:
        if _K1 is None:
            _K1 = Knife1(DB_PATH, DICT_PATH)
            log.info("knife1 init db=%s preds=%d hist_signals=%d", _K1.db_path, len(_PREDS), len(HISTORY_RES))
        return _K1


def observe(tenant: str, cards: list[dict[str, Any]]) -> None:
    get().observe(tenant, cards)


def filter_evidence(tenant: str, query: str, data: list[dict[str, Any]],
                    top_k: int) -> tuple[list[dict[str, Any]], str, int]:
    return get().filter_evidence(tenant, query, data, top_k)


def extra_fetch(tenant: str) -> int:
    return get().extra_fetch(tenant)


# ── [刀8.3·D2版本门+冲突注记20261016·62.09战报D2=46.67钉因库膨胀多版本混杂] ──
_D2_HIST_RE = re.compile(
    r"\b(?:used\s+to|before|previously|in\s+the\s+past|history|historical)\b|以前|之前|过去|历史", re.I)


def d2_window_gate(tenant: str, query: str, data: list[dict[str, Any]],
                   top_k: int = 100) -> list[dict[str, Any]]:
    """[刀8.3] D2检索收口(env门BRAINOS_D2_VER·缺省0=AB对照期关·fail-soft原窗直通):
    ①版本门: superseded表在案的旧值卡→稳定沉窗尾(新值前置·零删除=零证据损失·
      历史意图query零触碰); ②冲突注记: 窗内同时命中conflicts对的双卡→注入
      "注意:"开头注记卡(双值原文引用+各自时间=纯摘录零生成·禁4✓·过csan消毒)。"""
    if not data or os.environ.get("BRAINOS_D2_VER", "0") != "1":
        return data
    try:
        k = get()
        ids = {str(d.get("id", "")) for d in data}
        hist_q = bool(_D2_HIST_RE.search(str(query or "")))
        if not hist_q:
            with k._lock:
                sup_rows = k._conn.execute(
                    "SELECT card_id FROM superseded WHERE tenant=?", (tenant,)).fetchall()
            stale = {r[0] for r in sup_rows} & ids
            if stale:
                fresh = [d for d in data if str(d.get("id", "")) not in stale]
                old = [d for d in data if str(d.get("id", "")) in stale]
                data = fresh + old  # 各组内稳定序保留
        # 冲突注记: 双卡同窗才注(单卡窗零噪音)
        if len(ids) >= 2:
            with k._lock:
                cf_rows = k._conn.execute(
                    "SELECT subject, pred, card_a, card_b, value_a, value_b"
                    " FROM conflicts WHERE tenant=? AND card_a IN (%s) AND card_b IN (%s)"
                    % (",".join("?" * len(ids)), ",".join("?" * len(ids))),
                    (tenant, *ids, *ids)).fetchall()
            if cf_rows:
                by_id = {str(d.get("id", "")): d for d in data}
                notes = []
                for subj, pred, ca, cb, va, vb in cf_rows[:2]:  # ≤2条·防注记灌窗
                    da = str(by_id.get(ca, {}).get("created_at", ""))[:10] or "?"
                    db_ = str(by_id.get(cb, {}).get("created_at", ""))[:10] or "?"
                    notes.append(
                        f"注意: 关于{subj}的{pred}存在多条不同时期的记录 — "
                        f"记录一({da}): «{str(va)[:60]}» 与 记录二({db_}): «{str(vb)[:60]}»，"
                        "请按时间先后与提问意图取用")
                if notes:
                    import hashlib as _h
                    nid = _h.sha1(f"{tenant}|d2note|{cf_rows[0][2]}|{cf_rows[0][3]}".encode()).hexdigest()[:16]
                    note_card = {"id": nid, "content": " ".join(notes), "created_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                    if nid not in ids and len(data) < max(int(top_k), 1):
                        data = [note_card] + data  # top_k契约硬保证: 满窗弃注记
        return data
    except Exception as exc:  # noqa: BLE001 — 任何异常=原窗直通(零回归)
        logging.getLogger("k1").error("d2 gate FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data
