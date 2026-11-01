# deep_taskbrief.py — deep_tf: task-index边车点查 + 结构化任务语料TASK BRIEF要素卡
# 法证根因(2026-10-10·SSH 196只读QLOG取证): I1=46.43 官方题"DB task index 289"/"ALF
# task index 86"(win=0)·语料卡'Streaming intra-transfer train trajectory:{"dataset":
# "DB",..}'卡内含"task index N"但query与其余卡文词面不交→organ检索不中; F1=0 官方题
# 未显形·历史档案(Q12)巨幅危机公关综合任务(12轮对话+9 Artifact+30天报告), deep_digest
# (facts/rules拼装)对结构化任务语料覆盖不足。
# 双引擎(边车纪律: 纯stdlib·确定性·fail-soft·零LLM·零题目硬编码):
#   task_index: ADD抽"task index (\d+)"键+卡全文→task_index(tenant,idx,card_text,seq PK
#     ·同idx后写胜); SEARCH"task index N"键匹配→原卡浮顶位1(前缀词优先·win0播种·≤2)。
#   task_brief: 信号词≥3类识别→源文去重入tbrief_src→全源重建"TASK BRIEF: 目标=..;
#     关键数据点=N项(Artifact名); 时间线=..; 干系人=..; 未决=.."→derived(kind="tbrief"
#     ·落前prune旧卡); SEARCH含report/summary/brief/报告/摘要/综合→浮顶位2。
# 与deep_digest并存: digest走cc.overlay_inject通道互不剥夺; 注入去已在窗。回滚: 摘本件+patch_tf.py逆还原(.bak-tf)。
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

log = logging.getLogger("deep_taskbrief")
_IDX_RE = re.compile(r"task[\s_]+index[\"':=\s]*(\d{1,6})", re.IGNORECASE)
_QIDX_RE = re.compile(r"(?:\b(\w{1,16})[\s_]+)?task[\s_]+index[\"':=\s]*(\d{1,6})", re.IGNORECASE)
_BRIEF_Q_RE = re.compile(r"\b(report|summary|summari[sz]e|brief\w*|recap)\b|报告|摘要|总结|综合", re.I)
_TI_Q_RE = re.compile(r"task[\s_]+index", re.IGNORECASE)
_SIGNALS = ("artifact", "deliverable", "milestone", "metric", "kpi", "timeline", "stakeholder",
            "deadline", "report", "报告", "指标", "时间线", "交付", "里程碑", "干系人", "目标", "待办")
_DDL = (
    "CREATE TABLE IF NOT EXISTS task_index(tenant TEXT NOT NULL, idx TEXT NOT NULL,"
    " card_text TEXT NOT NULL, seq INTEGER PRIMARY KEY AUTOINCREMENT)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ti_tidx ON task_index(tenant, idx)",
    "CREATE TABLE IF NOT EXISTS tbrief_src(tenant TEXT NOT NULL, thash TEXT NOT NULL,"
    " sess TEXT, dt TEXT, text TEXT NOT NULL, seq INTEGER PRIMARY KEY AUTOINCREMENT)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_tb_thash ON tbrief_src(tenant, thash)",
)
_ART_RE = re.compile(r"Artifact\s*(?:#\d+)?\s*[:：\-]?\s*[\"'“”]?([A-Za-z][\w &/.\-]{2,47})", re.I)
_NUM_RE = re.compile(r"(?<![\w-])[-+]?\d[\d,]*(?:\.\d+)?")
_ISO_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_DAY_RE = re.compile(r"\bDay\s*(\d{1,3})\b", re.I)
_NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b")
_ROLE_RE = re.compile(r"(?:owner|lead|head|spokesperson|contact|负责人|发言人|联系人)[:：\s]*([A-Z][\w ]{2,28})", re.I)
_OPEN_RE = re.compile(r"pending|tbd|unresolved|awaiting|open item|待定|未决|尚未|待确认", re.I)
_GOAL_RE = re.compile(r"deliverable|final report|submit|deliver|due (?:in|on|by)|提交|交付", re.I)
_STOP_HEAD = {"of", "the", "a", "an", "and", "is", "was", "to", "for", "on", "in", "this", "with"}
_BRIEF_MAX = 900

def _cc() -> Any:
    import card_construct as cc  # 延迟import: 复用_db()连接/_DB_LOCK/store_derived
    return cc

def _ensure(conn: Any) -> None:
    for stmt in _DDL:
        conn.execute(stmt)

def _card3(text: str) -> dict[str, str]:
    """3键注入卡契约(与corpus_fact._card3同形): id/content/created_at。"""
    return {"id": hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
            "content": text, "created_at": ""}

def _signals_of(text: str) -> int:
    low = str(text or "").lower()
    return sum(1 for s in _SIGNALS if s in low)

def observe(tenant: str, cards: list[dict[str, Any]], rid: str) -> int:
    """ADD相钩子(derive_all尾·fail-soft): index键+卡全文入task_index(同idx后写胜);
    信号词≥3类卡入tbrief_src(去重)→有新源才重建TASK BRIEF入derived(kind=tbrief)。"""
    if not tenant or not cards:
        return 0
    try:
        cc = _cc()
        idxs: list[tuple[str, str]] = []
        srcs: list[tuple[str, str, str, str]] = []
        for c in cards:
            text = str(c.get("text", ""))
            seen_i: set[str] = set()
            for m in _IDX_RE.finditer(text):
                if m.group(1) not in seen_i:
                    seen_i.add(m.group(1))
                    idxs.append((m.group(1), text))
            if _signals_of(text) >= 3:
                srcs.append((hashlib.sha1(text.encode("utf-8")).hexdigest()[:16],
                             str(c.get("sess", "")), str(c.get("dt", "")), text))
        if not idxs and not srcs:
            return 0
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            w = sum(conn.execute("INSERT OR REPLACE INTO task_index(tenant,idx,card_text)"
                                 " VALUES(?,?,?)", (tenant, idx, text)).rowcount
                    for idx, text in idxs)
            new_src = sum(conn.execute("INSERT OR IGNORE INTO tbrief_src"
                                       "(tenant,thash,sess,dt,text) VALUES(?,?,?,?,?)",
                                       (tenant, t, s, d, x)).rowcount for t, s, d, x in srcs)
            conn.commit()
        return w + (_rebuild_brief(cc, tenant, rid) if new_src else 0)
    except Exception as exc:  # noqa: BLE001 — 边车失败只丢索引/要素卡·绝不影响add
        log.error("deep_taskbrief observe FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return 0

def _rebuild_brief(cc: Any, tenant: str, rid: str) -> int:
    """全源重建TASK BRIEF(ADD序拼接)→prune旧tbrief→store_derived(kind=tbrief)。"""
    try:  # superseded_by列幂等确保(dsup.ensure_schema进程一次·直连observe时防缺列)
        import deep_supersede as dsup
        dsup.ensure_schema()
    except Exception:  # noqa: BLE001
        pass
    with cc._DB_LOCK:
        conn = cc._db()
        _ensure(conn)
        rows = conn.execute("SELECT text, sess, dt FROM tbrief_src WHERE tenant=?"
                            " ORDER BY seq", (tenant,)).fetchall()
    if not rows:
        return 0
    brief = _build_brief([str(r[0]) for r in rows])
    if not brief:
        return 0
    with cc._DB_LOCK:
        cc._db().execute("DELETE FROM derived WHERE tenant=? AND kind='tbrief'", (tenant,))
        cc._db().commit()
    cc.store_derived(tenant, [{"sess": rows[-1][1], "dia": f"{rid}:tbrief",
                               "dt": rows[-1][2], "speaker": "", "text": brief}], "tbrief")
    return 1

def _build_brief(texts: list[str]) -> str:
    """源文→任务要素卡(正则启发式): 目标/关键数据点/时间线/干系人/未决·<3类信号→""。"""
    blob = "\n".join(str(t or "") for t in texts)
    if _signals_of(blob) < 3:
        return ""
    da: dict[str, str] = {}
    for m in _ART_RE.finditer(blob):
        a = m.group(1).strip(" .,;:-_/\"'“”")
        if a and a.lower().split()[0] not in _STOP_HEAD:
            da.setdefault(a.lower(), a)
    arts = list(da.values())
    low_arts = [a.lower() for a in arts]
    nums = [v for v in dict.fromkeys(m.group(0) for m in _NUM_RE.finditer(blob))
            if len(v.lstrip("+-,")) >= 2]
    isos = sorted(set(_ISO_RE.findall(blob)))
    days = sorted({int(d) for d in _DAY_RE.findall(blob)})
    tls = ([f"{isos[0]}~{isos[-1]}"] if isos else []) + ([f"Day {days[0]}~Day {days[-1]}"] if days else [])
    stakes = [m.group(1).strip(" .,;:-") for m in _ROLE_RE.finditer(blob)]
    stakes = list(dict.fromkeys(s for s in stakes if s))
    for m in _NAME_RE.finditer(blob):
        v = m.group(0).lower()
        if v in low_arts or any(v in a for a in low_arts) or v in [s.lower() for s in stakes]:
            continue
        stakes.append(m.group(0))
        if len(stakes) >= 6:
            break
    sents = [s.strip() for s in re.split(r"[.;!?\n]", blob) if s.strip()]
    goal = next((s[:90] for s in sents if _GOAL_RE.search(s)), "")
    opens = list(dict.fromkeys(s[:70] for s in sents if _OPEN_RE.search(s)))[:3]
    kd = (f"关键数据点={len(nums)}项" + (f"({'; '.join(arts[:9])})" if arts else "")
          + (f" 数字:{'/'.join(nums[:8])}" if nums else ""))
    brief = ("TASK BRIEF: 目标=" + (goal or "未明") + "; " + kd
             + "; 时间线=" + ("; ".join(tls) or "未明") + "; 干系人="
             + (", ".join(stakes[:6]) or "未明") + "; 未决=" + (" ; ".join(opens) or "无"))
    if len(brief) > _BRIEF_MAX:
        cut = brief.rfind("; ", 0, _BRIEF_MAX)
        brief = brief[:cut if cut > 40 else _BRIEF_MAX]
    return brief

def _load_index(tenant: str) -> list[dict[str, str]]:
    """读租户task_index行(seq序·fail-soft→[])。"""
    try:
        cc = _cc()
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            raw = conn.execute("SELECT idx, card_text FROM task_index WHERE tenant=?"
                               " ORDER BY seq", (tenant,)).fetchall()
        return [{"idx": r[0], "card_text": r[1]} for r in raw]
    except Exception as exc:  # noqa: BLE001 — 读失败=零注入
        log.error("deep_taskbrief load_index FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return []

def _load_brief(tenant: str) -> str:
    """最新tbrief卡文本(derived边车·cc租户缓存·fail-soft→"")。"""
    try:
        rows = [d for d in _cc().load_derived(tenant) if d.get("kind") == "tbrief"]
        return str(rows[-1]["text"]) if rows else ""
    except Exception:  # noqa: BLE001
        return ""

def inject(tenant: str, query: str, data: list[dict[str, Any]], top_k: int) -> tuple[list, int]:
    """SEARCH相钩子(fail-soft原窗直通): task index N→键匹配原卡浮顶位1(前缀词优先·
    win0空窗播种·≤2·裸"task index"取最新); 摘要/报告信号→tbrief浮顶位2。rank0不动·
    去已在窗·3键契约·确定性·不裁窗(≤top_k由aml_service尾刀执法)。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        q = str(query or "")
        qm = list(_QIDX_RE.finditer(q))
        brief_q = bool(_BRIEF_Q_RE.search(q))
        ti_q = bool(_TI_Q_RE.search(q))
        if not qm and not brief_q and not ti_q:
            return data, 0
        rows = _load_index(tenant) if (qm or ti_q) else []
        out = list(data)
        existing = {str(d.get("content", "")) for d in data}
        n = 0
        for m in qm:
            if n >= 2:
                break
            pref = (m.group(1) or "").lower()
            cands = [r for r in rows if r["idx"] == m.group(2)]
            if pref:
                cands = [r for r in cands if re.search(rf"\b{re.escape(pref)}", r["card_text"], re.I)] or cands
            if cands and cands[0]["card_text"] not in existing:
                out.insert(min(1, len(out)), _card3(cands[0]["card_text"]))
                existing.add(cands[0]["card_text"])
                n += 1
        if not qm and ti_q and rows and n < 2 and rows[-1]["card_text"] not in existing:
            out.insert(min(1, len(out)), _card3(rows[-1]["card_text"]))  # 裸"task index"→最新
            n += 1
        if brief_q:
            b = _load_brief(tenant)
            if b and b not in existing:
                out.insert(min(2, len(out)), _card3(b))
                n += 1
        return (out, n) if n else (data, 0)
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_taskbrief inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0

def purge_tenant(tenant: str) -> int:
    """测试专用: 清租户task_index+tbrief_src+derived tbrief行(缓存失效)。"""
    try:
        cc = _cc()
        with cc._DB_LOCK:
            conn = cc._db()
            _ensure(conn)
            n = conn.execute("DELETE FROM task_index WHERE tenant=?", (tenant,)).rowcount
            n += conn.execute("DELETE FROM tbrief_src WHERE tenant=?", (tenant,)).rowcount
            n += conn.execute("DELETE FROM derived WHERE tenant=? AND kind='tbrief'", (tenant,)).rowcount
            conn.commit()
        cc._TENANT_CACHE.pop(str(tenant), None)
        return n
    except Exception:  # noqa: BLE001
        return -1
