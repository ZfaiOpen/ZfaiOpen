"""deep_digest.py — deep_f1: 跨请求聚合摘要master卡(纯stdlib·确定性·fail-soft)
病灶: v3c会话摘要卡(dia=rid:digest)只见本请求事实 — 官方F1考卷把对话拆散在多个
add请求里, 单请求digest答不了"Summarize what you know about Alice/我们聊过什么"。
治法: 每请求digest_text追加进digest_hist(租户隔离·append-only·末行同文不重复
追加=幂等重放安全), 聚合最近≤12条成master卡(dia=rid:master·kind=digest):
  "KNOWLEDGE DIGEST (all sessions): <旧>; ...; <新>"
装配=从最新往旧收(保最新事实·超900字符预算即停)·输出按时间正序·分号边界不切
半句; 单条历史不产master(纯前缀复制=零增益·白占overlay槽)。
落库: 复用card_construct._db()同一连接与DB锁(零新sqlite连接); master落前由调用
方先prune旧master行(防积累)。异常一律返回原digest_text(降级=现状不劣化)。
"""
from __future__ import annotations
from typing import Any

_MASTER_LIKE = "%:master"
_PREFIX = "KNOWLEDGE DIGEST (all sessions): "
_MAX_CHARS = 1400  # v091: 时间线段需要预算(原900)
_MAX_SEGS = 12
_DDL = (
    "CREATE TABLE IF NOT EXISTS digest_hist("
    "tenant TEXT, dt TEXT, seq INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT)",
    "CREATE INDEX IF NOT EXISTS idx_f1_tenant ON digest_hist(tenant)",
)


def _cc() -> Any:
    import card_construct as cc  # 延迟import: 复用_db()连接/_DB_LOCK/_TENANT_CACHE
    return cc


def _ensure_schema_locked(conn: Any) -> bool:
    """调用方已持_DB_LOCK: 建digest_hist+索引(IF NOT EXISTS·幂等)。"""
    try:
        for stmt in _DDL:
            conn.execute(stmt)
        conn.commit()
        return True
    except Exception:  # noqa: BLE001 — fail-soft
        return False


def ensure_schema(conn: Any = None) -> bool:
    """建digest_hist表+idx_f1_tenant索引(幂等)。conn缺省→card_construct._db()。"""
    try:
        cc = _cc()
        c = conn if conn is not None else cc._db()
        with cc._DB_LOCK:
            return _ensure_schema_locked(c)
    except Exception:  # noqa: BLE001
        return False


def append_and_build(tenant: str, digest_text: str, dt: str,
                     conn: Any = None) -> str:
    """本请求digest_text入历史 → 聚合最近≤12条(≤900字符·保最新·时间正序) →
    master文本。异常/历史≤1条/装配为空 → 返回原digest_text(不落master·不劣化)。"""
    try:
        cc = _cc()
        c = conn if conn is not None else cc._db()
        with cc._DB_LOCK:
            _ensure_schema_locked(c)
            row = c.execute("SELECT text FROM digest_hist WHERE tenant=? "
                            "ORDER BY seq DESC LIMIT 1", (tenant,)).fetchone()
            if not (row and row[0] == digest_text):  # 末行同文=重放·不重复追加
                c.execute("INSERT INTO digest_hist(tenant, dt, text) VALUES(?,?,?)",
                          (str(tenant), str(dt), str(digest_text)))
                c.commit()
            rows = c.execute("SELECT dt, text FROM digest_hist WHERE tenant=? "
                             "ORDER BY seq DESC LIMIT ?", (tenant, _MAX_SEGS)).fetchall()
    except Exception:  # noqa: BLE001 — 聚合失败=现状(session digest已在)不劣化
        return digest_text
    try:
        segs = [f"[{str(r[0])[:10]}] {r[1]}" if r and r[0] else str(r[1])
                for r in rows if r and (len(r) < 2 or r[1])]
        if len(segs) <= 1:  # 单请求: master=纯前缀复制·零增益不落
            return digest_text
        budget = _MAX_CHARS - len(_PREFIX)
        picked: list[str] = []
        used = 0
        for s in segs:  # segs最新在前: 保最新装配·旧事实给预算让路
            cost = len(s) + (2 if picked else 0)  # "; "分隔符
            if used + cost > budget:
                break
            picked.append(s)
            used += cost
        if not picked:
            return digest_text
        picked.reverse()  # 输出时间正序(旧→新)
        master = _PREFIX + "; ".join(picked)
        if len(master) > _MAX_CHARS:  # 兜底钳制: 分号边界截·不切半句
            cut = master.rfind("; ", 0, _MAX_CHARS)
            master = master[:cut if cut > len(_PREFIX) else _MAX_CHARS]
        master = _with_llm_summary(str(tenant), master)  # v2调用点: 聚合命中→门开时附LLM叙事摘要(关=原样)
        return master
    except Exception:  # noqa: BLE001
        return digest_text


def prune_master(tenant: str, conn: Any = None) -> int:
    """清本租户旧master行(dia LIKE '%:master'·防积累)·失效load_derived缓存。
    返回删除数(异常=-1·fail-soft)。调用方决定时机(本仓=落新master前)。"""
    try:
        cc = _cc()
        c = conn if conn is not None else cc._db()
        with cc._DB_LOCK:
            cur = c.execute("DELETE FROM derived WHERE tenant=? AND dia LIKE ?",
                            (str(tenant), _MASTER_LIKE))
            c.commit()
        cc._TENANT_CACHE.pop(str(tenant), None)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    except Exception:  # noqa: BLE001
        return -1


# ---------------------------------------------------------------------------
# v2新增: LLM叙事摘要(mem0式"## Summary")·纯stdlib(urllib)·零新顶层import。
# env门BRAINOS_LLM_F1(缺省0=关): 关=一行判定即返·零网络·append_and_build结果与
# v1完全一致。开=聚合master装配成功(聚合型query命中)后, 在装配try内(DB锁外)额外
# 调gpt-4o-mini生成叙事摘要, 以"\n## Summary: "附于master卡文本末 — master卡本
# 就由调用方置于摘要首位, 故摘要效果=插位1(函数签名不可变·不加卡位)。LLM任何
# 失败(网络/超时/5xx/空回包/解析异常)→log.warning+返""→调用方回退原master卡
# (fail-soft·不新增卡·不报错)。llm_summary()亦可独立调用(不落库·不改状态)。
_LLM_ENV = "BRAINOS_LLM_F1"
_LLM_ON = {"1", "true", "yes", "on"}
# [修20261025-4] LLM端点/key/模型改为env可覆盖(缺省原apimart): 实测2026-10-25 apimart
# key已失效("invalid API key"·fail-soft吞掉→## Summary:永不出)。启动脚本用
# BRAINOS_F1_LLM_URL/KEY/MODEL指向有效OpenAI格式端点(当前2dou deepseek)。
import os as _os
_LLM_URL = _os.environ.get("BRAINOS_F1_LLM_URL", "https://api.apimart.ai/v1/chat/completions")
_LLM_KEY = _os.environ.get("BRAINOS_F1_LLM_KEY", ""  # redacted-rotated-20261117)
_LLM_MODEL = _os.environ.get("BRAINOS_F1_LLM_MODEL", "gpt-4o-mini")
_LLM_TIMEOUT_S = 30          # 单次调用超时30s
_LLM_RETRIES = 2             # 最多2次重试(共≤3次·退避0.5s/1.0s)
_LLM_MAX_TOKENS = 300
_LLM_PROMPT_CHARS = 3600     # 送LLM的卡文本上限(防prompt膨胀)
_LLM_SUMMARY_CHARS = 700     # 返回摘要钳制(空格边界截)
_LLM_SYS = (
    "Summarize what is known about the user from the session digest below. "
    "Write a concise third-person profile (<=120 words): key facts, "
    "preferences, decisions, open items. Output the summary text only - "
    "no preamble, no headings, no markdown. Use the same language as the "
    "digest."
)


def _llm_on() -> bool:
    """v2新增: env门BRAINOS_LLM_F1 — 1/true/yes/on(大小写不敏感)=开·其余(含
    缺省0)=关。关门=append_and_build与v1字节级同结果(零LLM调用零改动)。"""
    import os  # 局部import: 关门路径零新import·不改变现有模块加载行为
    try:
        return str(os.environ.get(_LLM_ENV, "0")).strip().lower() in _LLM_ON
    except Exception:  # noqa: BLE001
        return False


def _with_llm_summary(tenant: str, master: str) -> str:
    """v2新增插桩: 聚合master装配成功(聚合型query命中)后调用·位于装配try内
    (DB锁外·不占锁做网络IO)。env门关(缺省0)=原样返回master; 开=调llm_summary
    生成mem0式叙事摘要附于master文本末"\n## Summary: ..."(master卡由调用方置于
    摘要首位→效果=插位1); LLM失败/空回包=原样返回master(fail-soft·不新增卡·
    不报错·本函数自身兜底全部异常)。"""
    try:
        if not _llm_on():
            return master
        narrative = llm_summary(tenant, master)
        if not narrative:
            return master
        return master + "\n## Summary: " + narrative
    except Exception:  # noqa: BLE001 — 兜底fail-soft(绝不影响原digest卡)
        return master


def llm_summary(tenant: str, cards_text: str) -> str:
    """v2新增: gpt-4o-mini叙事摘要(mem0式"## Summary")。env门关→直接返""(零
    import零网络)。超时30s/次·最多2次重试(共≤3次·退避0.5/1.0s)。任何失败→
    log.warning+返""(调用方fail-soft回退原digest卡)·绝不向上抛。成功→摘要文本
    (≤700字符)。tenant仅用于日志关联·不落库。"""
    import json  # 局部import: 关门路径零新import(不改变现有行为)
    import logging
    import time
    import urllib.request
    log = logging.getLogger("deep_digest")
    try:
        if not _llm_on():
            return ""
        text = str(cards_text or "").strip()[:_LLM_PROMPT_CHARS]
        if not text:
            log.warning("llm_summary skip: empty cards_text tenant=%s", tenant)
            return ""
        payload = json.dumps({
            "model": _LLM_MODEL,
            "messages": [
                {"role": "system", "content": _LLM_SYS},
                {"role": "user", "content": text},
            ],
            "temperature": 0.2,
            "max_tokens": _LLM_MAX_TOKENS,
            # [fix_f 20261025] 显式stream:false — 本函数只解析JSON整体回包, 而NewAPI网关
            # 对缺省stream键默认SSE(text/event-stream实测)→JSONDecodeError→fail-soft
            # 恒空摘要。显式false=声明本就假设的非流式语义·零行为外溢。
            "stream": False,
        }).encode("utf-8")
        req = urllib.request.Request(
            _LLM_URL, data=payload, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + _LLM_KEY},
        )
        attempts = 1 + max(0, int(_LLM_RETRIES))
        err = ""
        for i in range(attempts):  # 1次调用+最多2次重试
            try:
                with urllib.request.urlopen(req, timeout=_LLM_TIMEOUT_S) as resp:
                    data = json.loads(resp.read().decode("utf-8", "replace"))
                out = str((((data or {}).get("choices") or [{}])[0]
                           .get("message") or {}).get("content") or "").strip()
                if out:
                    if len(out) > _LLM_SUMMARY_CHARS:  # 摘要钳制·空格边界截
                        cut = out.rfind(" ", 0, _LLM_SUMMARY_CHARS)
                        out = out[:cut if cut > 0 else _LLM_SUMMARY_CHARS].rstrip()
                    log.info("llm_summary ok tenant=%s chars=%d", tenant, len(out))
                    return out
                err = "empty content"
            except Exception as exc:  # noqa: BLE001 — 单次失败→退避后重试
                err = repr(exc)[:200]
            if i + 1 < attempts:
                time.sleep(0.5 * (i + 1))
        log.warning("llm_summary fail-soft tenant=%s attempts=%d err=%s",
                    tenant, attempts, err)
        return ""
    except Exception as exc:  # noqa: BLE001 — 兜底fail-soft(绝不向上抛)
        try:
            logging.getLogger("deep_digest").warning(
                "llm_summary fail-soft(outer) tenant=%s err=%s",
                tenant, repr(exc)[:200])
        except Exception:  # noqa: BLE001
            pass
        return ""
