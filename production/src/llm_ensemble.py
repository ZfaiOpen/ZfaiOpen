# llm_ensemble.py — M8 检索期 LLM 集成器(轨道I·20261025)·三件套: expand + hyde + filter
# ═══════════════════════════════════════════════════════════════════════════════
# 定位: 打"纯描述 query"检索盲区(方言检测盲区42%·该队列增益0.198 vs 0.087)的最高天花板
# 武器。三个独立子开关各管一件, 总门 BRAINOS_LLM_ENS(缺省0=关·关=字节直通零外部调用):
#   ① expand(BRAINOS_ENS_EXPAND): query→1次LLM→2条变体检索式(q1=错误签名/技术细节聚焦·
#      q2=功能/意图聚焦·温度0·max_tokens 120·输出JSON["q1","q2"])→每条变体走完整检索路径
#      (k13.recall_by_fingerprint + memory_service._tenant_fetch 语义)→与原query结果RRF
#      融合(复用B轨RRF惯例k=60)。变体里幻觉路径无害: 指纹匹配不上就零行(k13倒排精确IN)。
#   ② hyde(BRAINOS_ENS_HYDE): query→1次LLM→假想记忆卡(≤120 token·英文·traj卡形: 问题+
#      根因+修法概述)→_tenant_fetch(hyde_text)取语义近邻→RRF第三榜。仅描述型query触发
#      (is_coding(q)==False ∨ 指纹为空才启——编程指纹query不需要HyDE)。假想卡只作检索键
#      永不进窗(红线: 零生成内容入响应)。
#   ③ filter(BRAINOS_ENS_FILTER): 终窗前1次批量调用: query+前12张卡各前200字符→JSON
#      {"keep":[id...]}。护栏: keep数∈[3,12], 越界/解析失败/超时→全部保留(SWEContextBench:
#      选错上下文是负收益→误删风险用保守护栏兜)。
# 时序: expand/hyde 两个LLM线程在 aml_service 检索段 SVC.search 调用前点火·与主检索完全
# 并行(净增时延=收割等待·非LLM全程); 收割先到先用不互等(轮询50ms·共享等待预算)。filter
# 在 coding_window.apply 之后契约自检之前同步跑。全局死线 t0+10s: 超时跳过后续LLM步骤。
#
# 合规(全采G轨已实证惯例·dev_g/LLM_DISTILL_DECISION.md):
#   · 模型硬门: BRAINOS_ENS_MODEL(缺省gpt-4o-mini)配其他模型名→armed()恒False+log.error
#     (官方规则"LLM相关组件须使用gpt-4o-mini"; 配错拒绝启用防不合规误配)。
#   · 请求必带 "stream": false(G/C实测apimart/NewAPI缺省SSE坑); SSE回包防御拼装双保险。
#   · 双端点主备failover: 主 NewAPI http://192.168.28.223:3000/v1 · 备 apimart(键同
#     scripts/llm_keys.json 与 scripts/start_run8094.sh 既有配置·env可覆盖)。
#   · 温度0确定性·单次不重试(Add相时延敏感)·socket级硬超时(urlopen timeout只护连接不护
#     慢读体·G教训)·TTL 600s缓存(同C惯例·温度0同输入同输出·缓存只省时省钱)。
#   · 红线遵守: 仅检索辅助(变体检索/语义近邻/窗口精选)·零答案文本生成·零响应体改写。
# 回滚 = 摘 aml_service 4处挂接 + 删本件(无独立DB·零持久状态·纯进程内)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable

log = logging.getLogger("llm_ensemble")

# ── 门与端点(env·缺省=开箱即用·键取自项目既有配置) ─────────────────────────────
_MASTER_ENV = "BRAINOS_LLM_ENS"
_ARM_ENVS = {"expand": "BRAINOS_ENS_EXPAND", "hyde": "BRAINOS_ENS_HYDE",
             "filter": "BRAINOS_ENS_FILTER"}
_URL_ENV, _KEY_ENV = "BRAINOS_ENS_URL", "BRAINOS_ENS_KEY"
_URL_BAK_ENV, _KEY_BAK_ENV = "BRAINOS_ENS_URL_BAK", "BRAINOS_ENS_KEY_BAK"
_MODEL_ENV = "BRAINOS_ENS_MODEL"
_ALLOWED_MODEL = "gpt-4o-mini"   # 合规硬门(官方: "LLM相关组件须使用gpt-4o-mini")
_DEF_URL = "http://192.168.28.223:3000/v1/chat/completions"      # 主·NewAPI自建中转
_DEF_KEY = "sk-sYfuSHb3KTh3xPkFyrzWIEOuDpwJ9JHoKYZ98kgyHzuRxuoH"
_DEF_URL_BAK = "https://api.apimart.ai/v1/chat/completions"      # 备·apimart公网
_DEF_KEY_BAK = "sk-Tcq8h7vkM9pHhZYjVoAMcO7zWsFiZuSqzw5xP4Tvbq0iq5IP"

# ── 时延硬预算(env化·铁律: LLM净增≤6s p95·全局10s死线超时跳过后续LLM步骤) ─────────
def _f(name: str, dft: float) -> float:
    try:
        v = float(os.environ.get(name, "") or dft)
        return v if v > 0 else dft
    except (TypeError, ValueError):
        return dft


def _i(name: str, dft: int) -> int:
    try:
        return int(os.environ.get(name, "") or dft)
    except (TypeError, ValueError):
        return dft


def DEADLINE_S() -> float:   return _f("BRAINOS_ENS_DEADLINE_S", 10.0)   # 全局死线(自请求t0)
def WAIT_S() -> float:       return _f("BRAINOS_ENS_WAIT_S", 4.0)        # 收割共享等待预算
def LLM_TIMEOUT_S() -> float: return _f("BRAINOS_ENS_TIMEOUT_S", 4.5)     # 单次调用链预算(主备共分)
def FILTER_TIMEOUT_S() -> float: return _f("BRAINOS_ENS_FILTER_TIMEOUT_S", 4.0)
def TFETCH_K() -> int:       return max(5, _i("BRAINOS_ENS_TFETCH_K", 40))   # _tenant_fetch取深
def RRF_K() -> float:        return _f("BRAINOS_ENS_RRF_K", 60.0)        # B轨RRF惯例k=60
def W_ENS() -> float:        return _f("BRAINOS_ENS_RRF_WENS", 1.0)      # 变体/假想榜权重
def W_KEEP() -> float:       return _f("BRAINOS_ENS_RRF_WKEEP", 1.0)     # 原窗保序权重
def FILTER_N() -> int:       return max(4, _i("BRAINOS_ENS_FILTER_N", 12))
def FILTER_CHARS() -> int:   return max(60, _i("BRAINOS_ENS_FILTER_CHARS", 200))
def FILTER_MIN() -> int:     return max(1, _i("BRAINOS_ENS_FILTER_MIN", 3))
def VARIANTS() -> int:       return max(1, min(2, _i("BRAINOS_ENS_EXPAND_VARIANTS", 2)))
_CACHE_TTL_S = 600.0         # 同C/G惯例(温度0确定性·缓存只省时省钱)

# ── stats 计数器全套(观测用·零行为) ────────────────────────────────────────────
_STATS_LOCK = threading.Lock()
_STATS = {name: {"calls": 0, "ok": 0, "parse_fail": 0, "guard": 0, "timeout": 0,
                 "skip_gate": 0, "skip_cond": 0, "skip_deadline": 0, "fallback": 0,
                 "ms_total": 0, "lists_fused": 0, "cards_added": 0, "removed": 0}
          for name in _ARM_ENVS}
_MODEL_SEEN = [""]


def stats() -> dict[str, Any]:
    """三臂调用计数器深拷贝(零行为)。calls/ok/parse_fail/guard(护栏越界回退)/
    timeout/skip_gate(子开关关)/skip_cond(hyde编程指纹不触发)/skip_deadline(死线跳过)/
    fallback/ms_total + lists_fused/cards_added(expand·hyde)/removed(filter)。"""
    with _STATS_LOCK:
        out = {k: dict(v) for k, v in _STATS.items()}
        out["master_on"] = _master_on()
        out["model_allowed"] = _ALLOWED_MODEL
        out["model_seen"] = _MODEL_SEEN[0]
        return out


def _stat(arm: str, key: str, n: int = 1) -> None:
    with _STATS_LOCK:
        _STATS[arm][key] += n


# ── 门语义 ─────────────────────────────────────────────────────────────────────
def _master_on() -> bool:
    try:
        return str(os.environ.get(_MASTER_ENV, "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


def armed() -> bool:
    """总门: BRAINOS_LLM_ENS=1 且 模型名==gpt-4o-mini。模型误配→log.error+恒False
    (拒绝启用=本模块等效不存在·合规硬门)。门关零外部调用零行为。"""
    if not _master_on():
        return False
    try:
        model = (os.environ.get(_MODEL_ENV, "") or _ALLOWED_MODEL).strip()
    except Exception:  # noqa: BLE001
        return False
    if model != _ALLOWED_MODEL:
        log.error("llm_ensemble REFUSED: BRAINOS_ENS_MODEL=%r != %r "
                  "(academic-board compliance: LLM components must be gpt-4o-mini)",
                  model, _ALLOWED_MODEL)
        return False
    return True


def arm_on(name: str) -> bool:
    """子开关: 总门开 ∧ 对应BRAINOS_ENS_<ARM>=1。"""
    if not armed():
        return False
    try:
        return str(os.environ.get(_ARM_ENVS[name], "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


# ── LLM 内核(全采G轨惯例: stream:false·温度0·socket级硬超时·SSE防御·主备failover) ──
def _ll_call(sys_prompt: str, user_prompt: str, max_tokens: int,
             budget_s: float, arm: str) -> str:
    """单次调用链: 主端点(≤min(3.0,budget))失败且剩余≥1s→备端点(剩余全给)。
    任何失败=""(fail-soft·不重试)。温度0·stream:false必带·socket级双保险硬超时。"""
    import socket as _s
    import urllib.request
    model = (os.environ.get(_MODEL_ENV, "") or _ALLOWED_MODEL).strip()
    url = (os.environ.get(_URL_ENV, "") or _DEF_URL).strip()
    key = (os.environ.get(_KEY_ENV, "") or _DEF_KEY).strip()
    url_bak = (os.environ.get(_URL_BAK_ENV, "") or _DEF_URL_BAK).strip()
    key_bak = (os.environ.get(_KEY_BAK_ENV, "") or _DEF_KEY_BAK).strip()
    payload = json.dumps({
        "model": model,
        "temperature": 0,          # 确定性(缓存有效性的构造保证)
        "max_tokens": int(max_tokens),
        "stream": False,           # G/C实测: apimart/NewAPI缺省SSE·必须显式关
        "messages": [{"role": "system", "content": sys_prompt},
                     {"role": "user", "content": user_prompt}],
    }).encode("utf-8")
    t0 = time.time()
    tries = [(url, key, min(3.0, budget_s))]
    if url_bak and budget_s - 3.0 >= 1.0:
        tries.append((url_bak, key_bak, budget_s - 3.0))
    out = ""
    for _u, _k, _to in tries:
        if _to <= 0 or time.time() - t0 >= budget_s:
            break
        req = urllib.request.Request(_u, data=payload, method="POST",
                                     headers={"Content-Type": "application/json",
                                              "Authorization": "Bearer " + _k})
        _stat(arm, "calls")
        try:
            # socket级硬超时双保险(G教训): urlopen(timeout=)只护连接/响应头,
            # 慢响应读体不吃该参数——时延契约必须在socket层兜住。
            _def_t = _s.getdefaulttimeout()
            _s.setdefaulttimeout(_to)
            try:
                with urllib.request.urlopen(req, timeout=_to) as resp:
                    raw = resp.read().decode("utf-8", "replace")
            finally:
                _s.setdefaulttimeout(_def_t)
            out = _extract_text(raw)
            if out:
                _remember_model(raw)
                _stat(arm, "ok")
                break
            _stat(arm, "parse_fail")
        except Exception as exc:  # noqa: BLE001 — 超时/网络/解析全fail-soft
            if isinstance(exc, TimeoutError) or "timed out" in str(exc).lower():
                _stat(arm, "timeout")
            log.info("llm_ensemble %s call fail-soft endpoint=%s err=%s",
                     arm, str(_u)[:40], type(exc).__name__)
        out = ""
    with _STATS_LOCK:
        _STATS[arm]["ms_total"] += int((time.time() - t0) * 1000)
    return out


def _extract_text(raw: str) -> str:
    """回包→文本(标准JSON·SSE防御拼装双保险·G同款)。"""
    try:
        if raw.lstrip().startswith("data:"):  # SSE防御: 网关无视stream:false时逐行拼装
            parts: list[str] = []
            for ln in raw.splitlines():
                ln = ln.strip()
                if not ln.startswith("data:") or ln == "data: [DONE]":
                    continue
                try:
                    d = json.loads(ln[5:].strip())
                except Exception:  # noqa: BLE001
                    continue
                ch = ((d.get("choices") or [{}])[0].get("delta") or {})
                parts.append(str(ch.get("content") or ""))
            return "".join(parts).strip()
        d = json.loads(raw)
        return str((((d or {}).get("choices") or [{}])[0]
                    .get("message") or {}).get("content") or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _remember_model(raw: str) -> None:
    """合规审计留痕: 首次成功记录回包model指纹(观测用)。"""
    try:
        with _STATS_LOCK:
            if _MODEL_SEEN[0]:
                return
        d = json.loads(raw)
        m = str((d or {}).get("model") or "")
        with _STATS_LOCK:
            if not _MODEL_SEEN[0]:
                _MODEL_SEEN[0] = m
    except Exception:  # noqa: BLE001
        pass


# ── TTL 缓存(同C惯例600s·进程内·上限粗剪) ─────────────────────────────────────
_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_LOCK = threading.Lock()


def _cached(arm: str, key: str, fn: Callable[[], str]) -> str:
    now = time.time()
    ck = f"{arm}|{key}"
    with _CACHE_LOCK:
        hit = _CACHE.get(ck)
        if hit and now - hit[0] < _CACHE_TTL_S:
            return hit[1]
        if len(_CACHE) > 256:
            _CACHE.clear()
    got = fn()
    with _CACHE_LOCK:
        _CACHE[ck] = (now, got)
    return got


# ── prompts(全英文·JSON-only输出·温度0) ───────────────────────────────────────
_EXP_SYS = (
    "You optimize retrieval queries for a developer memory search engine over past "
    "coding-session memory cards. Given a user query, produce exactly 2 alternative "
    "search queries as a JSON array of 2 strings: the first focuses on concrete error "
    "signatures, technical details, library and symbol names; the second focuses on "
    "the functionality and user intent in plain words. Keep each under 30 words, in "
    "the same natural language as the query. Output ONLY the JSON array, no prose.")
_EXP_USER = "Query: {q}\nJSON array:"
_HYDE_SYS = (
    "You write one hypothetical memory card for a retrieval system. Given a developer's "
    "query, imagine the most relevant memory card from a past debugging session that "
    "would help answer it. Write in English, at most 120 tokens, exactly this shape: "
    "'Problem: <one line>. Root cause: <one line>. Fix: <one line overview>.' Use only "
    "generic engineering knowledge; do NOT invent file paths, function names, class "
    "names, version numbers or project-specific identifiers. Output the card text only.")
_HYDE_USER = "Query: {q}\nHypothetical memory card:"
_FILTER_SYS = (
    "You select relevant context cards for answering a developer query about their past "
    "work. You get the query and numbered candidates (id: snippet). Return STRICT JSON "
    '{\"keep\":[\"<id>\",...]} listing only ids of candidates that could contain useful '
    "evidence for the query. Keep at least 3 and at most 12 ids. Output JSON only.")
_FILTER_USER = "Query: {q}\nCandidates:\n{cands}"


# ── 解析与护栏 ──────────────────────────────────────────────────────────────────
def _strip_fence(t: str) -> str:
    t = (t or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[A-Za-z0-9]*\s*", "", t)
        t = re.sub(r"\s?```\s*$", "", t)
    return t.strip()


def _parse_variants(txt: str) -> list[str]:
    """expand回包→≤2条变体检索式(剥fence/容错抽数组/空白归一/300字符钳)。"""
    if not txt:
        return []
    t = _strip_fence(txt)
    v: Any = None
    try:
        v = json.loads(t)
    except Exception:  # noqa: BLE001
        m = re.search(r"\[.*\]", t, re.S)
        if m:
            try:
                v = json.loads(m.group(0))
            except Exception:  # noqa: BLE001
                return []
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v:
        s = re.sub(r"\s+", " ", str(x or "").strip())[:300]
        if len(s) >= 3:
            out.append(s)
        if len(out) >= VARIANTS():
            break
    return out


def _parse_hyde(txt: str) -> str:
    """hyde回包→假想卡文本(800字符钳·只作检索键永不进窗·幻觉无害=语义近邻兜底)。"""
    t = re.sub(r"\s+", " ", _strip_fence(txt or ""))[:800]
    return t if len(t) >= 24 else ""


def _parse_keep(txt: str, shown_ids: list[str]) -> set[str] | None:
    """filter回包→keep集合(id精确匹配+序号容错)。None=解析失败(回退全保留)。"""
    if not txt:
        return None
    try:
        v = json.loads(_strip_fence(txt))
    except Exception:  # noqa: BLE001
        m = re.search(r"\{.*\}", txt, re.S)
        if not m:
            return None
        try:
            v = json.loads(m.group(0))
        except Exception:  # noqa: BLE001
            return None
    if not isinstance(v, dict):
        return None
    keep = v.get("keep")
    if not isinstance(keep, list):
        return None
    shown = set(shown_ids)
    idx_by = {str(i + 1): iid for i, iid in enumerate(shown_ids)}
    out: set[str] = set()
    for x in keep:
        s = str(x or "").strip().strip('"').strip("'")
        if s in shown:
            out.add(s)
        elif s in idx_by:  # 序号容错(LLM抄16位hex易错位)
            out.add(idx_by[s])
    return out


# ── handle 与点火 ──────────────────────────────────────────────────────────────
class _Handle:
    """一次search的集成器状态: 两个LLM worker线程 + 产出榜 + 死线。"""

    def __init__(self, query: str, tenant: str, top_k: int, t0: float,
                 svc: Any, eid_fn: Callable[[dict], str]) -> None:
        self.query = str(query or "")
        self.tenant = str(tenant or "default")
        self.top_k = int(top_k)
        self.t0 = float(t0)
        self.svc = svc
        self.eid_fn = eid_fn
        self.threads: list[tuple[str, threading.Thread]] = []
        self.expand_lists: list[list[dict[str, Any]]] = []   # 每变体一榜
        self.hyde_list: list[dict[str, Any]] = []
        self._done = threading.Event()

    def expired(self) -> bool:
        return time.time() > self.t0 + DEADLINE_S()


def _hyde_eligible(query: str) -> bool:
    """HyDE触发条件: 非编程query ∨ 零指纹(编程指纹query不需要HyDE)。fail-soft=True。"""
    coding = False
    try:
        import coding_window as cw  # noqa: PLC0415 — 纯函数·无DB
        coding = bool(cw.is_coding(query))
    except Exception:  # noqa: BLE001
        coding = False
    if not coding:
        return True
    try:
        import knife13_repo as k13  # noqa: PLC0415
        fp = k13.fingerprint(query) or {}
        return not any(fp.get(k) for k in ("paths", "errsig", "tests", "cmds", "symbols"))
    except Exception:  # noqa: BLE001
        return True


def fire(query: str, tenant: str, top_k: int, t0: float, svc: Any,
         eid_fn: Callable[[dict], str]) -> "_Handle | None":
    """点火: 总门+子开关检查→expand/hyde两个daemon线程并行起跑(LLM净耗时与主检索重叠)。
    门关/无条件→None(零开销)。返回handle供harvest/apply_filter使用。"""
    if not armed() or not str(query or "").strip():
        return None
    h = _Handle(query, tenant, top_k, t0, svc, eid_fn)
    if arm_on("expand"):
        _th = threading.Thread(target=_expand_worker, args=(h,),
                               name="ens-expand", daemon=True)
        h.threads.append(("expand", _th))
        _th.start()
    else:
        _stat("expand", "skip_gate")
    if arm_on("hyde"):
        if _hyde_eligible(h.query):
            _th = threading.Thread(target=_hyde_worker, args=(h,),
                                   name="ens-hyde", daemon=True)
            h.threads.append(("hyde", _th))
            _th.start()
        else:
            _stat("hyde", "skip_cond")  # 编程指纹query: 不需要HyDE
    else:
        _stat("hyde", "skip_gate")
    # filter-only臂也要返回handle(无线程→harvest即空转·apply_filter可用)
    return h if (h.threads or arm_on("filter")) else None


# ── 检索路径(k13指纹直召回 ∪ _tenant_fetch语义·幻觉路径无害) ──────────────────────
def _retrieve(h: _Handle, text: str) -> list[dict[str, Any]]:
    """一条检索式→完整检索路径→统一卡列表[{id,content,created_at}](已去重)。
    ①k13.recall_by_fingerprint: 幻觉路径/符号匹配不上=零行(倒排精确IN·无害);
    ②svc._tenant_fetch(语义): 租户级单语料三通道min-max融合(B轨同语义)。"""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    texts: set[str] = set()
    if not text:
        return out
    try:  # ① 指纹直召回
        import knife13_repo as k13  # noqa: PLC0415
        for r in (k13.recall_by_fingerprint(h.tenant, text, 40) or []):
            cid, ct = str(r.get("id", "")), str(r.get("content", ""))
            if cid and ct and cid not in seen and ct not in texts:
                seen.add(cid)
                texts.add(ct)
                out.append({"id": cid, "content": ct,
                            "created_at": str(r.get("created_at") or r.get("dt") or "")})
    except Exception as exc:  # noqa: BLE001 — 直召回失败=纯语义臂
        log.info("ens retrieve k13 fail-soft err=%s", type(exc).__name__)
    try:  # ② 租户直取语义近邻(organ卡→证据投影→_evidence_id同源)
        cards, _sc = h.svc._tenant_fetch(text, h.tenant, TFETCH_K())
        for c in (cards or []):
            ct = str(c.get("text", ""))
            if not ct or ct in texts:
                continue
            cid = str(h.eid_fn({"session": c.get("sess", ""), "dt": c.get("dt", ""),
                                "text": ct}))
            if cid and cid not in seen:
                seen.add(cid)
                texts.add(ct)
                out.append({"id": cid, "content": ct,
                            "created_at": str(c.get("dt", "") or "")})
    except Exception as exc:  # noqa: BLE001 — 语义臂失败=纯指纹臂
        log.info("ens retrieve tenant_fetch fail-soft err=%s", type(exc).__name__)
    return out


def _expand_worker(h: _Handle) -> None:
    """expand臂: 1次LLM→2变体→每变体完整检索路径→handle.expand_lists(先到先存)。"""
    if h.expired():
        _stat("expand", "skip_deadline")
        return
    budget = min(LLM_TIMEOUT_S(), max(0.8, h.t0 + DEADLINE_S() - time.time()))
    key = hashlib.sha1(f"expv1|{(os.environ.get(_MODEL_ENV, '') or _ALLOWED_MODEL)}|{h.query}"
                       .encode("utf-8")).hexdigest()[:24]
    txt = _cached("expand", key,
                  lambda: _ll_call(_EXP_SYS, _EXP_USER.format(q=h.query[:4000]),
                                   120, budget, "expand"))
    variants = _parse_variants(txt)
    if not variants:
        _stat("expand", "parse_fail" if txt else "fallback")
        return
    lists: list[list[dict[str, Any]]] = []
    for v in variants:
        if h.expired():
            break
        ranked = _retrieve(h, v)
        if ranked:
            lists.append(ranked)
    with _STATS_LOCK:
        h.expand_lists = lists
        _STATS["expand"]["lists_fused"] += 0  # 融合计数在harvest记(避免重复)


def _hyde_worker(h: _Handle) -> None:
    """hyde臂: 1次LLM→假想记忆卡→_tenant_fetch语义近邻→handle.hyde_list。"""
    if h.expired():
        _stat("hyde", "skip_deadline")
        return
    budget = min(LLM_TIMEOUT_S(), max(0.8, h.t0 + DEADLINE_S() - time.time()))
    key = hashlib.sha1(f"hydev1|{(os.environ.get(_MODEL_ENV, '') or _ALLOWED_MODEL)}|{h.query}"
                       .encode("utf-8")).hexdigest()[:24]
    txt = _cached("hyde", key,
                  lambda: _ll_call(_HYDE_SYS, _HYDE_USER.format(q=h.query[:4000]),
                                   120, budget, "hyde"))
    card = _parse_hyde(txt)
    if not card:
        _stat("hyde", "parse_fail" if txt else "fallback")
        return
    ranked = [] if h.expired() else _retrieve(h, card)
    with _STATS_LOCK:
        h.hyde_list = ranked


# ── 收割与融合(k13b后挂接点) ────────────────────────────────────────────────────
def harvest(handle: "_Handle | None", data: list[dict[str, Any]], query: str,
            tenant: str, top_k: int) -> list[dict[str, Any]]:
    """收割expand/hyde先到者(50ms轮询·共享等待预算WAIT_S·全局死线t0+DEADLINE_S·
    先到先用不互等)→新卡过k11墓碑闸(+k2隐私闸可选·与k13b块同口径)→与原窗RRF融合
    (k=60·rank0钉扎·裁回top_k)。handle=None(门关)=data原样直通(同一对象·字节不变)。"""
    if handle is None or not isinstance(data, list) or not data:
        return data
    try:
        deadline = handle.t0 + DEADLINE_S()
        wait_until = min(time.time() + WAIT_S(), deadline)
        fused: set[str] = set()
        # 先到先用不互等: 50ms轮询·谁先完成谁先融合·共享预算耗尽即弃余者。
        # (修20261025: 以fused集合为唯一真源——线程若在is_alive检查与列表重建之间
        # 死亡·旧写法会在重建时静默丢臂(不融合不计数)·现未融合者永不脱离轮询。)
        while True:
            for name, th in handle.threads:
                if name in fused:
                    continue
                th.join(timeout=0.05)
                if not th.is_alive():
                    fused.add(name)
                    _fuse_arm(handle, name, data, query, tenant, top_k)
            if len(fused) >= len(handle.threads):
                break
            if time.time() >= wait_until:  # 预算耗尽仍未完成=跳过(不互等·不拖累已到结果)
                for name, _th in handle.threads:
                    if name not in fused:
                        _stat(name, "timeout" if time.time() >= deadline else "skip_deadline")
                        fused.add(name)
                break
        return data
    except Exception as exc:  # noqa: BLE001 — 收割失败=原窗直通
        log.error("ens harvest FAILED err=%s", type(exc).__name__)
        return data


def _fuse_arm(handle: _Handle, name: str, data: list[dict[str, Any]], query: str,
              tenant: str, top_k: int) -> None:
    """单臂融合: 变体/假想榜∪原窗 RRF(k=60·W榜×原窗·rank0钉扎·稳定序)。
    新卡(不在窗)过k11墓碑闸+k2隐私闸(BRAINOS_K2GATE=1时·k13b同口径)·content去重。"""
    with _STATS_LOCK:
        lists = list(handle.expand_lists) if name == "expand" else \
            ([list(handle.hyde_list)] if handle.hyde_list else [])
    lists = [l for l in lists if l]
    if not lists:
        _stat(name, "fallback")
        return
    k = RRF_K()
    scores: dict[str, float] = {}
    in_win: set[str] = set()
    win_txt: set[str] = set()
    for i, d in enumerate(data):
        did = str(d.get("id", ""))
        scores[did] = scores.get(did, 0.0) + W_KEEP() / (k + i + 1)
        in_win.add(did)
        win_txt.add(str(d.get("content", "")))
    new_items: dict[str, dict[str, Any]] = {}
    n_lists = 0
    for lst in lists:
        n_lists += 1
        for r, item in enumerate(lst):
            iid, ict = str(item.get("id", "")), str(item.get("content", ""))
            if not iid or not ict:
                continue
            scores[iid] = scores.get(iid, 0.0) + W_ENS() / (k + r + 1)
            if iid not in in_win and iid not in new_items and ict not in win_txt:
                new_items[iid] = {"id": iid, "content": ict,
                                  "created_at": str(item.get("created_at", "") or "")}
    new_list = list(new_items.values())
    if new_list:
        try:  # 墓碑闸同口径(k13b直召回同款·审计分支注入卡忽略)
            import knife11_forget as k11  # noqa: PLC0415
            new_list, _d, _ = k11.filter_gate(tenant, query, new_list)
        except Exception:  # noqa: BLE001
            pass
        if os.environ.get("BRAINOS_K2GATE", "0") == "1":
            try:
                import knife2_sensitive as k2  # noqa: PLC0415
                new_list, _ = k2.filter_gate(tenant, query, new_list)
            except Exception:  # noqa: BLE001
                pass
    if not new_list and n_lists:
        _stat(name, "lists_fused", n_lists)  # 榜已在但全是窗内卡/被闸滤空=序仍受RRF影响
        return
    data.extend(new_list)
    head, rest = data[:1], data[1:]  # rank0钉扎(家规·top1不动)
    rest.sort(key=lambda d: -scores.get(str(d.get("id", "")), 0.0))  # stable·零分保原序
    data[:] = (head + rest)[:max(1, int(top_k))]
    _stat(name, "lists_fused", n_lists)
    _stat(name, "cards_added", len(new_list))
    log.info("ens fuse arm=%s lists=%d new=%d win=%d", name, n_lists, len(new_list), len(data))


# ── filter(终窗精滤·cwin后挂接点) ───────────────────────────────────────────────
def apply_filter(handle: "_Handle | None", data: list[dict[str, Any]],
                 query: str) -> list[dict[str, Any]]:
    """终窗LLM精滤: query+前FILTER_N卡各前FILTER_CHARS字符→{"keep":[id]}。
    护栏: keep数∈[FILTER_MIN, min(12, 窗前段数)]·越界/解析失败/超时/死线过期→全部保留。
    只删不增不重排(契约len≤top_k维持·序保持)。handle=None=原窗直通。"""
    if handle is None or not isinstance(data, list) or len(data) < 4:
        if handle is not None:
            _stat("filter", "skip_cond")
        return data
    if not arm_on("filter"):
        _stat("filter", "skip_gate")
        return data
    if handle.expired():
        _stat("filter", "skip_deadline")
        return data
    try:
        n = FILTER_N()
        head, tail = data[:n], data[n:]
        shown_ids = [str(d.get("id", "")) for d in head]
        cands = "\n".join(
            f"{i + 1}. id={sid} : {str(d.get('content', ''))[:FILTER_CHARS()]}"
            for i, (sid, d) in enumerate(zip(shown_ids, head)))
        budget = min(FILTER_TIMEOUT_S(),
                     max(0.8, handle.t0 + DEADLINE_S() - time.time()))
        key = hashlib.sha1(
            f"flt v1|{(os.environ.get(_MODEL_ENV, '') or _ALLOWED_MODEL)}|{query[:2000]}|"
            f"{'|'.join(shown_ids)}".encode("utf-8")).hexdigest()[:24]
        txt = _cached("filter", key,
                      lambda: _ll_call(_FILTER_SYS,
                                       _FILTER_USER.format(q=query[:3000], cands=cands),
                                       160, budget, "filter"))
        keep = _parse_keep(txt, shown_ids)
        cap = min(12, len(shown_ids))
        if keep is None:
            _stat("filter", "parse_fail" if txt else "fallback")
            return data  # 解析失败→全部保留
        if not (FILTER_MIN() <= len(keep) <= cap):
            _stat("filter", "guard")
            return data  # 护栏越界→全部保留(SWEContextBench: 选错上下文=负收益·误删兜底)
        kept = [d for sid, d in zip(shown_ids, head) if sid in keep]
        removed = len(head) - len(kept)
        _stat("filter", "removed", removed)
        log.info("ens filter keep=%d/%d removed=%d tail=%d", len(keep), len(head),
                 removed, len(tail))
        return kept + tail
    except Exception as exc:  # noqa: BLE001 — 精滤失败=原窗直通
        log.error("ens filter FAILED err=%s", type(exc).__name__)
        return data
