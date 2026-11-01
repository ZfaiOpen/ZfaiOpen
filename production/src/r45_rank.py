# -*- coding: utf-8 -*-
"""r45_rank.py — U-R45 检索排序层四修(F1-F4·UF 归因报告检索侧问题手术·2026-10-28)

背景(UF 75题盲测归因·~/exam75/analysis/attribution_report.md):
  §3.1 qB 取 problem 前480字符, 开头全是 "Describe the bug" 样板文本, 词面上恰好
       与垃圾卡同布 → 样板词稀释语义信号(检索空窗主因之一)。
  §3.2 同一张 sklearn linear_model/ridge.py 样板卡出现在 23/67 道 failed 的窗内
       → 高频卡对泛化 query 有系统性吸引力, 挤掉真卡。
  §3.3 蒸馏毒卡比时间线卡可读性更好排名更前被照抄(C3 10题); 毒卡声称的文件在目标
       仓库不存在(asgi_djangotoolbar_bug/middleware.py 等) → 存在性机检可过滤大半。
  §4.4 检索迭代: qA/qB 去样板 + 同 repo 卡强加权 + 跨 repo 高频卡降权。

四修(全部 env 门缺省关=恒等直通·零 LLM·零 organ 内部改动·纯应用层):
  F1 BRAINOS_R45_QSTRIP   query 去样板: issue 模板 header 行 + "fix the bug in"类
                          样板前缀在检索前剥离(挡在 organ 检索之前=全链路受益)。
  F2 BRAINOS_R45_REPO     repo 强加权: query 命中 repo 标识(路径首段/包名/GitHub
                          slug)时, 同 repo 卡权重 ×1.5(organ 融合分之后·重排之前)。
  F3 BRAINOS_R45_HFCUT    高频垃圾卡降权: 卡的跨 query 全局入窗计数(跨租户聚合),
                          N 次入窗率超阈 → 权重 ×0.5(ridge 类样板卡主修)。
  F4 BRAINOS_R45_PATHCHK  毒卡文件存在性机检: 蒸馏/英雄体例卡声称的文件路径在 organ
                          对应租户全部卡文本中(除自身外)零出现 → 权重 ×0.3;
                          英雄位(窗头1-3)毒卡 stable 沉底(C3 锚定主修)。

确定性: 纯函数+进程内锁计数; 排序=稳定 sort(同权保原序); 同输入逐字节同输出。
fail-soft: 本模块任何函数不抛(脏输入→原样直通); 调用方 try 兜底双保险。
合规: 日志零内容(只记计数/长度); 不落 query/卡文本。
"""
from __future__ import annotations

import hashlib
import os
import re
import threading
from typing import Any, Callable, Iterable

# ── 门(缺省全 0 = 与在役 arm-final 逐字节等价) ──────────────────────────────
def _on(name: str) -> bool:
    return os.environ.get(name, "0") == "1"


def qstrip_on() -> bool: return _on("BRAINOS_R45_QSTRIP")
def repo_on() -> bool: return _on("BRAINOS_R45_REPO")
def hfcut_on() -> bool: return _on("BRAINOS_R45_HFCUT")
def pathchk_on() -> bool: return _on("BRAINOS_R45_PATHCHK")


# ══ F1: query 去样板 ══════════════════════════════════════════════════════
# header 词典(来源=357 query 逐行频次扫描·仅收纯 header 行, 带内容的行绝不剥)
_TPL_HEADERS = frozenset("""
describe the bug
describe your bug
describe your issue
expected behavior
expected behaviour
expected outcome
expected results
expected result
actual behavior
actual behaviour
actual outcome
actual result
actual results
current behavior
current behaviour
steps to reproduce
steps/code to reproduce
step to reproduce
reproduction steps
reproduction
to reproduce
how to reproduce
minimal complete verifiable example
minimal reproducible example
minimal reproducer
additional context
additional information
anything else we need to know
anything else
environment
environment information
environment info
your environment
your project
your system
system info
system information
operating system
python version
python versions
version report
installed versions
screenshots
screen shots
output
what happened
what did you expect to happen
what is the expected behavior
see also
note
software versions
component versions
built from source
""".split("\n"))

# 剥行后统一小写/去 markdown 修饰再比对; 行内还有别的内容=不匹配=不剥
_TPL_LINE_RES = (
    re.compile(r"^what version of .{1,60} are (you )?using$"),
    re.compile(r"^how did you install .{1,60}$"),
    re.compile(r"^how do you run .{1,60}$"),
    re.compile(r"^do you want to request a (feature|bug)? ?or report a (feature|bug)?$"),
    re.compile(r"^[\w.+-]{1,28} version(s)?$"),          # "<pkg> version" 环境模板行
    re.compile(r"^(os|platform|gcc|clang|java|node|npm|pip|cargo|rustc)[: ]*$"),
)
# "fix the bug in ..." 类样板前缀(query 起始·剥到非样板为止·≤3轮)
_TPL_PREFIX_RES = tuple(re.compile(p, re.I) for p in (
    r"^\s*please\s+(?:help\s+)?(?:me\s+)?fix(?:\s+the)?\s*(?:bug|issue|problem|error)?\s*(?:in|on|:|,)?\s*",
    r"^\s*help\s+me\s+fix\s+(?:the\s+)?(?:bug|issue|problem|error)?\s*(?:in|on|:|,)?\s*",
    r"^\s*(?:i\s+)?(?:want|need|try(?:ing)?|tried)\s+to\s+fix\s+(?:the\s+)?(?:bug|issue|problem|error)?\s*(?:in|on|:|,)?\s*",
    r"^\s*fix\s+(?:the\s+)?(?:bug|issue|problem|error)\s+(?:in|on|with|:|,)\s*",
    r"^\s*fix\s+this\s+(?:bug|issue|problem|error)\s*(?:in|on|:|,)?\s*",
    r"^\s*(?:can|could)\s+you\s+(?:please\s+)?(?:help\s+)?(?:me\s+)?fix\s+(?:the\s+)?(?:bug|issue|problem|error)?\s*(?:in|on|:|,)?\s*",
    r"^\s*(?:how\s+(?:do\s+i|to)\s+)fix\s+(?:the\s+)?(?:bug|issue|problem|error)?\s*(?:in|on|:|,)?\s*",
    r"^\s*resolve\s+(?:the\s+)?(?:bug|issue|problem|error)\s*(?:in|on|:|,)?\s*",
    r"^\s*address\s+the\s+(?:bug|issue|problem)\s+(?:in|on|:|,)?\s*",
))


def _norm_header_line(ln: str) -> str:
    s = ln.strip()
    s = re.sub(r"^#{1,6}\s*", "", s)          # markdown header
    s = s.strip("*_` \t").strip()             # 加粗/斜体/反引号
    s = s.rstrip(":：").strip()
    return re.sub(r"\s+", " ", s).lower()


def strip_boilerplate_ex(query: str) -> tuple[str, int]:
    """F1 主函数: (剥后 query, 剥除处数)。确定性·幂等·零抛。

    规则: ①整行=模板 header(可带 markdown 修饰/冒号)→剥行 ②query 起始样板前缀→剥。
    护栏: 剥后为空或 <max(30, 30%·原长) → 原样返回(防过度剥离·保底语义)。
    """
    q = str(query or "")
    if not q or not qstrip_on():
        return q, 0
    orig = q
    removed = 0
    lines = q.splitlines()
    kept: list[str] = []
    for ln in lines:
        if ln.strip():
            h = _norm_header_line(ln)
            if (h in _TPL_HEADERS
                    or any(rx.match(h) for rx in _TPL_LINE_RES)):
                removed += 1
                continue
        kept.append(ln)
    q = "\n".join(kept)
    # 样板前缀(仅 query 起始·≤3轮)
    for _ in range(3):
        m = None
        for rx in _TPL_PREFIX_RES:
            m = rx.match(q)
            if m:
                break
        if not m:
            break
        q = q[m.end():]
        removed += 1
    q = re.sub(r"\n{3,}", "\n\n", q).strip(" \t")
    # 护栏: 过度剥离→原样
    if not q or len(q) < max(30, 0.3 * len(orig)):
        return orig, 0
    if q == orig:
        return orig, 0
    return q, removed


def strip_boilerplate(query: str) -> str:
    return strip_boilerplate_ex(query)[0]


# ══ F2: repo 标识与同 repo 加权 ═══════════════════════════════════════════
# 文件系统/通用目录噪音(出现在路径首段≠repo 身份)
_SEG_STOP = frozenset("""
www http https com org net io dev home users user tmp var usr opt etc windows
program files data build dist bin lib libs doc docs test tests testing example
examples sample samples script scripts tool tools src source node_modules
site-packages __pycache__ images assets img css js static media logs log cover
coverage html man bench benchmarks github gitlab bitbubble bitbucket raw git
""".split())
_RX_PATH_SEG = re.compile(
    r"(?<![A-Za-z0-9_])([a-z][a-z0-9_]{1,24})[/\\][A-Za-z0-9_.\\/\\-]{1,120}")
_RX_MODULE = re.compile(
    r"(?<![A-Za-z0-9_/.])([a-z][a-z0-9_]{2,24})(?:\.[a-z][a-z0-9_]{1,24}){1,3}")
_RX_GH_SLUG = re.compile(
    r"github\.com/([A-Za-z0-9_.-]{1,40})/([A-Za-z0-9_.-]{1,60})", re.I)
_MARKER_CAP = 12


def repo_marker_set(text: str) -> frozenset:
    """文本→repo 标识集(路径首段/点分模块首段/GitHub slug·小写·≤12·零抛)。
    纯结构抽取零硬编码 repo 表=零题目先验。"""
    try:
        t = str(text or "").lower()
        if not t:
            return frozenset()
        out: set[str] = set()
        for m in _RX_GH_SLUG.finditer(t):
            out.add(m.group(1).lower())
            out.add(m.group(2).split(".")[0].lower())
            if len(out) >= _MARKER_CAP:
                break
        if len(out) < _MARKER_CAP:
            for m in _RX_PATH_SEG.finditer(t):
                seg = m.group(1)
                if seg not in _SEG_STOP and len(seg) >= 3:
                    out.add(seg)
                    if len(out) >= _MARKER_CAP:
                        break
        if len(out) < _MARKER_CAP:
            for m in _RX_MODULE.finditer(t):
                seg = m.group(1)
                if seg not in _SEG_STOP and len(seg) >= 3:
                    out.add(seg)
                    if len(out) >= _MARKER_CAP:
                        break
        return frozenset(out)
    except Exception:  # noqa: BLE001
        return frozenset()


# ══ F3: 跨 query 高频卡全局入窗计数 ═══════════════════════════════════════
_HF_LOCK = threading.Lock()
_HF_HITS: dict[str, int] = {}     # content_hash → 入窗次数(跨 query·跨租户聚合)
_HF_QUERIES = 0                   # 已计数 query 数(入窗率分母)


def _chash(content: str) -> str:
    return hashlib.sha1("".join(str(content or "").split()).encode("utf-8")) \
        .hexdigest()[:16]


def hf_n_min() -> int:
    try:
        return max(2, int(os.environ.get("BRAINOS_R45_HF_N", "6") or "6"))
    except ValueError:
        return 6


def hf_rate() -> float:
    try:
        return min(1.0, max(0.0, float(os.environ.get("BRAINOS_R45_HF_RATE", "0.15") or "0.15")))
    except ValueError:
        return 0.15


def note_served(cards: Iterable[Any]) -> None:
    """终窗卡入窗计数(门关=零开销零写)。调用点=响应前·每 search 恰一次。"""
    global _HF_QUERIES
    if not hfcut_on():
        return
    items = [c for c in (cards or []) if isinstance(c, dict)]
    with _HF_LOCK:
        for d in items:
            ct = str(d.get("content", ""))
            if not ct:
                continue
            k = _chash(ct)
            _HF_HITS[k] = _HF_HITS.get(k, 0) + 1
        _HF_QUERIES += 1


def hf_weight(content: str) -> float:
    """高频卡权重: hits≥N 且 hits≥rate×queries → 0.5, 否则 1.0。"""
    if not hfcut_on():
        return 1.0
    k = _chash(content)
    with _HF_LOCK:
        hits = _HF_HITS.get(k, 0)
        seen = _HF_QUERIES
    if hits >= hf_n_min() and seen > 0 and hits >= hf_rate() * seen:
        return 0.5
    return 1.0


def hf_stats() -> dict:
    with _HF_LOCK:
        return {"queries": _HF_QUERIES, "cards": len(_HF_HITS),
                "top": sorted(_HF_HITS.items(), key=lambda x: -x[1])[:8]}


# ══ F4: 毒卡文件存在性机检 ════════════════════════════════════════════════
# 蒸馏/英雄体例标记(UF归因§3.3: 毒卡="**Bug Symptom:**"蒸馏体例/英雄卡被照抄)
_RX_HERO_PREFIX = re.compile(r"^\s*(?:FIX\[|ANTI\[|COMPR\[|ISSUE-MAP:)")
_CLAIM_MARKERS = ("**bug symptom", "bug symptom:", "changed files:",
                  "changed file:", "files changed:", "```diff", "diff --git")
_RX_ANY_PATH = re.compile(r"\b[A-Za-z0-9_.-]{1,40}/[A-Za-z0-9_.\-/]{0,120}")
_RX_DIFF_HDR = re.compile(r"^(?:--- a/|\+\+\+ b/|diff --git a/)(\S+)", re.M)
_PATH_CAP = 10


def _claimed_paths(text: str) -> list:
    """卡声称的目标仓库文件路径(全路径形态·去自身噪声·≤10·零抛)。"""
    try:
        t = str(text or "")
        if not t:
            return []
        low = t.lower()
        out: list[str] = []
        # diff 头路径(最可靠)
        for m in _RX_DIFF_HDR.finditer(t[:6000]):
            p = m.group(1).strip('"\'`')
            if "/" in p:
                out.append(p)
        # "Changed files:" 声明行
        for mk in ("changed files:", "changed file:", "files changed:"):
            i = low.find(mk)
            if i >= 0:
                seg = t[i + len(mk): i + len(mk) + 400]
                out.extend(m.group(0).strip("\"'`,;")
                           for m in _RX_ANY_PATH.finditer(seg))
        # 全文路径兜底(蒸馏卡正文提及)
        if not out:
            out.extend(m.group(0).strip("\"'`,;")
                       for m in _RX_ANY_PATH.finditer(t[:4000]))
        seen: set[str] = set()
        uniq: list[str] = []
        for p in out:
            p = p.strip("./")
            if len(p) >= 3 and p[:2] in ("a/", "b/"):   # diff header 前缀噪声
                p = p[2:]
            if len(p) < 5 or "/" not in p or "://" in p:
                continue
            if p.lower() not in seen:
                seen.add(p.lower())
                uniq.append(p)
            if len(uniq) >= _PATH_CAP:
                break
        return uniq
    except Exception:  # noqa: BLE001
        return []


def _is_distill_or_hero(text: str) -> bool:
    try:
        t = str(text or "")
        if not t:
            return False
        if _RX_HERO_PREFIX.match(t):
            return True
        low = t[:4000].lower()
        return any(mk in low for mk in _CLAIM_MARKERS)
    except Exception:  # noqa: BLE001
        return False


def path_weight(text: str, corpus_all: str, pcount: dict | None = None,
                nself: dict | None = None, ckey: str = "") -> float:
    """F4 权重: 蒸馏/英雄体例卡 ∧ 声称路径≥1 ∧ 每条路径在租户其余卡文本零出现
    → 0.3, 否则 1.0。corpus_all=租户全部卡文本拼接; 自身排除=精确扣减自拷贝数。
    pcount/nself=跨请求缓存(路径计数/自拷贝数·键含租户)·steady-state零扫描。"""
    try:
        if not pathchk_on() or not corpus_all:
            return 1.0
        t = str(text or "")
        if not _is_distill_or_hero(t):
            return 1.0
        claims = _claimed_paths(t)
        if not claims:
            return 1.0
        tk = ckey or "t"
        n_self = -1
        if nself is not None:
            k = (tk, _chash(t))
            hit = nself.get(k)
            if hit is not None:
                n_self = hit
        if n_self < 0:
            n_self = 0
            if 0 < len(t) < len(corpus_all):
                n_self = corpus_all.count(t)   # 卡全文verbatim自拷贝数(不在corpus=0)
            if nself is not None:
                nself[k] = n_self
        for p in claims:
            cnt = None
            if pcount is not None:
                cnt = pcount.get((tk, p))
            if cnt is None:
                cnt = corpus_all.count(p)
                if pcount is not None:
                    pcount[(tk, p)] = cnt
            # 其余卡文本出现过该路径(总计数−自拷贝贡献) → 文件存在 → 不罚
            if cnt > n_self * t.count(p):
                return 1.0
        return 0.3
    except Exception:  # noqa: BLE001
        return 1.0


# ══ 应用层加权组合(F2×F3×F4·融合后·重排前) ═══════════════════════════════
# F4 corpus/路径计数跨请求缓存(steady-state零扫描·租户卡数变化即失效·有界8租户)
_CC_LOCK = threading.Lock()
_CC_CORPUS: dict[str, tuple[int, str]] = {}     # tenant → (organ卡数, corpus拼接)
_CC_PCOUNT: dict[tuple[str, str], int] = {}     # (tenant, path) → 全corpus出现数
_CC_NSELF: dict[tuple[str, str], int] = {}      # (tenant, 卡hash) → 自拷贝数
_CC_TENANT_CAP = 8


def _corpus_all(tenant: str, corpus_fn: Callable[[], list]) -> str:
    """租户corpus拼接(跨请求缓存·键=organ卡数·变化即重建并清该租户路径缓存)。"""
    n = -1
    try:
        cards = corpus_fn() or []
        n = len([c for c in cards if isinstance(c, dict)])
        with _CC_LOCK:
            hit = _CC_CORPUS.get(tenant)
            if hit is not None and hit[0] == n:
                return hit[1]
            corpus_all = "\n".join(str(c.get("text", ""))
                                   for c in cards if isinstance(c, dict))
            # 失效该租户路径/自拷贝缓存(内容可能已变)·租户数有界
            stale = [k for k in _CC_PCOUNT if k[0] == tenant]
            for k in stale:
                _CC_PCOUNT.pop(k, None)
            for k in [k for k in _CC_NSELF if k[0] == tenant]:
                _CC_NSELF.pop(k, None)
            if len(_CC_CORPUS) >= _CC_TENANT_CAP and tenant not in _CC_CORPUS:
                _CC_CORPUS.pop(next(iter(_CC_CORPUS)))
            _CC_CORPUS[tenant] = (n, corpus_all)
            return corpus_all
    except Exception:  # noqa: BLE001
        return ""


def apply_weights(data: list, query: str,
                  corpus_fn: Callable[[], list] | None = None,
                  tenant: str = "") -> tuple[list, int]:
    """加权重排: 每卡 weight=repo_w×hf_w×path_w, key=(n-i)×weight 稳定降序。
    只动序不动集不改内容; 全权=1.0 时输出序=输入序(逐位等价)。
    返回 (新列表, 位移张数≠0 计数)。任何异常→(原列表, 0)。"""
    try:
        if not isinstance(data, list) or len(data) < 2:
            return data, 0
        f2, f3, f4 = repo_on(), hfcut_on(), pathchk_on()
        if not (f2 or f3 or f4):
            return data, 0
        n = len(data)
        qmarks = repo_marker_set(query) if (f2 and query) else frozenset()
        corpus_all = _corpus_all(str(tenant), corpus_fn) if (f4 and corpus_fn) else ""
        keyed: list[tuple[float, int, Any]] = []
        for i, d in enumerate(data):
            t = str(d.get("content", "")) if isinstance(d, dict) else str(d)
            w = 1.0
            if f2 and qmarks:
                cm = repo_marker_set(t[:4000])
                if cm and (qmarks & cm):
                    w *= 1.5
            if f3:
                w *= hf_weight(t)
            if f4:
                w *= path_weight(t, corpus_all, _CC_PCOUNT, _CC_NSELF, str(tenant))
            keyed.append(((n - i) * w, i, d))
        keyed.sort(key=lambda x: (-x[0], x[1]))   # 稳定: 同分保原序
        out = [d for _, _, d in keyed]
        moved = sum(1 for a, b in zip(out, data) if a is not b)
        return out, moved
    except Exception:  # noqa: BLE001 — fail-soft 原窗直通
        return data, 0


def demote_poison_heroes(data: list,
                         corpus_fn: Callable[[], list] | None = None,
                         tenant: str = "") -> tuple[list, int]:
    """F4b 英雄位毒卡沉底: 窗头(前6位)中蒸馏/英雄体例 ∧ 声称路径全不存在 → stable
    移至窗尾。cw.apply 英雄置顶后收口·只动序不动集·门关/异常=零操作。"""
    try:
        if not pathchk_on() or not isinstance(data, list) or len(data) < 2:
            return data, 0
        if corpus_fn is None:
            return data, 0
        corpus_all = _corpus_all(str(tenant), corpus_fn)
        if not corpus_all:
            return data, 0
        head_bad: set[int] = set()
        for i, d in enumerate(data[:6]):
            t = str(d.get("content", "")) if isinstance(d, dict) else ""
            if t and path_weight(t, corpus_all, _CC_PCOUNT, _CC_NSELF, str(tenant)) < 1.0:
                head_bad.add(i)
        if not head_bad:
            return data, 0
        out = [d for i, d in enumerate(data) if i not in head_bad]
        out += [data[i] for i in sorted(head_bad)]
        return out, len(head_bad)
    except Exception:  # noqa: BLE001
        return data, 0
