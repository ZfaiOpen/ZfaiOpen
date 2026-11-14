# ~/exam/memory_service.py — HippocampalRecall 检索器官 AML Cycle 2 合规 Add/Search API 服务件
# ═══════════════════════════════════════════════════════════════════════════════
# 本文件为独立服务件(不属 brainos 主仓, 不随仓 commit), 仅以 import 方式只读复用
# ~/brainos_new/brainos/memory/hippocampal_recall.py 的 HippocampalRecall。
#
# 合规契约 (AML Cycle 2, 官方规则 agentmemoryleaderboard.ai, 2026-10-03 核对):
#   ① 证据只读: /search 仅返回已存证据卡原文四字段 {session, dt, speaker, text}
#      与通用证据充分度分 evidence_score(窗口最高归一融合分, 零题目先验)，
#      严禁返回任何生成/改写的答案文本——答案由评测平台侧生成。
#   ② 禁跨样本状态: 服务内不建任何按题/按样本缓存; 检索路径零写入——
#      全代码不调 organ.reinforce()(评测期一律禁用, 仅限离线生命周期)，
#      不做任何命中回写，样本间互不影响。
#   ③ 零题目硬编码: 通用 Add/Search 内存服务, 不含任何公开题目/数据集特判。
#   ④ 索引时点: ingest 之后需 finalize_ingest() 重建三通道索引(BM25词+字符3gram+语义)
#      ——/add 支持 "finalize": true 显式触发; 批量 add 完成后应显式 finalize 一次;
#      /search 发现存在未 finalize 的脏增量时兜底自动 finalize(保证证据完整性)。
#
# 运行: ~/venvs/brainos/bin/python ~/exam/memory_service.py   (默认 0.0.0.0:8090)
#   环境变量: EXAM_PORT(默认8090) / EXAM_HOST(默认0.0.0.0) / EXAM_USE_EMBED(0=禁语义通道,
#   零外网零模型加载, 供单测与降级场景) / BRAINOS_HOME(默认~/brainos_new) /
#   BRAINOS_SNAPSHOT(=快照目录路径, 有则启动时状态直恢复load——治"重启即全忘") /
#   BRAINOS_API_KEY(B-0 AL 鉴权: 设置后除 GET /health 与 GET /metrics 外全部端点要求
#   "Authorization: Bearer <key>", 缺/错=401; 未设=本地开发模式全放行, 启动日志 WARN) /
#   BRAINOS_TENANT_KEY(B-0 MR 多租户: 可选, 格式 "<tenant>:<key>[,<tenant>:<key>...]";
#   配置后多租户各持独立 key, 服务端以 key 反推 tenant(强制映射), 客户端自报 tenant
#   与映射不符=403, 无有效映射 key=401; 未配置=客户端可选自报 tenant(缺省"default"))
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import functools
import hmac
import logging
import os
import sys
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

BRAINOS_HOME = Path(os.environ.get("BRAINOS_HOME", str(Path.home() / "brainos_new")))
if str(BRAINOS_HOME) not in sys.path:
    sys.path.insert(0, str(BRAINOS_HOME))

from fastapi import Depends, FastAPI, Header, HTTPException, Response  # HTTPException: /snapshot 失败显式报错(R1-PERSIST); Response: /metrics Prometheus 文本
from pydantic import BaseModel, Field

from brainos.memory.hippocampal_recall import HippocampalRecall
from brainos.memory.recall_store import PersistentRecall  # R1-PERSIST: 持久化包装器

USE_EMBED = os.environ.get("EXAM_USE_EMBED", "1") not in ("0", "false", "False")
TOP_K_MIN, TOP_K_MAX = 1, 64


class _AddItem(BaseModel):
    """证据卡六字段; 除 text 外可缺省(服务端归一为空串, 器官跳过无 text 卡)"""
    conv: str = ""
    sess: str = ""
    dia: str = ""
    speaker: str = ""
    text: str = ""
    dt: str = ""


class _AddRequest(BaseModel):
    items: list[_AddItem] = Field(default_factory=list)
    finalize: bool = False
    tenant: str | None = None  # B0-MR: 可选租户(缺省"default"; key映射模式下以key反推为准)


class _SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    conv_scope: str | None = None
    top_k: int = 8
    tenant: str | None = None  # B0-MR: 与 conv_scope 正交组合(tenant>conv>key 三级作用域)


class MemoryService:
    """单进程持一把器官实例; 一把锁串行化 写相(add/finalize) 与 读相(search),
    防并发下索引半建可见与 last_best_score 竞态。评测客户端为单线程, 锁仅为兜底。"""

    def __init__(self) -> None:
        self.organ = HippocampalRecall(use_embed=USE_EMBED)
        self.lock = threading.RLock()
        self._total = 0        # 器官内证据卡总数(ingest_cards 返回值维护, 不窥私有字段)
        self._dirty = False    # 有已 ingest 未 finalize 的增量
        self._snapshot_ts = 0.0  # 最近一次 /snapshot 成功落盘的 Unix 时间戳(从未=0; MT 可观测量规源)
        # R1-PERSIST: 启动快照直恢复(BRAINOS_SNAPSHOT=<快照目录>)——有则load,
        # 损坏fail-soft零卡起(器官内建容错, 绝不因坏快照拒启动)
        snap = os.environ.get("BRAINOS_SNAPSHOT", "")
        if snap:
            man = PersistentRecall(organ=self.organ).load(snap)
            if man:
                self._total = int(man.get("cards", 0))

    def add(self, items: list[_AddItem], finalize: bool, tenant: str = "default") -> dict[str, Any]:
        # B0-MR: 服务层租户命名空间(organ 零改动)——卡片 conv 键统一加前缀 "{tenant}:{conv}",
        # organ 键形如 "{conv}|{dia}" 故单卡键随之租户唯一; 租户串经 _norm_tenant 校验(禁冒号)。
        cards = []
        for x in items:
            d = x.model_dump()
            d["conv"] = f"{tenant}:{d['conv']}"
            cards.append(d)
        with self.lock:
            added = 0
            if cards:
                before = self._total
                self._total = int(self.organ.ingest_cards(cards))
                added = max(0, self._total - before)
            finalized = False
            if finalize:
                self.organ.finalize_ingest()
                self._dirty = False
                finalized = True
            elif cards:
                self._dirty = True
            return {"added": added, "total": self._total, "finalized": finalized}

    def search(self, query: str, conv_scope: str | None, top_k: int,
               tenant: str = "default") -> dict[str, Any]:
        # B0-MR 三级作用域: tenant(租户隔离)>conv(会话隔离)>key(单卡, 见 /forget)。
        # conv_scope 与 tenant 正交组合: 会话作用域=租户前缀+scope 精确单会话;
        # 仅租户作用域=超采后按前缀过滤——organ.search 对不在 _idx 的 conv_scope 会
        # 回退搜全库, 故两条路径都做前缀过滤兜底, 他租户文本零泄漏。
        k = max(TOP_K_MIN, min(TOP_K_MAX, int(top_k)))
        eff_scope = f"{tenant}:{conv_scope}" if conv_scope else None
        with self.lock:
            if self._total and self._dirty:  # 兜底: 脏增量必须可见, 防漏 finalize 丢证据
                self.organ.finalize_ingest()
                self._dirty = False

            def _tenant_only(cs: list[dict]) -> list[dict]:
                return [c for c in cs if str(c.get("conv", "")).split(":", 1)[0] == tenant]

            if eff_scope is not None:
                cards, score = self.organ.search_ex(query, conv_scope=eff_scope, topk=k)
                # 严格限本 conv: organ 对不在 _idx 的 scope 会回退搜全库(且赫布邻接
                # 可能跨 conv 扩展)——精确 conv 过滤同时兜底租户隔离与作用域越界
                cards = [c for c in cards if str(c.get("conv", "")) == eff_scope][:k]
            else:
                total = len(self.organ._cards)
                fetch = min(total, k * 8)  # 超采 8 倍防他租户占满窗口
                cards, score = self.organ.search_ex(query, conv_scope=None, topk=fetch)
                cards = _tenant_only(cards)
                if len(cards) < k and fetch < total:  # 租户命中不足: 全库重搜一次(精确 top-k)
                    cards, score = self.organ.search_ex(query, conv_scope=None, topk=total)
                    cards = _tenant_only(cards)
                cards = cards[:k]
        # 合规①: 只投影四字段原文, 不携带 organ 内部字段(content/key/sal/dup 等),
        # 不做任何文本生成/改写/拼接——证据原样出。
        evidence = [{"session": c.get("sess", ""),
                     "dt": c.get("dt", ""),
                     "speaker": c.get("speaker", ""),
                     "text": c.get("text", "")} for c in cards]
        return {"evidence": evidence, "evidence_score": float(score)}

    def health(self) -> dict[str, Any]:
        # /health 免锁(B-0 AL): 纯 int 属性读 GIL 下原子, 鉴权探活/负载均衡检查
        # 不得被写路径长锁阻塞。
        return {"status": "ok", "cards": self._total}

    def snapshot(self, path: str) -> dict[str, Any]:
        """R1-PERSIST: 全量快照落盘(目录级dump, 非WAL法·与zfaios持久化方案无关)。
        落盘前先 finalize 脏增量(与 /search 兜底同一防线), 防半建索引入快照。"""
        with self.lock:
            if self._total and self._dirty:
                self.organ.finalize_ingest()
                self._dirty = False
            man = PersistentRecall(organ=self.organ).save(path)
        self._snapshot_ts = time.time()  # 落盘成功才记时点, 供 /metrics 量规暴露
        return {"saved": True, "path": str(path), "version": man["version"],
                "cards": man["cards"], "convs": man["convs"], "emb": man["emb"]}

    def tenant_stats(self) -> dict[str, dict[str, int]]:
        """B0-MR: lifecycle/stats 按租户分组计数(服务层从卡片 conv 前缀聚合, organ 零改动)"""
        groups: dict[str, set[str]] = defaultdict(set)
        counts: dict[str, int] = defaultdict(int)
        for c in self.organ._cards.values():
            t = str(c.get("conv", "")).split(":", 1)[0]
            counts[t] += 1
            groups[t].add(str(c.get("conv", "")))
        return {t: {"cards": counts[t], "convs": len(groups[t])}
                for t in sorted(counts)}


SVC = MemoryService()


# ═══════════════ B0-MT-OBSERVABILITY v1: /metrics Prometheus 可观测(零依赖) ═══════════════
# PA 审计硬基础第6维"可观测"收口(TOP 三家全有 metrics/telemetry): 手写 Prometheus 文本格式
# (text/plain; version=0.0.4), 零新依赖(不用 prometheus_client)。采集三件:
#   ① 计数器 brainos_requests_total{endpoint,status} — 纯 ASGI 中间件逐请求采集;
#   ② 直方图 brainos_request_duration_seconds{endpoint} — 每端点定长环采样, 出 p50/p95
#     (summary 量化线)+ _sum/_count + brainos_request_duration_seconds_max 量规;
#   ③ 量规 brainos_cards / brainos_convs / brainos_snapshot_last_timestamp —
#     渲染时从 SVC 现取(GIL 下轻读)。brainos_journal_lines 暂省: recall_store 显式
#     非WAL法, WJ 件未落本服务可见路径, 落地后在此补量规即可(渲染器按传入列表出)。
_METRICS_RING = 1024  # 每端点时长环采样上限(足够近期分布, 内存 O(端点×1024))


class _MetricsCollector:
    """线程安全零依赖采集器: 采集热路径 O(1) 加锁累加, 渲染路径拷贝后出文本。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[tuple[str, str], int] = defaultdict(int)      # (endpoint,status)→n
        self._rings: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=_METRICS_RING))                          # endpoint→时长环
        self._sums: dict[str, float] = defaultdict(float)
        self._ns: dict[str, int] = defaultdict(int)

    def record(self, endpoint: str, status: int, seconds: float) -> None:
        with self._lock:
            self._counts[(endpoint, str(status))] += 1
            self._rings[endpoint].append(seconds)
            self._sums[endpoint] += seconds
            self._ns[endpoint] += 1

    @staticmethod
    def _quantile(sorted_vals: list[float], q: float) -> float:
        if not sorted_vals:
            return 0.0
        i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
        return sorted_vals[i]

    def render(self, gauges: list[tuple[str, str, float]]) -> str:
        """出 Prometheus 文本格式; gauges=[(名, help, 值)], 不在列表即不出现(省略制)。"""
        with self._lock:
            counts = dict(self._counts)
            rings = {ep: sorted(r) for ep, r in self._rings.items()}
            sums, ns = dict(self._sums), dict(self._ns)
        out = ["# HELP brainos_requests_total HTTP 请求总数(按端点×状态码)。",
               "# TYPE brainos_requests_total counter"]
        for (ep, st), n in sorted(counts.items()):
            out.append(f'brainos_requests_total{{endpoint="{ep}",status="{st}"}} {n}')
        out += ["# HELP brainos_request_duration_seconds 请求耗时秒(环采样分位数+累计)。",
                "# TYPE brainos_request_duration_seconds summary"]
        for ep in sorted(rings):
            v = rings[ep]
            out.append(f'brainos_request_duration_seconds{{endpoint="{ep}",quantile="0.5"}} '
                       f'{self._quantile(v, 0.5):.6f}')
            out.append(f'brainos_request_duration_seconds{{endpoint="{ep}",quantile="0.95"}} '
                       f'{self._quantile(v, 0.95):.6f}')
            out.append(f'brainos_request_duration_seconds_sum{{endpoint="{ep}"}} {sums[ep]:.6f}')
            out.append(f'brainos_request_duration_seconds_count{{endpoint="{ep}"}} {ns[ep]}')
        out += ["# HELP brainos_request_duration_seconds_max 环采样窗内请求耗时最大值(秒)。",
                "# TYPE brainos_request_duration_seconds_max gauge"]
        for ep in sorted(rings):
            mx = rings[ep][-1] if rings[ep] else 0.0
            out.append(f'brainos_request_duration_seconds_max{{endpoint="{ep}"}} {mx:.6f}')
        for name, help_txt, val in gauges:
            out += [f"# HELP {name} {help_txt}", f"# TYPE {name} gauge", f"{name} {val:.0f}"]
        return "\n".join(out) + "\n"


METRICS = _MetricsCollector()

# 未匹配路由不落原始 path(防标签基数爆炸): 已知路径白名单兜底, 其余归 "unmatched"
_KNOWN_PATHS = frozenset({"/add", "/search", "/snapshot", "/health", "/metrics",
                          "/forget", "/lifecycle/dream", "/lifecycle/stats"})


class _ObservabilityMiddleware:
    """纯 ASGI 中间件(非 BaseHTTPMiddleware, 零额外线程/队列): 逐 http 请求记
    endpoint×status 计数与耗时; 响应未 start 即异常时按 500 计。不改请求/响应流。"""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":  # lifespan 等非 http 作用域直通
            await self.app(scope, receive, send)
            return
        t0 = time.perf_counter()
        status = {"code": 500}

        async def _send(message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, _send)
        finally:
            route = scope.get("route")  # Starlette/FastAPI 路由命中后回填 scope["route"]
            ep = getattr(route, "path", None)
            if ep is None:
                raw = scope.get("path", "")
                ep = raw if raw in _KNOWN_PATHS else "unmatched"
            METRICS.record(ep, status["code"], time.perf_counter() - t0)


def _metrics_gauges() -> list[tuple[str, str, float]]:
    """量规现值(渲染时取, 零缓存): cards/convs/snapshot 时点。
    convs: list(dict.values()) 为单条 C 级原子物化, 后续集合推导在物化副本上, 免锁安全。"""
    values = list(SVC.organ._cards.values())
    convs = len({str(c.get("conv", "")) for c in values})
    return [
        ("brainos_cards", "当前证据卡总数(与 /health cards 同源)。", float(SVC._total)),
        ("brainos_convs", "当前会话(distinct conv)总数。", float(convs)),
        ("brainos_snapshot_last_timestamp",
         "最近一次 /snapshot 成功落盘 Unix 时间戳(从未=0)。", float(SVC._snapshot_ts)),
    ]


# ═══════════════ B0-AL-AUTH+LOCK v1: API 鉴权 + 端点级并发锁 ═══════════════
# 鉴权(P0 "服务无鉴权=部署即死"): env BRAINOS_API_KEY 设置后, 除 GET /health 与
# GET /metrics 外全部端点要求 "Authorization: Bearer <key>"(FastAPI dependency 实现,
# 缺/错=401, scheme 大小写不敏感 RFC7235, token 常量时比较防时序侧信道);
# env 未设=本地开发模式全放行(启动 WARN)。
# key 只从 env 读, 禁硬编码。
def _require_auth(authorization: str | None = Header(default=None)) -> None:
    key = os.environ.get("BRAINOS_API_KEY", "")
    if not key:  # env 未设: 本地开发模式放行
        return
    parts = authorization.split(" ", 1) if authorization else []
    if (len(parts) != 2 or parts[0].lower() != "bearer"
            or not hmac.compare_digest(parts[1].strip(), key)):
        raise HTTPException(status_code=401, detail="unauthorized: 缺或错 Bearer 凭证")


# 并发锁(P0 "服务无锁=FastAPI 线程池下竞态"): 全局 RLock(=SVC.lock 单实例) 包住端点
# 整段——写路径(/add /forget /dream /snapshot)与读路径(/search /stats)全部串行化,
# organ 非线程安全由服务层一把锁兜底; /health 免锁。内层 SVC 方法再取锁为 RLock 同线程
# 可重入, 无死锁; 阻塞只发生在 FastAPI 线程池 worker 内, 事件循环不被卡。
def _locked(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with SVC.lock:
            return fn(*args, **kwargs)
    return wrapper


# ═══════════════ B0-MR-MULTITENANT v1: 服务层多租户作用域(PA 审计 P1 收口) ═══════════════
# 语义(TOP 惯例): 作用域三级 tenant(租户隔离)>conv(会话隔离)>key(单卡)。
# 实现边界: organ/hippocampal_recall.py 零改动——租户命名空间纯服务层完成:
#   ① 命名空间: 卡片 conv 键写入时统一前缀 "{tenant}:{conv}"(organ 单卡键 "{conv}|{dia}"
#     随之租户唯一); 读路径按同规则前缀过滤/前缀精确 scope, organ 全库回退亦被剔除。
#   ② 租户串校验: ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$ 且禁冒号(冒号是命名空间分隔符,
#     租户含冒号会破坏前缀解析=隔离破口), 违者 422。
#   ③ 鉴权联动: AL 的 BRAINOS_API_KEY 逻辑不动; 可选 env BRAINOS_TENANT_KEY=
#     "<tenant>:<key>[,<tenant>:<key>...]" 配置多租户独立 key——配置后服务端以 key
#     反推 tenant(强制映射): 无有效映射 key=401, 客户端自报 tenant 与映射不符=403
#     (禁客户端自报), 自报与映射一致或缺省=放行用映射租户。未配置=客户端可选自报
#     tenant(缺省 "default", 与既有单租户行为完全兼容)。
#   ④ 覆盖面: 映射配置后 /add /search /forget /snapshot /lifecycle/* 与 BRAINOS_API_KEY
#     同覆盖(除 GET /health 与 GET /metrics 白名单), guard 为纯追加依赖不改 AL 逻辑。
import re  # noqa: E402  (多租户区局部所需)

_TENANT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")


def _norm_tenant(tenant: str | None) -> str:
    """租户串归一+校验: 空/None→"default"; 含冒号或非法字符=422(命名空间分隔符保护)"""
    t = (tenant or "").strip()
    if not t:
        return "default"
    if not _TENANT_RE.match(t):
        raise HTTPException(status_code=422,
                            detail=f"非法 tenant: {tenant!r} (限字母/数字/_.- 开头字母数字, 禁冒号, ≤64)")
    return t


def _parse_tenant_key_env() -> dict[str, str]:
    """BRAINOS_TENANT_KEY → {key: tenant}(key→tenant 反推映射; 重复 key 首个生效)"""
    raw = os.environ.get("BRAINOS_TENANT_KEY", "").strip()
    if not raw:
        return {}
    mapping: dict[str, str] = {}
    for part in raw.split(","):
        t, _, k = part.strip().partition(":")
        t, k = t.strip(), k.strip()
        if t and k and k not in mapping:
            mapping[k] = t
    return mapping


def _tenant_guard(authorization: str | None = Header(default=None)) -> str | None:
    """FastAPI 依赖: 多租户 key 映射已配置→以 key 反推 tenant 返回(无有效 key=401);
    未配置→返回 None(客户端自报 tenant 缺省 default)。BRAINOS_API_KEY 逻辑零改动。"""
    mapping = _parse_tenant_key_env()
    if not mapping:
        return None
    parts = authorization.split(" ", 1) if authorization else []
    token = parts[1].strip() if len(parts) == 2 else ""
    for k, t in mapping.items():
        if hmac.compare_digest(token, k):
            if not _TENANT_RE.match(t):
                raise HTTPException(status_code=500,
                                    detail=f"BRAINOS_TENANT_KEY 配置非法 tenant: {t!r}")
            return t
    raise HTTPException(status_code=401,
                        detail="unauthorized: 多租户 key 映射已启用, 需有效租户 Bearer key")


def _effective_tenant(req_tenant: str | None, derived: str | None) -> str:
    """合成生效租户: 映射模式(derived 非 None)下禁客户端自报——自报 tenant 与 key
    反推租户不符=403, 一致/缺省=用映射租户; 非映射模式=客户端自报(缺省 default)。"""
    if derived is not None:
        if req_tenant is not None and _norm_tenant(req_tenant) != derived:
            raise HTTPException(status_code=403,
                                detail="forbidden: 多租户 key 映射模式下禁客户端自报 tenant "
                                       f"(key 映射为 {derived!r}, 自报为 {req_tenant!r})")
        return derived
    return _norm_tenant(req_tenant)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    log = logging.getLogger("uvicorn.error")
    if os.environ.get("BRAINOS_API_KEY", ""):
        log.info("API 鉴权已启用: 除 GET /health 与 GET /metrics 外全部端点要求 Authorization: Bearer <BRAINOS_API_KEY>")
    else:
        log.warning("BRAINOS_API_KEY 未设置: 本地开发模式, 全端点免鉴权——生产部署必设, 否则部署即裸奔!")
    if _parse_tenant_key_env():
        log.info("多租户 key 映射已启用(BRAINOS_TENANT_KEY): 以 key 反推 tenant, 禁客户端自报; "
                 "未配置租户命名空间=客户端可选自报 tenant(缺省 default)")
    yield


app = FastAPI(title="brainos HippocampalRecall AML Service",
              description="AML Cycle 2: evidence-only Add/Search, read-only retrieval",
              version="1.2.0", lifespan=_lifespan)
app.add_middleware(_ObservabilityMiddleware)  # B0-MT: 可观测采集(纯 ASGI, 零依赖)


@app.post("/add", dependencies=[Depends(_require_auth)])
@_locked
def add(req: _AddRequest, derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """批量入证据卡; "finalize": true 时重建三通道索引(批量 add 后调一次);
    可选 "tenant"(B0-MR: 缺省 default; key 映射模式下以 key 反推为准)"""
    return SVC.add(req.items, req.finalize, tenant=_effective_tenant(req.tenant, derived))


@app.post("/search", dependencies=[Depends(_require_auth)])
@_locked
def search(req: _SearchRequest, derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """只返回证据卡[{session,dt,speaker,text}] + evidence_score; 禁返回任何生成答案;
    可选 "tenant" 与 "conv_scope" 正交组合(tenant>conv>key 三级作用域, B0-MR)"""
    return SVC.search(req.query, req.conv_scope, req.top_k,
                      tenant=_effective_tenant(req.tenant, derived))


class _SnapshotRequest(BaseModel):
    """快照目标目录(不存在则创建; 已有旧快照被原子覆盖)"""
    path: str = Field(min_length=1)


@app.post("/snapshot", dependencies=[Depends(_require_auth)])
@_locked
def snapshot(req: _SnapshotRequest,
             _derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """R1-PERSIST 全量快照落盘: body {"path": "<目录>"} → save(进评测前持久化, 重启不 WTF 忘)"""
    try:
        return SVC.snapshot(req.path)
    except Exception as e:  # 落盘失败显式报错(路径不可写等), 进程不崩
        raise HTTPException(status_code=500, detail=f"snapshot失败: {e}") from e


@app.get("/health")
def health() -> dict[str, Any]:
    return SVC.health()


@app.get("/metrics")
def metrics() -> Response:
    """B0-MT 可观测端点: Prometheus 文本格式(version 0.0.4, 零依赖手写)。
    免鉴权 = Prometheus 抓取惯例(与 /health 同列 dependency 白名单——即不加
    Depends(_require_auth)); 生产可挂网关层鉴权(网关对 /metrics 加 Bearer 或内网
    白名单), 服务内不逐端点设防。"""
    return Response(content=METRICS.render(_metrics_gauges()),
                    media_type="text/plain; version=0.0.4; charset=utf-8")



# ═══════════════ R3-LIFECYCLE-BLOCK v1: 生命周期 API ═══════════════
# 客户长期使用功能: /forget(主动遗忘) / /lifecycle/dream(离线睡眠整理) / /lifecycle/stats(衰减观测)。
# 合规注记: AML 评测期(Cycle 2)这三个端点不背任何调用——评测客户端只走 /add /search /health;
# 本区属离线生命周期/客户运营路径, 不进检索路径, 不产生跨样本状态。
# 实现边界: 纯服务层组合逻辑(把目标卡从 organ 既有结构 _cards/_idx/_spk_idx/_emb 剔除后
# finalize_ingest 重建索引), 不改 organ/hippocampal_recall.py 本体, 不调 organ.reinforce()。
from fastapi import HTTPException  # noqa: E402  (生命周期区局部所需)


class _ForgetRequest(BaseModel):
    """遗忘目标: key=单卡 / conv=整会话, 至少一项; 同时给出时以 key 为准(单卡精度优先);
    tenant=可选租户(缺省 default; key 映射模式下以 key 反推为准)"""
    key: str | None = None
    conv: str | None = None
    tenant: str | None = None


def _svc_forget(key: str | None, conv: str | None, tenant: str = "default") -> dict[str, Any]:
    """遗忘=服务层组合: 从 organ._cards/_idx/_spk_idx 剔除目标卡后 finalize_ingest 重建。
    索引重建由 organ 公有方法完成, 服务件只做结构剔除与陈旧向量清理, 零 organ 改动。
    B0-MR 租户化: conv 先加前缀 "{tenant}:{conv}" 精确遗忘; key 先字面后 "{tenant}:{key}"
    前缀解析(兼容无前缀裸键), 命中卡必须属于请求租户——跨租户键 found=False 零泄漏。"""
    with SVC.lock:
        organ = SVC.organ
        if key:
            card = organ._cards.get(key)
            if card is None:
                pk = f"{tenant}:{key}"
                if pk in organ._cards:
                    key, card = pk, organ._cards[pk]
            if card is not None and str(card.get("conv", "")).split(":", 1)[0] != tenant:
                card = None  # 键属他租户: 对本租户不可见(租户隔离)
            if card is None:
                return {"forgotten": 0, "found": False, "tenant": tenant, "total": SVC._total}
            target_conv = str(card.get("conv", ""))
        else:
            target_conv = f"{tenant}:{conv or ''}"
        if not target_conv:
            return {"forgotten": 0, "found": False, "tenant": tenant, "total": SVC._total}
        if key:
            victims = [key] if key in organ._cards else []
        else:
            victims = [k for k, c in organ._cards.items()
                       if str(c.get("conv", "")) == target_conv]
        if not victims:
            return {"forgotten": 0, "found": False, "conv": target_conv,
                    "tenant": tenant, "total": SVC._total}
        vs = set(victims)
        for k in vs:
            organ._cards.pop(k, None)
        spk = organ._spk_idx.get(target_conv)
        if spk:
            for s in list(spk):
                spk[s] -= vs
                if not spk[s]:
                    del spk[s]
            if not spk:
                organ._spk_idx.pop(target_conv, None)
        organ._idx.pop(target_conv, None)
        if organ._emb:  # 语义通道陈旧向量同步剔除
            for k in vs:
                organ._emb.pop(k, None)
        organ.finalize_ingest()          # 剩余 _cards 全量重建三通道索引(含目标 conv)
        if organ._emb:                   # 防 embed 缓存整载复活已遗忘键
            for k in list(organ._emb.keys() - organ._cards.keys()):
                organ._emb.pop(k, None)
        SVC._total = len(organ._cards)
        SVC._dirty = False               # finalize 已全局重建, 无脏增量
        return {"forgotten": len(vs), "found": True, "conv": target_conv,
                "tenant": tenant, "total": SVC._total}


@app.post("/forget", dependencies=[Depends(_require_auth)])
@_locked
def forget(req: _ForgetRequest, derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """主动遗忘: {"key": "..."} 单卡 / {"conv": "conv-X"} 整会话(至少一项);
    可选 "tenant" 与 conv 组合成 "{tenant}:{conv}" 精确遗忘(B0-MR)"""
    if not req.key and not req.conv:
        raise HTTPException(status_code=422, detail="key 与 conv 至少给一项")
    return _svc_forget(req.key, req.conv, tenant=_effective_tenant(req.tenant, derived))


@app.post("/lifecycle/dream", dependencies=[Depends(_require_auth)])
@_locked
def lifecycle_dream(_derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """离线睡眠整理: XMemory.sleep_consolidate(层级晋升) + FSRS.dream_consolidate(重复合并)"""
    with SVC.lock:
        xm = SVC.organ.xm.sleep_consolidate()
        merged = SVC.organ.fsrs.dream_consolidate()
    return {"merged": int(merged), "consolidated": int(xm.get("promoted", 0))}


@app.get("/lifecycle/stats", dependencies=[Depends(_require_auth)])
@_locked
def lifecycle_stats(_derived: str | None = Depends(_tenant_guard)) -> dict[str, Any]:
    """记忆生命周期观测: organ.stats() 直出 + "tenants" 按租户分组计数(B0-MR 追加字段,
    既有扁平字段零改动); organ 零改动, 分组由服务层 tenant_stats() 从 conv 前缀聚合"""
    with SVC.lock:
        out = SVC.organ.stats()
        out["tenants"] = SVC.tenant_stats()
        return out


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("EXAM_HOST", "0.0.0.0"),
                port=int(os.environ.get("EXAM_PORT", "8090")))
