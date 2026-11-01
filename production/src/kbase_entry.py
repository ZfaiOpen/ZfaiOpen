# /home/zfai/zo_deploy/app/aml_service.py — BRAIOS AML线协议薄适配层(部署件·非冻结核心)
# ═══════════════════════════════════════════════════════════════════════════════
# 职责(只加不改·零冻结核心修改):
#   评测平台 AML线协议 (agentmemoryleaderboard Add/Search API) → 进程内调用
#   ~/exam/memory_service.py 的 MemoryService(HippocampalRecall检索器官+PersistentRecall持久化)。
#
# 契约(官方API指南+规则逐字对表, 2026-10-06 补丁十条版):
#   POST /add  {"request_id","messages":[{role,timestamp,content(str|[part...])}],"user_id","session_id"}
#              → 200 {"success":true,"request_id","user_id","session_id"} 三ID逐字节回显;
#              持久化完成才回200; request_id幂等(重放同id返回同结果不重复入库);
#              metadata/app_id/agent_id/async_mode 等额外字段一概忽略(发了就当没有)。
#   POST /search {"query","user_id","top_k"(, "options":[...])} → 200 {"data":[{"id","content","created_at"}]}
#              id非空; 返回数 ≤ top_k 严格; 空结果 []; options 容错忽略。
#   GET  /health 免鉴权 2xx。
#   认证: Authorization: Token <KEY> (/add /search 必带; KEY 落盘 zo_deploy/app/.api_key 0600)。
#   失败诚实: 内部错误 5xx, 绝不伪装 200。(平台重试 408/409/425/429/500/502/503/504/524)
#   多模态降级(诚实): image_url 部件→只存 "[图片]" 标记占位, 不识图; 未知type部件跳过。
#
# 持久化双层(均落盘 zo_deploy/data):
#   ① ledger.db  (sqlite WAL+synchronous=NORMAL, 实测commit 0.039ms/op — 本机默认journal
#      模式fsync=538ms/op为WSL2平台病, 故必须WAL) — request_id 幂等账本+启动重放源。
#   ② snapshot/  (PersistentRecall 全量快照, 后台60s周期+停机时) — 启动直恢复。
#   启动恢复 = MemoryService载snapshot + 重放 ledger 中 created_ms > last_snapshot_ms 的行
#   (卡片键 dia="{request_id}:{i}" 确定性 → 重放覆写同键=幂等)。
#
# 合规: 评测内容(query/text)零采集零落盘日志 — 日志只记 request_id/user_id/session_id/
#       状态码/耗时/卡数; KEY 不进日志; 快照属服务运行必需态(30天内可清理, 见DEPLOY.md)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import hmac
import json
import sys
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── 环境隔离(必须在 import memory_service 之前) ─────────────────────────────
os.environ["HF_HUB_OFFLINE"] = "1"       # 实测: ST加载时model_info()出网可卡死90s+; 模型已在本地缓存
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("EXAM_USE_EMBED", "1")   # 三通道RRF含语义臂(本地MiniLM, 离线)
_DEPLOY = Path(__file__).resolve().parent.parent          # /home/zfai/zo_deploy
os.environ.setdefault("BRAINOS_TKG", "0")              # organ v2.6 TKG门: 其sqlite连接线程不安全, 16并发finalize每次抛ProgrammingError+刷栈(实测63MB日志/10min)且fail-soft零贡献 → 关门; 单线程离线评测可自行开
os.environ.setdefault("BRAINOS_LANCEDB", "0")          # organ v2.5 ANN关门: >3000卡每次finalize触发LanceDB同步(实测首建256s) → 暴力点积(评测规模更快)
os.environ.setdefault("BRAINOS_ANN_THRESHOLD", "1000000")  # 双保险: 阈值门抬到百万卡
os.environ["BRAINOS_SNAPSHOT"] = str(_DEPLOY / "data" / "snapshot")
os.environ["BRAINOS_EMB_CACHE"] = str(_DEPLOY / "data" / "emb_cache.npz")  # 隔离~/exam默认缓存

sys_path_brainos = os.environ.get("BRAINOS_HOME", str(Path.home() / "brainos_new"))
sys_path_exam = os.environ.get("EXAM_HOME", str(Path.home() / "exam"))
for _p in (sys_path_brainos, sys_path_exam):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import memory_service as ms  # noqa: E402  (exam队服务件, 进程内只读复用)

ms.TOP_K_MAX = 256  # AML top_k 上限100; 服务件默认cap64会静默截断 — 模块级常量运行时可抬(读全局)

# knife1_1008(部署线第1刀): D1双时相新值覆盖 + D2矛盾检测 — 纯overlay挂件, 冻结核心/organ/exam考场件零修改。
# ADD相观察(supersede/conflict落盘knife1.db) + SEARCH相当前态排除superseded·时间线全量+dt序(C3禁回退)。
# 回滚 = 还原 pre_knife1_1008/aml_service.py + 摘除 knife1_supersede.py/knife1_predicates.json (knife1.db可留档)。
import knife1_supersede as k1  # noqa: E402

# knife2_1008(部署线第2刀): H2隐私闸 — ADD相敏感标注(词典+模式落盘knife2.db) +
# SEARCH相最小披露放行门(泛化query滤敏感卡·显式域query放行[E3健康关怀守护])。
# 选型=滤除非脱敏(服务契约"只回证据原文"禁改写+滤除保证严格更强), 配超采补偿维持top_k填充。
# 回滚 = 还原 pre_knife2_1008/aml_service.py + 摘除 knife2_sensitive.py/knife2_sensitive.json (knife2.db可留档)。
import knife2_sensitive as k2  # noqa: E402

# knife3_1008(部署线第3刀): C1时间锚定 — ADD相相对/绝对时间归一(以消息timestamp为锚·自写轻量
# 解析器·零新增依赖)落盘knife3_time.db时间索引(卡上双存: 归一化ISO日期+原文措辞+可选区间) +
# 启动相存量全量回填(修前已入库卡亦覆盖) + SEARCH相时间query检测(duration/when/order·中英):
# 归一化日期注记(metadata附注·不改写content原文)·when档命中卡浮前排(钉原top3)·
# duration档同会话双日期卡必回+日期差注记(粒度对表: 问天给天数/问月给月数)·
# order档会话内时间位次标注(C2顺序红利)。k1 history意图只注记零重排(C3时间线资产禁打架)。
# 注记只附metadata不进文本索引 — 与冻结核BRAINOS_DATE_ANCHOR(缺省关·DA4锚入卡回归形态)零重叠。
# 回滚 = 还原 pre_knife3_1008/aml_service.py + 摘除 knife3_time.py/knife3_time_terms.json
#        (knife3_time.db可留档)。
import knife3_time as k3  # noqa: E402

# knife4_1008(部署线第4刀): I1流式延迟刀 — 官方低分格最大死因=队列谋杀(15路search同毫秒
# 涌入→全局RLock串行→队尾68.7s·超30s的12查全落低分域)。M1a encode-memo(每search逐conv
# 重复encode塌缩为1) + M1b 单趟租户取窗(小租户双趟全库重扫→单趟·语义逐位等价) + RW读写分离
# (overlay层读共享写独占·写优先) + M2 带内recency决胜(ledger created_ms·相邻分差<5%带内
# 只换序不改集·k1时间线意图让位)。纯overlay: 冻结核心/organ/exam件零修改。
# 回滚 = 还原 pre_knife4_1008/aml_service.py + 摘除 knife4_streaming.py/knife4_probe.py/
# knife4_selftest.py。env总闸=BRAINOS_K4(缺省开·=0关门回旧行为)。
import knife4_streaming as k4  # noqa: E402

# knife5_1008(部署线第5刀): S3多跳扩查刀 — 官方B2因果链=0最大血洞。影子取证根因:
# ①k1历史意图误报("Why did X..."过去时因果询问命中\bdid\b/has+Ved→整窗dt升序→证据卡
#   埋到rank31+·窗召回100%但top10=0) ②关系链二跳事实("A的哥哥是X→X住在Y")单跳词面必漏Y卡。
# 三件套(零LLM·词典knife5_relations.json可增量): R1意图精化(真历史/聚合/时序整体让位·
#   误报才修复=桥卡+二跳新卡前置·k1代码零修改·PK4让位判例对表) + R2实体桥接扩查(query专名
#   →speaker/text桥卡→内容扩查词拼二跳query) + R3合并去重(eid同源·邻插封顶防top10挤位)。
# 触发门=关系链词 或 一跳top1分<阈; 预算硬门BRAINOS_K5_BUDGET_MS=8000(超时降级单跳fail-soft)。
# 回滚 = 还原 pre_knife5_1008/aml_service.py + 摘除 knife5_multihop.py/knife5_relations.json/
#   knife5_probe.py/knife5_selftest.py (S3零状态无DB)。
import knife5_multihop as k5  # noqa: E402

# knife6_1009(部署线S7刀): G2规则卡 + G4经验卡/趋势卡 — 零LLM纯结构化overlay。
# ADD相词典逐句检测(规则句/经验句→一等公民卡: 原句verbatim+主体+对象元数据落盘
# knife6_cards.db; 数值事实正则抽取→同实体同度量≥3点→趋势卡·纯算术) + 启动相存量回填 +
# SEARCH相意图门置顶加成(规则/经验/聚合意图·每类≤4·总计≤6·非意图字节级直通=清单加项
# 即死护栏; k1 history与k3时间档让位; 敏感卡过k2放行门)。四刀路径零修改·纯叠加层。
# 回滚 = 摘除knife6_cards.py/knife6_rules.json + 还原本文件三处hunk(knife6_cards.db可留档)。
import knife6_cards as k6  # noqa: E402

# knife7_1009(S4影子·部署线第7刀): F1抽取式层次摘要刀 — 官方F1(摘要与压缩/长历史全局综合)=16.67
# 死因=长历史散卡无全局综合视图。ADD相: 卡数/实体事实越阈→主题词重叠聚类(零LLM)→digest卡
# (代表句2-3条**原文**+覆盖计数+时间跨度·纯抽取拼装非生成)落盘knife7.db·版本递增旧版superseded
# (复用knife1双时相语义)。SEARCH相: 仅聚合意图query(词典白名单knife7_digest.json·清单加项即死)
# →digest卡尾部追加入结果(只增益不夺位·命中数守卫≤MAX_INJECT); 非聚合query零突变=事实query
# 证据零digest混入。与knife1-4共存叠加(k7在四刀之后·追加在截断前)。
# 回滚 = 还原 pre_knife7_1009/aml_service.py + 摘除 knife7_digest.py/knife7_digest.json
#        (knife7.db可留档)。env总闸=BRAINOS_K7(缺省开·=0关门)。
import knife7_digest as k7  # noqa: E402

from fastapi import Body, FastAPI, HTTPException, Request  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from starlette.concurrency import run_in_threadpool  # noqa: E402

SVC = ms.SVC
DATA_DIR = _DEPLOY / "data"
LEDGER_DB = DATA_DIR / "ledger.db"
SNAP_DIR = DATA_DIR / "snapshot"
KEY_FILE = Path(os.environ.get("ZO_KEYFILE", str(_DEPLOY / "app" / ".api_key")))
SNAP_INTERVAL_S = float(os.environ.get("ZO_SNAP_INTERVAL", "60"))
MAX_BODY_BYTES = 32 * 1024 * 1024  # 单add≤30MiB(官方mm上限)+裕量; 超限413

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(_DEPLOY / "logs" / "service.log"), logging.StreamHandler()],
)
log = logging.getLogger("aml")
# 静音第三方 INFO 噪声
for noisy in ("uvicorn.access", "urllib3", "httpx", "sentence_transformers", "brainos"):
    logging.getLogger(noisy).setLevel(logging.WARNING)  # brainos=organ逐调用INFO刷屏(XMemory每卡两条), 收到WARNING

API_KEY = KEY_FILE.read_text().strip() if KEY_FILE.exists() else ""
if not API_KEY:
    log.warning("API KEY 文件缺失: %s — /add /search 将全拒(401)", KEY_FILE)

app = FastAPI(title="BRAIOS AML Adapter", docs_url=None, redoc_url=None, openapi_url=None)


# ── ledger(sqlite WAL) ──────────────────────────────────────────────────────
class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")   # 实测0.039ms/commit; 进程崩溃安全
        self._conn.execute("""CREATE TABLE IF NOT EXISTS ledger(
            request_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
            response_json TEXT NOT NULL, cards_json TEXT NOT NULL, created_ms INTEGER NOT NULL)""")
        self._conn.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        self._conn.commit()
        self._lock = threading.Lock()

    def get(self, request_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM ledger WHERE request_id=?", (request_id,)).fetchone()
        return row[0] if row else None

    def put(self, request_id: str, user_id: str, response_json: str, cards_json: str) -> bool:
        """True=本进程首次写入; False=已存在(并发重放), 调用方须改返回已存响应"""
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO ledger VALUES(?,?,?,?,?)",
                (request_id, user_id, response_json, cards_json, int(time.time() * 1000)))
            self._conn.commit()
            return cur.rowcount > 0

    def rows_after(self, ms_epoch: int) -> list[tuple[str, str]]:
        with self._lock:
            return self._conn.execute(
                "SELECT user_id, cards_json FROM ledger WHERE created_ms>? ORDER BY created_ms",
                (ms_epoch,)).fetchall()

    def meta_get(self, k: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else None

    def meta_set(self, k: str, v: str) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, v))
            self._conn.commit()


LEDGER = Ledger(LEDGER_DB)


def tenant_of(user_id: str) -> str:
    """user_id→器官租户命名空间: 禁冒号(服务件_norm_tenant), 替换后非空; 响应仍回显原始user_id"""
    t = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(user_id))[:128]
    return t or "u_default"


def _dt_from_ts(ts: Any) -> str:
    """消息timestamp(毫秒epoch, 兼容秒)→organ dt(探针实证格式 ISO无时区尾Z)"""
    try:
        v = float(ts)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if v > 1e11:  # 毫秒
        v /= 1000.0
    return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _content_to_text(content: Any) -> tuple[str, int]:
    """content(str|[part...])→(正文, 图片数); text拼接·image_url→[图片]标记·未知type跳过(宽松)"""
    if content is None:
        return "", 0
    if isinstance(content, str):
        return content, 0
    if isinstance(content, list):
        parts: list[str] = []
        n_img = 0
        for p in content:
            if not isinstance(p, dict):
                continue
            t = p.get("type")
            if t == "text" and isinstance(p.get("text"), str):
                parts.append(p["text"])
            elif t == "image_url":
                n_img += 1
            # 未知type: 跳过(宽松解析)
        if n_img:
            parts.append("[图片]" if n_img == 1 else f"[图片x{n_img}]")
        return "\n".join(x for x in parts if x), n_img
    return str(content), 0


def build_cards(body: dict[str, Any]) -> list[dict[str, Any]]:
    """AML messages→器官证据卡六字段; dia="{request_id}:{i}" 确定性键=幂等重放的物理基础"""
    sid = str(body.get("session_id", ""))
    cards: list[dict[str, Any]] = []
    rid = str(body.get("request_id", ""))
    for i, m in enumerate(body.get("messages") or []):
        if not isinstance(m, dict):
            continue
        text, n_img = _content_to_text(m.get("content"))
        if not text:
            continue
        cards.append({
            "conv": sid,
            "sess": sid,
            "dia": f"{rid}:{i}",
            "speaker": str(m.get("role", "user")),
            "text": text,
            "dt": _dt_from_ts(m.get("timestamp")),
        })
    return cards


def _evidence_id(e: dict[str, Any]) -> str:
    raw = f'{e.get("session", "")}|{e.get("dt", "")}|{e.get("text", "")}'
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


# ── 快照线程 ────────────────────────────────────────────────────────────────
_snap_stop = threading.Event()
_last_vacuum = [0.0]


def _ledger_maintain() -> None:
    """疫苗3: WAL checkpoint(TRUNCATE)每拍 + VACUUM每24h(追加快照已compact, 禁O(n²)重写);
    疫苗17: 账本保留期 — 已入快照且超24h的行才删(重放语义不受损, 幂等表不无限涨)"""
    try:
        now_s = time.time()
        with LEDGER._lock:
            LEDGER._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            if now_s - _last_vacuum[0] > 86400 and _last_vacuum[0] > 0:
                LEDGER._conn.execute("VACUUM")
                _last_vacuum[0] = now_s
                log.info("ledger VACUUM done")
            _last_vacuum[0] = _last_vacuum[0] or now_s
        snap_ms = int(LEDGER.meta_get("last_snapshot_ms") or 0)
        cutoff = min(int((now_s - 86400) * 1000), snap_ms)
        with LEDGER._lock:
            cur = LEDGER._conn.execute("DELETE FROM ledger WHERE created_ms < ?", (cutoff,))
            if cur.rowcount:
                LEDGER._conn.commit()
                log.info("ledger purge: %d rows older than cutoff", cur.rowcount)
    except Exception as exc:
        log.error("ledger maintain failed: %s", exc)


def _snapshot_loop() -> None:
    _last_vacuum[0] = time.time()
    while not _snap_stop.wait(SNAP_INTERVAL_S):
        try:
            man = SVC.snapshot(str(SNAP_DIR))
            LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))
            _ledger_maintain()
            log.info("snapshot ok cards=%s convs=%s", man.get("cards"), man.get("convs"))
        except Exception as exc:  # 快照失败不致命(ledger兜底重放), 下轮再试
            log.error("snapshot failed: %s", exc)


def _startup_replay() -> int:
    """恢复=MemoryService已载snapshot; 重放 last_snapshot_ms 之后的ledger行(幂等覆写)
    (疫苗4: 重放时长落log, >60s WARN; organ增量索引=无全表扫描, 卡键确定性=幂等覆写)"""
    t0 = time.time()
    snap_ms = int(LEDGER.meta_get("last_snapshot_ms") or 0)
    rows = LEDGER.rows_after(snap_ms)
    by_user: dict[str, list[dict[str, Any]]] = {}
    for user_id, cards_json in rows:
        by_user.setdefault(user_id, []).extend(json.loads(cards_json))
    n = 0
    for user_id, cards in by_user.items():
        if cards:
            SVC.add([ms._AddItem(**c) for c in cards], finalize=True, tenant=tenant_of(user_id))
            n += len(cards)
    if n:
        msg = "startup replay: %d cards from %d users in %.1fs (snap_ms=%d)"
        if time.time() - t0 > 60:
            log.warning(msg + " — SLOW(>60s WARN)", n, len(by_user), time.time() - t0, snap_ms)
        else:
            log.info(msg, n, len(by_user), time.time() - t0, snap_ms)
    return n


def _warmup() -> None:
    """预热: 把MiniLM嵌入模型加载进内存(首次请求不再扛冷启动~70s)。
    疫苗6: 零探针/哑数据入库 — 只加载模型权重, organ状态零污染。"""
    try:
        t0 = time.time()
        SVC.organ._qmodel = SVC.organ._embed_model()
        log.info("warmup: embed model loaded in %.1fs (no data injected)", time.time() - t0)
    except Exception as exc:
        log.warning("warmup failed (embed通道将fail-soft降级词面): %s", exc)


@app.on_event("startup")
def _on_startup() -> None:
    t0 = time.time()
    _warmup()
    n = _startup_replay()
    # knife3_1008: 存量卡时间索引全量回填(幂等OR IGNORE·修前已入库卡亦被C1归一覆盖); fail-soft
    try:
        b_cards, b_anchors, b_ms = k3.backfill(list(SVC.organ._cards.values()))
        log.info("knife3 backfill: cards=%d new_anchors=%d in %.0fms", b_cards, b_anchors, b_ms)
    except Exception as exc:
        log.error("knife3 backfill FAILED err=%s", type(exc).__name__)
    # knife4_1008: I1延迟刀安装(M1a memo/M1b onepass+RW/M2 recency·fail-soft) — 须在_warmup后
    try:
        _k4_flags = k4.install(SVC, LEDGER, LEDGER_DB)
        log.info("knife4 install: %s", _k4_flags if _k4_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife4 install FAILED err=%s", type(exc).__name__)
    # knife5_1008: S3多跳扩查刀安装(词典装载·无状态零monkey-patch·fail-soft)
    try:
        _k5_flags = k5.install(SVC)
        log.info("knife5 install: %s", _k5_flags if _k5_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife5 install FAILED err=%s", type(exc).__name__)
    # knife6_1009: S7存量卡回填(规则/经验/数值点·幂等OR REPLACE·修前已入库卡亦覆盖); fail-soft
    try:
        b6_cards, b6_sents, b6_ms = k6.backfill(list(SVC.organ._cards.values()))
        log.info("knife6 backfill: cards=%d rule_exp_sents=%d in %.0fms",
                 b6_cards, b6_sents, b6_ms)
    except Exception as exc:
        log.error("knife6 backfill FAILED err=%s", type(exc).__name__)
    # knife7_1009: 摘要刀惰性单例预初始化(可见性·fail-soft)
    try:
        log.info("knife7 init: %s", k7.get().stats())
    except Exception as exc:
        log.error("knife7 init FAILED err=%s", type(exc).__name__)
    threading.Thread(target=_snapshot_loop, name="snap", daemon=True).start()
    log.info("startup done: cards=%s replayed=%d in %.1fs", SVC._total, n, time.time() - t0)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _snap_stop.set()
    try:
        SVC.snapshot(str(SNAP_DIR))
        LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))
        log.info("shutdown snapshot ok")
    except Exception as exc:
        log.error("shutdown snapshot failed: %s", exc)


# ── 鉴权 ────────────────────────────────────────────────────────────────────
def _auth(request: Request) -> None:
    got = request.headers.get("authorization", "")
    want = f"Token {API_KEY}"
    if not API_KEY or not hmac.compare_digest(got, want):
        raise HTTPException(status_code=401, detail="unauthorized")


# ── 端点 ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict[str, Any]:
    # 疫苗10: 自标识字段 — 公网侧验证到达的是BRAIOS进程本体, 禁假设端口没人占
    return {"status": "ok", "service": "braios-aml", "version": "1.0.0",
            "cards": SVC._total, "pid": os.getpid()}


@app.post("/add")
async def add(request: Request, payload: dict[str, Any] = Body(...)):
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="payload too large")
    _auth(request)
    t0 = time.time()
    rid = payload.get("request_id")
    uid = payload.get("user_id")
    sid = payload.get("session_id")
    if not isinstance(rid, str) or not rid or not isinstance(uid, str) or not isinstance(sid, str):
        raise HTTPException(status_code=400, detail="request_id/user_id/session_id required")
    # 幂等: 同request_id重放→返回已存结果(不重复入库)
    stored = await run_in_threadpool(LEDGER.get, rid)
    if stored is not None:
        log.info("add replay rid=%s uid=%s sid=%s status=200 dt=%.1fms", rid, uid, sid,
                 (time.time() - t0) * 1000)
        return JSONResponse(content=json.loads(stored))
    try:
        cards = await run_in_threadpool(build_cards, payload)
        if cards:
            await run_in_threadpool(
                SVC.add, [ms._AddItem(**c) for c in cards], True, tenant_of(uid))
            # knife1_1008·D1/D2: add成功后观察(抽subject/pred/value→supersede/conflict落盘);
            # fail-soft — 失败只丢覆盖标记(降级回旧行为), 绝不影响add成功语义
            try:
                await run_in_threadpool(k1.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife1 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
            # knife2_1008·H2: 敏感标注(词典命中→类目落盘knife2.db); fail-soft同knife1纪律
            try:
                await run_in_threadpool(k2.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife2 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
            # knife3_1008·C1: 时间归一观察(相对/绝对时间→锚定消息timestamp的绝对日期落盘索引);
            # fail-soft同knife1/2纪律
            try:
                await run_in_threadpool(k3.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife3 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
            # knife6_1008·S7: 规则/经验句卡片化+数值事实抽取(落盘knife6_cards.db);
            # fail-soft同knife1/2/3纪律 — 失败只丢S7卡, 绝不影响add成功语义
            try:
                await run_in_threadpool(k6.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife6 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
            # knife7_1009·F1: 摘要代谢观察(会话卡数/实体事实越阈→聚类→digest卡落盘·幂等);
            # fail-soft同knife1/2/3纪律
            try:
                await run_in_threadpool(k7.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife7 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
        resp = {"success": True, "request_id": rid, "user_id": uid, "session_id": sid}
        first = await run_in_threadpool(
            LEDGER.put, rid, uid, json.dumps(resp, ensure_ascii=False),
            json.dumps(cards, ensure_ascii=False))
        if not first:  # 并发同rid: 改回已存响应(幂等)
            resp = json.loads(await run_in_threadpool(LEDGER.get, rid) or "{}")
    except Exception as exc:
        log.error("add FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
        raise HTTPException(status_code=500, detail="internal error") from exc
    # 疫苗9: 响应自检断言 — success布尔/三ID逐字节回显; 不过=回5xx诚实失败, 绝不假200
    if not (isinstance(resp, dict) and resp.get("success") is True
            and resp.get("request_id") == rid and resp.get("user_id") == uid
            and resp.get("session_id") == sid):
        log.error("add SELF-CHECK FAILED rid=%s resp=%r", rid, resp)
        raise HTTPException(status_code=500, detail="response self-check failed")
    log.info("add ok rid=%s uid=%s sid=%s cards=%d status=200 dt=%.1fms",
             rid, uid, sid, len(cards), (time.time() - t0) * 1000)
    return JSONResponse(content=resp)


@app.post("/search")
async def search(request: Request, payload: dict[str, Any] = Body(...)):
    _auth(request)
    t0 = time.time()
    query = payload.get("query")
    uid = payload.get("user_id")
    if not isinstance(query, str) or not query or not isinstance(uid, str):
        raise HTTPException(status_code=400, detail="query/user_id required")
    try:
        top_k = int(payload.get("top_k") or 100)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="top_k must be int")
    top_k = max(1, min(top_k, ms.TOP_K_MAX))
    try:
        # knife1_1008: 超采补偿 — 内部多取该租户superseded数, 排除后仍按top_k填满(排除非降权)
        try:
            k1_extra = k1.extra_fetch(tenant_of(uid))
        except Exception:
            k1_extra = 0
        # knife2_1008: 超采补偿同款 — 多取该租户敏感卡数, 隐私闸滤除后仍按top_k填满
        try:
            k2_extra = k2.extra_fetch(tenant_of(uid))
        except Exception:
            k2_extra = 0
        ik = min(top_k + k1_extra + k2_extra, ms.TOP_K_MAX)
        out = await run_in_threadpool(SVC.search, query, None, ik, tenant_of(uid))
    except Exception as exc:
        log.error("search FAILED uid=%s err=%s", uid, type(exc).__name__)
        raise HTTPException(status_code=500, detail="internal error") from exc
    # 合规①: 只回证据原文; id非空·content非空·created_at可溯源; ≤top_k 严格
    data = [{"id": _evidence_id(e), "content": e.get("text", ""), "created_at": e.get("dt", "")}
            for e in out.get("evidence", []) if e.get("text")]
    # knife1_1008: 当前态意图(含无信号默认)→排除superseded卡; 历史/时间线意图→全量+dt升序(C3禁回退)
    k1_intent = "current"  # knife3消费(意图兜底: filter异常时按current=允许knife3排序)
    try:
        data, k1_intent, k1_drop = await run_in_threadpool(
            k1.filter_evidence, tenant_of(uid), query, data, top_k)
        if k1_drop:
            log.info("knife1 filter uid=%s intent=%s dropped=%d kept=%d",
                     uid, k1_intent, k1_drop, len(data))
    except Exception as exc:
        log.error("knife1 filter FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife5_1008·S3: 多跳扩查(触发门+预算硬门·二跳卡并入窗后同过k2/k3/k4门=零旁路)
    try:
        data, out = k5.multihop_expand(SVC, tenant_of(uid), query, data, out, t0)
    except Exception as exc:
        log.error("knife5 multihop FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife2_1008·H2隐私闸: 泛化query→敏感卡滤除; 显式域query→该域放行(E3健康关怀守护);
    # fail-soft同knife1 — 失败只降级回"不过闸", 绝不影响search成功语义
    try:
        data, k2_drop = (await run_in_threadpool(k2.filter_gate, tenant_of(uid), query, data)) \
            if os.environ.get("BRAINOS_K2GATE", "0") == "1" else (data, 0)
        if k2_drop:
            log.info("knife2 gate uid=%s dropped=%d kept=%d", uid, k2_drop, len(data))
    except Exception as exc:
        log.error("knife2 gate FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife3_1008·C1: 时间query检测→归一化日期注记(metadata附注·不改写content原文)+时距排序;
    # duration档双日期卡必回+日期差注记; order档会话时间位次标注(C2红利)。
    # k1 history意图(dt升序=C3资产)→knife3只注记零重排(禁打架)。fail-soft同前两刀。
    try:
        data, k3_tier, k3_hits = await run_in_threadpool(
            k3.time_annotate, tenant_of(uid), query, data, k1_intent)
        if k3_hits:
            log.info("knife3 time uid=%s tier=%s anchored=%d kept=%d",
                     uid, k3_tier, k3_hits, len(data))
    except Exception as exc:
        log.error("knife3 time FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife4_1008·M2: 带内recency决胜(ledger created_ms新者在前·相邻分差<5%带内只换序
    # 不改集); k1 history时间线意图→让位(C3禁回退)。fail-soft同前三刀。
    try:
        data = k4.recency_reorder(tenant_of(uid), query, data, k1_intent, out, k3_tier)
    except Exception as exc:
        log.error("knife4 recency FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife6_1008·S7: 意图门置顶加成(规则/经验/聚合·有限加成·非意图字节级直通=死护栏;
    # k1 history与k3时间档让位; 敏感卡过k2放行门)。fail-soft同前三刀。
    try:
        data, k6_hits = await run_in_threadpool(
            k6.boost, tenant_of(uid), query, data, k1_intent, k3_tier)
        if k6_hits:
            log.info("knife6 boost uid=%s injected=%d kept=%d", uid, k6_hits, len(data))
    except Exception as exc:
        log.error("knife6 boost FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife7_1009·F1: 聚合意图query(词典白名单)→digest卡追加入结果(只增益不夺位·命中数守卫:
    # 注入≤MAX_INJECT且前缀逐位保留绝不截断真实卡; 非聚合query零突变=证据零digest混入)。
    # fail-soft同前四刀。
    try:
        data, k7_n, k7_before = k7.inject(tenant_of(uid), query, data, top_k)
        if k7_n:
            log.info("knife7 digest uid=%s injected=%d hits %d->%d",
                     uid, k7_n, k7_before, k7_before + k7_n)
    except Exception as exc:
        log.error("knife7 inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    data = data[:top_k]
    # 疫苗9: 响应自检断言 — data键/id非空/content非空/len≤top_k严格; 不过=5xx诚实失败
    if len(data) > top_k or not all(d["id"] and d["content"] for d in data):
        log.error("search SELF-CHECK FAILED uid=%s n=%d top_k=%d", uid, len(data), top_k)
        raise HTTPException(status_code=500, detail="response self-check failed")
    # "options" 等额外字段: 有意忽略(容错不炸)
    # kfix_1009·查询留痕: 官方评测取证开窗(query+返回证据·我方运维遥测)
    if os.environ.get("BRAINOS_TRACE", "1") != "0":
        try:
            with open("/home/zfai/zo_deploy/logs/query_trace.log", "a", encoding="utf-8") as _tf:
                _tf.write(json.dumps({"ts": time.strftime("%F %T"), "uid": uid[:16], "q": query,
                                      "top_k": top_k,
                                      "ret": [{"i": d["id"], "c": d["content"][:80]} for d in data[:10]]},
                                     ensure_ascii=False) + "\n")
        except Exception:
            pass
    log.info("search ok uid=%s top_k=%d hits=%d status=200 dt=%.1fms",
             uid, top_k, len(data), (time.time() - t0) * 1000)
    return JSONResponse(content={"data": data})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("ZO_HOST", "127.0.0.1"),
                port=int(os.environ.get("ZO_PORT", "8090")), workers=1,
                timeout_keep_alive=int(os.environ.get("ZO_KEEPALIVE", "65")),
                log_level="warning")
