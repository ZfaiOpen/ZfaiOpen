# spanvec.py — 轨道P·L4多向量分片嵌入 SPANVEC(ADD相分片嵌入 → SEARCH相"查询×分片"父卡回溯直召回·20261025)
# ═══════════════════════════════════════════════════════════════════════════════
# 病根: 每张卡一个整卡向量——长轨迹卡(数千字符)的单向量语义被中部细节稀释
#       (V4_VALIDATION §1.1: v4 纯通道 Q1 R@10=0.585 仍有空间; dev_a §9: gold 常埋长卡中段)。
# 处方: Add 时把卡切语义分片(~400-600字符, 按 tool_result/段落/diff/Exit code 边界), 每片独立
#       嵌入; Search 时 query×分片命中→回溯父卡(得分=最佳片分+覆盖片数加成)。分片嵌入与
#       organ._qmodel 同源(享受 k4 M1a encode-memo / v4shim md5 缓存), 引擎嵌入 Provider
#       无关(MiniLM dev 迭代 / v4 生产语义由轨道H报告背书, 本件不碰 v4 API 路径)。
#
# 既有件复用(对齐家规·零重复造轮):
#   knife13_repo  = 边车骨架范本(sqlite WAL/observe fail-soft/模块级单例) + card_id_of
#                   卡身份同源(sha1(sess|dt|text)[:16]·与 _evidence_id 同值) + content_hash_of
#                   fact-time 面剥除(窗 id 漂移对齐用)。
#   memory_service._tenant_fetch = 租户内暴力 numpy 点积 + 按行数版本化缓存范式(本件同法)。
#   knife4_streaming._EncodeMemo = organ._qmodel.encode 实例级包装(本件经同实例自动享受)。
#
# 做法(纯 overlay·冻结核心/organ 零修改):
#   ADD相    observe(tenant, cards, qmodel): 逐卡 split_spans→批量 encode(块内聚合批·
#            BRAINOS_SV_BS 一批)→ sqlite spans(tenant,card_id,span_idx,vec BLOB,text_head)
#            + cards 全文镜像(父卡回溯自持·不借 k13 texts: 实测 k13 texts 由 BRAINOS_K13 门
#            控制写入, SPANVEC 单门独立使能时 k13 texts 可为空——自持镜像消除跨门依赖,
#            代价=文本二次落盘 ≈ 池文本量级, 实测 1007 池 spanvec.db 记分板产物 §成本账)。
#            幂等: card_id 已存在→零增量直通(重放/重灌安全)。短卡(len≤SPLIT_MIN)不切=整卡单片。
#            fail-soft: 单卡分片/嵌入异常→该卡退整卡单片路径重试一次; 再败→跳过该卡计数
#            (organ 自有索引不受影响=卡主数据零丢失); 本件任何异常绝不影响 add 成功语义。
#   SEARCH相 recall_by_spanvec(tenant, query, top_n=25, qmodel)→[{id,content,dt,sess,score}]:
#            query 向量×租户全分片(numpy 批量点积·行数版本化缓存)→按父卡聚合
#            score=max片分 + HIT_BONUS×(命中片数-1) → 全分片向量/空库/零向量/门关→[]。
#   融合     aml_service k13b 块: 与 k13.recall_by_fingerprint 并列第二直召回源, RRF 第三榜
#            (复用 B 惯例 k=60·rank0钉扎·k11墓碑闸+k2隐私闸同口径·id+content 去重·裁回top_k;
#            env BRAINOS_SV_RRF_W 缺省1.0 对齐 K13_RRF_WREC)。
# 挂接(3处·回滚=摘3处+删本件): ①aml_service import区 ②_knives_process k13块后 spv.observe
#   ③search相 k13b 块内 spanvec 直召回并入(独立 env BRAINOS_SPANVEC 门·缺省0=关=零行为差)。
# 持久化 spanvec.db (sqlite WAL + synchronous=NORMAL + busy_timeout=5000·knife1/2/3/7/11/13
#   同款纪律·重启不丢)。
# 合规: 日志零内容 — 只记计数/维度/时延; 不落 query/卡文本。env 总闸 BRAINOS_SPANVEC(缺省0=关)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

log = logging.getLogger("spanvec")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("SPANVEC_DB", str(APP_DIR.parent / "data" / "spanvec.db")))


# ── 总闸与参数(env可调·缺省=交付报告口径) ─────────────────────────────────────
def master_on() -> bool:
    return os.environ.get("BRAINOS_SPANVEC", "0") == "1"       # 缺省关(评测门控纪律)


def _f(name: str, dft: str) -> float:
    try:
        return float(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return float(dft)


def _i(name: str, dft: str) -> int:
    try:
        return int(os.environ.get(name, dft) or dft)
    except (TypeError, ValueError):
        return int(dft)


SPLIT_MIN  = _i("BRAINOS_SV_SPLIT_MIN", "1200")  # ≤此长度卡不切(整卡单片·organ向量已足)
TARGET     = _i("BRAINOS_SV_TARGET", "500")      # 分片目标字符(~400-600 中值)
HARD_CAP   = _i("BRAINOS_SV_HARD_CAP", "900")    # 单段超此→按行/字符强制再分
MAX_SPANS  = _i("BRAINOS_SV_MAX_SPANS", "12")    # 每卡分片上限(超出并入尾片·尾片截断)
TAIL_CLIP  = _i("BRAINOS_SV_TAIL_CLIP", "1500")  # 尾片嵌入文本截断
BS         = _i("BRAINOS_SV_BS", "16")           # 批量encode块大小
TOP_N      = _i("BRAINOS_SV_TOP_N", "25")        # 直召回父卡上限(aml挂接同值)
MIN_SCORE  = _f("BRAINOS_SV_MIN_SCORE", "0.20")  # 父卡入榜最低最佳片分(cos)
HIT_TH     = _f("BRAINOS_SV_HIT_TH", "0.20")     # 片分≥此值计一次"命中"(覆盖加成分母)
HIT_BONUS  = _f("BRAINOS_SV_HIT_BONUS", "0.3")   # score = max片分 + bonus×(命中片数-1)


# ── 分片边界正则(零LLM·确定性·规格四类) ───────────────────────────────────────
_RX_BOUND = re.compile(
    r"\n\n+"                                   # ① 段落边界
    r"|\[tool_result\]|\[/tool_result\]"       # ② 工具结果/调用标记(含闭合)
    r"|\[tool_use\]|\[/tool_use\]"
    r"|^diff --git "                           # ③ diff 文件边界(行首)
    r"|^Exit code:?\s*\d+\s*$",                # ④ Exit code 行(行首)
    re.M)


def _segments(text: str) -> list[str]:
    """按边界正则切段(边界符归下段头=标记引领语义段: "diff --git a/.." "[tool_result].."
    "Exit code: N"整行开启新片)。\n\n边界符剥除由strip承担。"""
    out: list[str] = []
    last = 0
    for m in _RX_BOUND.finditer(text):
        if m.start() > last:
            out.append(text[last:m.start()])
            last = m.start()
    if last < len(text):
        out.append(text[last:])
    return [s for s in (seg.strip() for seg in out) if s.strip()]


def _hard_split(seg: str, hard: int, target: int) -> list[str]:
    """超硬上限段再分: 先按单换行, 仍超→定长切片(确定性)。"""
    if len(seg) <= hard:
        return [seg]
    out: list[str] = []
    for line_block in seg.split("\n"):
        if len(line_block) <= hard:
            out.append(line_block)
        else:
            for i in range(0, len(line_block), target):
                out.append(line_block[i:i + target])
    # 相邻碎块回填(避免大量 <target 小片)
    merged: list[str] = []
    for b in out:
        if merged and len(merged[-1]) + len(b) + 1 <= target:
            merged[-1] = merged[-1] + "\n" + b
        else:
            merged.append(b)
    return merged or [seg[:target]]


def split_spans(text: str, split_min: int | None = None, target: int | None = None,
                hard_cap: int | None = None, max_spans: int | None = None) -> list[str]:
    """卡文本→语义分片列表(纯函数·确定性·零异常容忍脏输入)。
    短卡(≤split_min)→整卡单片; 长卡按四类边界贪心装箱到~target; 超max_spans并入尾片
    (尾片文本截TAIL_CLIP防嵌入超长)。空文本→[]。"""
    t = str(text or "").strip()
    if not t:
        return []
    # ── U7 hunk a: AST 优先变体(BRAINOS_AST=1 且 BRAINOS_AST_SPAN=1 才激活·缺省关=
    #    逐字节 pristine; AST 不适用/解析失败→None→原路正则·fail-soft 绝不阻塞) ──
    if os.environ.get("BRAINOS_AST", "0") == "1" and \
            os.environ.get("BRAINOS_AST_SPAN", "0") == "1":
        try:
            import ast_supply
            _alt = ast_supply.split_spans_ast(t, base_segments=_segments,
                                              base_hard_split=_hard_split)
            if _alt is not None:
                return _alt
        except Exception:  # noqa: BLE001 — 任何意外→pristine 原路
            pass
    smin = SPLIT_MIN if split_min is None else int(split_min)
    tgt = TARGET if target is None else int(target)
    hard = HARD_CAP if hard_cap is None else int(hard_cap)
    msp = MAX_SPANS if max_spans is None else int(max_spans)
    if len(t) <= smin:
        return [t]
    pieces: list[str] = []
    for seg in _segments(t):
        pieces.extend(_hard_split(seg, hard, tgt))
    spans: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for piece in pieces:  # 贪心装箱: 现片≥0.6×target 且加下段超target→封片
        if cur and cur_len + len(piece) > tgt and cur_len >= tgt * 0.6:
            spans.append("\n".join(cur))
            cur, cur_len = [], 0
        cur.append(piece)
        cur_len += len(piece) + 1
    if cur:
        spans.append("\n".join(cur))
    if not spans:
        spans = [t]
    if len(spans) > msp:  # 超上限: 尾部并入尾片(截TAIL_CLIP防嵌入超长)
        head = spans[:msp - 1]
        tail = "\n".join(spans[msp - 1:])[:TAIL_CLIP]
        spans = head + [tail]
    if spans and len(spans[-1]) > TAIL_CLIP:
        spans[-1] = spans[-1][:TAIL_CLIP]
    return spans


def card_id_of(sess: str, dt: str, text: str) -> str:
    """卡身份=knife13_repo.card_id_of 同源同值(sha1(sess|dt|text)[:16]·_evidence_id同法)。
    本地实现同式(避免为纯函数拉起k13单例·逐字节一致由单测对拍保证)。"""
    return hashlib.sha1(f"{sess or ''}|{dt or ''}|{text or ''}".encode("utf-8")).hexdigest()[:16]


def text_head(text: str, n: int = 64) -> str:
    """片文本头(诊断用·日志禁落正文→只入库不进日志)。"""
    h = re.sub(r"\s+", " ", str(text or ""))[:n]
    return h


# ── 持久化(sqlite WAL·knife13同款纪律) ────────────────────────────────────────
class SpanVec:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        self._ver_lock = threading.Lock()
        self._cache: dict[str, tuple[int, list[str], np.ndarray, np.ndarray, list[str]]] = {}
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            # 父卡全文镜像(父卡回溯自持·不借k13 texts: SPANVEC单门独立使能时k13可缺席)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS cards(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, sess TEXT NOT NULL,
                dt TEXT NOT NULL, text TEXT NOT NULL, n_spans INTEGER NOT NULL,
                obs_ms INTEGER NOT NULL, PRIMARY KEY(tenant, card_id))""")
            # 分片向量(规格主表)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS spans(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, span_idx INTEGER NOT NULL,
                dim INTEGER NOT NULL, vec BLOB NOT NULL, text_head TEXT NOT NULL,
                PRIMARY KEY(tenant, card_id, span_idx))""")
            self._conn.commit()
        except Exception as exc:  # noqa: BLE001 — DB损坏/只读→本件整体no-op(服务不死)
            self._conn = None
            log.error("spanvec init FAILED db=%s err=%s", self.db_path, type(exc).__name__)
        self.n_obs = 0         # 观察卡数(累计)
        self.n_new = 0         # 新卡数(首见)
        self.n_spans = 0       # 落库分片数(累计)
        self.n_fb = 0          # 分片失败退整卡单片回退数
        self.n_skip = 0        # 嵌入失败跳过卡数
        self.n_recall = 0      # 直召回返回父卡次
        self.n_q = 0           # 召回查询次数
        self.q_ms = 0.0        # 召回累计耗时ms(自计时)
        self.n_err = 0

    # ── 内部小工具(容忍_conn=None) ──
    def _q(self, sql: str, args: tuple = ()) -> list[tuple]:
        with self._lock:
            return list(self._conn.execute(sql, args).fetchall())

    def _bump_ver(self, tenant: str) -> None:
        """分片缓存版本失效(observe后调用·O(1)·与_search_version成对)。"""
        with self._ver_lock:
            self._cache.pop(str(tenant), None)

    def _encode_batch(self, qmodel: Any, texts: list[str]) -> list[Any] | None:
        """批量嵌入(organ._qmodel同源→k4 memo/v4shim缓存自动生效)。失败→None(fail-soft)。"""
        if qmodel is None or not texts:
            return None
        try:
            vecs = qmodel.encode(texts, batch_size=max(1, min(BS, len(texts))),
                                 show_progress_bar=False, normalize_embeddings=True)
            out = [np.asarray(v, dtype=np.float32) for v in vecs]
            return out if len(out) == len(texts) else None
        except Exception as exc:  # noqa: BLE001 — 嵌入异常=调用方回退路径
            log.warning("spanvec encode FAILED n=%d err=%s", len(texts), type(exc).__name__)
            return None

    # ── ADD相 ──
    def observe(self, tenant: str, cards: list[dict[str, Any]] | None,
                qmodel: Any) -> int:
        """逐卡: 分片→批量嵌入→落库。fail-soft: 单卡异常→退整卡单片→再败跳过;
        总闸关/门关/空cards/无qmodel→0(零行为差)。返回新卡数。"""
        if not cards or not master_on() or self._conn is None or qmodel is None:
            return 0
        tn = str(tenant)
        todo: list[tuple[str, str, str, str, str]] = []  # (cid, sess, dt, text, raw)
        for c in cards:
            try:
                if not isinstance(c, dict):
                    continue
                raw = c.get("text")
                if not isinstance(raw, str) or not raw.strip():
                    continue
                cid = card_id_of(str(c.get("sess") or ""), str(c.get("dt") or ""), raw)
                if self._q("SELECT 1 FROM cards WHERE tenant=? AND card_id=? LIMIT 1",
                           (tn, cid)):
                    continue  # 幂等: 重放卡零增量直通
                todo.append((cid, str(c.get("sess") or ""), str(c.get("dt") or ""), raw, raw))
            except Exception:  # noqa: BLE001 — 单卡脏数据不拖垮批
                self.n_err += 1
        n_new = 0
        for cid, sess, dt, text, raw in todo:
            try:
                spans = split_spans(text)
                if not spans:
                    continue
                vecs = self._encode_batch(qmodel, spans)
                if vecs is None and len(spans) > 1:
                    # 回退: 该卡退整卡向量路径(单片=整卡·规格铁律⑤)
                    spans = [text if len(text) <= TAIL_CLIP else text[:TAIL_CLIP]]
                    vecs = self._encode_batch(qmodel, spans)
                    self.n_fb += 1
                if vecs is None:
                    self.n_skip += 1
                    continue  # organ自有索引照常=卡主数据零丢失
                now_ms = int(time.time() * 1000)
                with self._lock:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO cards VALUES(?,?,?,?,?,?,?)",
                        (tn, cid, sess, dt, text, len(spans), now_ms))
                    for idx, (sp, vec) in enumerate(zip(spans, vecs)):
                        if vec.ndim != 1 or not np.isfinite(vec).all():
                            raise ValueError("bad vec")
                        self._conn.execute(
                            "INSERT OR REPLACE INTO spans VALUES(?,?,?,?,?,?)",
                            (tn, cid, idx, int(vec.shape[0]), vec.astype(np.float32).tobytes(),
                             text_head(sp)))
                    self._conn.commit()
                n_new += 1
                self.n_spans += len(spans)
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡失败不拖垮批
                self.n_skip += 1
                self.n_err += 1
                log.error("spanvec observe card FAILED tenant=%s err=%s", tn, type(exc).__name__)
        if n_new:
            self._bump_ver(tn)
        self.n_new += n_new
        return n_new

    # ── SEARCH相 ──
    def _tenant_matrix(self, tenant: str, dim: int
                       ) -> tuple[list[str], np.ndarray, np.ndarray, list[str]] | None:
        """租户全分片矩阵(按行数版本化缓存·_tenant_fetch同范式)。dim=查询向量维度,
        维度不符分片剔除(防384/1024混入毒化·H缺陷#4同款防御)。
        返回(片键,矩阵,父卡行号,父卡id列表)或None(dim不符)。"""
        n = 0
        for (v,) in self._q("SELECT COUNT(*) FROM spans WHERE tenant=?", (tenant,)):
            n = int(v)
        with self._ver_lock:
            ent = self._cache.get(tenant)
        if ent is not None and ent[0] == n:
            keys, mat, par, pcids = ent[1], ent[2], ent[3], ent[4]
            if dim <= 0 or mat.shape[1] == dim:
                return (keys, mat, par, pcids)
            return None  # 维度不符→本轮弃用(防毒化·不重建)
        keys: list[str] = []
        chunk_dim: list[np.ndarray] = []
        rows = self._q("SELECT card_id, span_idx, dim, vec FROM spans WHERE tenant=?"
                       " ORDER BY card_id, span_idx", (tenant,))
        for cid, sidx, d, blob in rows:
            if int(d) != dim:
                continue  # 维度甄别(逐片·混维不炸通道)
            try:
                v = np.frombuffer(bytes(blob), dtype=np.float32)
            except Exception:  # noqa: BLE001
                continue
            if v.shape[0] != dim:
                continue
            keys.append(f"{cid}#{int(sidx)}")
            chunk_dim.append(v)
        if not keys:
            with self._ver_lock:
                self._cache[tenant] = (n, [], np.zeros((0, max(1, dim)), np.float32),
                                       np.zeros(0, np.int64), [])
            return ([], np.zeros((0, max(1, dim)), np.float32), np.zeros(0, np.int64), [])
        mat = np.vstack(chunk_dim)
        # 父卡行号: 键按(card_id, span_idx)有序→同卡片连续→行号=首现索引
        par = np.zeros(len(keys), dtype=np.int64)
        pcids: list[str] = []
        last_cid = None
        for i, k in enumerate(keys):
            cid = k.rsplit("#", 1)[0]
            if cid != last_cid:
                pcids.append(cid)
                last_cid = cid
            par[i] = len(pcids) - 1
        with self._ver_lock:
            self._cache[tenant] = (n, keys, mat, par, pcids)
        return (keys, mat, par, pcids)

    def recall_by_spanvec(self, tenant: str, query: str, top_n: int = TOP_N,
                          qmodel: Any = None) -> list[dict[str, Any]]:
        """query×分片→父卡回溯直召回[{id,content,dt,sess,score}]。
        score=max片分 + HIT_BONUS×(命中片数-1); max片分<MIN_SCORE不入榜。
        零向量/空库/门关/异常→[](fail-soft·服务不死)。"""
        t0 = time.perf_counter()
        out: list[dict[str, Any]] = []
        try:
            if not master_on() or self._conn is None or not str(query or "").strip():
                return []
            if qmodel is None:
                return []
            qv = np.asarray(qmodel.encode([str(query)], normalize_embeddings=True)[0],
                            dtype=np.float32)
            if qv.ndim != 1 or qv.shape[0] == 0 or not np.isfinite(qv).all() \
                    or float(np.linalg.norm(qv)) < 1e-6:
                return []  # 零向量→[](规格铁律)
            ent = self._tenant_matrix(str(tenant), int(qv.shape[0]))
            if ent is None:
                return []
            keys, mat, par, pcids = ent
            if mat.shape[0] == 0:
                return []
            scores = mat @ qv  # 批量点积(cos·已归一)
            n_par = len(pcids)
            best = np.full(n_par, -np.inf, np.float32)
            hits = np.zeros(n_par, np.int32)
            np.maximum.at(best, par, scores)
            np.add.at(hits, par, (scores >= np.float32(HIT_TH)).astype(np.int32))
            order = np.argsort(-best)[:max(1, int(top_n) * 3)]  # 粗排再过滤
            cids: list[str] = []
            cscore: dict[str, float] = {}
            for p in order:
                s = float(best[p])
                if s < MIN_SCORE or len(cids) >= max(1, int(top_n)):
                    break
                cid = pcids[int(p)]
                cids.append(cid)
                cscore[cid] = round(s + HIT_BONUS * max(0, int(hits[p]) - 1), 4)
            if not cids:
                return []
            # 父卡全文回溯(自有cards镜像·分块IN)
            got: dict[str, tuple[str, str, str]] = {}
            for i in range(0, len(cids), 100):
                chunk = cids[i:i + 100]
                sql = ("SELECT card_id, text, dt, sess FROM cards WHERE tenant=? AND card_id IN ("
                       + ",".join("?" * len(chunk)) + ")")
                for cid, text, dt, sess in self._q(sql, (str(tenant), *chunk)):
                    got[str(cid)] = (str(text), str(dt or ""), str(sess or ""))
            for cid in cids:
                row = got.get(cid)
                if row is None or not row[0]:
                    continue
                out.append({"id": cid, "content": row[0], "dt": row[1], "sess": row[2],
                            "score": cscore.get(cid, 0.0)})
            self.n_recall += len(out)
            return out
        except Exception as exc:  # noqa: BLE001 — 直召回失败=空(服务不死)
            self.n_err += 1
            log.error("spanvec recall FAILED err=%s", type(exc).__name__)
            return []
        finally:
            self.n_q += 1
            self.q_ms += (time.perf_counter() - t0) * 1000.0

    def stats(self) -> dict[str, Any]:
        try:
            if self._conn is None:
                return {"db": str(self.db_path), "dead": True, "master": master_on()}
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            n_sp = g("SELECT COUNT(*) FROM spans")
            n_cd = g("SELECT COUNT(*) FROM cards")
            return {
                "db": str(self.db_path),
                "cards": n_cd, "spans": n_sp,
                "spans_per_card": round(n_sp / n_cd, 3) if n_cd else 0.0,
                "obs_total": self.n_obs, "new_total": self.n_new,
                "spans_total": self.n_spans, "fallback_total": self.n_fb,
                "skip_total": self.n_skip, "recall_total": self.n_recall,
                "queries": self.n_q,
                "recall_ms_mean": round(self.q_ms / self.n_q, 2) if self.n_q else 0.0,
                "err_total": self.n_err,
                "master": master_on()}
        except Exception:  # noqa: BLE001
            return {"db": str(self.db_path), "dead": True, "master": master_on()}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部分片状态行。"""
        if self._conn is None:
            return 0
        n = 0
        for tbl in ("cards", "spans"):
            n += max(0, self._x(f"DELETE FROM {tbl} WHERE tenant=?", (str(tenant),)))  # noqa: S608
        self._bump_ver(str(tenant))
        return n

    def _x(self, sql: str, args: tuple = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
            return cur.rowcount


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ────────────────────────────
_SV: SpanVec | None = None
_SV_LOCK = threading.Lock()


def get() -> SpanVec:
    global _SV
    with _SV_LOCK:
        if _SV is None:
            _SV = SpanVec(DB_PATH)
            log.info("spanvec init db=%s master=%s", _SV.db_path, master_on())
        return _SV


def observe(tenant: str, cards: list[dict[str, Any]] | None, qmodel: Any) -> int:
    return get().observe(tenant, cards, qmodel)


def recall_by_spanvec(tenant: str, query: str, top_n: int = TOP_N,
                      qmodel: Any = None) -> list[dict[str, Any]]:
    """[L4] 分片直召回: query×片→父卡[{id,content,dt,sess,score}]。"""
    return get().recall_by_spanvec(tenant, query, top_n, qmodel)


def stats() -> dict[str, Any]:
    return get().stats()
