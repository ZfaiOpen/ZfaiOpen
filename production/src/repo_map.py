# repo_map.py — M9仓库地图(REPO MAP英雄卡·跨域移植Aider repo-map技术·20261025轨道J)
# ═══════════════════════════════════════════════════════════════════════════════
# 血洞: newFeature类query(51题无报错可查)面前, agent最需要的不是"某张卡"而是"这个仓库的
#       结构地图"——哪些文件是枢纽、哪些符号是命脉、最近修过什么(Aider repo-map:
#       tree-sitter+PageRank是2025-26代码检索共识·SWE-Bench Lite文件定位70.3%)。
#       全榜无人做"仓库级结构先验"——本刀把 k13 已建的repo社区升维成 per-community 地图卡,
#       作为编程窗"第4英雄位"(FIX/ANTI/COMPR之后·证据卡之前)。
# 既有件复用(对齐家规·零重复造轮):
#   knife13_repo  = 指纹与社区归属唯一owner(本件零指纹正则零聚类: import k13取
#                   fingerprint/card_id_of/communities_of/query_community·纯消费)。
#   knife11/13    = 边车骨架范本(模块级单例/observe fail-soft/sqlite WAL/幂等/purge)。
#   coding_window = 英雄卡置顶纪律(地图卡=heroes构建后追加·计入不挤占前3)。
# 做法(零LLM零外部API·纯PageRank+sqlite·对冻结核心/organ零修改·纯overlay可整件摘除):
#   ADD相    observe(tenant, cards): 逐卡 cid=k13.card_id_of → comm=k13.communities_of →
#            k13.fingerprint 取 paths/symbols/tests→建图(自己sqlite):
#            节点=文件f/符号s/测试t(社区内·n=覆盖卡数), 边=同卡共现无向对(权重+1·
#            全对≤300/卡·每类前缀cap防大diff爆库·文件节点全路径优先防字母序系统性丢全路径)。
#            幂等: gcards INSERT OR IGNORE rowcount=0=重放直通。
#   PageRank 纯python幂迭代(阻尼0.85·≤20轮·L1收敛1e-6·加权无向·dangling质量均摊):
#            社区内图=数百节点毫秒级。确定性: 节点/边排序后固定序浮点运算。
#   地图卡   每社区: top-8文件(全路径形态) + top-12符号(df≥50%社区卡的通用符号hub
#            不入卡面·k13 DF_ZERO同哲学) + 最近3个errsig(按卡dt·复用k13指纹) + 卡数 →
#            `REPO MAP[<社区名>]: files: … | symbols: … | recent fixes: … | cards: <n>`
#            ≤420字符。id=sha1(社区+内容)[:16]确定性。
#            增量更新防抖: observe只标dirty(comm入dirty表·旧ranks作废), 首次query命中
#            该社区才懒构建PageRank+缓存ranks表(评测/线上零query的社区零开销)。
#   SEARCH相 map_card(tenant, query)→[card]: k13.query_community命中 且 社区卡数≥8
#            (小社区地图无信息量) 才出·恒≤1张。FEAT_ONLY子闸(缺省开): query带errsig/
#            tests(报错复现形态)不出——m0配对实测地图卡按构造非gold·对bug query纯挤占
#            (0好/22坏·首gold位次+0.4)·其收益面=newFeature纯描述query的仓库定向。
#            fail-soft: 门关/k13缺席/无社区/异常→[]。
#   挂接(2处·回滚=摘2处+删本件+删repo_map.db): ①aml_service import区+
#            _knives_process的rmap.observe(k13.observe后·同款try) ②coding_window.apply
#            的heroes构建截断后追加heroes+=rmap.map_card(...)(hunk见交付文档·≤1张·
#            计入HERO_MAX语义但不挤占前3: len(heroes)入k_t扩张·keep数=KMIN+min(n_sig,…)
#            与heroes数消去=证据位不被挤占·仅窗顶时收缩)。
# 持久化   repo_map.db (sqlite WAL + synchronous=NORMAL + busy_timeout=5000·
#            knife13同款纪律·重启不丢; 表: gcards/gnodes/gedges/dirty/ranks)。
# 合规: 日志零内容 — 只记计数/类型/id哈希; 不落query/卡文本。env总闸BRAINOS_REPO_MAP
#            (缺省0=关·关=observe零行为+map_card恒[])。k13缺席=整体no-op(绝不抛)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("repo_map")

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("REPO_MAP_DB", str(APP_DIR.parent / "data" / "repo_map.db")))


# ── 总闸与参数(env可调·缺省=交付报告参数表) ─────────────────────────────────
def ON() -> bool:
    """env总闸BRAINOS_REPO_MAP(缺省0=关): 关=observe零行为·map_card恒[]。"""
    try:
        return str(os.environ.get("BRAINOS_REPO_MAP", "0")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:  # noqa: BLE001
        return False


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


def DAMPING() -> float:   return min(0.99, max(0.1, _f("BRAINOS_REPO_MAP_DAMPING", "0.85")))
def MAX_ITER() -> int:    return max(1, _i("BRAINOS_REPO_MAP_ITERS", "20"))
def TOL() -> float:       return max(1e-9, _f("BRAINOS_REPO_MAP_TOL", "1e-6"))
def MIN_CARDS() -> int:   return max(2, _i("BRAINOS_REPO_MAP_MIN_CARDS", "8"))   # 社区卡数门槛
def MAX_CHARS() -> int:   return max(120, _i("BRAINOS_REPO_MAP_CHARS", "420"))  # 地图卡长度硬闸
def SYM_DF_MAX() -> float: return min(1.0, max(0.0, _f("BRAINOS_REPO_MAP_SYM_DF", "0.5")))
# 符号展示df阈(≥此占比社区卡的符号不入卡面·k13 DF_ZERO同哲学)
def FEAT_ONLY() -> bool:
    """特征query子闸(缺省1=开): query带errsig/tests(报错复现形态)→出地图卡纯挤占
    (m0配对实测0好/22坏·地图卡按构造非gold·bug query已有FIX/ANTI英雄+k13硬加权服务);
    纯描述/新增功能query(paths/symbols/库名)才是本卡的目标面(newFeature 51题定向)。"""
    return str(os.environ.get("BRAINOS_REPO_MAP_FEAT_ONLY", "1")).strip().lower() in ("1", "true", "yes", "on")
def CAP_FILE() -> int:    return max(1, _i("BRAINOS_REPO_MAP_CAP_FILE", "10"))  # 单卡入图文件数
def CAP_SYM() -> int:     return max(1, _i("BRAINOS_REPO_MAP_CAP_SYM", "10"))   # 单卡入图符号数
def CAP_TEST() -> int:    return max(0, _i("BRAINOS_REPO_MAP_CAP_TEST", "5"))   # 单卡入图测试名数
TOP_FILES = 8   # 地图卡文件数(规格钉死·不入env=卡面契约)
TOP_SYMS = 12   # 地图卡符号数
TOP_FIX = 3     # 最近errsig数
FIX_CAP = 48    # 单条errsig渲染截断


def _k13():
    """knife13软引用(fail-soft): 缺席/损坏→None=本件整体no-op(绝不抛)。"""
    try:
        import knife13_repo as k13  # noqa: PLC0415
        return k13
    except Exception:  # noqa: BLE001
        return None


# ── PageRank(纯python·加权无向·幂迭代·确定性) ────────────────────────────────
def pagerank(nodes: list[str], edges: list[tuple[str, str, float]],
             damping: float | None = None, max_iter: int | None = None,
             tol: float | None = None) -> dict[str, float]:
    """节点表+边表[(a,b,w)]→{node: rank}(和归一1)。加权无向: rank按 w/加权度 分流;
    dangling(零度)质量均摊。确定性: 调用方传入已排序nodes/edges·固定序浮点运算。
    收敛: L1差<tol提前停·硬上限max_iter。空图→{}·单点→{n:1.0}。"""
    d = DAMPING() if damping is None else damping
    mi = MAX_ITER() if max_iter is None else max_iter
    tv = TOL() if tol is None else tol
    n = len(nodes)
    if n == 0:
        return {}
    if n == 1:
        return {nodes[0]: 1.0}
    idx = {v: i for i, v in enumerate(nodes)}
    out_w = [0.0] * n
    inn: list[list[tuple[int, float]]] = [[] for _ in range(n)]
    for a, b, w in edges:
        ia, ib = idx.get(a), idx.get(b)
        if ia is None or ib is None or ia == ib or w <= 0:
            continue
        inn[ib].append((ia, w))
        inn[ia].append((ib, w))
        out_w[ia] += w
        out_w[ib] += w
    r = [1.0 / n] * n
    base = (1.0 - d) / n
    for _ in range(mi):
        nr = [base] * n
        dmass = 0.0
        for i in range(n):
            if out_w[i] <= 0.0:
                dmass += r[i]
                continue
            share = d * r[i] / out_w[i]
            for j, w in inn[i]:
                nr[j] += share * w
        if dmass > 0.0:
            dm = d * dmass / n
            for i in range(n):
                nr[i] += dm
        diff = 0.0
        for i in range(n):
            diff += abs(nr[i] - r[i])
        r = nr
        if diff < tv:
            break
    s = sum(r)
    if s <= 0.0:
        return {v: 1.0 / n for v in nodes}
    return {nodes[i]: r[i] / s for i in range(n)}


def _clean_item(s: str) -> str:
    """渲染项清洗: 压空白+竖线换斜线(保地图卡单行管道格式可读)。"""
    return " ".join(str(s or "").split()).replace("|", "/")


# ── 持久化(sqlite WAL·knife13同款纪律) ───────────────────────────────────────
class RepoMap:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            # 卡账(幂等闸+社区归属+errsig近账) — cid与k13镜像同源同值
            self._conn.execute("""CREATE TABLE IF NOT EXISTS gcards(
                tenant TEXT NOT NULL, card_id TEXT NOT NULL, comm_id TEXT NOT NULL,
                dt TEXT NOT NULL DEFAULT '', errsig TEXT NOT NULL DEFAULT '',
                obs_ms INTEGER NOT NULL,
                PRIMARY KEY(tenant, card_id))""")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_rm_comm_dt"
                               " ON gcards(tenant, comm_id, dt)")
            # 社区图节点(文件f/符号s/测试t)·n=含本节点的不同卡数(展示期df过滤用)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS gnodes(
                tenant TEXT NOT NULL, comm_id TEXT NOT NULL,
                ntype TEXT NOT NULL, nval TEXT NOT NULL, n INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(tenant, comm_id, ntype, nval))""")
            # 社区图边(无向对·(a_type,a_val)<(b_type,b_val)字典序规范向·w=共现次数)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS gedges(
                tenant TEXT NOT NULL, comm_id TEXT NOT NULL,
                a_type TEXT NOT NULL, a_val TEXT NOT NULL,
                b_type TEXT NOT NULL, b_val TEXT NOT NULL, w INTEGER NOT NULL,
                PRIMARY KEY(tenant, comm_id, a_type, a_val, b_type, b_val))""")
            # 脏标记(observe后标·首次query懒构建消)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS dirty(
                tenant TEXT NOT NULL, comm_id TEXT NOT NULL,
                PRIMARY KEY(tenant, comm_id))""")
            try:  # 旧库无n列→补列(k13 texts同款迁移纪律·新库CREATE已含)
                self._conn.execute("ALTER TABLE gnodes ADD COLUMN n INTEGER NOT NULL DEFAULT 1")
            except Exception:  # noqa: BLE001 — 列已存在
                pass
            # PageRank缓存(懒构建产物·json={f:[[val,rank]…],s:…,t:…}全排序表)
            self._conn.execute("""CREATE TABLE IF NOT EXISTS ranks(
                tenant TEXT NOT NULL, comm_id TEXT NOT NULL,
                built_ms INTEGER NOT NULL, ranks_json TEXT NOT NULL,
                PRIMARY KEY(tenant, comm_id))""")
            self._conn.commit()
        except Exception as exc:  # noqa: BLE001 — DB损坏/只读→本件整体no-op(服务不死)
            self._conn = None
            log.error("repo_map init FAILED db=%s err=%s", self.db_path, type(exc).__name__)
        self.n_obs = 0     # 观察卡数(累计)
        self.n_new = 0     # 新卡(入图)数
        self.n_edge = 0    # 新边行数(累计·含权重增)
        self.n_build = 0   # PageRank构建次数
        self.n_card_out = 0  # 地图卡出卡次
        self.n_err = 0

    def _q(self, sql: str, args: tuple = ()) -> list[tuple]:
        with self._lock:
            return list(self._conn.execute(sql, args).fetchall())

    def _x(self, sql: str, args: tuple = ()) -> int:
        with self._lock:
            cur = self._conn.execute(sql, args)
            self._conn.commit()
            return cur.rowcount

    # ── ADD相: 建图(指纹与社区全部复用k13·本件零指纹正则) ─────────────────────
    def observe(self, tenant: str, cards: list[dict[str, Any]] | None) -> int:
        """逐卡观察→社区图(节点+共现边+errsig近账)+标dirty。fail-soft: 单卡失败只计数
        不抛(绝不影响add成功语义)。幂等: 同卡重放零增量(gcards rowcount=0直通)。
        返回新入图卡数。门关/k13缺席/DB死→0。集成契约: 须在k13.observe之后调用
        (社区归属由k13本批先写)。"""
        if not cards or not ON() or self._conn is None:
            return 0
        k13 = _k13()
        if k13 is None:
            return 0
        tn = str(tenant)
        n_new = 0
        for c in cards:
            try:
                if self._observe_card(k13, tn, c):
                    n_new += 1
                self.n_obs += 1
            except Exception as exc:  # noqa: BLE001 — 单卡脏数据不拖垮add
                self.n_err += 1
                log.error("repo_map observe card FAILED tenant=%s err=%s",
                          tn, type(exc).__name__)
        return n_new

    def _observe_card(self, k13: Any, tenant: str, c: dict[str, Any]) -> bool:
        raw = c.get("text")
        if not isinstance(raw, str) or not raw:  # 脏text(非str/空)→跳过(同k13闸面)
            return False
        sess, dt = str(c.get("sess") or ""), str(c.get("dt") or "")
        cid = k13.card_id_of(sess, dt, raw)
        comm = k13.communities_of(tenant, [cid]).get(cid)
        if not comm:
            return False  # k13未归属(零指纹卡/门关期入库)→无社区不建图
        fp = k13.fingerprint(raw)
        # 文件节点选取: 全路径(含"/")优先·词干/目录组件补位 — k13 fp按字母序·全路径
        # 排序靠后·裸前缀cap会系统性丢全路径(实测django/sympy files节空·20261025修)。
        _paths = [str(x) for x in (fp.get("paths") or [])]
        files = ([p for p in _paths if "/" in p] + [p for p in _paths if "/" not in p])[:CAP_FILE()]
        syms = [str(x) for x in (fp.get("symbols") or [])[:CAP_SYM()]]
        tests = [str(x) for x in (fp.get("tests") or [])[:CAP_TEST()]]
        if not (files or syms or tests):
            return False  # 无图节点可建(纯文本卡)
        fixes = [_clean_item(x)[:80] for x in (fp.get("errsig") or [])[:TOP_FIX]]
        now_ms = int(time.time() * 1000)
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO gcards(tenant,card_id,comm_id,dt,errsig,obs_ms)"
                " VALUES(?,?,?,?,?,?)",
                (tenant, cid, str(comm), dt, "\x1f".join(fixes), now_ms))
            if cur.rowcount == 0:
                return False  # 重放卡: 零增量直通(幂等)
            nodes: list[tuple[str, str]] = []
            seen_n: set[tuple[str, str]] = set()
            for ntype, vals in (("f", files), ("s", syms), ("t", tests)):
                for v in vals:
                    v = _clean_item(v).lower()
                    if len(v) < 2:
                        continue
                    key = (ntype, v)
                    if key not in seen_n:
                        seen_n.add(key)
                        nodes.append(key)
            for ntype, nval in nodes:
                self._conn.execute(
                    "INSERT INTO gnodes VALUES(?,?,?,?,1)"
                    " ON CONFLICT(tenant,comm_id,ntype,nval) DO UPDATE SET n=gnodes.n+1",
                    (tenant, str(comm), ntype, nval))
            # 边=同卡共现全对(≤(10+10+5)选2=300·前缀cap防大diff爆库)·规范向防重
            snodes = sorted(nodes)
            for i in range(len(snodes)):
                at, av = snodes[i]
                for j in range(i + 1, len(snodes)):
                    bt, bv = snodes[j]
                    self._conn.execute(
                        "INSERT INTO gedges VALUES(?,?,?,?,?,?,1)"
                        " ON CONFLICT(tenant,comm_id,a_type,a_val,b_type,b_val)"
                        " DO UPDATE SET w=w+1",
                        (tenant, str(comm), at, av, bt, bv))
                    self.n_edge += 1
            # 防抖: 只标dirty+作废旧ranks·PageRank懒构建
            self._conn.execute("INSERT OR IGNORE INTO dirty VALUES(?,?)", (tenant, str(comm)))
            self._conn.execute("DELETE FROM ranks WHERE tenant=? AND comm_id=?",
                               (tenant, str(comm)))
            self._conn.commit()
        self.n_new += 1
        return True

    # ── PageRank懒构建(社区内子图·数百节点毫秒级) ─────────────────────────────
    def _ranks_for(self, tenant: str, comm: str) -> dict[str, list[list[Any]]]:
        """社区→{f:[[val,rank]…],s:…,t:…}(rank降序)。dirty或无缓存→重建+缓存。
        重建失败→[]值(fail-soft·下次再试)。确定性: 节点/边全排序后进pagerank。"""
        tn, cm = str(tenant), str(comm)
        try:
            row = self._q("SELECT ranks_json FROM ranks WHERE tenant=? AND comm_id=?",
                          (tn, cm))
            is_dirty = bool(self._q("SELECT 1 FROM dirty WHERE tenant=? AND comm_id=?",
                                    (tn, cm))[:1])
            if row and not is_dirty:
                return json.loads(str(row[0][0]) or "{}")
            nodes = [(str(r[0]), str(r[1]), int(r[2])) for r in
                     self._q("SELECT ntype, nval, n FROM gnodes WHERE tenant=? AND comm_id=?"
                             " ORDER BY ntype, nval", (tn, cm))]
            if not nodes:
                return {}
            edges = [(str(r[0]), str(r[1]), str(r[2]), str(r[3]), float(r[4])) for r in
                     self._q("SELECT a_type,a_val,b_type,b_val,w FROM gedges"
                             " WHERE tenant=? AND comm_id=? ORDER BY a_type,a_val,b_type,b_val",
                             (tn, cm))]
            nlist = [f"{t}\x1f{v}" for t, v, _n in nodes]
            elist = [(f"{at}\x1f{av}", f"{bt}\x1f{bv}", w) for at, av, bt, bv, w in edges]
            pr = pagerank(nlist, elist)
            ncnt = {f"{t}\x1f{v}": n for t, v, n in nodes}
            out: dict[str, list[list[Any]]] = {"f": [], "s": [], "t": []}
            for key, rk in pr.items():
                t, v = key.split("\x1f", 1)
                if t in out:
                    out[t].append([v, round(float(rk), 8), int(ncnt.get(key, 1))])
            for t in out:
                out[t].sort(key=lambda x: (-x[1], x[0]))
            blob = json.dumps(out, sort_keys=True)
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO ranks VALUES(?,?,?,?)",
                    (tn, cm, int(time.time() * 1000), blob))
                self._conn.execute("DELETE FROM dirty WHERE tenant=? AND comm_id=?", (tn, cm))
                self._conn.commit()
            self.n_build += 1
            return out
        except Exception as exc:  # noqa: BLE001 — 构建失败=本query无地图(服务不死)
            self.n_err += 1
            log.error("repo_map pagerank build FAILED comm=%s err=%s",
                      hashlib.sha1(cm.encode()).hexdigest()[:8], type(exc).__name__)
            return {}

    # ── 地图卡渲染(≤MAX_CHARS硬闸·确定性贪心填充) ─────────────────────────────
    def _recent_fixes(self, tenant: str, comm: str) -> list[str]:
        """社区最近TOP_FIX个errsig(按卡dt降序·dt同则card_id序·空errsig卡不计)。
        近账窗口LIMIT 12张卡(fail-soft·异常→[]·不足3条有多少出多少)。"""
        try:
            rows = self._q("SELECT errsig FROM gcards WHERE tenant=? AND comm_id=?"
                           " AND errsig!='' ORDER BY dt DESC, card_id DESC LIMIT 12",
                           (str(tenant), str(comm)))
            out: list[str] = []
            for (blob,) in rows:
                for s in str(blob).split("\x1f"):
                    s = s.strip()
                    if s and s not in out:
                        out.append(s)
                    if len(out) >= TOP_FIX:
                        return out
            return out
        except Exception:  # noqa: BLE001
            return []

    def _render(self, tenant: str, comm: str, ranks: dict[str, list[list[Any]]],
                n_cards: int) -> str:
        budget = MAX_CHARS()
        head = f"REPO MAP[{_clean_item(comm)}]:"
        tail = f" | cards: {int(n_cards)}"
        # 文件展示: 全路径形态→剥URL主机/版本号形态首组件(k13 _seed_of同规则: 首组件
        # 含"."=hostname/版本非repo路径)→top8(词干hub不占文件位)
        def _is_repo_path(v: str) -> bool:
            first = str(v).split("/", 1)[0]
            return "/" in str(v) and "." not in first
        files = [_clean_item(v) for v, _r, *_n in (ranks.get("f") or [])
                 if _is_repo_path(v)][:TOP_FILES]
        # 符号展示df过滤(k13 DF_ZERO同哲学): 出现在≥SYM_DF_MAX社区卡的符号=通用词
        # (import/内置hub)无定位信息量→不占卡面(仍在图内参与PageRank传导)。
        sym_df_max = SYM_DF_MAX()
        syms = [_clean_item(v) for v, _r, *n in (ranks.get("s") or [])
                if not n or (n[0] / max(1, n_cards)) < sym_df_max][:TOP_SYMS]
        fixes = [_clean_item(v)[:FIX_CAP] for v in self._recent_fixes(tenant, comm)]

        def _fit(out: str, section: str, items: list[str], sep: str) -> str:
            """贪心整卡填充: 逐项试加·超预算即停; 一项都放不下→整节省略(节结构恒完整)。"""
            if not items:
                return out
            cur = out + f" | {section}: "
            first = True
            for it in items:
                if not it:
                    continue
                joiner = "" if first else sep
                if len(cur) + len(joiner) + len(it) + len(tail) > budget:
                    break
                cur += joiner + it
                first = False
            return out if first else cur

        out = _fit(head, "files", files, " ")
        out = _fit(out, "symbols", syms, " ")
        out = _fit(out, "recent fixes", fixes, " ; ")
        return (out + tail)[:budget].rstrip()

    # ── SEARCH相: 地图卡(第4英雄位) ───────────────────────────────────────────
    def map_card(self, tenant: str, query: str) -> list[dict[str, Any]]:
        """query→社区地图卡[{"id","content","created_at"}](恒≤1张)。出卡门:
        总闸开 ∧ k13在 ∧ query_community命中 ∧ 社区卡数≥MIN_CARDS ∧ 图非空。
        任何不满足/异常→[](fail-soft·零行为)。id=sha1(社区+内容)[:16]确定性。"""
        try:
            if not ON() or not str(query or "").strip() or self._conn is None:
                return []
            k13 = _k13()
            if k13 is None:
                return []
            tn = str(tenant)
            q = str(query)
            if FEAT_ONLY():
                qfp = k13.fingerprint(q)
                if qfp.get("errsig") or qfp.get("tests"):
                    return []  # 报错复现query: 地图卡让位给FIX/ANTI英雄(实测纯挤占)
            comm = k13.query_community(tn, q)
            if not comm:
                return []
            n_cards = 0
            for (v,) in self._q("SELECT COUNT(*) FROM gcards WHERE tenant=? AND comm_id=?",
                                (tn, str(comm))):
                n_cards = int(v)
            if n_cards < MIN_CARDS():
                return []  # 小社区地图无信息量(规格门)
            ranks = self._ranks_for(tn, str(comm))
            if not (ranks.get("f") or ranks.get("s")):
                return []
            content = self._render(tn, str(comm), ranks, n_cards)
            if len(content) < 20:
                return []
            cid = hashlib.sha1(f"{comm}|{content}".encode("utf-8")).hexdigest()[:16]
            self.n_card_out += 1
            return [{"id": cid, "content": content, "created_at": ""}]
        except Exception as exc:  # noqa: BLE001 — 地图失败=无此卡(服务不死)
            self.n_err += 1
            log.error("repo_map map_card FAILED err=%s", type(exc).__name__)
            return []

    # ── 运维面(k13同款惯例) ──────────────────────────────────────────────────
    def stats(self) -> dict[str, Any]:
        try:
            if self._conn is None:
                return {"db": str(self.db_path), "dead": True, "master": ON()}
            g = lambda q: int(self._conn.execute(q).fetchone()[0])  # noqa: E731
            return {
                "db": str(self.db_path),
                "cards": g("SELECT COUNT(*) FROM gcards"),
                "nodes": g("SELECT COUNT(*) FROM gnodes"),
                "edges": g("SELECT COUNT(*) FROM gedges"),
                "dirty": g("SELECT COUNT(*) FROM dirty"),
                "ranked_comms": g("SELECT COUNT(*) FROM ranks"),
                "obs_total": self.n_obs, "new_total": self.n_new,
                "edge_incr_total": self.n_edge, "build_total": self.n_build,
                "card_out_total": self.n_card_out, "err_total": self.n_err,
                "master": ON()}
        except Exception:  # noqa: BLE001
            return {"db": str(self.db_path), "dead": True, "master": ON()}

    def purge_tenant(self, tenant: str) -> int:
        """探针数据清理: 删本tenant全部repo_map状态行。"""
        if self._conn is None:
            return 0
        n = 0
        for tbl in ("gcards", "gnodes", "gedges", "dirty", "ranks"):
            n += max(0, self._x(f"DELETE FROM {tbl} WHERE tenant=?", (str(tenant),)))  # noqa: S608
        return n


# ── 模块级单例(aml_service经此挂接; env可换DB路径) ────────────────────────────
_RM: RepoMap | None = None
_RM_LOCK = threading.Lock()


def get() -> RepoMap:
    global _RM
    with _RM_LOCK:
        if _RM is None:
            _RM = RepoMap(DB_PATH)
            log.info("repo_map init db=%s master=%s", _RM.db_path, ON())
        return _RM


def observe(tenant: str, cards: list[dict[str, Any]] | None) -> int:
    return get().observe(tenant, cards)


def map_card(tenant: str, query: str) -> list[dict[str, Any]]:
    return get().map_card(tenant, query)


def stats() -> dict[str, Any]:
    return get().stats()


def purge_tenant(tenant: str) -> int:
    return get().purge_tenant(tenant)
