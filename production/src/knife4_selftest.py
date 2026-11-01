#!/usr/bin/env python3
"""knife4_selftest.py — knife4_streaming离线自检(零网络·零服务DB·fake organ/svc)
跑法: /home/zfai/venvs/brainos/bin/python knife4_selftest.py  (退出码0=全过)
覆盖: A M1a memo(命中/单飞并发/TTL过期/批量直通/copy隔离) B M1b onepass(小租户单趟·
      输出与参照实现逐位等价·二趟回退保留) C RW(写排他/读共享/写优先) D M2 recency
      (带内换序+集合恒等/带外禁动/同rid次序/history让位/无分保序/关门) E install幂等
      +分闸 F fail-soft(organ异常回退原路径·k1/k2/k3同款端点守卫契约) G organ冻结哨兵
"""
from __future__ import annotations

import hashlib
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import knife4_streaming as k4  # noqa: E402

results: list[bool] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {detail}" if detail else ""))


def reset_module() -> None:
    k4._installed = False
    k4._svc = None
    k4._orig_search = None
    k4._orig_add = None
    k4._created_ms.clear()
    k4._stats.clear()
    for f in ("BRAINOS_K4", "BRAINOS_K4_ENCODE_MEMO", "BRAINOS_K4_ONEPASS",
              "BRAINOS_K4_RW", "BRAINOS_K4_RECENCY"):
        os.environ.pop(f, None)


def sha16(sess: str, dt: str, text: str) -> str:
    return hashlib.sha1(f"{sess}|{dt}|{text}".encode("utf-8")).hexdigest()[:16]


def core(d: dict) -> dict:
    """响应级契约(memory_service返回面): _k4为进程内stash不对外。"""
    return {"evidence": d["evidence"], "evidence_score": d["evidence_score"]}


# ── fakes ───────────────────────────────────────────────────────────────────
class FakeQM:
    def __init__(self, delay: float = 0.0):
        self.calls = 0
        self.delay = delay
        self._lk = threading.Lock()

    def encode(self, sentences, *a, **kw):
        with self._lk:
            self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        if isinstance(sentences, list) and len(sentences) == 1:
            return bytearray(b"v:" + str(sentences[0]).encode())
        return [bytearray(b"v:" + str(s).encode()) for s in sentences]


class FakeOrgan:
    def __init__(self, tenants: int, per: int):
        self._cards: dict = {}
        self.search_ex_calls: list = []
        self.compose_calls = 0
        self.boom = False
        for t in range(tenants):
            for j in range(per):
                conv = f"t{t:02d}:s{j}"
                c = {"conv": conv, "sess": conv, "dia": f"r{t:02d}:{j}", "speaker": "user",
                     "text": f"tenant t{t:02d} meeting {j} about zebra-{t:02d} budget {j}",
                     "dt": f"2026-10-08T00:{j:02d}:00"}
                self._cards[f"{conv}|{c['dia']}"] = dict(c, key=f"{conv}|{c['dia']}")
        self._qmodel = FakeQM()

    def search_ex(self, query, conv_scope=None, topk=10):
        self.search_ex_calls.append((query, conv_scope, int(topk)))
        if self.boom:
            raise RuntimeError("organ boom (fail-soft test)")
        scored = []
        for k, c in self._cards.items():
            s = sum(1 for w in query.lower().split() if w in c["text"].lower())
            scored.append((s, k))
        scored.sort(key=lambda x: (-x[0], x[1]))
        top = scored[:topk]
        ranked = [(kk, float(s)) for s, kk in top]
        self._compose_window(ranked, set(), int(topk))
        return [self._cards[kk] for _, kk in top], float(top[0][0] if top else 0.0)

    def _compose_window(self, ranked, spk_keys, topk):
        self.compose_calls += 1
        return [kk for kk, _ in ranked[:topk]]


def ref_search(svc, query, conv_scope, top_k, tenant="default"):
    """参照实现=~/exam/memory_service.py search逐字逻辑(仅TOP_K界1..256)。"""
    k = max(1, min(256, int(top_k)))
    eff_scope = f"{tenant}:{conv_scope}" if conv_scope else None
    with svc.lock:
        if svc._total and svc._dirty:
            svc.organ.finalize_ingest()
            svc._dirty = False

        def _t(cs):
            return [c for c in cs if str(c.get("conv", "")).split(":", 1)[0] == tenant]

        if eff_scope is not None:
            cards, score = svc.organ.search_ex(query, conv_scope=eff_scope, topk=k)
            cards = [c for c in cards if str(c.get("conv", "")) == eff_scope][:k]
        else:
            total = len(svc.organ._cards)
            fetch = min(total, k * 8)
            cards, score = svc.organ.search_ex(query, conv_scope=None, topk=fetch)
            cards = _t(cards)
            if len(cards) < k and fetch < total:
                cards, score = svc.organ.search_ex(query, conv_scope=None, topk=total)
                cards = _t(cards)
            cards = cards[:k]
    ev = [{"session": c.get("sess", ""), "dt": c.get("dt", ""),
           "speaker": c.get("speaker", ""), "text": c.get("text", "")} for c in cards]
    return {"evidence": ev, "evidence_score": float(score)}


class FakeSvc:
    def __init__(self, organ: FakeOrgan):
        self.organ = organ
        self.lock = threading.RLock()
        self._total = len(organ._cards)
        self._dirty = False
        self.add_calls = 0

    def add(self, items, finalize, tenant="default"):
        with self.lock:
            self.add_calls += 1
            return {"added": len(items)}

    def search(self, query, conv_scope, top_k, tenant="default"):
        return ref_search(self, query, conv_scope, top_k, tenant)


# ── A: M1a memo ─────────────────────────────────────────────────────────────
def test_memo() -> None:
    fm = FakeQM(delay=0.03)
    memo = k4._EncodeMemo(0.5)
    enc = memo.wrap(fm.encode)
    v1 = enc(["hello world"])
    v2 = enc(["hello world"])
    check("A1 memo命中: 同串只编1次", fm.calls == 1 and bytes(v1) == bytes(v2))
    v1[:0] = b"X"  # copy隔离: 改返回体不污染缓存
    v3 = enc(["hello world"])
    check("A2 copy隔离", bytes(v3) == b"v:hello world")
    enc(["other query"])
    check("A3 不同串各自编", fm.calls == 2)
    vb = enc(["a", "b"])  # 批量直通
    check("A4 批量直通不入memo", isinstance(vb, list) and len(vb) == 2 and fm.calls == 3)
    # A5 单飞: 5线程同串同拍 → 只编1次
    fm2 = FakeQM(delay=0.05)
    enc2 = k4._EncodeMemo(1.0).wrap(fm2.encode)
    bar = threading.Barrier(5)
    outs: list = []
    err: list = []

    def w():
        try:
            bar.wait()
            outs.append(bytes(enc2(["same q"])))
        except Exception as e:  # noqa: BLE001
            err.append(e)

    ths = [threading.Thread(target=w) for _ in range(5)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(5)
    check("A5 单飞: 5路同拍只编1次且结果一致",
          not err and fm2.calls == 1 and len(set(outs)) == 1, f"calls={fm2.calls}")
    # A6 TTL过期重编
    fm3 = FakeQM()
    enc3 = k4._EncodeMemo(0.05).wrap(fm3.encode)
    enc3(["ttl q"])
    time.sleep(0.12)
    enc3(["ttl q"])
    check("A6 TTL过期重编", fm3.calls == 2, f"calls={fm3.calls}")


# ── B: M1b onepass等价 ──────────────────────────────────────────────────────
def test_onepass() -> None:
    # B1 小租户(n_t=5<k=8·全库100>64): k4单趟 且 输出与参照逐位等价
    reset_module()
    org = FakeOrgan(tenants=20, per=5)
    svc_k = FakeSvc(org)
    k4.install(svc_k)
    org_ref = FakeOrgan(tenants=20, per=5)
    svc_r = FakeSvc(org_ref)
    out_k = svc_k.search("zebra budget meeting", None, 8, "t03")
    out_r = ref_search(svc_r, "zebra budget meeting", None, 8, "t03")
    n_calls = len(org.search_ex_calls)
    check("B1 小租户单趟: 1次search_ex(参照2次)", n_calls == 1, f"calls={n_calls}")
    check("B2 单趟输出与参照逐位等价", core(out_k) == core(out_r))
    # B3 大租户(n_t=90≥k): 原逻辑路径(fetch=k*8) 等价
    reset_module()
    org = FakeOrgan(tenants=2, per=90)
    svc_k = FakeSvc(org)
    k4.install(svc_k)
    org_ref = FakeOrgan(tenants=2, per=90)
    svc_r = FakeSvc(org_ref)
    out_k = svc_k.search("zebra budget", None, 8, "t00")
    out_r = ref_search(svc_r, "zebra budget", None, 8, "t00")
    check("B3 大租户输出等价", core(out_k) == core(out_r))
    # B4 深排租户(n_t=100≥k但目标词命中他租户→首趟租户命中0<k→二趟回退) 等价+兜底趟保留
    reset_module()
    org = FakeOrgan(tenants=3, per=100)
    svc_k = FakeSvc(org)
    k4.install(svc_k)
    org_ref = FakeOrgan(tenants=3, per=100)
    svc_r = FakeSvc(org_ref)
    out_k = svc_k.search("t00", None, 8, "t02")
    out_r = ref_search(svc_r, "t00", None, 8, "t02")
    check("B4 二趟回退保留且等价", core(out_k) == core(out_r) and len(org.search_ex_calls) == 2,
          f"calls={len(org.search_ex_calls)}")
    # B5 conv_scope路径直通原逻辑
    out_k = svc_k.search("zebra", "t05:s0", 8, "t05")
    out_r = ref_search(svc_r, "zebra", "t05:s0", 8, "t05")
    check("B5 conv_scope路径等价", core(out_k) == core(out_r))


# ── C: RW锁 ─────────────────────────────────────────────────────────────────
def test_rwlock() -> None:
    rw = k4._RWLock()
    # C1 写排他
    entered = threading.Event()
    release = threading.Event()

    def rd():
        with rw.reader():
            entered.set()
            release.wait(2)

    th = threading.Thread(target=rd)
    with rw.writer():
        th.start()
        time.sleep(0.15)
        blocked = not entered.is_set()
        release.set()
    th.join(2)
    check("C1 写者持锁时读者被排除", blocked and entered.is_set())
    # C2 读共享
    e1, e2 = threading.Event(), threading.Event()

    def rd1():
        with rw.reader():
            e1.set()
            e2.wait(2)

    def rd2():
        with rw.reader():
            e2.set()

    t1, t2 = threading.Thread(target=rd1), threading.Thread(target=rd2)
    t1.start()
    e1.wait(2)
    t2.start()
    ok = e2.wait(2)
    e2.set()
    t1.join(2)
    t2.join(2)
    check("C2 双读者共享进入", ok)
    # C3 写优先: 持读者→写者排队→新读者必须排在写者后
    got_writer, got_r2 = threading.Event(), threading.Event()
    order: list[str] = []
    stop_rd = threading.Event()

    def hold():
        with rw.reader():
            stop_rd.wait(2)

    def wr():
        with rw.writer():
            got_writer.set()
            order.append("w")

    def rd3():
        with rw.reader():
            got_r2.set()
            order.append("r")

    h = threading.Thread(target=hold)
    h.start()
    time.sleep(0.05)
    w = threading.Thread(target=wr)
    w.start()
    time.sleep(0.1)  # 写者已排队
    r3 = threading.Thread(target=rd3)
    r3.start()
    time.sleep(0.15)
    r2_blocked_early = not got_r2.is_set()
    stop_rd.set()
    w.join(2)
    r3.join(2)
    h.join(2)
    check("C3 写优先: 新读者被排队且写者先完成",
          r2_blocked_early and got_writer.is_set() and got_r2.is_set()
          and order[:1] == ["w"], f"order={order}")


# ── D: M2 recency ───────────────────────────────────────────────────────────
def test_recency() -> None:
    reset_module()
    k4._created_ms.update({"r1": 1000, "r2": 2000})
    k4._svc = None  # recency_reorder不触svc
    data = [{"id": "e1"}, {"id": "e2"}]
    out = {"_k4": {"scores": {"e1": 0.100, "e2": 0.098},
                   "keys": {"e1": "t:s|r1:0", "e2": "t:s|r2:3"}}}
    r = k4.recency_reorder("t", "q", data, "current", out)
    check("D1 带内(<5%)新者在前", [x["id"] for x in r] == ["e2", "e1"])
    check("D2 集合恒等(只换序不改集)", sorted(x["id"] for x in r) == ["e1", "e2"])
    out2 = {"_k4": {"scores": {"e1": 0.100, "e2": 0.080},
                    "keys": out["_k4"]["keys"]}}
    r2 = k4.recency_reorder("t", "q", data, "current", out2)
    check("D3 带外(20%)禁动", [x["id"] for x in r2] == ["e1", "e2"])
    # D4 同rid: created相同→dia位次新者(idx大)在前
    out3 = {"_k4": {"scores": {"e1": 0.100, "e2": 0.099},
                    "keys": {"e1": "t:s|r1:1", "e2": "t:s|r1:0"}}}
    r3 = k4.recency_reorder("t", "q", data, "current", out3)
    check("D4 同rid按dia位次新者在前", [x["id"] for x in r3] == ["e1", "e2"])
    # D5 history让位(C3)
    r4 = k4.recency_reorder("t", "q", data, "history", out)
    check("D5 history时间线让位", [x["id"] for x in r4] == ["e1", "e2"])
    # D6 无分卡保序
    out5 = {"_k4": {"scores": {"e1": 0.100, "e2": 0.098},
                    "keys": {"e1": "t:s|r1:0"}}}
    data5 = [{"id": "e1"}, {"id": "eX"}, {"id": "e2"}]
    r5 = k4.recency_reorder("t", "q", data5, "current", out5)
    check("D6 无分卡切断band链保原序", [x["id"] for x in r5] == ["e1", "eX", "e2"],
          f"{[x['id'] for x in r5]}")
    # D7 总闸关门
    os.environ["BRAINOS_K4"] = "0"
    r6 = k4.recency_reorder("t", "q", data, "current", out)
    os.environ.pop("BRAINOS_K4")
    check("D7 总闸关门直通", [x["id"] for x in r6] == ["e1", "e2"])
    # D8 链式带: e1-e2带内, e2-e3带内 → 整段按created排序
    k4._created_ms.update({"r3": 3000})
    data8 = [{"id": "e1"}, {"id": "e2"}, {"id": "e3"}]
    out8 = {"_k4": {"scores": {"e1": 0.100, "e2": 0.098, "e3": 0.097},
                    "keys": {"e1": "t:s|r1:0", "e2": "t:s|r2:0", "e3": "t:s|r3:0"}}}
    r8 = k4.recency_reorder("t", "q", data8, "current", out8)
    check("D8 链式带内整段按入库时序", [x["id"] for x in r8] == ["e3", "e2", "e1"],
          f"{[x['id'] for x in r8]}")
    # D9 k3档让位(duration/when/order=C1/C2排序资产·PK4判例回归)
    r9 = k4.recency_reorder("t", "q", data, "current", out, k3_tier="duration")
    r9b = k4.recency_reorder("t", "q", data, "current", out, k3_tier="when")
    r9c = k4.recency_reorder("t", "q", data, "current", out, k3_tier="order")
    check("D9 k3非none档整体让位", [x["id"] for x in r9] == ["e1", "e2"]
          and [x["id"] for x in r9b] == ["e1", "e2"]
          and [x["id"] for x in r9c] == ["e1", "e2"])


# ── E/F: install幂等/分闸/fail-soft ─────────────────────────────────────────
def test_install_failsoft() -> None:
    reset_module()
    org = FakeOrgan(tenants=3, per=5)
    svc = FakeSvc(org)
    flags = k4.install(svc)
    check("E0 RW缺省关(红蓝实测留痕: GIL下读汇聚7.9s→3.4s)",
          flags.get("add") is False and flags.get("search") is True)
    check("E1 install全开(RW缺省关·其余三挂件True)",
          all(flags[k] for k in ("memo", "search", "compose")))
    reset_module()
    os.environ["BRAINOS_K4_RW"] = "1"
    org_rw = FakeOrgan(tenants=3, per=5)
    svc_rw = FakeSvc(org_rw)
    flags_rw = k4.install(svc_rw)
    os.environ.pop("BRAINOS_K4_RW", None)
    check("E1b RW=1时add挂件装", flags_rw.get("add") is True)
    flags2 = k4.install(svc)
    check("E2 install幂等", flags2 == {"installed": True})
    # F1 fail-soft: organ异常→原路径兜底(与pre-k4同抛) — fresh安装保证模块态一致
    reset_module()
    org_f = FakeOrgan(tenants=3, per=5)
    svc_f = FakeSvc(org_f)
    k4.install(svc_f)
    org_f.boom = True
    try:
        svc_f.search("x", None, 8, "t01")
        raised = "none"
    except RuntimeError:
        raised = "RuntimeError"
    org_f.boom = False
    check("F1 organ异常语义与原路径一致(同抛RuntimeError)", raised == "RuntimeError")
    # F2 关门路径: 总闸0 → 直通原实现
    reset_module()
    org2 = FakeOrgan(tenants=3, per=5)
    svc2 = FakeSvc(org2)
    os.environ["BRAINOS_K4"] = "0"
    flags3 = k4.install(svc2)
    os.environ.pop("BRAINOS_K4")
    check("F2 总闸关门install零挂件", flags3 == {})
    # F3 memo分闸
    reset_module()
    org3 = FakeOrgan(tenants=1, per=2)
    svc3 = FakeSvc(org3)
    os.environ["BRAINOS_K4_ENCODE_MEMO"] = "0"
    flags4 = k4.install(svc3)
    os.environ.pop("BRAINOS_K4_ENCODE_MEMO")
    check("F3 memo分闸关门", flags4.get("memo") is False and not org3._qmodel.__dict__.get("_k4_memo", False))
    # F4 add包装走writer(行为透传·RW=1显式开)
    reset_module()
    org4 = FakeOrgan(tenants=1, per=2)
    svc4 = FakeSvc(org4)
    os.environ["BRAINOS_K4_RW"] = "1"
    k4.install(svc4)
    os.environ.pop("BRAINOS_K4_RW", None)
    svc4.add([1, 2], True, "t00")
    check("F4 add包装透传(RW=1)", svc4.add_calls == 1)
    # G: organ冻结哨兵(md5变更=WARN不FAIL)
    try:
        frozen = Path.home() / "brainos_frozen/brainos/memory/hippocampal_recall.py"
        got = hashlib.md5(frozen.read_bytes()).hexdigest()
        check("G1 organ冻结哨兵md5", got == "4d880d5f65301a83427d27ce71fd1065",
              "WARN-if-changed: " + got[:8])
    except OSError as e:  # noqa: PERF203
        check("G1 organ冻结哨兵(文件不可达→跳过)", True, str(e))


if __name__ == "__main__":
    test_memo()
    test_onepass()
    test_rwlock()
    test_recency()
    test_install_failsoft()
    n_pass = sum(results)
    print(f"\nSELFTEST: {n_pass}/{len(results)} PASS, {len(results) - n_pass} FAIL")
    sys.exit(0 if n_pass == len(results) else 1)
