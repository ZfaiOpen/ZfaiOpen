#!/usr/bin/env python3
"""test_d1.py — D1新值覆盖全链本地测试(sys.path指向d1/补丁后副本)
场景(时间戳递增·同tenant): T1 Paris→T2 London(EN对撞)·北京→上海(CN对撞)·
planned(will move/计划搬到)不参战·无对撞场景与baseline逐字节对照(回归)。
断言①-⑧全过打印 ALL_GREEN。"""
import datetime
import importlib.util
import os
import sys
from pathlib import Path

D1 = Path(__file__).resolve().parent
BASE = D1.parent / "baseline"
sys.path.insert(0, str(D1))
sys.path.insert(1, str(BASE))

DB_D1, DB_BASE = D1 / "test_d1.db", D1 / "test_base.db"
for p in (DB_D1, DB_BASE):
    for suf in ("", "-wal", "-shm"):
        q = Path(str(p) + suf)
        if q.exists():
            q.unlink()
os.environ["BRAINOS_CC_DB"] = str(DB_D1)
os.environ["BRAINOS_CC_OVERLAY_MAX"] = "8"  # 预算旋钮放开: 测对撞语义非测picks截断(两模块同值公平)

PASS: list[str] = []
FAIL: list[str] = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  <- {detail}" if detail and not cond else ""))


def ms(date):
    t = datetime.datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    return int(t.timestamp() * 1000)


def card_of(module, tenant, text, date):
    return {"conv": f"{tenant}:c1", "sess": f"{tenant}:s1", "dia": "d0", "speaker": "",
            "text": text, "dt": module.dt_of(ms(date))}


SEED = {"id": "organ-0", "content": "unrelated filler about weather", "created_at": ""}

import card_construct as cc  # noqa: E402 — d1/补丁后副本

# ── PART A 回归对照(⑧: 无对撞场景 vs baseline 逐字节一致) ────────────────────
print("== PART A 无对撞回归对照(d1 vs baseline) ==")
os.environ["BRAINOS_CC_DB"] = str(DB_BASE)
spec = importlib.util.spec_from_file_location("cc_baseline_d1test", str(BASE / "card_construct.py"))
ccb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ccb)
msg_a = "Carol: I like hiking and I live in Denver."
out_b = ccb.derive_all([card_of(ccb, "reg_t", msg_a, "2025-03-01")], "rg1", None, "reg_t")
os.environ["BRAINOS_CC_DB"] = str(DB_D1)
out_d = cc.derive_all([card_of(cc, "reg_t", msg_a, "2025-03-01")], "rg1", None, "reg_t")
check("A1 derive_all原文卡流与baseline逐字节一致", out_b == out_d)
same, det = True, ""
for q in ("Where does Carol live now?", "Did Carol ever live in Denver?", "Where does Carol live?"):
    ib, nb = ccb.overlay_inject("reg_t", q, [dict(SEED)], 12)
    idg, nd = cc.overlay_inject("reg_t", q, [dict(SEED)], 12)
    if [d["content"] for d in ib] != [d["content"] for d in idg] or nb != nd:
        same, det = False, f"q={q!r} base={ib} d1={idg}"
check("A2 无对撞场景overlay注入(含intent词query)与baseline一致", same, det)
tb = sorted(r["text"] for r in ccb.load_derived("reg_t"))
td = sorted(r["text"] for r in cc.load_derived("reg_t"))
check("A3 边车卡集与baseline一致且零changed-from缀", tb == td and not any("changed from" in t for t in td))

# ── PART B EN对撞主场景(Alice Paris→London) ──────────────────────────────────
print("== PART B EN新旧值对撞(Alice) ==")
T = "d1_t"


def add(text, date, rid):
    return cc.derive_all([card_of(cc, T, text, date)], rid, None, T)


def db_rows():
    with cc._DB_LOCK:
        return cc._db().execute(
            "SELECT cid, dt, text, kind, fact_key, fact_obj, superseded_by "
            "FROM derived WHERE tenant=? ORDER BY seq", (T,)).fetchall()


PARIS = "Alice lives in Paris (date: 2025-01-01)"
LONDON = "Alice lives in London (date: 2025-06-01) (changed from Paris, was 2025-01-01)"
add("Alice: I live in Paris and I work as a software engineer.", "2025-01-01", "r1")
rows = db_rows()
check("B1 T1两fact卡落库·无changed-from缀",
      any(r[2] == PARIS for r in rows) and not any("changed from" in r[2] for r in rows))
w0 = cc.overlay_inject(T, "Where does Alice live now?", [dict(SEED)], 12)[0]
check("B0 对撞前current查询Paris卡在窗(基线行为保持)", any(d["content"] == PARIS for d in w0))

add("Alice: I live in London now. I moved for family reasons.", "2025-06-01", "r2")
rows = db_rows()
lod = next((r for r in rows if r[2] == LONDON), None)  # 精确匹配fact卡(防命中atom卡"London now.")
par = next((r for r in rows if r[2] == PARIS), None)
check("① T2 London卡text含'(changed from Paris'", lod is not None and "(changed from Paris" in lod[2], str(lod))
check("② 库内Paris卡superseded_by=London卡cid",
      par is not None and lod is not None and par[6] == lod[0], f"par={par}")
check("②c fact_key/fact_obj已持久化(Paris行)",
      par is not None and par[4] == f"{T}|LOC|alice" and par[5] == "Paris", str(par))
lrow = next((r for r in cc.load_derived(T) if r["text"] == LONDON), None)
prow = next((r for r in cc.load_derived(T) if r["text"] == PARIS), None)
lod_cid = cc._cid_of(lrow["sess"], lrow["dt"], lrow["text"]) if lrow else ""
check("②b commit后租户缓存失效·load_derived读到superseded_by",
      prow is not None and prow.get("superseded_by") == lod_cid, f"prow={prow} lod_cid={lod_cid}")

w1 = cc.overlay_inject(T, "Where does Alice live now?", [dict(SEED)], 12)[0]
c1 = [d["content"] for d in w1]
check("③ current意图: London卡出席且Paris卡不在窗", LONDON in c1 and PARIS not in c1, f"c1={c1}")
w2 = cc.overlay_inject(T, "Did Alice ever live in Paris?", [dict(SEED)], 12)[0]
c2 = [d["content"] for d in w2]
check("④ history意图: 两卡都出席", PARIS in c2 and LONDON in c2, f"c2={c2}")
w3 = cc.overlay_inject(T, "Where does Alice live?", [dict(SEED)], 12)[0]
c3 = [d["content"] for d in w3]
il, ip = (c3.index(LONDON), c3.index(PARIS)) if LONDON in c3 and PARIS in c3 else (-1, -1)
check("⑤ 无意图词: London卡可浮顶(idx≤2)", il in (1, 2), f"c3={c3}")
check("⑤ 无意图词: Paris卡不浮顶仅尾注(idx>2且在窗)", ip > 2, f"idx_l={il} idx_p={ip}")

# ── PART C CN对撞 + planned不参战 ────────────────────────────────────────────
print("== PART C CN对撞 + planned不参战 ==")
BJ = "李明 住在 北京 (日期: 2025-07-01)"
add("李明: 我住在北京。", "2025-07-01", "r3")
add("李明: 我搬到上海了。", "2025-08-01", "r4")
rows = db_rows()
bj = next((r for r in rows if r[2] == BJ), None)
sh = next((r for r in rows if r[2].startswith("李明 搬到 上海")), None)
check("⑥ CN对撞: 上海卡含'(changed from 北京'",
      sh is not None and "(changed from 北京" in sh[2], str(sh))
check("⑥ CN对撞: 北京卡superseded_by=上海卡cid",
      bj is not None and sh is not None and bj[6] == sh[0], f"bj={bj} sh={sh}")

add("Bob: I live in Osaka.", "2025-09-01", "r5")
add("Bob: I will move to Tokyo next year.", "2025-10-01", "r6")
add("王小明: 我打算搬到深圳。", "2025-10-02", "r7")
rows = db_rows()
osa = next((r for r in rows if r[2].startswith("Bob lives in Osaka")), None)
tok = next((r for r in rows if "plans to move to Tokyo" in r[2]), None)
sz = next((r for r in rows if r[2].startswith("王小明 计划搬到 深圳")), None)
check("⑦ planned卡(EN will move/CN 计划搬到)生成且无changed-from缀",
      tok is not None and sz is not None and "(changed from" not in tok[2] and "(changed from" not in sz[2],
      f"tok={tok} sz={sz}")
check("⑦ planned不触发对撞: Osaka卡仍开放(superseded_by空)",
      osa is not None and not (osa[6] or ""), str(osa))
check("⑦ planned卡自身开放且不落fact_key(永不被发现永不取代)",
      tok is not None and sz is not None and not (tok[6] or "") and not (sz[6] or "")
      and tok[4] is None and sz[4] is None, f"tok={tok} sz={sz}")
w4 = cc.overlay_inject(T, "Where does Alice live now?", [dict(SEED)], 12)[0]
check("⑧ 追加CN/planned后EN对撞行为不回归",
      any(d["content"] == LONDON for d in w4) and not any(d["content"] == PARIS for d in w4))

print(f"\n== 结果: PASS={len(PASS)} FAIL={len(FAIL)} ==")
if FAIL:
    for f in FAIL:
        print("  !!", f)
    print("NOT_GREEN")
    sys.exit(1)
print("ALL_GREEN")
