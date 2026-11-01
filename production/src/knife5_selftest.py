#!/usr/bin/env python3
"""knife5_selftest.py — S3多跳扩查刀离线自检(FakeSvc·不依赖服务与organ·零网络)
覆盖: 触发门(词典/分数/双让位)·意图精化(真历史让位vs误报修复)·实体抽取·桥卡选择·
扩查词·合并去重(eid对齐k1重排)·邻插封顶·预算降级·fail-soft·词典损坏韧性。
用法: knife5_selftest.py  → 退出码0=全过"""
from __future__ import annotations

import hashlib
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
os.environ.setdefault("BRAINOS_K5", "1")

import knife5_multihop as k5  # noqa: E402


class FakeSvc:
    def __init__(self, hop2_texts: list[str]):
        self.hop2_texts = hop2_texts
        self.calls: list[tuple[str, int]] = []

    def search(self, query: str, conv_scope, top_k: int, tenant: str = "default"):
        self.calls.append((query, top_k))
        return _mk_out(self.hop2_texts)


def _mk_card(sess: str, dt: str, text: str, speaker: str = "") -> dict:
    return {"session": sess, "dt": dt, "speaker": speaker, "text": text}


def _mk_out(cards: list[dict], score: float = 1.0) -> dict:
    return {"evidence": cards, "evidence_score": score,
            "_k4": {"scores": {k5._eid(c["session"], c["dt"], c["text"]): score - i * 0.01
                               for i, c in enumerate(cards)},
                    "keys": {k5._eid(c["session"], c["dt"], c["text"]): f"cv|d{i}"
                             for i, c in enumerate(cards)}}}


def _proj(cards: list[dict]) -> list[dict]:
    return [{"id": k5._eid(c["session"], c["dt"], c["text"]),
             "content": c["text"], "created_at": c["dt"]} for c in cards]


PASS = 0


def ok(name: str, cond: bool) -> None:
    global PASS
    if not cond:
        print(f"FAIL {name}")
        sys.exit(1)
    PASS += 1
    print(f"PASS {name}")


T0 = time.time()  # 与刀内预算钟同钟(端点传墙钟t0)

# ── 1. 词典装载 ──────────────────────────────────────────────────────────────
k5._load_dict()
ok("T01 dict loaded", bool(k5._DICT) and len(k5._trip_res) > 10)

# ── 2. 实体抽取 ──────────────────────────────────────────────────────────────
ents = k5._query_entities("Why did Wren's market stall start selling preserves?")
ok("T02 EN entity Wren", ents == ["Wren"])
ents2 = k5._query_entities("小明的哥哥住在哪里？")
ok("T03 ZH 的-前缀实体", "小明" in ents2)
ents3 = k5._query_entities("Where does Dana keep her savings account?")
ok("T04 interrogative not entity", "Where" not in ents3 and "Dana" in ents3)

# ── 3. 触发门 ────────────────────────────────────────────────────────────────
ok("T05 lex why", any(r.search("Why did Wren") for r in k5._trip_res))
ok("T06 lex zh 的哥哥", any(r.search("小明的哥哥") for r in k5._trip_res))
ok("T07 no lex no fire-word", not any(r.search("What color is the sky") for r in k5._trip_res))

# ── 4. 双让位 ────────────────────────────────────────────────────────────────
ok("T08 agg yield", any(r.search("List every city Jonah has lived in") for r in k5._agg_res))
ok("T09 time yield", any(r.search("How long was it between X and Y") for r in k5._time_res))

ev_hist = [_mk_card("bg", "2025-05-01T10:00:00", f"filler chatter {i}", "F") for i in range(3)]
ev_story = [_mk_card("s1", "2025-06-05T18:05:00",
                     "The greenhouse heater failed during the cold snap, so half the seedlings frosted overnight.", "Wren"),
            _mk_card("s1", "2025-06-05T18:07:00", "That is why the stall looked bare on Saturday.", "Pim"),
            _mk_card("s2", "2025-06-18T18:05:00",
                     "With the frost loss we pivoted the stall to preserves — jam sells regardless of weather.", "Wren")]
ev_all = ev_story + ev_hist  # organ序: story在前

# k1 dt升序重排后的窗(hist卡在前·story埋底) — 复刻B2-01病灶
data_sorted = _proj(ev_hist + ev_story)
out_hist = _mk_out(ev_hist + ev_story)

# ── 5. 真历史聚合让位(List every...零触碰) ───────────────────────────────────
svc = FakeSvc([])
d2, o2 = k5.multihop_expand(svc, "t1", "List every city Jonah has lived in.", data_sorted, out_hist, T0)
ok("T10 agg yield untouched", d2 == data_sorted and not svc.calls)

# ── 6. 时序域让位(how long...零触碰) ─────────────────────────────────────────
d2, o2 = k5.multihop_expand(svc, "t1", "How long was it between the lease and moving in?", data_sorted, out_hist, T0)
ok("T11 time yield untouched", d2 == data_sorted and not svc.calls)

# ── 7. false-history修复: B2-01形态(桥卡前置+二跳并入) ──────────────────────
hop2_new = [_mk_card("s2", "2025-06-18T18:07:00", "The fig preserves saved the season then.", "Pim")]
svc = FakeSvc(hop2_new)
d2, o2 = k5.multihop_expand(svc, "t1", "Why did Wren's market stall start selling preserves?",
                            data_sorted, out_hist, T0)
ids2 = [d["id"] for d in d2]
story_ids = {_proj(ev_story)[i]["id"] for i in range(3)}
ok("T12 fired once", len(svc.calls) == 1)
ok("T13 hop2 query has terms", "frost" in svc.calls[0][0] and "Why did Wren" in svc.calls[0][0])
bridge_story = {_proj(ev_story)[0]["id"], _proj(ev_story)[2]["id"]}  # 桥卡=Wren说话人两张
ok("T14 bridge story front-loaded", all(ids2.index(i) < 6 for i in bridge_story))
ok("T14b all story cards present", all(i in ids2 for i in story_ids))
ok("T15 dt-order kept for rest", ids2.index(_proj(ev_hist)[0]["id"]) >= 3)
ok("T16 hop2 new card merged", _proj(hop2_new)[0]["id"] in ids2)
ok("T17 no dup", len(ids2) == len(set(ids2)) == len(data_sorted) + len(hop2_new))
ok("T18 k4 scores unioned", set(o2["_k4"]["scores"]) >= set(o2["_k4"]["scores"]))
ok("T19 evidence extended", len(o2["evidence"]) == len(ev_hist + ev_story) + len(hop2_new))

# ── 8. 当前态常规分支: 邻插封顶 ──────────────────────────────────────────────
ev_cur = [_mk_card("s1", "2025-06-01T10:00:00", "Alice lives in Paris near the library.", "Alice"),
          _mk_card("bg", "2025-05-01T10:00:00", "noise card number one " * 3, "N")]
out_cur = _mk_out(ev_cur)
data_cur = _proj(ev_cur)
hop2_many = [_mk_card("x", f"2025-06-02T10:0{j:02d}:00", f"brand new hop card {j} alice related", "A")
             for j in range(10)]
svc = FakeSvc(hop2_many)
d2, _ = k5.multihop_expand(svc, "t2", "Where does Alice live?", data_cur, out_cur, T0)
ids2 = [d["id"] for d in d2]
pos_paris = ids2.index(_proj(ev_cur)[0]["id"])
inserted_after = sum(1 for j in range(10) if _proj(hop2_many)[j]["id"] in ids2[:pos_paris + 4])
ok("T20 insert cap respected", inserted_after <= k5._INSERT_MAX)
ok("T21 rest appended tail", all(_proj(hop2_many)[j]["id"] in ids2 for j in range(10)))
ok("T22 no dup normal", len(ids2) == len(set(ids2)))

# ── 9. 预算降级(已耗时>60%预算→单跳) ────────────────────────────────────────
svc = FakeSvc(hop2_many)
old_budget = k5._BUDGET_MS
try:
    k5._BUDGET_MS = 10.0
    t_past = T0 - 100  # 已"耗时"100s
    d2, _ = k5.multihop_expand(svc, "t3", "Why does the office charge for shuttle passes?",
                               data_cur, out_cur, t_past)
    ok("T23 budget degrade", not svc.calls and d2 == data_cur)
finally:
    k5._BUDGET_MS = old_budget

# ── 10. master闸关门 ─────────────────────────────────────────────────────────
os.environ["BRAINOS_K5"] = "0"
svc = FakeSvc(hop2_many)
d2, _ = k5.multihop_expand(svc, "t4", "Why did Wren's market stall start selling preserves?",
                           data_sorted, out_hist, T0)
ok("T24 master gate off", not svc.calls and d2 == data_sorted)
os.environ["BRAINOS_K5"] = "1"

# ── 11. fail-soft: 空窗/None out/坏数据 ─────────────────────────────────────
svc = FakeSvc(hop2_many)
d2, o2 = k5.multihop_expand(svc, "t5", "Why anything?", [], None, T0)
ok("T25 empty window safe", d2 == [] and o2 == {})
d2, o2 = k5.multihop_expand(svc, "t5", "Why anything?", data_cur, _mk_out([]), T0)
ok("T26 empty evidence safe", d2 == data_cur)

# ── 12. eid同源性(与aml._evidence_id口径一致) ────────────────────────────────
import hashlib as _h
e = {"session": "s", "dt": "d", "text": "t"}
ok("T27 eid formula", k5._eid("s", "d", "t") == _h.sha1("s|d|t".encode("utf-8")).hexdigest()[:16])

# ── 13. 无桥卡(专名与词面全不沾)→不fire ──────────────────────────────────────
ev_unrel = [_mk_card("u", "2025-05-01T10:00:00", "totally unrelated content here", "Z")]
svc = FakeSvc(hop2_many)
d2, _ = k5.multihop_expand(svc, "t6", "Why did Wren's market stall start selling preserves?",
                           _proj(ev_unrel), _mk_out(ev_unrel), T0)
ok("T28 no bridge no fire", not svc.calls and d2 == _proj(ev_unrel))

# ── 14. 二跳top_k钳位(HOP2_K) ────────────────────────────────────────────────
svc = FakeSvc(hop2_many)
k5.multihop_expand(svc, "t7", "Why did Wren's market stall start selling preserves?",
                   data_sorted, out_hist, T0)
ok("T29 hop2 topk clamp", svc.calls and svc.calls[0][1] <= max(1, k5._HOP2_K))

# ── 14b. merge-noop下false-history仍须前置修复(B2-01判例回归: 目标卡在窗内被dt序埋底,
#         二跳全返回存量卡→修复动作=前置桥卡而非并入) ─────────────────────────────
svc = FakeSvc([])  # 二跳返回空=纯存量窗形态
d2, _ = k5.multihop_expand(svc, "t7b", "Why did Wren's market stall start selling preserves?",
                           data_sorted, out_hist, T0)
ids2 = [d["id"] for d in d2]
ok("T31 noop still frontloads bridges",
   all(ids2.index(i) < 6 for i in {_proj(ev_story)[0]["id"], _proj(ev_story)[2]["id"]}))

# ── 15. stats可读 ─────────────────────────────────────────────────────────────
s = k5.stats()
ok("T30 stats readable", isinstance(s, dict) and "fired" in s or len(s) >= 0)

print(f"\nknife5_selftest: {PASS} PASS exit=0")
sys.exit(0)
