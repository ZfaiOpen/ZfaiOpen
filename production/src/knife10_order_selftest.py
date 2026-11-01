#!/usr/bin/env python3
"""knife10_order_selftest.py — k10 C2顺序注记刀模块级自测(零服务器·临时DB·20261009)
S1 总闸关=字节直通  S2 非order档让位  S3 history意图让位  S4 事件序重排(obs_seq主键)
S5 dia_idx次键(同obs_seq tie组)  S6 墙钟免疫(毒dt零参与)  S7 序号注记零改写
S8 逐退守卫严格档(注记零重排)  S9 k3缺席卡沉尾不炸  S10 空窗/k3库缺席直通
S11 散布生效档(GUARD=0缺省·事件卡自窗深处上浮)
输出: 逐例PASS/FAIL+终判; 退出码0=全过。"""
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

TD = Path(tempfile.mkdtemp(prefix="k10self_"))
os.environ["KNIFE3_DB"] = str(TD / "k3.db")
os.environ["KNIFE10_DB"] = str(TD / "k10.db")
os.environ["KNIFE3_DICT"] = str(Path(__file__).resolve().parent / "knife3_time_terms.json")

APP = Path(__file__).resolve().parent
sys.path.insert(0, str(APP))

import knife3_time as k3  # noqa: E402
import knife10_order as k10  # noqa: E402

R: dict[str, bool] = {}


def cid_of(sess, dt, text):
    return k3.card_id_of(sess, dt, text)


def setup_session(tenant, sess, events, dt_all):
    """events=[(dt,text)]按到达序observe进k3(逐卡obs_seq)+k10(dia_idx)。"""
    cards = [{"sess": sess, "dt": dt, "text": t, "dia": f"req{j}:{j}", "conv": f"{tenant}:{sess}"}
             for j, (dt, t) in enumerate(events)]
    k3.observe(tenant, cards)
    k10.observe(tenant, cards)
    return [cid_of(sess, dt, t) for dt, t in events]


def data_of(cids_events, order):
    """cids_events=[(cid,text,dt)]; order=呈现位次→事件号(0基)。"""
    return [{"id": cids_events[i][0], "content": cids_events[i][1],
             "created_at": cids_events[i][2]} for i in order]


# ── 装床: 单会话5事件·到达序=事件序·毒dt(全同dt=墙钟无法分辨) ─────────────────────
T, SESS = "t_self", "s_bake"
EV = ["Nana bought flour at the mill.", "Nana mixed the dough by hand.",
      "Nana preheated the oven to 220.", "Nana baked the bread for forty minutes.",
      "Nana cooled the loaves on the rack."]
POISON_DT = "2026-10-01T10:00:00"  # 全同dt(官方毒dt形态·dt排序无法分辨先后)
cids = setup_session(T, SESS, [(POISON_DT, t) for t in EV], POISON_DT)
ev_of = {cid: i for i, cid in enumerate(cids)}
Q = "In what order did Nana make the bread, sequence from start to finish?"

# 修前形态: 相关度乱序(末事件居首·中段次之)
pre_order = [4, 2, 0, 3, 1]

# S1 总闸关=字节直通
os.environ["BRAINOS_K10"] = "0"
d0 = data_of(list(zip(cids, EV, [POISON_DT] * 5)), pre_order)
import copy as _cp
snap = _cp.deepcopy(d0)
out, n = k10.order_annotate(T, Q, d0, "current", "order")
R["S1_gate_off_passthrough"] = (out == snap and n == 0)

# S2 非order档让位(when/duration/none)
os.environ["BRAINOS_K10"] = "1"
ok2 = True
for tier in ("when", "duration", "none"):
    d = data_of(list(zip(cids, EV, [POISON_DT] * 5)), pre_order)
    out, n = k10.order_annotate(T, Q, d, "current", tier)
    ok2 = ok2 and [x["id"] for x in out] == [cids[i] for i in pre_order] and n == 0 \
        and all("event_order" not in x.get("metadata", {}) for x in out)
R["S2_nonorder_tier_yield"] = ok2

# S3 history意图让位
d = data_of(list(zip(cids, EV, [POISON_DT] * 5)), pre_order)
out, n = k10.order_annotate(T, Q, d, "history", "order")
R["S3_history_intent_yield"] = ([x["id"] for x in out] == [cids[i] for i in pre_order] and n == 0)

# S4+S6+S7 事件序重排(毒dt零参与)+序号注记零改写
d = data_of(list(zip(cids, EV, [POISON_DT] * 5)), pre_order)
out, n = k10.order_annotate(T, Q, d, "current", "order")
got = [ev_of[x["id"]] for x in out[:5]]
md0 = out[0].get("metadata", {}).get("event_order", {})
R["S4_event_order_reorder"] = (got == [0, 1, 2, 3, 4] and n == 5)
R["S6_wallclock_immune"] = (got == [0, 1, 2, 3, 4])  # 全同dt下仍按到达序=免疫实证
R["S7_ordinal_annotate"] = (
    md0.get("seq") == 1 and md0.get("of") == 5 and md0.get("label") == "第1"
    and md0.get("basis") == "ledger_arrival"
    and out[4].get("metadata", {}).get("event_order", {}).get("label") == "第5"
    and out[0]["content"] == EV[0] and out[4]["content"] == EV[4])

# S5 dia_idx次键: 同obs_seq(模拟backfill共享seq)→dia_idx定序
conn = sqlite3.connect(str(TD / "k3.db"))
conn.execute("UPDATE cards SET obs_seq=777 WHERE tenant=?", (T,))
conn.commit()
conn.close()
d = data_of(list(zip(cids, EV, [POISON_DT] * 5)), pre_order)
out, n = k10.order_annotate(T, Q, d, "current", "order")
got5 = [ev_of[x["id"]] for x in out[:5]]
R["S5_dia_idx_tiebreak"] = (got5 == [0, 1, 2, 3, 4] and n == 5)

# S8 逐退守卫(严格档GUARD=1): 2事件在旧top10外(位11/12)→块前插致旧top10尾部噪声跌出→注记零重排
os.environ["BRAINOS_K10_GUARD"] = "1"
noise = [{"id": f"nz{i}", "content": f"unrelated noise card {i} about weather", "created_at": ""}
         for i in range(7)]
d = [*data_of(list(zip(cids, EV, [POISON_DT] * 5)), [4, 2, 0]), *noise,
     {"id": cids[3], "content": EV[3], "created_at": POISON_DT},
     {"id": cids[1], "content": EV[1], "created_at": POISON_DT}]
# 旧top10=ev4,ev2,ev0,nz0-6; 块5卡(含ev3,ev1自top10外入窗)前插→nz5,nz6跌出→守卫触发
snap_ids = [x["id"] for x in _cp.deepcopy(d)]
out, n = k10.order_annotate(T, Q, d, "current", "order")
R["S8_guard_annotate_only"] = ([x["id"] for x in out] == snap_ids and n == 5
                               and any("event_order" in x.get("metadata", {}) for x in out))

# S11 散布生效档(GUARD=0缺省): 同构造放行重排→5事件卡上浮头部按事件序(被换下噪声卡沉窗后)
os.environ["BRAINOS_K10_GUARD"] = "0"
d = [*data_of(list(zip(cids, EV, [POISON_DT] * 5)), [4, 2, 0]), *noise,
     {"id": cids[3], "content": EV[3], "created_at": POISON_DT},
     {"id": cids[1], "content": EV[1], "created_at": POISON_DT}]
out, n = k10.order_annotate(T, Q, d, "current", "order")
got11 = [ev_of[x["id"]] for x in out[:5]]
tail11 = [x["id"] for x in out[5:]]
R["S11_scatter_effective"] = (got11 == [0, 1, 2, 3, 4] and n == 5
                              and set(tail11) == {f"nz{i}" for i in range(7)})
os.environ["BRAINOS_K10_GUARD"] = "1"  # 还原严格档供S9/S10(不涉守卫·双档同形)

# S9 k3缺席卡沉尾不炸(2已知+1未知)
d = [{"id": cids[0], "content": EV[0], "created_at": POISON_DT},
     {"id": "ghost_card", "content": "card absent from k3 index", "created_at": ""},
     {"id": cids[1], "content": EV[1], "created_at": POISON_DT}]
out, n = k10.order_annotate(T, Q, d, "current", "order")
R["S9_missing_card_tail"] = (out[-1]["id"] == "ghost_card"
                             and [x["id"] for x in out[:2]] == [cids[0], cids[1]])

# S10 空窗直通
out, n = k10.order_annotate(T, Q, [], "current", "order")
R["S10_empty_passthrough"] = (out == [] and n == 0)

print(json.dumps({k: ("PASS" if v else "FAIL") for k, v in R.items()}, ensure_ascii=False))
print(f"stats: {json.dumps(k10.stats(), ensure_ascii=False)}")
print(f"== knife10_order_selftest 终判: {sum(R.values())}/{len(R)} PASS ==")
sys.exit(0 if all(R.values()) else 1)
