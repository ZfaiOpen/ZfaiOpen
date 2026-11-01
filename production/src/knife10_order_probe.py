#!/usr/bin/env python3
"""knife10_order_probe.py — k10 C2顺序注记刀实弹探针(部署线·同租户跨phase配对·零官方题依赖)
用法: knife10_order_probe.py <BASE> <KEY> <PHASE: pre|post>
配对设计: 两phase共用同一组uid(add幂等重放零增量)·唯一差异=服务BRAINOS_K10闸——
  pre (K10=0): P1/P2判"血洞复现"=证据窗内事件卡非事件序(普通相关度乱序/毒dt乱序);
  post(K10=1): P1/P2判"事件序+序号注记"; P3非order档零动作; P4 order词非时序锚卡守位。
探针集:
  P1 狠题·全同毒dt: 5事件会话(到达序=事件序·chatter交错·全部消息同毫秒ts)→顺序query。
      墙钟免疫实证: dt全同=任何按dt排序法失效, 到达序(ledger)是唯一真序。
  P2 狠题·反转毒dt: 5事件会话ts精确反转(dt序=事件逆序)→顺序query。dt排序若参与即翻车。
  P3 让位协议: duration/when档query→结果零event_order注记(k3其他档k10零动作)。
  P4 守护: "coffee order"(order词非时序·k3 order档误命中面)→锚卡仍在top10+content零改写。
输出: 逐探针PASS/FAIL+终判行; 退出码0=全过(按phase口径)。"""
import json
import sys
import urllib.request

BASE = sys.argv[1].rstrip("/")
KEY = sys.argv[2]
PHASE = sys.argv[3] if len(sys.argv) > 3 else "pre"
H = {"Content-Type": "application/json", "Authorization": f"Token {KEY}"}
_n = [0]
TS_SAME = "2026-10-01T09:00:00Z"  # 全同毒dt


def call(path, body):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(),
                                 headers=H, method="POST")
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read().decode())


def add(uid, sid, turns):
    _n[0] += 1
    return call("/add", {"request_id": f"{uid}_r{_n[0]:03d}", "user_id": uid,
                         "session_id": sid,
                         "messages": [{"role": "user" if i % 2 == 0 else "assistant",
                                       "timestamp": ts, "content": t}
                                      for i, (t, ts) in enumerate(turns)]})


def search(uid, q, top_k=30):
    r = call("/search", {"query": q, "user_id": uid, "top_k": top_k})
    return r.get("data", [])


def verdict(name, ok, detail=""):
    print(f"VERDICT {name}: {'PASS' if ok else 'FAIL'} {detail}")
    return ok


R = {}

# ── P1 狠题·全同毒dt(到达序=事件序·dt零分辨力) ──────────────────────────────────
u1 = "k10probe_p1_user"
add(u1, "s_bake", [("Hey, what should I cook this weekend?", TS_SAME),
                   ("Nana bought flour at the mill on Monday.", TS_SAME)])
add(u1, "s_bake", [("Nana mixed the dough with her hands.", TS_SAME),
                   ("I have a meeting at noon, remind me later.", TS_SAME)])
add(u1, "s_bake", [("Nana preheated the oven to 220 degrees.", TS_SAME)])
add(u1, "s_bake", [("Nana baked the bread for forty minutes.", TS_SAME),
                   ("Nana cooled the loaves on the window rack.", TS_SAME),
                   ("The kitchen smelled amazing all evening.", TS_SAME)])
EV1 = ["bought flour", "mixed the dough", "preheated the oven",
       "baked the bread", "cooled the loaves"]
Q1 = "In what order should Nana's bread-making steps be listed? Give the sequence of steps."
d1 = search(u1, Q1)
ev_pos = []
for e in EV1:
    p = next((i for i, x in enumerate(d1) if e in x.get("content", "")), -1)
    ev_pos.append(p)
in_win = [p for p in ev_pos if 0 <= p < len(d1)]
ev_seq = [EV1[i] for i, p in sorted(enumerate(ev_pos), key=lambda kv: (kv[1] < 0, kv[1]))
          if p >= 0]
is_event_order = [EV1.index(s) for s in ev_seq] == list(range(len(ev_seq)))
md1 = [d1[p].get("metadata", {}).get("event_order") for p in ev_pos
       if 0 <= p < len(d1)]
top10_all_events = all(0 <= p < 10 for p in ev_pos)
md_seqs = [m.get("seq") if m else None for m in md1]
md_monotonic = (all(s is not None for s in md_seqs)
                and all(a < b for a, b in zip(md_seqs, md_seqs[1:])))
if PHASE == "pre":
    R["P1_hole_visible"] = verdict(
        "P1_hole_visible(pre)", bool(in_win) and not is_event_order,
        f"ev_pos={ev_pos} seq={[EV1.index(s) for s in ev_seq]}(血洞=非事件序)")
else:
    R["P1_event_order_annotated"] = verdict(
        "P1_event_order_annotated(post)", is_event_order and top10_all_events
        and md_monotonic and all(m.get("basis") == "ledger_arrival" for m in md1),
        f"ev_pos={ev_pos} md_seq={md_seqs}(事件相对序+序号严格递增·chatter卡含叙述位次)")

# ── P2 狠题·反转毒dt(dt序=事件逆序·dt排序若参与即翻车) ────────────────────────────
u2 = "k10probe_p2_user"
BASE_TS = 1791924000  # 2026-10-14T10:00:00Z epoch-s
turns2 = []
for i, txt in enumerate([
        "Otis shipped the parcel from Lisbon on Monday.",
        "The parcel cleared customs in Madrid on Tuesday.",
        "The parcel arrived at the Paris hub on Wednesday.",
        "The parcel was loaded for Berlin on Thursday.",
        "Otis received the parcel in Berlin on Friday."]):
    ts_rev = BASE_TS + (5 - i) * 3600  # 反转: 事件1拿最晚ts·事件5拿最早ts
    turns2.append((txt, f"{ts_rev}"))
    if i == 1:
        add(u2, "s_parcel", turns2); turns2 = []
    if i == 3:
        add(u2, "s_parcel", turns2 + [("Hope it is not lost, tracking says in transit.",
                                       f"{BASE_TS + 6 * 3600}")]); turns2 = []
add(u2, "s_parcel", turns2) if turns2 else None
EV2 = ["shipped the parcel from Lisbon", "cleared customs in Madrid",
       "arrived at the Paris hub", "loaded for Berlin", "received the parcel in Berlin"]
Q2 = "What is the route of Otis's parcel? Give the sequence of stops in order."
d2 = search(u2, Q2)
pos2 = [next((i for i, x in enumerate(d2) if e in x.get("content", "")), -1) for e in EV2]
seq2 = [EV2[i] for i, p in sorted(enumerate(pos2), key=lambda kv: (kv[1] < 0, kv[1])) if p >= 0]
is_ord2 = [EV2.index(s) for s in seq2] == list(range(len(seq2)))
md2 = [d2[p].get("metadata", {}).get("event_order") for p in pos2 if 0 <= p < len(d2)]
if PHASE == "pre":
    R["P2_hole_visible"] = verdict(
        "P2_hole_visible(pre)", bool([p for p in pos2 if p >= 0]) and not is_ord2,
        f"pos={pos2}(毒dt反转下按dt/相关度皆乱)")
else:
    R["P2_wallclock_immune"] = verdict(
        "P2_wallclock_immune(post)", is_ord2
        and all(m and m.get("basis") == "ledger_arrival" for m in md2),
        f"pos={pos2} md_seq={[m.get('seq') if m else None for m in md2]}(到达序=事件序·dt反转零影响)")

# ── P3 让位协议: 非order档零动作(结果零event_order) ───────────────────────────────
u3 = "k10probe_p3_user"
add(u3, "s_gym", [("Mara joined the riverside gym on March 3, 2025.", "2026-10-02T09:00:00Z"),
                  ("She has been very consistent since then.", "2026-10-02T09:05:00Z")])
add(u3, "s_gym", [("Mara ran her first 10k race on May 15, 2025.", "2026-10-02T09:10:00Z")])
ok3 = True
for q3, tier in (("How long between joining the gym and the 10k race?", "duration"),
                 ("When did Mara join the riverside gym?", "when")):
    d3 = search(u3, q3)
    leak = [x.get("metadata", {}).get("event_order") for x in d3
            if x.get("metadata", {}).get("event_order")]
    ok3 = ok3 and not leak
    print(f"  P3 tier={tier} leak={len(leak)}")
R["P3_nonorder_zero_action"] = verdict("P3_nonorder_zero_action", ok3,
                                       "(duration/when档零event_order=k10让位)")

# ── P4 守护: "coffee order"(order词非时序)→锚卡守位+content零改写 ──────────────────
u4 = "k10probe_p4_user"
add(u4, "s_cafe", [("Deshawn tries many cafes near the office.", "2026-10-03T08:00:00Z"),
                   ("Deshawn always orders a flat white with oat milk.", "2026-10-03T08:05:00Z"),
                   ("The barista knows him by name now.", "2026-10-03T08:10:00Z")])
d4 = search(u4, "What is Deshawn's usual coffee order?")
anchor = next((x for x in d4 if "flat white with oat milk" in x.get("content", "")), None)
in10 = anchor is not None and d4.index(anchor) < 10 and len(d4) >= 1
R["P4_coffee_order_guard"] = verdict(
    "P4_coffee_order_guard", in10 and anchor is not None
    and anchor["content"] == "Deshawn always orders a flat white with oat milk.",
    f"pos={d4.index(anchor) if anchor else -1}(锚卡守位·原文零改写)")

core = all(R.values())
print(json.dumps({k: ("PASS" if v else "FAIL") for k, v in R.items()}, ensure_ascii=False))
print(f"== knife10_order_probe({PHASE}) 终判(uid前缀=k10probe): "
      f"{'ALL PASS' if core else 'HAS FAIL'} ==")
sys.exit(0 if core else 1)
