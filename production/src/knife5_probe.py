#!/usr/bin/env python3
"""knife5_probe.py — S3多跳扩查刀验收探针(HTTP实弹)
形态(对表 briefing B2探针规约+官方因果链/关系链子能力·query自造非官方题·零题目先验):
  每案=多轮链式会话(桥卡→目标卡)+30卡填充压载(复刻官方"不缺召回缺分辨"床形·单跳漏可见化)
  P1 关系链(中): "小明的哥哥是小智" → "小智住在上海" → 查"小明的哥哥住在哪里?" → top10含上海卡
  P2 关系链(英): brother Theo → Lisbon → 查 "Where does my brother live?"
  P3 因果链(英·B2形态): grill故障根因 → 查 why → top10含根因卡
  P4 零回退守护: 聚合history query→S3让位(窗召回不缩·序为dt升序)
用法: knife5_probe.py <base> <uid前缀>   (独立uid防串扰·判例_139)
输出: 逐案PASS/FAIL(含修前rank) + 终判行; 退出码0=全过
注意: 同一NS幂等重放(rid固定)·可对K5=0/K5=1两态复跑同窗对照。"""
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8093").rstrip("/")
NS = (sys.argv[2] if len(sys.argv) > 2 else f"k5probe1009_{int(time.time())}")
KEY = (Path.home() / "zo_deploy/app/.api_key").read_text().strip()

FILLERS = [
    ("Nadia", "The community garden plot finally drained after all that rain."),
    ("Oskar", "Rain season always wrecks the gravel path by the shed."),
    ("Nadia", "I replanted the basil rows where the water pooled."),
    ("Oskar", "Poolings aside, the tomatoes look happy this year."),
    ("Nadia", "Happy tomatoes, sad lettuce — that is gardening."),
    ("Oskar", "The compost heap is doing its thing, slowly."),
    ("Nadia", "Slow is fine, smells are not."),
    ("Oskar", "Fair. Lime helps with the smell, a little."),
    ("Nadia", "I will try lime this weekend then."),
    ("Oskar", "Wear gloves, that stuff is caustic."),
    ("Petra", "The ferry timetable changed for the winter schedule."),
    ("Sune", "Winter ferities always cut the late crossings.".replace("ferities", "ferries")),
    ("Petra", "The last one leaves at seven now, not nine."),
    ("Sune", "Seven is tight after my shift ends."),
    ("Petra", "Carpool with Ingrid, she drives past the dock."),
    ("Sune", "Ingrid's car smells of herring, but fine."),
    ("Petra", "Herring is the price of punctuality."),
    ("Sune", "Punctual and salty, what a combo."),
    ("Petra", "Bring coffee and I stop complaining."),
    ("Sune", "Thermos coffee, the official currency of ferries."),
    ("Ravi", "My bike chain snapped halfway up the viaduct."),
    ("Lena", "The viaduct climb breaks chains and spirits."),
    ("Ravi", "I walked it home, chain over my shoulder."),
    ("Lena", "Chains are cheap, knees are not."),
    ("Ravi", "Ordered a spare and a tiny multitool."),
    ("Lena", "Multitool belongs in the saddle bag, always."),
    ("Ravi", "Saddle bag it is, plus a spare tube."),
    ("Lena", "Now you are basically self-sufficient."),
    ("Ravi", "Self-sufficient until the next snap."),
    ("Lena", "Bikes keep us humble, that is their job."),
]


def call(path: str, body: dict, timeout: float = 120.0):
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json", "Authorization": f"Token {KEY}"}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:200]


def add(uid: str, sid: str, rid: str, msgs: list[tuple[str, int, str]]) -> None:
    st, resp = call("/add", {"request_id": f"{NS}-{rid}", "user_id": uid, "session_id": sid,
                             "messages": [{"role": r, "timestamp": ts, "content": c}
                                          for r, ts, c in msgs]})
    assert st == 200, f"add {rid} -> {st}: {resp}"


def search(uid: str, q: str, top_k: int = 100):
    st, resp = call("/search", {"query": q, "user_id": uid, "top_k": top_k})
    assert st == 200, f"search -> {st}: {resp}"
    return resp.get("data", [])


def rank_of(rows: list[dict], anchor: str):
    for i, c in enumerate(rows, 1):
        if anchor.lower() in str(c.get("content", "")).lower():
            return i
    return None


FAILS: list[str] = []
T_MS = 1730000000000


def chain_case(pid: str, q: str, anchor: str) -> None:
    uid = f"{NS}_{pid.lower()}"
    if pid == "P1":
        add(uid, "s1", f"{pid}a", [("user", T_MS, "我的名字叫小明。"),
                                   ("assistant", T_MS + 60000, "好的小明，记下了。"),
                                   ("user", T_MS + 120000, "我的哥哥是小智，他比我一岁。")])
        add(uid, "s2", f"{pid}b", [("user", T_MS + 600000, "小智最近怎么样？"),
                                   ("assistant", T_MS + 660000, "他常联系你吗？"),
                                   ("user", T_MS + 720000, "小智的公司就在上海，他每天通勤两小时。")])
    elif pid == "P2":
        add(uid, "s1", f"{pid}a", [("user", T_MS, "My brother Theo calls me every Sunday."),
                                   ("assistant", T_MS + 60000, "That is a nice routine."),
                                   ("user", T_MS + 120000, "Theo is two years older than me.")])
        add(uid, "s2", f"{pid}b", [("user", T_MS + 600000, "Theo signed a lease in Lisbon last week."),
                                   ("assistant", T_MS + 660000, "Lisbon is a beautiful city."),
                                   ("user", T_MS + 720000, "Theo says the Lisbon hills already feel like home.")])
    elif pid == "P3":
        add(uid, "s1", f"{pid}a", [("user", T_MS, "The cafe's grill broke last month."),
                                   ("assistant", T_MS + 60000, "Did they fix it?"),
                                   ("user", T_MS + 120000, "The grill repair is still waiting on parts.")])
        add(uid, "s2", f"{pid}b", [("user", T_MS + 600000, "So the cafe stopped serving breakfast until the grill is fixed."),
                                   ("assistant", T_MS + 660000, "That explains the menu change."),
                                   ("user", T_MS + 720000, "The compressor died and that grill has been dead since.")])
    # 填充压载: 3会话×10卡(与校准床同形·把语料撑到~36卡让top10只覆盖~28%)
    # 填充全部早于链式会话(对表床形: dt升序窗里30填充在前·故事卡埋31-36=修前漏形态)
    for fi in range(3):
        msgs = [(spk, T_MS - (30 - (fi * 10 + j)) * 86400000, tx)
                for j, (spk, tx) in enumerate(FILLERS[fi * 10:(fi + 1) * 10])]
        add(uid, f"bg{fi}", f"{pid}f{fi}", msgs)
    rows = search(uid, q)
    r = rank_of(rows, anchor)
    ok = r is not None and r <= 10
    print(f"[{'PASS' if ok else 'FAIL'}] {pid} anchor={anchor!r} rank={r} window={len(rows)} "
          f"{'(top10)' if ok else '(MISS 修前单跳漏形态)'}")
    if not ok:
        FAILS.append(pid)


chain_case("P1", "小明的哥哥住在哪里？", "上海")
chain_case("P2", "Where does my brother live these days?", "lisbon")
chain_case("P3", "Why did the cafe stop serving breakfast?", "grill")

# P4 让位守护: 真历史聚合query在K5两态下窗召回一致(三城卡全在窗·S3零触碰)
u4 = f"{NS}_p4"
add(u4, "s1", "p4a", [("user", T_MS, "Mira grew up in Harrowgate."),
                      ("assistant", T_MS + 60000, "A market town, nice."),
                      ("user", T_MS + 120000, "Then Mira moved to Penlow for university.")])
add(u4, "s2", "p4b", [("user", T_MS + 600000, "Now Mira lives in Ashcombe."),
                      ("assistant", T_MS + 660000, "Three cities is a good map."),
                      ("user", T_MS + 720000, "Ashcombe is where Mira plans to stay.")])
for fi in range(3):
    msgs = [(spk, T_MS - (30 - (fi * 10 + j)) * 86400000, tx)
            for j, (spk, tx) in enumerate(FILLERS[fi * 10:(fi + 1) * 10])]
    add(u4, f"bg{fi}", f"p4f{fi}", msgs)
rows = search(u4, "List every city Mira has lived in.")
win_hit = sum(1 for a in ("harrowgate", "penlow", "ashcombe") if rank_of(rows, a))
print(f"[{'PASS' if win_hit == 3 else 'FAIL'}] P4 yield-guard window recall={win_hit}/3 (S3让位·窗不缩)")
if win_hit != 3:
    FAILS.append("P4")

print(f"\nknife5_probe: {'ALL PASS' if not FAILS else 'FAIL ' + ','.join(FAILS)} base={BASE} ns={NS}")
sys.exit(0 if not FAILS else 1)
