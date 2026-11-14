#!/usr/bin/env python3
"""knife1_selftest.py — knife1_supersede.py离线自检(部署前必过·零网络·零organ)
跑法(196): KNIFE1_DB=<自检db> /home/zfai/venvs/brainos/bin/python knife1_selftest.py
覆盖: 抽取正例/负例(不可解主语跳过)/值归一边界/同值幂等/supersede链/conflict对/意图检测
      /search过滤(排除非降权·时间线全量+dt序)/重启持久(重开db句柄再查)/脏输入不炸。
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import knife1_supersede as k1  # noqa: E402

SELF_DB = Path(os.environ.get("KNIFE1_DB", str(Path(__file__).resolve().parent / "selftest_knife1.db")))
if SELF_DB.exists():
    SELF_DB.unlink()
os.environ["KNIFE1_DB"] = str(SELF_DB)

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {detail}" if detail else ""))


def facts_of(text, speaker="user"):
    return k1.extract_facts(text, speaker)


def one(text, speaker="user"):
    f = facts_of(text, speaker)
    return f[0] if f else None


# ── 1 抽取: D1核心对(live_in同pred→supersede可触发) ──
f = one("Alice lives in Paris.")
check("X1 alice live_in paris", f == ("alice", "live_in", "supersede", "paris"), str(f))
f = one("Alice moved to London.")
check("X2 alice live_in london(moved to)", f == ("alice", "live_in", "supersede", "london"), str(f))
f = one("I moved to London last month.")
check("X3 @user live_in london·时间尾剥离", f == ("@user", "live_in", "supersede", "london"), str(f))
f = one("Alice moved from Paris to London.")
check("X4 moved-from-to值=尾城市", f == ("alice", "live_in", "supersede", "london"), str(f))
f = one("我住在巴黎")
check("X5 中文住巴黎", f == ("@user", "live_in", "supersede", "巴黎"), str(f))
f = one("我搬到伦敦了")
check("X6 中文搬到伦敦·剥了", f == ("@user", "live_in", "supersede", "伦敦"), str(f))
f = one("爱丽丝最近搬到了伦敦")
check("X7 中文专名主语+剥最近", f == ("爱丽丝", "live_in", "supersede", "伦敦"), str(f))
f = one("Bob works at Google.")
check("X8 work_at", f == ("bob", "work_at", "supersede", "google"), str(f))
f = one("Alice is currently living in Tokyo")
check("X9 currently living in→tokyo·主语跳is", f == ("alice", "live_in", "supersede", "tokyo"), str(f))
f = one("I got married to Sarah.")
check("X10 spouse sarah", f == ("@user", "spouse", "supersede", "sarah"), str(f))
f = one("我买了一台特斯拉")
check("X11 owns 特斯拉", f == ("@user", "owns", "supersede", "一台特斯拉"), str(f))
f = one("I like sushi.")
check("X12 likes sushi(preference)", f == ("@user", "likes", "preference", "sushi"), str(f))
f = one("My favorite food is pizza.")
check("X13 favorite pizza", f == ("@user", "favorite", "preference", "pizza"), str(f))

# ── 2 抽取负例: 不可解主语跳过(零LLM边界·宁缺勿错) ──
check("N1 三人称代词He跳过", one("He moved to London.") is None)
check("N2 she跳过", one("she works at Google now.") is None)
check("N3 空文本不炸", facts_of("") == [])
check("N4 纯标点不炸", facts_of("。。。!!!") == [])
check("N5 无谓语模式不产事实", one("Alice had lunch with Bob.") is None)
f = one("I live in Paris.")
check("N6 大写I=代词非专名(@user)", f is not None and f[0] == "@user", str(f))
fa = one("I moved to London.", speaker="user")
fb = one("I moved to Rome.", speaker="assistant")
check("N7 双说话人I不互踩(@user/@assistant)", fa is not None and fb is not None
      and fa[0] == "@user" and fb[0] == "@assistant", f"{fa} {fb}")
f = one("My wife is Sarah.")
check("N8 My=代词→@user", f is not None and f[0] == "@user" and f[1] == "spouse", str(f))

# ── 3 值归一边界 ──
check("V1 同义值归一(Paris, France.)", one("Alice lives in Paris, France.")[3] == "paris")
check("V2 for子句截断", one("Alice moved to London for a new job.")[3] == "london")
check("V3 now尾剥离", one("I live in Paris now")[3] == "paris")
check("V4 ago尾剥离", one("I bought a Tesla two years ago.")[3].startswith("tesla"), one("I bought a Tesla two years ago.")[3])

# ── 4 全流程: observe→filter(supersede排除·时间线保留·幂等·持久) ──
K = k1.Knife1(SELF_DB)
T = "probe_selftest"
cid = {}


def addcard(K, T, n, text, dt):
    c = {"conv": f"s{n}", "sess": f"s{n}", "dia": f"r{n}:0", "speaker": "user", "text": text, "dt": dt}
    K.observe(T, [c])
    return k1.card_id_of(f"s{n}", dt, text)


p1 = addcard(K, T, 1, "Alice lives in Paris.", "2026-10-07T09:00:00")
p2 = addcard(K, T, 2, "Alice moved to London.", "2026-10-08T10:00:00")
data = [{"id": p1, "content": "Alice lives in Paris.", "created_at": "2026-10-07T09:00:00"},
        {"id": p2, "content": "Alice moved to London.", "created_at": "2026-10-08T10:00:00"}]

kept, intent, dropped = K.filter_evidence(T, "Where does Alice live?", list(data), 10)
check("F1 D1当前态: 排除paris留london",
      intent == "current" and dropped == 1 and [d["id"] for d in kept] == [p2], f"{intent} drop={dropped}")
kept2, intent2, dropped2 = K.filter_evidence(T, "Where has Alice lived?", list(data), 10)
check("F2 时间线: 全量+dt升序",
      intent2 == "history" and dropped2 == 0 and [d["id"] for d in kept2] == [p1, p2], f"{intent2}")
kept3, _, _ = K.filter_evidence(T, "我住哪", list(data), 10)
check("F3 中文当前态默认排除", [d["id"] for d in kept3] == [p2])
kept4, _, _ = K.filter_evidence(T, "我住过哪些城市", list(data), 10)
check("F4 中文历史全量", len(kept4) == 2)
K.observe(T, [{"conv": "s1", "sess": "s1", "dia": "r1:0", "speaker": "user",
               "text": "Alice lives in Paris.", "dt": "2026-10-07T09:00:00"}])
st = K.stats()
check("F5 同卡重放幂等(superseded仍=1)", st["superseded"] == 1, str(st))

# supersede链: london再被tokyo覆盖→双旧卡都排除
p3 = addcard(K, T, 3, "Alice relocated to Tokyo.", "2026-10-08T11:00:00")
data3 = data + [{"id": p3, "content": "Alice relocated to Tokyo.", "created_at": "2026-10-08T11:00:00"}]
kept5, _, d5 = K.filter_evidence(T, "Where does Alice live?", list(data3), 10)
check("F6 supersede链: 只剩tokyo", d5 == 2 and [d["id"] for d in kept5] == [p3])

# conflict对: preference双卡共存
q1 = addcard(K, T, 4, "Alice likes sushi.", "2026-10-07T11:00:00")
q2 = addcard(K, T, 5, "Alice likes ramen.", "2026-10-08T12:00:00")
pq = [{"id": q1, "content": "Alice likes sushi.", "created_at": "2026-10-07T11:00:00"},
      {"id": q2, "content": "Alice likes ramen.", "created_at": "2026-10-08T12:00:00"}]
kept6, _, d6 = K.filter_evidence(T, "What does Alice like?", pq, 10)
check("F7 D2 conflict对共存不排除", d6 == 0 and len(kept6) == 2)

# ── 5 意图检测边界 ──
for q, want in [("Where does Alice live?", "current"), ("Where has Alice lived?", "history"),
                ("Where did Alice work?", "history"), ("Did Alice ever live in Paris?", "history"),
                ("Which project did Alice choose for the Q1 launch?", "history"),
                ("What phone does Alice use?", "current"), ("Who is Alice's advisor?", "current"),
                ("我住哪", "current"), ("我住过哪些城市", "history"), ("Alice的时间线", "history"),
                ("Tell me Alice's history of jobs", "history"), ("What is her favorite food?", "current")]:
    got, _ = k1.detect_intent(q)
    check(f"I {q!r}→{want}", got == want, got)

# ── 6 重启持久: 新句柄重开同db, superseded关系仍在 ──
K2 = k1.Knife1(SELF_DB)
kept7, _, d7 = K2.filter_evidence(T, "Where does Alice live?", list(data3), 10)
check("P1 重启后superseded仍生效(落盘WAL)", d7 == 2 and [d["id"] for d in kept7] == [p3])

# ── 7 脏输入不炸 ──
try:
    K.observe(T, [{"conv": "", "sess": "", "dia": "", "speaker": "", "text": None, "dt": ""}])
    K.observe(T, [])
    K.observe(T, None)
    check("D1 脏输入fail-soft不炸", True)
except Exception as e:  # noqa: BLE001
    check("D1 脏输入fail-soft不炸", False, repr(e))
try:
    K.filter_evidence(T, "", [], 5)
    K.filter_evidence("ghost_tenant", "Where does Alice live?", list(data), 10)
    check("D2 空data/陌生租户不炸", True)
except Exception as e:  # noqa: BLE001
    check("D2 空data/陌生租户不炸", False, repr(e))

npass, total = sum(results), len(results)
print(f"\n== knife1_selftest 终判: {npass}/{total} PASS ==")
sys.exit(0 if npass == total else 1)
