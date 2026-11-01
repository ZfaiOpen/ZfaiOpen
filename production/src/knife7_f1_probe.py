# knife7_f1_probe.py — S4抽取式摘要刀·F1探针(对BASE影子服务HTTP·20261009)
# 用法: python3 knife7_f1_probe.py <BASE> <KEY>
# 靶: ①长会话(30+卡)→聚合query("总结一下这段时间")→digest卡在证据;
#     ②事实query("他周二做了什么"等)→证据零digest混入(清单护栏);
#     ③滚动更新(40卡→v2·v1标superseded只回v2); ④实体触发(8事实→实体digest);
#     ⑤命中数守卫(注入≤2·只追加不夺位)。
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/")
KEY = sys.argv[2]
TS = time.strftime("%m%d_%H%M%S")
UID = f"k7f1_{TS}"

RESULTS: list[bool] = []


def check(name: str, ok: bool, ev: str = "") -> None:
    RESULTS.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {ev}" if ev else ""))


def call(path: str, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if path != "/health":
        headers["Authorization"] = f"Token {KEY}"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:200]
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def ep(iso: str) -> int:
    import datetime
    return int(datetime.datetime.strptime(
        iso, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc).timestamp() * 1000)


def add(rid: str, sid: str, turns: list[tuple[str, str, str]]) -> tuple[int, dict]:
    return call("/add", {"request_id": rid, "user_id": UID, "session_id": sid,
                         "messages": [{"role": "user", "content": t,
                                       "timestamp": ep(ts)} for t, ts in turns]})


def search(q: str, top_k: int = 100):
    st, body = call("/search", {"user_id": UID, "query": q, "top_k": top_k})
    data = body.get("data", []) if isinstance(body, dict) else []
    return st, data


def digest_ids(data) -> list[str]:
    return [d["id"] for d in data if str(d.get("id", "")).startswith("k7")]


st, h = call("/health", method="GET")
check("P0 影子health=200", st == 200, f"HTTP={st}")

S1 = "long_sess"
_TEXTS = ["周二上午陈明去市图书馆借了两本天文画册和一册星图手册。",
          "周二下午陈明在图书馆的期刊区整理了读书笔记。",
          "周三晚上陈明去体育馆上瑜伽课, 练了一组平衡拉伸。",
          "周四陈明和姐姐小雨通了电话, 小雨说家里一切安好。",
          "周五陈明在社区花园做志愿者, 给花圃浇水除草。",
          "周六陈明去老城门拍照, 顺便画了一张速写。"]


def week_turns(w: int) -> list[tuple[str, str]]:
    import datetime
    d0 = datetime.datetime(2026, 9, 1) + datetime.timedelta(days=7 * w)
    return [(t, (d0 + datetime.timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"))
            for i, t in enumerate(_TEXTS)]


# ── Phase0: 18卡(未越20阈)·_fact基线捕获(零digest态) ──
turns = week_turns(0) + week_turns(1) + week_turns(2)          # 18卡·09-01~09-21
check("P0x 长会话装配18卡", len(turns) == 18)
st_a, _b = add(f"{UID}-p0", S1, turns[:18])
check("P0a 18卡add=200", st_a == 200, f"HTTP={st_a}")
FACT_QS = ["他周二做了什么", "陈明周三晚上上了什么课", "陈明的姐姐叫什么名字",
           "陈明周六去老城做了什么"]
base_fact = {}
for q in FACT_QS:
    st, data = search(q)
    check(f"P0b 事实query基线可检索[{q[:10]}]", st == 200 and len(data) > 0,
          f"HTTP={st} n={len(data)}")
    base_fact[q] = data
st, base_agg = search("总结一下这段时间")
check("P0c 18卡时聚合query零digest(未越阈)", st == 200 and not digest_ids(base_agg),
      f"n={len(base_agg)} k7={digest_ids(base_agg)}")

# ── Phase1: +4卡=22 → 会话digest v1 ──
st_a, _b = add(f"{UID}-p1", S1, week_turns(3)[:4])
check("P1a 22卡add=200(越20阈)", st_a == 200)
st, agg1 = search("总结一下这段时间")
d1 = digest_ids(agg1)
check("P1b 聚合query证据含digest卡", st == 200 and len(d1) >= 1, f"k7={d1}")
check("P1c digest内容=覆盖计数+跨度+原文(抽取式)",
      any("[digest v1]" in d["content"] and "覆盖22卡" in d["content"]
          and "时间跨度" in d["content"] and "«" in d["content"]
          for d in agg1 if d["id"] in d1),
      "marker/digest v1+覆盖22卡+«»")
# 命中数守卫: 聚合query只追加不夺位(旧卡全保留·增量=新增真实卡+digest)
old_ids = {d["id"] for d in base_agg}
new_ok = old_ids <= {d["id"] for d in agg1}
delta = len(agg1) - len(base_agg)
check("P1d 命中数守卫: 只追加不夺位且增量≤新卡4+digest2", new_ok and 0 <= delta <= 6,
      f"hits {len(base_agg)}->{len(agg1)} (+{delta}) old_kept={new_ok}")
# ── 清单护栏(法典铁律): 事实query绝不掺digest ──
guard_ok = True
for q in FACT_QS:
    st, data = search(q)
    k7in = digest_ids(data)
    kept = {d["id"] for d in base_fact[q]} <= {d["id"] for d in data}
    d = len(data) - len(base_fact[q])
    ok = st == 200 and not k7in and kept and 0 <= d <= 4
    guard_ok = guard_ok and ok
    check(f"P1e 护栏: 事实query零digest[{q[:12]}]", ok,
          f"n={len(data)}(+{d}) k7={k7in} old_kept={kept}")
check("P1f 护栏总判: 全部事实query零digest混入", guard_ok)

# ── Phase2: +20卡=42 → v2滚动·v1 superseded ──
more = week_turns(4) + week_turns(5) + week_turns(6) + week_turns(7)[:2]  # 6+6+6+2=20
check("P2x 滚动装配20卡", len(more) == 20)
st_a, _b = add(f"{UID}-p2", S1, more[:20])
check("P2a 42卡add=200(越40阈)", st_a == 200)
st, agg2 = search("总结一下这段时间")
d2 = digest_ids(agg2)
has_v2 = any("[digest v2]" in d["content"] for d in agg2 if d["id"] in d2)
has_v1 = any("[digest v1]" in d["content"] for d in agg2 if d["id"] in d2)
check("P2b 滚动更新: v2在·v1标superseded不入结果", len(d2) >= 1 and has_v2 and not has_v1,
      f"k7={len(d2)} v2={has_v2} v1={has_v1}")
check("P2c v2覆盖计数=42", any("覆盖42卡" in d["content"] for d in agg2 if d["id"] in d2))

# ── Phase3: 实体触发(8条同实体事实·跨2会话·复用knife1抽取) ──
ent1 = [("I live in Ardenvale near the old mill.", "2026-10-01T09:00:00Z"),
        ("I work at Northlake Labs on the reactor team.", "2026-10-01T10:00:00Z"),
        ("I bought a beagle named Biscuit last spring.", "2026-10-01T11:00:00Z"),
        ("I use the night bus on Tuesdays.", "2026-10-01T12:00:00Z")]
ent2 = [("I live in Ardenvale by the canal now.", "2026-10-02T09:00:00Z"),
        ("I work at Northlake Labs day shift.", "2026-10-02T10:00:00Z"),
        ("I bought a telescope for star watching.", "2026-10-02T11:00:00Z"),
        ("I use a paper planner for everything.", "2026-10-02T12:00:00Z")]
st_a, _b = add(f"{UID}-p3a", "ent_sess_1", ent1)
st_b, _b = add(f"{UID}-p3b", "ent_sess_2", ent2)
check("P3a 实体会话add=200×2", st_a == 200 and st_b == 200)
st, agge = search("总结一下我说的所有事情")
de = digest_ids(agge)
check("P3b 实体digest在证据(k7e前缀·跨会话同实体8事实)",
      st == 200 and any(i.startswith("k7e") for i in de), f"k7={de}")
check("P3c 实体digest含实体原文引用",
      any(("ardenvale" in d["content"].lower() or "northlake" in d["content"].lower())
          for d in agge if d["id"] in de))

# ── 终判 ──
npass = sum(RESULTS)
print(f"\n== knife7_f1_probe 终判: {npass}/{len(RESULTS)} PASS · UID={UID} ==")
sys.exit(0 if npass == len(RESULTS) else 1)
