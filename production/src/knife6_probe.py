#!/usr/bin/env python3
"""knife6_probe.py — S7刀实弹探针(G2规则卡/G4趋势卡·独立uid·零内容依赖官方评分)
用法: knife6_probe.py <BASE> <KEY> <PHASE>     (PHASE=pre/post — 探针uid带相位, 幂等互不重放)
探针集:
  PR1 G2规则卡(中): add"以后每周都去健身房三次，雷打不动。" → search"他的锻炼规则是什么？"
      修后要求: 证据含规则原句卡(含"健身房")且置于前3位(metadata.knife6.kind=rule)
  PR2 G2规则卡(EN·床G2同构): add"no food or drink near the lathes, ever — label everything."
      → search"What are the workshop rules?" → 规则卡在前3位
  PR3 G4趋势卡: 三次体重(70/72/74kg·不同日) → search"他的体重趋势如何？"
      修后要求: 趋势卡在前3位·含"range [70, 74]"·含"rising"(metadata.knife6.kind=trend)
  PR4 死护栏: 同租户事实query"他周一做了什么？" → 证据中metadata.knife6卡数=0(命中数守卫)
      且事实卡照常返回
输出: 逐探针PASS/FAIL+终判行; 退出码0=全过。pre相位预期FAIL(血洞复现), post预期全PASS。
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/")
KEY = sys.argv[2]
PHASE = sys.argv[3] if len(sys.argv) > 3 else "post"
TAG = f"k6probe_s7_{PHASE}_" + time.strftime("%m%d_%H%M%S")
H = {"Content-Type": "application/json", "Authorization": f"Token {KEY}"}

results = []


def check(name, ok, evidence=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {evidence}" if evidence else ""))


def call(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=H, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:200]
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def add(uid, sid, rid, text, ts):
    return call("/add", {"request_id": f"{TAG}-{rid}", "user_id": uid, "session_id": sid,
                         "messages": [{"role": "user", "content": text, "timestamp": ts}]})


def search(uid, q, top_k=100):
    return call("/search", {"user_id": uid, "query": q, "top_k": top_k})


def epoch_ms(iso):
    import datetime
    dt = datetime.datetime.fromisoformat(iso).replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def k6_cards(data):
    out = []
    for i, d in enumerate(data):
        md = (d.get("metadata") or {}).get("knife6") or {}
        if md:
            out.append((i, md.get("kind"), d.get("content", "")))
    return out


# ── PR0 健康 ──
st, h = call("/health", method="GET")
check("PR0 health 2xx", 200 <= st < 300, f"HTTP={st} {h if st!=200 else ''}")

# ── PR1 G2规则卡(中) ──
U1 = f"{TAG}_rule_zh"
st, _ = add(U1, "s1", "a", "以后每周都去健身房三次，雷打不动。", epoch_ms("2026-10-01T09:00:00"))
st2, b = search(U1, "他的锻炼规则是什么？")
data = b.get("data", []) if isinstance(b, dict) else []
k6s = k6_cards(data)
gym_pos = next((i for i, d in enumerate(data) if "健身房" in d.get("content", "")), None)
ok1 = st == 200 and st2 == 200 and gym_pos is not None and gym_pos < 3 \
    and any(k == "rule" for _, k, _ in k6s)
check("PR1 G2规则卡(中)·规则卡在证据且前3", ok1,
      f"gym_pos={gym_pos} k6={[(i, k) for i, k, _ in k6s][:3]}")

# ── PR2 G2规则卡(EN·床G2同构) ──
U2 = f"{TAG}_rule_en"
add(U2, "s1", "b", "Wen: Makerspace orientation: no food or drink near the lathes, ever "
                   "— and label everything you store in the bins.", epoch_ms("2026-10-01T09:10:00"))
st, b = search(U2, "What are the workshop rules?")
data = b.get("data", []) if isinstance(b, dict) else []
lat_pos = next((i for i, d in enumerate(data) if "lathes" in d.get("content", "")), None)
k6s2 = k6_cards(data)
ok2 = bool(st == 200 and lat_pos is not None and lat_pos < 3
           and any(k == "rule" for _, k, _ in k6s2))
check("PR2 G2规则卡(EN)·前3且kind=rule", ok2, f"lathes_pos={lat_pos} k6={[(i, k) for i, k, _ in k6s2][:3]}")

# ── PR3 G4趋势卡 ──
U3 = f"{TAG}_trend"
add(U3, "s1", "c1", "Weight check: I weigh 70 kg today.", epoch_ms("2026-10-01T09:00:00"))
add(U3, "s1", "c2", "Weight check: I weigh 72 kg today.", epoch_ms("2026-10-04T09:00:00"))
add(U3, "s1", "c3", "Weight check: I weigh 74 kg today.", epoch_ms("2026-10-07T09:00:00"))
st, b = search(U3, "他的体重趋势如何？")
data = b.get("data", []) if isinstance(b, dict) else []
k6s = k6_cards(data)
trend_cards = [(i, c) for i, k, c in k6s if k == "trend"]
ok3 = bool(st == 200 and trend_cards and trend_cards[0][0] < 3
           and "range [70, 74]" in trend_cards[0][1] and "rising" in trend_cards[0][1])
check("PR3 G4趋势卡·前3·带区间与方向", ok3,
      f"trend={[(i, c[:60]) for i, c in trend_cards][:1]}")

# ── PR4 死护栏(命中数守卫) ──
st, b = search(U1, "他周一做了什么？")
data = b.get("data", []) if isinstance(b, dict) else []
n_k6 = len(k6_cards(data))
ok4 = st == 200 and n_k6 == 0
check("PR4 事实query证据零knife6卡(守卫=0)", ok4, f"k6_cards={n_k6} total={len(data)}")
st, b = search(U3, "What did he do on Monday?")
n_k6b = len(k6_cards(b.get("data", []) if isinstance(b, dict) else []))
check("PR4b EN事实query守卫=0", st == 200 and n_k6b == 0, f"k6_cards={n_k6b}")

npass = sum(results)
print(f"\n== knife6_probe[{PHASE}] 终判: {npass}/{len(results)} PASS · BASE={BASE} · tags={TAG} ==")
sys.exit(0 if npass == len(results) else 1)
