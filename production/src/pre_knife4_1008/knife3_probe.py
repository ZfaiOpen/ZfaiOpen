#!/usr/bin/env python3
"""knife3_probe.py — C1时间锚定实弹探针(部署线·独立uid·零内容依赖官方评分)
用法: knife3_probe.py <BASE> <KEY> <UID>
探针集:
  PK1 when归一注记: add含"three days ago/last Friday"会话 → search"When..." →
     返回卡metadata.time.normalized_dates带归一化绝对日期(锚定消息timestamp)+原文措辞, content原文零改写。
  PK2 间隔双卡+日期差: 同会话两张绝对日期卡("March 3, 2025"/"May 15, 2025")排在top10外构造 →
     search"How long between..." → 双卡必回(前2位)+interval.days=73注记(粒度对表: 问时长给天数)。
  PK3 中文相对时间: add"两天前" → search"什么时候" → 归一注记日期正确。
  PK4 官方C1同构: "How many months passed between..." → months+days双粒度注记。
输出: 逐探针PASS/FAIL+终判行; 退出码0=全过。修前(未换装)预期FAIL(metadata缺失=血洞复现)。
"""
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = sys.argv[1].rstrip("/")
KEY = sys.argv[2]
UID = sys.argv[3]
H = {"Content-Type": "application/json", "Authorization": f"Token {KEY}"}
_n = [0]

ADDS = [
    # (session, text, ts) — s1: 相对时间表达锚定2026-10-07(周三): dentist=10-04, yoga=10-02
    ("s1", "Maya: I visited the dentist three days ago.", "2026-10-07T09:00:00Z"),
    ("s1", "Maya: I started yoga last Friday.", "2026-10-07T09:05:00Z"),
    # s2: 间隔双卡(官方C1同构·两日期卡同会话), 会话时间2025-06-27, 日期卡含绝对日期
    ("s2", "Ren: I signed the apartment lease on March 3, 2025.", "2025-06-27T18:05:00Z"),
    ("s2", "Ren: I moved into the apartment on May 15, 2025.", "2025-06-27T18:09:00Z"),
    # s3: 中文相对时间(两天前锚定2026-10-07 → 2026-10-05)
    ("s3", "小明：我两天前去了上海出差。", "2026-10-07T20:00:00Z"),
]
TEXT = {t: t.split(": ", 1)[-1] for _, t, _ in ADDS}  # add时带说话人前缀(content原样含前缀)


def call(path, body):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=H)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:300]
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def ts(iso):
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc).timestamp() * 1000)


results = []


def verdict(name, ok, detail=""):
    results.append(ok)
    print(f"VERDICT {name}: {'PASS' if ok else 'FAIL'} {detail}")


def times_of(data):
    """[(metadata.timedict, content)] — 全部返回卡的time注记收集。"""
    return [(d.get("metadata", {}).get("time", {}), d.get("content", "")) for d in data]


print(f"== knife3_probe uid={UID} ==")
for i, (sid, text, t) in enumerate(ADDS):
    _n[0] += 1
    body = {"request_id": f"{UID}-r{_n[0]}", "user_id": UID, "session_id": f"{UID}-{sid}",
            "messages": [{"role": "user", "content": text, "timestamp": ts(t)}]}
    st, b = call("/add", body)
    print(f"ADD[{sid}] {text[:40]!r} -> {st}")
    results.append(200 <= st < 300)

ok_all = all(results)

# PK1 when归一注记
st, b = call("/search", {"user_id": UID, "query": "When did Maya visit the dentist?", "top_k": 20})
tms = times_of(b.get("data", []) if isinstance(b, dict) else [])
hit = [(tm, c) for tm, c in tms if any(nd.get("date") == "2026-10-04"
       and "three days ago" in nd.get("said_as", "") for nd in tm.get("normalized_dates", []))]
verdict("PK1_when_归一注记", bool(hit),
        f"注记={hit[0][0]['normalized_dates'] if hit else '无'} hits={len(tms)}")

# PK1b content原文零改写(原文逐字比对+无冻结核锚后缀)
orig_set = {t for _, t, _ in ADDS}
contents = {c for _, c in tms}
no_anchor_suffix = all("[= " not in c for _, c in tms)
exact = contents <= orig_set
verdict("PK1b_content零改写", no_anchor_suffix and exact,
        f"锚后缀={'无' if no_anchor_suffix else '有'} 逐字比对={'过' if exact else '差'}")

# PK2 间隔双卡+天数差
st, b = call("/search", {"user_id": UID,
                         "query": "How long was it between Ren signing the lease and moving in?",
                         "top_k": 20})
d2 = b.get("data", []) if isinstance(b, dict) else []
front2 = d2[:2]
both_front = (any("March 3, 2025" in d.get("content", "") for d in front2)
              and any("May 15, 2025" in d.get("content", "") for d in front2))
ivs = [d.get("metadata", {}).get("time", {}).get("interval") for d in front2]
iv_ok = any(iv and iv.get("days") == 73 and iv.get("from") == "2025-03-03"
            and iv.get("to") == "2025-05-15" and "73 days" in iv.get("note", "") for iv in ivs)
verdict("PK2_间隔双卡必回", both_front, f"top2={[d.get('content', '')[:36] for d in front2]}")
verdict("PK2b_日期差注记73天", iv_ok, f"iv={ivs[0] or ivs[1] if ivs else ivs}")

# PK3 中文相对时间归一
st, b = call("/search", {"user_id": UID, "query": "小明什么时候去的上海？", "top_k": 20})
tms = times_of(b.get("data", []) if isinstance(b, dict) else [])
hit3 = [(tm, c) for tm, c in tms if any(nd.get("date") == "2026-10-05"
        and "两天前" in nd.get("said_as", "") for nd in tm.get("normalized_dates", []))]
verdict("PK3_中文两天前归一", bool(hit3),
        f"注记={hit3[0][0]['normalized_dates'] if hit3 else '无'}")

# PK4 官方C1同构 months粒度
st, b = call("/search", {"user_id": UID,
                         "query": "How many months passed between Ren signing the lease and moving in?",
                         "top_k": 20})
d4 = b.get("data", []) if isinstance(b, dict) else []
iv4 = [d.get("metadata", {}).get("time", {}).get("interval", {}) for d in d4[:2]]
months_ok = any(iv.get("months") == 2 and "months" in iv.get("note", "") and "73 days" in iv.get("note", "")
                for iv in iv4)
verdict("PK4_问月给月数附天数", months_ok, f"iv={iv4[0] if iv4 else iv4}")

ok_all = ok_all and all(results[len(ADDS):])
print(f"\nPROBE-VERDICT uid={UID}: {'ALL PASS' if all(results) else 'HAS FAIL'}")
sys.exit(0 if all(results) else 1)
