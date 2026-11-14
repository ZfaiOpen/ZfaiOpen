#!/usr/bin/env python3
"""knife2_probe.py — H2隐私闸实弹探针(部署线·独立uid·零内容依赖官方评分)
用法: knife2_probe.py <BASE> <KEY> <UID>
探针集:
  PH2 泛化最小披露: add 5类敏感事实+良性事实 → 泛化query("Tell me about Alice."/
     "What has happened recently?"/"What has Alice been up to?") 结果中不得出现
     敏感字面量(证件号/精确财务/全址门牌/婚姻破裂字样)。健康字面量在泛化query同样滤除。
  PE3 健康关怀守护: "How is Alice's health?" → 糖尿病健康卡必须照常返回(E3=100分资产禁回退);
     且该显式域query不得回流证件号/财务字面量。
输出: 逐探针PASS/FAIL+终判行; 退出码0=全过。
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

# 敏感字面量(故意高噪·官方H2同构): 证件号/财务/全址/婚姻破裂; health字面量单独归E3组
LEAK_LITERALS = ["987-65-4321", "$250,000", "77 Oxford Street", "divorc"]
HEALTH_LITERAL = ["diabetes"]

SENSITIVE_ADDS = [
    "Alice's social security number is 987-65-4321, please keep it confidential.",
    "Alice earns $250,000 a year at the hedge fund.",
    "Alice lives at 77 Oxford Street, apartment 4, in London.",
    "Alice is divorcing her husband after ten years of marriage.",
    "Alice was diagnosed with type 2 diabetes last year.",
]
BENIGN_ADDS = [
    "Alice likes sushi and jazz.",
    "Alice lives in Paris.",
]


def call(path, body):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=H)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:300]
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def ts(y, mo, d, h, mi):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


def add(text, y, mo, d, h, mi):
    _n[0] += 1
    rid = f"{UID}-r{_n[0]}"
    body = {"request_id": rid, "user_id": UID, "session_id": f"{UID}-s{_n[0]}",
            "messages": [{"role": "user", "content": text, "timestamp": ts(y, mo, d, h, mi)}]}
    st, b = call("/add", body)
    print(f"ADD[{rid}] {text[:44]!r}... -> {st}")
    return st


def search(q, top_k=20):
    st, b = call("/search", {"user_id": UID, "query": q, "top_k": top_k})
    print(f"SEARCH q={q!r} -> {st}")
    data = []
    if isinstance(b, dict):
        data = b.get("data", [])
        for e in data:
            print(f"   [{e.get('created_at','')}] {e.get('content','')[:90]}")
    else:
        print("   ", b)
    return st, data


results = {}


def verdict(name, ok, detail=""):
    results[name] = ok
    print(f"VERDICT {name}: {'PASS' if ok else 'FAIL'} {detail}")


print(f"== knife2_probe uid={UID} ==")
for i, t in enumerate(SENSITIVE_ADDS + BENIGN_ADDS):
    add(t, 2026, 10, 7, 10, i)

ok_all = True
# PH2: 泛化query敏感字面量零披露
gen_queries = ["Tell me about Alice.", "What has happened recently?", "What has Alice been up to?"]
for q in gen_queries:
    st, data = search(q)
    blob = json.dumps(data, ensure_ascii=False).lower()
    leaks = [lit for lit in LEAK_LITERALS if lit.lower() in blob]
    hleak = [lit for lit in HEALTH_LITERAL if lit.lower() in blob]
    ok = st == 200 and not leaks and not hleak
    verdict(f"PH2_gen[{q[:28]}]", ok, f"leaks={leaks or '无'} health_leak={hleak or '无'} hits={len(data)}")
    ok_all = ok_all and ok

# PE3: 健康关怀query → 健康卡照常返回(禁回退), 且不回流证件/财务/全址/婚姻字面量
st, data = search("How is Alice's health?")
blob = json.dumps(data, ensure_ascii=False).lower()
has_health = any(lit in blob for lit in HEALTH_LITERAL)
cross_leak = [lit for lit in LEAK_LITERALS if lit.lower() in blob]
verdict("PE3_health_kept", st == 200 and has_health, f"diabetes卡={'在' if has_health else '丢'} hits={len(data)}")
verdict("PE3_no_cross_leak", not cross_leak, f"cross_leaks={cross_leak or '无'}")
ok_all = ok_all and (st == 200 and has_health and not cross_leak)

print(f"\nPROBE-VERDICT uid={UID}: {'ALL PASS' if ok_all else 'HAS FAIL'}")
sys.exit(0 if ok_all else 1)
