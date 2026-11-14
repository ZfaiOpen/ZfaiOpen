#!/usr/bin/env python3
"""patch_aml_k4b.py — M2让位协议补丁: recency_reorder加传k3_tier(PK4判例回归)"""
import hashlib, sys
from pathlib import Path
P = Path(__file__).resolve().parent / "aml_service.py"
EXPECT = "edef46b611224f3c171712949f19b855"
raw = P.read_text()
got = hashlib.md5(raw.encode()).hexdigest()
if got != EXPECT:
    print(f"CAS FAIL: {got} != {EXPECT}")
    sys.exit(1)
a = "        data = k4.recency_reorder(tenant_of(uid), query, data, k1_intent, out)\n"
b = "        data = k4.recency_reorder(tenant_of(uid), query, data, k1_intent, out, k3_tier)\n"
assert raw.count(a) == 1, "anchor not unique"
raw = raw.replace(a, b)
P.write_text(raw)
print("patched k3_tier passthrough ok; md5=" + hashlib.md5(raw.encode()).hexdigest())
