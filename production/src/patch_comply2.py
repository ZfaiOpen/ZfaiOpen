#!/usr/bin/env python3
"""patch_comply2.py — W8合规: 去ANSWER:前缀→推导证据行格式(精确锚·实测文本)"""
import hashlib, shutil, sys
from pathlib import Path
FAILS = []

def patch(T, subs, tag):
    src = T.read_text()
    bak = T.with_suffix(T.suffix + ".bak-comply2")
    shutil.copy2(T, bak)
    cur = src
    for i, (o, n) in enumerate(subs):
        c = cur.count(o)
        if c == 0 and n in cur:
            print(f"  {tag}#{i}: SKIP(已施)")
            continue
        if c != 1:
            FAILS.append(f"{tag}#{i} count={c}")
            continue
        cur = cur.replace(o, n, 1)
    if FAILS:
        for f in FAILS: print("  !!", f)
        sys.exit(1)
    T.write_text(cur)
    print(f"[OK] {tag} -> {hashlib.md5(T.read_bytes()).hexdigest()[:8]}")

root = Path("/home/zfai/zo_deploy/app")

# ① deep_closure.py
patch(root / "deep_closure.py", [
    ('''    cards.append(f"ANSWER: {start} via {rels} = {final} (chain: {chain}; {n}-hop{post_any})")''',
     '''    cards.append(f"[推导|源:{start}] {start} → {final} ({chain}; {n}-hop{post_any})")'''),
], "closure-comply")

# ② deep_timeline.py
patch(root / "deep_timeline.py", [
    ('''            text = (f"AS-OF ANSWER: {g['subject']} {attr} = {_pick(segs, qd)} (valid at {qd}; timeline: {tail})" if qd else f"ATTRIBUTE TIMELINE [{attr}] ({g['subject']}): {tail}")''',
     '''            text = (f"[推导|源:{g['subject']}] {attr} as of {qd} = {_pick(segs, qd)} (timeline: {tail})" if qd else f"ATTRIBUTE TIMELINE [{attr}] ({g['subject']}): {tail}")'''),
], "timeline-comply")

# ③ aml_service.py wcap _CONCL
patch(root / "aml_service.py", [
    ('''            _CONCL = ("AS-OF ANSWER", "RESOLVED", "STEP ", "ANSWER: ", "AS-OF VERDICT",
                      "TENURE FACTS", "SEQUENCE ", "DURATION ", "COMPLIANCE", "TASK BRIEF",
                      "MEMORY STATUS", "RULE:", "KNOWLEDGE DIGEST", "SESSION DIGEST")''',
     '''            _CONCL = ("[推导", "RESOLVED", "STEP ", "AS-OF", "AS-OF VERDICT",
                      "TENURE FACTS", "SEQUENCE ", "DURATION ", "COMPLIANCE", "TASK BRIEF",
                      "MEMORY STATUS", "RULE:", "KNOWLEDGE DIGEST", "SESSION DIGEST")'''),
], "aml-wcap")

print("\nALL COMPLIANCE PATCHES OK")
