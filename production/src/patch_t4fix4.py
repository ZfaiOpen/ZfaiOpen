#!/usr/bin/env python3
"""patch_t4fix4.py — 条目体剥离策略复述句(正文尾部复述"不得披露条目N"不得作为本条目机密标注)"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/deep_policy.py")
src = T.read_text()
old = '''    entries = _split_entries(doc_text)
    if len(entries) < 2:
        return None
    nums, body, pols = [n for n, _ in entries], dict(entries), sorted({p for p in policies if p})'''
new = '''    entries = _split_entries(doc_text)
    if len(entries) < 2:
        return None
    pols = sorted({p for p in policies if p} | set(_policy_lines(doc_text)))
    # t4fix4: 条目体剥离策略复述句(正文尾部复述"不得披露条目N"不得作为本条目机密标注)
    import re as _re4
    entries = [(n, _re4.sub("|".join(_re4.escape(p) for p in pols) or r"(?!x)x", "", b).strip(" ,，.。;；"))
               for n, b in entries]
    nums, body = [n for n, _ in entries], dict(entries)'''
if src.count(old) != 1:
    print("ANCHOR FAIL", src.count(old)); sys.exit(1)
bak = T.with_suffix(".py.bak-t4f4")
shutil.copy2(T, bak)
T.write_text(src.replace(old, new, 1))
print("[OK] t4fix4 ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
