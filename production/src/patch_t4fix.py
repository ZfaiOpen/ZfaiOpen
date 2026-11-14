#!/usr/bin/env python3
"""patch_t4fix.py — 条目头正则修(行首^→行内·官方docpp卡条目同行连排)"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/deep_policy.py")
src = T.read_text()
old = '_ENTRY_HEAD_RE = re.compile(r"^[ \\t]*(?:[-*•>]*)?[ \\t]*(?:条目|Item|Entry)[ \\t]*(\\d{1,2})[ \\t]*[:：（(]", re.M | re.I)'
new = '_ENTRY_HEAD_RE = re.compile(r"(?:^|[\\n\\r])[ \\t]*(?:[-*•>]*)?[ \\t]*(?:条目|Item|Entry)[ \\t]*(\\d{1,2})[ \\t]*[:：（(]|[ \\t](?:条目|Item|Entry)[ \\t]*(\\d{1,2})[ \\t]*[:：（(]", re.M | re.I)'
if src.count(old) != 1:
    print("ANCHOR FAIL", src.count(old)); sys.exit(1)
bak = T.with_suffix(".py.bak-t4f")
shutil.copy2(T, bak)
T.write_text(src.replace(old, new, 1))
print("[OK] t4fix ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
