#!/usr/bin/env python3
"""patch_fh.py — faulthandler注入(死锁取证器·代码级·无遮蔽风险)
SIGUSR1→全线程堆栈落地 ~/zo_deploy/logs/faulthandler.log
用法: kill -USR1 $(cat ~/zo_deploy/run/service.pid)"""
import hashlib
import shutil
import sys
from pathlib import Path

T = Path("/home/zfai/zo_deploy/app/aml_service.py")
src = T.read_text()
if "faulthandler.register" in src:
    print("已注入·跳过")
    sys.exit(0)
anchor = 'log = logging.getLogger("aml")'
new = anchor + """

# 死锁取证器(20261010): SIGUSR1触发全线程堆栈落地(诊断用·零行为影响)
import faulthandler as _fh
import signal as _sig
try:
    _fh.register(_sig.SIGUSR1, file=open("/home/zfai/zo_deploy/logs/faulthandler.log", "a"), all_threads=True)
except Exception:
    pass"""
if src.count(anchor) != 1:
    print("ANCHOR FAIL", src.count(anchor))
    sys.exit(1)
bak = T.with_suffix(".py.bak-fh")
shutil.copy2(T, bak)
T.write_text(src.replace(anchor, new, 1))
print("[OK] faulthandler ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
