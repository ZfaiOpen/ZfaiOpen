#!/usr/bin/env python3
"""patch_dlfix2.py — 死锁三针(meta_get锚修正版)"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/aml_service.py")
src = T.read_text()
subs = [
    ('''        self._conn = sqlite3.connect(str(path), check_same_thread=False)''',
     '''        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA busy_timeout=5000")'''),
    ('''            LEDGER._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")''',
     '''            LEDGER._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")'''),
    ('''    def meta_get(self, k: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else None''',
     '''    def meta_get(self, k: str) -> str | None:
        with self._lock:
            rows = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchall()
        return rows[0][0] if rows else None'''),
]
bak = T.with_suffix(".py.bak-dl2")
shutil.copy2(T, bak)
cur = src
for i, (o, n) in enumerate(subs):
    if cur.count(o) != 1:
        print(f"ANCHOR#{i} FAIL count={cur.count(o)}"); sys.exit(1)
    cur = cur.replace(o, n, 1)
T.write_text(cur)
print("[OK] dlfix2 ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
