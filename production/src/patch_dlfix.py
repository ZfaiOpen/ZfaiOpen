#!/usr/bin/env python3
"""patch_dlfix.py — 死锁根治三针(20261010点火4号死锁定谳)
①checkpoint TRUNCATE→PASSIVE(TRUNCATE等待全部读事务=无限阻塞源)
②ledger连接加busy_timeout=5000(等待变抛错不挂死)
③meta_get/rows_after游标耗尽(fetchone留未关闭读事务=checkpoint的等待源)"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/aml_service.py")
src = T.read_text()
subs = [
    # ①+②: 连接busy_timeout
    ('''        self._conn = sqlite3.connect(str(path), check_same_thread=False)''',
     '''        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA busy_timeout=5000")'''),
    # ①: TRUNCATE→PASSIVE
    ('''            LEDGER._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")''',
     '''            LEDGER._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")'''),
    # ③: meta_get游标耗尽
    ('''    def meta_get(self, k: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else None''',
     '''    def meta_get(self, k: str) -> str | None:
        with self._lock:
            rows = self._conn.execute(
                "SELECT v FROM meta WHERE k=?", (k,)).fetchall()  # fetchall耗尽游标(留活游标=读事务不闭→checkpoint死等)
        return rows[0][0] if rows else None'''),
]
bak = T.with_suffix(".py.bak-dl")
shutil.copy2(T, bak)
cur = src
for i, (o, n) in enumerate(subs):
    if cur.count(o) != 1:
        print(f"ANCHOR#{i} FAIL count={cur.count(o)}"); sys.exit(1)
    cur = cur.replace(o, n, 1)
T.write_text(cur)
print("[OK] dlfix ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
