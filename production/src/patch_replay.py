#!/usr/bin/env python3
"""patch_replay.py — 启动重放O(N²)嵌入根治
病: _startup_replay对每租户finalize=True→每租户一次全量pending嵌入=重复编码O(N²)。
重放数千卡(压测/官方场积压)时启动编码数十分钟=uvicorn未开机全请求门口排队=假死。
修: 重放全程finalize=False(只ingest不嵌)→末尾单次finalize_ingest(一批线性嵌完)
→立即snapshot推进snap_ms(下次重启近零重放)。"""
import hashlib
import shutil
import sys
from pathlib import Path

T = Path("/home/zfai/zo_deploy/app/aml_service.py")
src = T.read_text()
old = '''    n = 0
    for user_id, cards in by_user.items():
        if cards:
            SVC.add([ms._AddItem(**c) for c in cards], finalize=True, tenant=tenant_of(user_id))
            n += len(cards)
    if n:'''
new = '''    n = 0
    for user_id, cards in by_user.items():
        if cards:
            SVC.add([ms._AddItem(**c) for c in cards], finalize=False, tenant=tenant_of(user_id))
            n += len(cards)
    if n:
        try:
            SVC.organ.finalize_ingest()  # replay-fix: 全量重放后单批嵌入(原逐租户finalize=O(N²)重复编码)
            SVC._dirty = False
        except Exception as exc:
            log.error("startup replay finalize failed: %s", exc)
        try:
            SVC.snapshot(str(SNAP_DIR))
            LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))  # 推进水位→下次重启近零重放
        except Exception as exc:
            log.error("startup replay snapshot failed: %s", exc)'''
if src.count(old) != 1:
    print("ANCHOR FAIL", src.count(old))
    sys.exit(1)
bak = T.with_suffix(".py.bak-rp")
shutil.copy2(T, bak)
T.write_text(src.replace(old, new, 1))
print("[OK] replay-fix ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
