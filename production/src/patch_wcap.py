#!/usr/bin/env python3
"""patch_wcap.py — 评审减负窗口(v2干净版)
证据: 同卷37题两场对比, win100大窗下评审±25-50随机翻转=剩余全部分数差距根源。
窗>cap截断·结论卡晋级(截断区结论卡永不丢)·评审读40张精选而非100张噪声。
env BRAINOS_WIN_CAP缺省40·=0关门。"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/aml_service.py")
src = T.read_text()
anchor = 'log.info("STAGE S11_boundary n=%d", len(data))'
new = '''log.info("STAGE S11_boundary n=%d", len(data))
    # wcap评审减负(20261010): win100大窗=评审±25-50随机翻转根源。窗>cap截断·
    # 结论卡晋级(截断区结论卡永不丢)·评审读cap张精选。env BRAINOS_WIN_CAP=0关门。
    try:
        _cap = int(os.environ.get("BRAINOS_WIN_CAP", "40"))
        if _cap > 0 and len(data) > _cap:
            _CONCL = ("AS-OF ANSWER", "RESOLVED", "STEP ", "ANSWER: ", "AS-OF VERDICT",
                      "TENURE FACTS", "SEQUENCE ", "DURATION ", "COMPLIANCE", "TASK BRIEF",
                      "MEMORY STATUS", "RULE:", "KNOWLEDGE DIGEST", "SESSION DIGEST")
            _before = len(data)
            _head, _tail = data[:_cap], data[_cap:]
            _rescued = [d for d in _tail if str(d.get("content", "")).startswith(_CONCL)]
            if len(_head) + len(_rescued) > _cap + 6:
                _rescued = _rescued[: max(0, _cap + 6 - len(_head))]
            data = _head + _rescued
            log.info("wcap trim %d->%d rescued=%d", _before, len(data), len(_rescued))
    except Exception:
        pass'''
if src.count(anchor) != 1:
    print("ANCHOR FAIL", src.count(anchor)); sys.exit(1)
bak = T.with_suffix(".py.bak-wcap")
shutil.copy2(T, bak)
T.write_text(src.replace(anchor, new, 1))
print("[OK] wcap ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
