#!/usr/bin/env python3
"""patch_pg.py — PG毒卡清除席·精确补丁器(锚点md5守门·失配拒改·.bak-pg可回滚)
动作(仅两步, 重建走独立脚本rebuild_derived.py·不在本补丁内):
  ① 用本目录修复版deep_fact.py整体替换 /home/zfai/zo_deploy/app/deep_fact.py
     (替换前锚点校验: 目标文件当前md5==锚md5才动刀; 已是修复版=幂等exit 0;
      失配=环境与预期不符·exit 1拒改——精确锚点纪律, 禁盲替);
  ② py_compile验证替换后目标文件可编译(编译不过=立即回滚bak-pg·exit 1)。
每次动刀: .bak-pg备份 + md5前后打印(终端可复现·诚实交付)。

用法(在196部署机上跑):
  python3 patch_pg.py [锚md5]      # 锚md5缺省=baseline2快照深考md5;
                                   # 196现场与快照有差异时显式传当前生产md5
exit: 0=已打补丁(或幂等重入) / 1=锚失配或编译失败(目标零改动·或已回滚)
"""
from __future__ import annotations

import hashlib
import os
import py_compile
import shutil
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(SCRIPT_DIR, "deep_fact.py")                # 修复版(purge席出品)
TARGET = "/home/zfai/zo_deploy/app/deep_fact.py"              # 生产目标(196)
# 锚=baseline2快照原件md5(2026-10-09提取; 生产文件若与此异·现场核实后显式传参)
ANCHOR_MD5_DEFAULT = "09b69253a8506ca6292076126e4b5521"
BAK_SUFFIX = ".bak-pg"


def _md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    anchor = sys.argv[1] if len(sys.argv) > 1 else ANCHOR_MD5_DEFAULT
    if not os.path.isfile(SRC):
        print(f"[patch_pg] FAIL: 修复版不存在: {SRC}")
        return 1
    if not os.path.isfile(TARGET):
        print(f"[patch_pg] FAIL: 目标不存在(须在196部署机运行): {TARGET}")
        return 1
    new_md5 = _md5(SRC)
    # 预检: 修复版自身必须可编译(带毒上膛禁入)
    try:
        py_compile.compile(SRC, cfile=os.path.join(SCRIPT_DIR, ".pg_compile_check.pyc"),
                           doraise=True)
    except py_compile.PyCompileError as exc:
        print(f"[patch_pg] FAIL: 修复版py_compile不过(拒改): {exc}")
        return 1
    finally:
        pyc = os.path.join(SCRIPT_DIR, ".pg_compile_check.pyc")
        if os.path.exists(pyc):
            os.remove(pyc)
    old_md5 = _md5(TARGET)
    print(f"[patch_pg] md5 before: {old_md5}  ({TARGET})")
    if old_md5 == new_md5:
        print("[patch_pg] 目标已是修复版(幂等重入·零改动)")
        return 0
    if old_md5 != anchor:
        print(f"[patch_pg] FAIL: 锚失配拒改 — 目标md5={old_md5} != 锚md5={anchor}"
              f" (环境与预期不符·先摸底再动刀·勿盲替)")
        return 1
    # 动刀: 备份→替换→编译验证(不过即回滚)
    bak = TARGET + BAK_SUFFIX
    shutil.copy2(TARGET, bak)
    shutil.copyfile(SRC, TARGET)
    print(f"[patch_pg] md5 after : {_md5(TARGET)}  (bak={bak})")
    try:
        py_compile.compile(TARGET, doraise=True)
    except py_compile.PyCompileError as exc:
        shutil.copy2(bak, TARGET)  # 编译不过=立即回滚·生产不留残骸
        print(f"[patch_pg] FAIL: 替换后py_compile不过·已回滚bak-pg: {exc}")
        return 1
    print("[patch_pg] OK: deep_fact.py毒卡修复版已上生产(重启服务后生效;"
          " 存量毒卡清洗=rebuild_derived.py独立跑·重启前)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
