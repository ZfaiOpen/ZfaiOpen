#!/usr/bin/env python3
# patch_f2_rerank.py — F2席接线: evidence_rerank挂入search handler(全闸门后·QLOG前)
# .bak-f2先行; 锚点精确替换; 任一锚点缺失=拒改(哑). 幂等: 已打过=拒重打。
import shutil
import sys

P = "/home/zfai/zo_deploy/app/aml_service.py"
src = open(P, encoding="utf-8").read()

if "erank.rerank" in src:
    print("ALREADY-PATCHED")
    sys.exit(0)

# 锚1: import区(k8 import行后追加一行)
A1 = "import knife8_rank as k8  # noqa: E402\n"
N1 = A1 + "import evidence_rerank as erank  # noqa: E402  f2_1012代码证据重排\n"

# 锚2: 全闸门后(data[:top_k])·契约守卫注释前 — QLOG/trace将呈现重排后窗口(法证所见即所得)
A2 = ('    _mark("k8rk")\n'
      '    data = data[:top_k]\n')
N2 = (A2
      + '    # f2_1012·代码证据重排: 代码语境query→[tool_result]/diff卡浮前·[thinking]叙述卡让位\n'
      + '    # (冒烟0分结构根因修复·只动序不动内容·非代码query原序逐位=文本赛道零回归·fail-soft)。\n'
      + '    try:\n'
      + '        data = erank.rerank(data, query)\n'
      + '    except Exception as exc:\n'
      + '        log.error("evidence rerank FAILED uid=%s err=%s", uid, type(exc).__name__)\n'
      + '    _mark("erank")\n')

if src.count(A1) != 1 or src.count(A2) != 1:
    print("ANCHOR-MISS", src.count(A1), src.count(A2))
    sys.exit(1)

shutil.copy2(P, P + ".bak-f2")
out = src.replace(A1, N1).replace(A2, N2)
assert out.count("erank.rerank") == 1 and out.count("import evidence_rerank as erank") == 1
open(P, "w", encoding="utf-8").write(out)
print("PATCHED import+1 body+7 =", out.count("erank"))
