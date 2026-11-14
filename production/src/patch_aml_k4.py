#!/usr/bin/env python3
"""patch_aml_k4.py — aml_service.py三处knife4锚点(CAS式: md5核验→精确替换→计数断言)
用法: patch_aml_k4.py   (在app目录内跑; 任何锚点失配=拒改退出1)"""
import hashlib
import sys
from pathlib import Path

P = Path(__file__).resolve().parent / "aml_service.py"
EXPECT = "946953a61bbe611b2d924cd3153607e7"
raw = P.read_text()
got = hashlib.md5(raw.encode()).hexdigest()
if got != EXPECT:
    print(f"CAS FAIL: md5 {got} != {EXPECT} — 拒改")
    sys.exit(1)

n = 0

# 锚1: k3 import后挂k4 import
a1 = "import knife3_time as k3  # noqa: E402\n"
b1 = a1 + """
# knife4_1008(部署线第4刀): I1流式延迟刀 — 官方低分格最大死因=队列谋杀(15路search同毫秒
# 涌入→全局RLock串行→队尾68.7s·超30s的12查全落低分域)。M1a encode-memo(每search逐conv
# 重复encode塌缩为1) + M1b 单趟租户取窗(小租户双趟全库重扫→单趟·语义逐位等价) + RW读写分离
# (overlay层读共享写独占·写优先) + M2 带内recency决胜(ledger created_ms·相邻分差<5%带内
# 只换序不改集·k1时间线意图让位)。纯overlay: 冻结核心/organ/exam件零修改。
# 回滚 = 还原 pre_knife4_1008/aml_service.py + 摘除 knife4_streaming.py/knife4_probe.py/
# knife4_selftest.py。env总闸=BRAINOS_K4(缺省开·=0关门回旧行为)。
import knife4_streaming as k4  # noqa: E402
"""
assert raw.count(a1) == 1, "anchor1 not unique"
raw = raw.replace(a1, b1)
n += 1

# 锚2: _on_startup内 k3.backfill except 块后装 k4
a2 = """    except Exception as exc:
        log.error("knife3 backfill FAILED err=%s", type(exc).__name__)
    threading.Thread(target=_snapshot_loop, name="snap", daemon=True).start()"""
b2 = """    except Exception as exc:
        log.error("knife3 backfill FAILED err=%s", type(exc).__name__)
    # knife4_1008: I1延迟刀安装(M1a memo/M1b onepass+RW/M2 recency·fail-soft) — 须在_warmup后
    try:
        _k4_flags = k4.install(SVC, LEDGER, LEDGER_DB)
        log.info("knife4 install: %s", _k4_flags if _k4_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife4 install FAILED err=%s", type(exc).__name__)
    threading.Thread(target=_snapshot_loop, name="snap", daemon=True).start()"""
assert raw.count(a2) == 1, "anchor2 not unique"
raw = raw.replace(a2, b2)
n += 1

# 锚3: search端点 k3之后 data[:top_k] 之前 挂M2
a3 = "    data = data[:top_k]\n"
b3 = """    # knife4_1008·M2: 带内recency决胜(ledger created_ms新者在前·相邻分差<5%带内只换序
    # 不改集); k1 history时间线意图→让位(C3禁回退)。fail-soft同前三刀。
    try:
        data = k4.recency_reorder(tenant_of(uid), query, data, k1_intent, out)
    except Exception as exc:
        log.error("knife4 recency FAILED uid=%s err=%s", uid, type(exc).__name__)
    data = data[:top_k]
"""
assert raw.count(a3) == 1, "anchor3 not unique"
raw = raw.replace(a3, b3)
n += 1

P.write_text(raw)
print(f"patched {n}/3 anchors ok; new md5={hashlib.md5(raw.encode()).hexdigest()}")
