#!/usr/bin/env python3
"""knife3_profile.py — 部署线S6提速刀前置·轻量profile(只测不改·零写服务DB/零服务重启)
三计时点(@top_k=100·最大租户规模): ①embed查询(MiniLM编码) ②检索扫描+融合(search总时−embed·残差法)
③top-k排序(纯sort@N卡)。附加: knife3 time_annotate自身时延(scratch DB·用后即删)。
用法: knife3_profile.py   (服务venv: ~/venvs/brainos/bin/python knife3_profile.py)
合规: 进程内只读复用snapshot(embed模型加载·零卡入库零快照写); scratch DB在zo_deploy/data下用后删除。"""
import os
import random
import sys
import time
from pathlib import Path

# ── 与 aml_service.py 相同的环境隔离(在 import memory_service 之前) ──────────
_DEPLOY = Path(__file__).resolve().parent.parent
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ.setdefault("EXAM_USE_EMBED", "1")
os.environ.setdefault("BRAINOS_TKG", "0")
os.environ.setdefault("BRAINOS_LANCEDB", "0")
os.environ.setdefault("BRAINOS_ANN_THRESHOLD", "1000000")
os.environ["BRAINOS_SNAPSHOT"] = str(_DEPLOY / "data" / "snapshot")
os.environ["BRAINOS_EMB_CACHE"] = str(_DEPLOY / "data" / "emb_cache.npz")
SCRATCH_DB = Path(os.environ.get("KNIFE3_PROFILE_DB", str(_DEPLOY / "data" / "knife3_profile_scratch.db")))
os.environ["KNIFE3_DB"] = str(SCRATCH_DB)

sys_path_brainos = os.environ.get("BRAINOS_HOME", str(Path.home() / "brainos_new"))
sys_path_exam = os.environ.get("EXAM_HOME", str(Path.home() / "exam"))
for _p in (sys_path_brainos, sys_path_exam):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import memory_service as ms  # noqa: E402
ms.TOP_K_MAX = 256

SVC = ms.SVC
QUERIES = [
    "How long was it between signing the lease and moving in?",
    "When did the person visit the dentist?",
    "What happened recently in the project?",
    "Tell me about the weekend trip plans.",
]


def bench(fn, n=15):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t0) * 1000)
    ts.sort()
    return ts[len(ts) // 2], ts[-1]  # (median, max)


def main():
    t0 = time.time()
    qmodel = SVC.organ._embed_model()  # 与服务_warmup同源(只载权重·零数据注入)
    print(f"# organ载入+embed模型 {time.time() - t0:.1f}s, cards={SVC._total}")

    # 租户分布 → 取最大租户(profile口径=单租户窗)
    by_tenant: dict[str, int] = {}
    for c in SVC.organ._cards.values():
        t = str(c.get("conv", "")).split(":", 1)[0]
        by_tenant[t] = by_tenant.get(t, 0) + 1
    tenant, n_cards = max(by_tenant.items(), key=lambda kv: kv[1])
    print(f"# 租户数={len(by_tenant)} 最大租户={tenant} cards={n_cards}")

    # ①embed查询
    e_med, e_max = bench(lambda: qmodel.encode([QUERIES[0]], normalize_embeddings=True))
    print(f"[1] embed查询      median={e_med:8.1f}ms  max={e_max:8.1f}ms")

    # ②检索扫描+融合(全search−embed·残差法) @top_k=100
    s_meds = []
    for q in QUERIES:
        m, mx = bench(lambda q=q: SVC.search(q, None, 100, tenant))
        s_meds.append((m, mx))
    s_med = sum(m for m, _ in s_meds) / len(s_meds)
    s_max = max(mx for _, mx in s_meds)
    scan_med = s_med - e_med
    print(f"[2] 检索扫描+融合  median={scan_med:8.1f}ms  (search全时median={s_med:.1f}ms max={s_max:.1f}ms − embed)")

    # ③top-k排序(纯sort@N卡·排序环节物理量级)
    rnd = [random.random() for _ in range(n_cards)]
    so_med, so_max = bench(lambda: sorted(range(n_cards), key=lambda i: rnd[i], reverse=True), n=50)
    print(f"[3] top-k排序      median={so_med:8.2f}ms  max={so_max:8.2f}ms  (@{n_cards}卡全排序)")

    # 附加: knife3 time_annotate自身时延(scratch DB·回填后测·用后即删)
    import knife3_time as k3
    n_c, n_a, b_ms = k3.backfill(list(SVC.organ._cards.values()))
    win = SVC.search(QUERIES[0], None, 100, tenant)
    data = [{"id": k3.card_id_of(e.get("session", ""), e.get("dt", ""), e.get("text", "")),
             "content": e.get("text", ""), "created_at": e.get("dt", "")}
            for e in win.get("evidence", [])]
    k3_med, k3_max = bench(lambda: k3.time_annotate(tenant, QUERIES[0], [dict(d) for d in data], "current"))
    print(f"[+] knife3注记     median={k3_med:8.1f}ms  max={k3_max:8.1f}ms  "
          f"(窗口{len(data)}卡·新增锚{n_a}·回填{b_ms:.0f}ms)")
    SCRATCH_DB.unlink(missing_ok=True)
    for suf in ("-wal", "-shm"):
        Path(str(SCRATCH_DB) + suf).unlink(missing_ok=True)
    print("# scratch DB已清理")
    print(f"# 结论: search@top_k=100瓶颈=embed({e_med:.0f}ms)+扫描融合({scan_med:.0f}ms); "
          f"排序({so_med:.2f}ms)与knife3注记({k3_med:.1f}ms)为毫秒级尾数")


if __name__ == "__main__":
    main()
