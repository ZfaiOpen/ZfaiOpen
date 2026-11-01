#!/usr/bin/env python3
"""k4_profile_once.py — knife4后单查归因profile(离线·载snapshot+模型·cProfile top30)"""
import os, sys, cProfile, pstats, io, time
from pathlib import Path
os.environ["HF_HUB_OFFLINE"] = "1"; os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ.setdefault("EXAM_USE_EMBED", "1"); os.environ.setdefault("BRAINOS_TKG", "0")
os.environ.setdefault("BRAINOS_LANCEDB", "0"); os.environ.setdefault("BRAINOS_ANN_THRESHOLD", "1000000")
DEP = Path.home() / "zo_deploy"
os.environ["BRAINOS_SNAPSHOT"] = str(DEP / "data" / "snapshot")
os.environ["BRAINOS_EMB_CACHE"] = str(DEP / "data" / "emb_cache.npz")
sys.path.insert(0, str(Path.home() / "brainos_frozen"))
sys.path.insert(0, str(Path.home() / "exam"))
sys.path.insert(0, str(DEP / "app"))
import memory_service as ms
ms.TOP_K_MAX = 256
import knife4_streaming as k4
SVC = ms.SVC
SVC.organ._qmodel = SVC.organ._embed_model()
k4.install(SVC)
import logging; logging.disable(logging.CRITICAL)
Q = "project codename zebra milestone meeting log risk finding tenant 07"
SVC.search(Q, None, 100, "k4probe1008_flood_u07")  # 预热
t0 = time.time()
pr = cProfile.Profile(); pr.enable()
out = SVC.search(Q, None, 100, "k4probe1008_flood_u07")
pr.disable()
print(f"SEARCH dt={(time.time()-t0)*1000:.0f}ms hits={len(out[chr(101)+chr(118)+chr(105)+chr(100)+chr(101)+chr(110)+chr(99)+chr(101)])}")
s = io.StringIO(); pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(22)
print(s.getvalue()[:4200])
