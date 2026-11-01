#!/usr/bin/env python3
"""knife4_probe.py — knife4_streaming延迟刀验收探针(HTTP实弹·196部署服务)
官方形态重放(取证=~/exam/pipeline/SPEC_I1_streaming.md §1.2/§1.3):
  seed    15路洪峰fixture装弹: 15租户×40卡单笔add(固定rid幂等·重跑零增量)
  flood   15路search同毫秒实弹(官方21:03:22.93拍重放·独立uid) → 逐路dt+队尾max
  single  top_k=100单查×3(官方u_c38e14a1·791卡窗·对表knife3 profile 2308ms基线)
用法: knife4_probe.py <seed|flood|single> [base]
输出: 逐路dt行 + 终判行(SINGLE_MAX_MS=.. / FLOOD_TAIL_MS=..); 退出码0
合规: 探针uid全带k4probe1008前缀(与官方u_零混淆); query自造语料非官方题。
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

MODE = sys.argv[1] if len(sys.argv) > 1 else "flood"
BASE = (sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8090").rstrip("/")
KEY = (Path.home() / "zo_deploy/app/.api_key").read_text().strip()
SINGLE_UID = "u_c38e14a1e8cf9a5675379c210010b889694cf2ac86131fe88b91a135187cb1bd"
SINGLE_QUERY = "This PR introduces a change to the issue described in the thinking trace"
N_FLOOD = 15
N_CARDS = 40
NS = "k4probe1008"  # 固定命名空间: rid幂等 → 重装弹零增量


def call(path: str, body: dict, timeout: float = 180.0):
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json", "Authorization": f"Token {KEY}"}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method="POST")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode()), (time.time() - t0) * 1000
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")[:200], (time.time() - t0) * 1000


def flood_uid(i: int) -> str:
    return f"{NS}_flood_u{i:02d}"


def seed_msgs(i: int) -> list[dict]:
    base_ms = int(time.time() * 1000)
    msgs = []
    for j in range(N_CARDS):
        msgs.append({
            "role": "user" if j % 2 == 0 else "assistant",
            "timestamp": base_ms - (N_CARDS - j) * 60000,
            "content": (f"[tid{i:02d}] meeting log {j:02d}: project codename zebra-{i:02d}-{j:02d}, "
                        f"owner Alice{i} reviewed budget item {j}, next milestone set to week {j % 4 + 1}, "
                        f"risk register updated with finding R{j}{i} and mitigation owner Bob{i}."),
        })
    return msgs


def flood_query(i: int) -> str:
    return f"project codename zebra milestone meeting log risk finding tenant {i:02d}"


def run_seed() -> None:
    t_all = time.time()
    for i in range(N_FLOOD):
        st, resp, dt = call("/add", {
            "request_id": f"{NS}_seed_r{i:02d}",
            "user_id": flood_uid(i),
            "session_id": f"{NS}_s{i:02d}",
            "messages": seed_msgs(i),
        })
        flag = "ok" if st == 200 else "FAIL"
        print(f"seed uid={flood_uid(i)} status={st} dt={dt:.0f}ms {flag} {resp if st != 200 else ''}")
    print(f"SEED done in {time.time() - t_all:.1f}s (idempotent re-run = zero increment)")


def run_flood() -> None:
    bar = threading.Barrier(N_FLOOD)
    out: list[tuple[float, int, int]] = []

    def one(i: int) -> None:
        bar.wait()  # 15路同毫秒放行(官方21:03:22.93拍)
        st, resp, dt = call("/search", {"query": flood_query(i), "user_id": flood_uid(i), "top_k": 100})
        hits = len(resp.get("data", [])) if isinstance(resp, dict) else -1
        out.append((dt, st, hits))

    ths = [threading.Thread(target=one, args=(i,)) for i in range(N_FLOOD)]
    t0 = time.time()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    wall = (time.time() - t0) * 1000
    dts = sorted(d for d, _, _ in out)
    for d, st, hits in sorted(out):
        print(f"flood dt={d:8.0f}ms status={st} hits={hits}")
    print(f"FLOOD_TAIL_MS={dts[-1]:.0f} FLOOD_P50_MS={dts[len(dts)//2]:.0f} WALL_MS={wall:.0f}")


def run_single() -> None:
    dts = []
    for r in range(3):
        st, resp, dt = call("/search", {"query": SINGLE_QUERY, "user_id": SINGLE_UID, "top_k": 100})
        hits = len(resp.get("data", [])) if isinstance(resp, dict) else -1
        dts.append(dt)
        print(f"single#{r} dt={dt:.0f}ms status={st} hits={hits}")
    print(f"SINGLE_MAX_MS={max(dts):.0f} SINGLE_MIN_MS={min(dts):.0f}")


if __name__ == "__main__":
    {"seed": run_seed, "flood": run_flood, "single": run_single}[MODE]()
