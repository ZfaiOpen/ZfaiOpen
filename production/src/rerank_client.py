#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""rerank_client.py — BRAIOS M4 重排器客户端 (轨道D, 2026-10-25)

纯标准库 (urllib) · 零新依赖 · 设计为直接拷入引擎 src/ 树使用。
fail-soft 铁律: 超时/连接失败/5xx/响应畸形 → 一律返回原序 (score=0),
绝不抛异常打断调用方; 每次降级计数+日志, stats() 可观测。

用法 (引擎侧):
    import rerank_client as rr
    pairs = [{"id": c["id"], "text": c["content"]} for c in data]
    order = rr.rerank(query, pairs)            # → [{"id","score"}] 降序
    # 或直接拿排好序的卡:
    by_id = {c["id"]: c for c in data}
    data = [by_id[d["id"]] for d in order if d["id"] in by_id]

env: BRAINOS_RERANK_URL (默认 http://127.0.0.1:8098)
     BRAINOS_RERANK_TIMEOUT_MS (调用级覆盖亦可选参 timeout_ms)
     BRAINOS_RERANK_TOPN (只送前 N 候选, 默认 30; 0=不裁)
     BRAINOS_RERANK_MINCHAR (候选文本送入前截断字符数, 默认 6000)
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any

_LOG = logging.getLogger("rerank_client")
URL = os.environ.get("BRAINOS_RERANK_URL", "http://127.0.0.1:8098")
DEFAULT_TIMEOUT_MS = int(os.environ.get("BRAINOS_RERANK_TIMEOUT_MS", "8000"))
TOPN = int(os.environ.get("BRAINOS_RERANK_TOPN", "30"))
MINCHAR = int(os.environ.get("BRAINOS_RERANK_MINCHAR", "6000"))

_LOCK = threading.Lock()
_STATS = {"calls": 0, "ok": 0, "fallback": 0, "timeouts": 0,
          "conn_err": 0, "bad_resp": 0, "total_ms": 0.0, "max_ms": 0.0,
          "last_err": ""}


def stats() -> dict[str, Any]:
    with _LOCK:
        s = dict(_STATS)
    s["avg_ms"] = round(s["total_ms"] / s["calls"], 1) if s["calls"] else 0.0
    return s


def reset_stats() -> None:
    with _LOCK:
        for k in ("calls", "ok", "fallback", "timeouts", "conn_err",
                  "bad_resp", "total_ms", "max_ms"):
            _STATS[k] = 0 if k.endswith("_ms") else 0
        _STATS["total_ms"] = 0.0
        _STATS["max_ms"] = 0.0
        _STATS["last_err"] = ""


def _bump(key: str, err: str = "") -> None:
    with _LOCK:
        _STATS[key] += 1
        if err:
            _STATS["last_err"] = err[:200]


def rerank(query: str, pairs: list[dict[str, Any]],
           timeout_ms: int | None = None) -> list[dict[str, Any]]:
    """对 (query, [{id,text}...]) 重排。

    返回: [{"id": str, "score": float}, ...] 分数降序;
    任何失败 → 原序 [{id, 0.0}], 不抛异常。
    """
    timeout_ms = timeout_ms or DEFAULT_TIMEOUT_MS
    if not pairs:
        return []
    if not isinstance(query, str) or not query.strip():
        return [{"id": str(p.get("id", "")), "score": 0.0} for p in pairs]
    send = pairs[:TOPN] if TOPN > 0 else pairs
    tail = pairs[len(send):]
    payload = json.dumps({
        "query": query[:MINCHAR],
        "candidates": [{"id": str(p.get("id", "")),
                        "text": str(p.get("text", ""))[:MINCHAR]}
                       for p in send]}).encode()
    t0 = time.monotonic()
    with _LOCK:
        _STATS["calls"] += 1
    try:
        req = urllib.request.Request(
            URL.rstrip("/") + "/rerank", data=payload,
            headers={"Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=timeout_ms / 1000.0) as r:
            body = json.loads(r.read().decode("utf-8", "replace"))
        rows = body.get("data")
        if not isinstance(rows, list) or len(rows) != len(send):
            _bump("bad_resp", f"rows={len(rows) if isinstance(rows, list) else 'x'} expect={len(send)}")
            raise ValueError("bad /rerank response shape")
        seen = {str(x.get("id")) for x in rows}
        missed = [p for p in send if str(p.get("id")) not in seen]
        if missed:
            rows += [{"id": str(p.get("id")), "score": -1e9}
                     for p in missed]
        if tail:
            rows += [{"id": str(p.get("id")), "score": -1e9} for p in tail]
        ms = (time.monotonic() - t0) * 1000
        with _LOCK:
            _STATS["ok"] += 1
            _STATS["total_ms"] += ms
            _STATS["max_ms"] = max(_STATS["max_ms"], ms)
        return rows
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        if "timed out" in str(reason) or "timeout" in str(reason).lower():
            _bump("timeouts", f"URLError:{reason}")
        else:
            _bump("conn_err", f"URLError:{reason}")
    except TimeoutError:
        _bump("timeouts", "socket-timeout")
    except Exception as e:                     # 解析错/5xx/乱响应
        _bump("bad_resp" if "bad /rerank" in str(e) else "conn_err",
              f"{type(e).__name__}:{e}")
    ms = (time.monotonic() - t0) * 1000
    with _LOCK:
        _STATS["fallback"] += 1
        _STATS["total_ms"] += ms
        _STATS["max_ms"] = max(_STATS["max_ms"], ms)
    _LOG.warning("rerank fallback→原序 pairs=%d err=%s ms=%.0f",
                 len(pairs), _STATS["last_err"], ms)
    out = [{"id": str(p.get("id", "")), "score": 0.0} for p in pairs]
    return out


def health(timeout_ms: int = 2000) -> dict[str, Any] | None:
    """GET /health; 失败返回 None (调用方可决定是否禁用重排)。"""
    try:
        with urllib.request.urlopen(
                URL.rstrip("/") + "/health",
                timeout=timeout_ms / 1000.0) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return None


if __name__ == "__main__":                     # 冒烟: python rerank_client.py
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "fix pandas KeyError when merging dataframes"
    docs = [
        {"id": "d1", "text": "FIX: pandas merge raised KeyError on 'user_id' "
             "because left frame column was renamed; align column names before merge"},
        {"id": "d2", "text": "numpy array shape mismatch ValueError: operands "
             "could not be broadcast together"},
        {"id": "d3", "text": "pandas groupby agg example notebook, sales data "
             "analysis with pivot tables"},
    ]
    print("health:", health())
    for row in rerank(q, docs):
        print(f"  {row['score']:.4f}  {row['id']}")
    print("stats:", stats())
