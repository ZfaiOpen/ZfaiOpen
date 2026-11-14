#!/usr/bin/env python3
# test_evidence_rerank.py — F2席重排器单测(8case·构造验证)
import sys
sys.path.insert(0, "/home/zfai/zo_deploy/app")
from evidence_rerank import rerank, _query_is_code  # noqa: E402

PASS = []


def card(cid, content):
    return {"id": cid, "content": content, "created_at": "2026-10-12T00:00:00"}


def check(name, cond):
    PASS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


CODE_Q = ("TimeSeries invalid expected 'time' as the first columns "
          "required columns misleading exception")
TEXT_Q = "What is Sarah Garcia's middle name?"

# c1 非代码query原序直通(逐位)
w = [card("a", "plain one"), card("b", "[tool_result] diff --git a/x b/x"), card("c", "plain two")]
out = rerank(list(w), TEXT_Q)
check("c1_text_query_passthrough", [d["id"] for d in out] == ["a", "b", "c"])

# c2 代码query: tool_result/diff卡浮前
out = rerank(list(w), CODE_Q)
check("c2_code_float", [d["id"] for d in out][:1] == ["b"])

# c3 稳定性: 同分保持原相对序(两张普通卡不换位)
w2 = [card("n1", "alpha beta"), card("t", "[thinking] narrative only text here"),
      card("n2", "gamma delta"), card("d", "[tool_result] Exit code 1\nFAIL")]
out = rerank(w2, CODE_Q)
order = [d["id"] for d in out]
check("c3_stable_equal_order", order.index("n1") < order.index("n2"))
check("c3b_thinking_sinks", order[-2:] == ["n2", "t"] or order[-1] in ("n2", "t"))
check("c3c_tool_first", order[0] == "d")

# c4 不增不减卡数
check("c4_count_preserved", len(out) == len(w2) and
      {d["id"] for d in out} == {d["id"] for d in w2})

# c5 确定性: 双跑逐位一致
o1, o2 = rerank(w2, CODE_Q), rerank(w2, CODE_Q)
check("c5_deterministic", o1 == o2)

# c6 边界: 空/单卡/非str query/非dict项
check("c6_empty", rerank([], CODE_Q) == [])
check("c6_single", rerank([card("x", "y")], CODE_Q)[0]["id"] == "x")
check("c6_badquery", [d["id"] for d in rerank(w, None)] == ["a", "b", "c"])
check("c6_nondict", rerank(["stritem", card("x", "y")], CODE_Q)[0] in ("stritem", {"id": "x"}))

# c7 派生卡×0.4垫底
w3 = [card("at", "ATTRIBUTE TIMELINE ... text"), card("tr", "[tool_result] x"),
      card("ms", "MEMORY STATUS ...")]
out = rerank(w3, CODE_Q)
check("c7_derived_last", [d["id"] for d in out][-2:] == ["at", "ms"] or out[-1]["id"] in ("at", "ms"))
check("c7b_tool_first", out[0]["id"] == "tr")

# c8 thinking含代码特征不降权(有代码证据即非纯叙述)
w4 = [card("tc", "[thinking] I ran it:\n```python\ndef f():\n    return 1\n```"),
      card("pl", "plain words about weather")]
out = rerank(w4, CODE_Q)
check("c8_thinking_with_code_kept", out[0]["id"] == "tc")

# c9 触发词表覆盖抽查(≥15条静态信号)
probes = ["fix the .py file", "Traceback shows KeyError", "unit test fails",
          "column mismatch", 'File "a.py", line 12', "pytest assert",
          "import numpy fails", "class Foo(object)", "diff --git a/x",
          "exit code 1", "stack trace missing", "compile error", "expected 3 got 4",
          "bug in parser", "the def main() signature"]
check("c9_signal_coverage", all(_query_is_code(p) for p in probes))

fails = [n for n, ok in PASS if not ok]
print(f"== {len(PASS) - len(fails)}/{len(PASS)} pass ==")
sys.exit(1 if fails else 0)
