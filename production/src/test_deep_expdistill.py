#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_deep_expdistill.py — M3经验蒸馏器单测(8组·轨道C·2026-10-25)
覆盖: ①合成成功轨迹→FIX字段 ②对抗:中途"All tests pass"后又失败→必不产FIX
(erank v3血泪坑) ③两次Exit 1同测试→ANTI ④issue标题误导→COMPR模块归属
⑤hero_cards排序(errsig优先/无代码特征语义兜底不空不崩/ANTI失败语境提权)
⑥幂等(同轨迹重复observe不重复产卡) ⑦knife13 fingerprint契约(可选参数+import失败fail-soft)
⑧契约(id唯一稳定/content非空/无平台标记泄漏/门关零行为/租户隔离)。
运行: venv python test_deep_expdistill.py (exit 0=全绿)
"""
import os
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC))

# 独立测试DB(绝不触碰运行树card_construct.db)+门开
for suf in ("", "-wal", "-shm"):
    p = Path(str(SRC / "test_expdistill.db") + suf)
    if p.exists():
        p.unlink()
os.environ["BRAINOS_CC_DB"] = str(SRC / "test_expdistill.db")
os.environ["BRAINOS_EXP_DISTILL"] = "1"

import deep_expdistill as expd  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  <- {detail}" if detail and not cond else ""))


def card(sess, dia, dt, speaker, text):
    return {"conv": sess, "sess": sess, "dia": dia, "speaker": speaker,
            "text": text, "dt": dt}


# ── 合成轨迹材料 ────────────────────────────────────────────────────────────
T = "2026-10-25T10:00:00"
SUCCESS_TRAJECTORY = [
    card("s_ok", "r1:0", T, "user",
         "Fix the header parsing bug in the report generator"),
    card("s_ok", "r1:1", T, "assistant",
         "[thinking] The header parser fails because the delimiter regex splits on every comma."),
    card("s_ok", "r1:2", T, "assistant",
         "[tool_result] $ pytest tests/test_header.py\n"
         "FAILED tests/test_header.py::test_header_delim - ValueError: "
         "could not convert string to float: 'a,b'\nExit code 1"),
    card("s_ok", "r1:3", T, "assistant",
         "[tool_result] diff --git a/src/parser.py b/src/parser.py\n"
         "@@ -41,7 +41,7 @@ def parse_header\n"
         "-old = line.split(',')\n"
         "+old = line.split('|')\n"),
    card("s_ok", "r1:4", T, "assistant",
         "[thinking] The root cause is the delimiter hardcoded to comma instead of pipe."),
    card("s_ok", "r1:5", T, "assistant",
         "[tool_result] $ pytest tests/test_header.py\n1 passed in 0.02s\nExit code 0"),
]

# 对抗轨迹: 中途"All tests pass"+Exit 0 后又失败 (erank v3 中途假pass)
ADVERSARIAL_TRAJECTORY = [
    card("s_adv", "r2:0", T, "user", "Fix the header parsing bug in the report generator"),
    card("s_adv", "r2:1", T, "assistant",
         "[tool_result] $ pytest tests/test_header.py\n"
         "FAILED tests/test_header.py::test_header_delim - ValueError: "
         "could not convert string to float: 'a,b'\nExit code 1"),
    card("s_adv", "r2:2", T, "assistant",
         "[tool_result] diff --git a/src/parser.py b/src/parser.py\n"
         "@@ -41,7 +41,7 @@ def parse_header\n"
         "-old = line.split(',')\n"
         "+old = line.split('|')\n"),
    card("s_adv", "r2:3", T, "assistant",
         "[tool_result] All tests pass now\nExit code 0"),
    card("s_adv", "r2:4", T, "assistant",
         "[tool_result] FAILED tests/test_header.py::test_header_delim - ValueError: "
         "could not convert string to float: 'a,b'\nExit code 1"),
]

# 失败轨迹: 同一测试两次Exit 1·无diff
FAIL_TRAJECTORY = [
    card("s_fail", "r3:0", T, "user", "make the cli tests green"),
    card("s_fail", "r3:1", T, "assistant",
         "[tool_result] $ pytest tests/test_cli.py\n"
         "FAILED tests/test_cli.py::test_flags - SystemExit: 2\nExit code 1"),
    card("s_fail", "r3:2", T, "assistant",
         "[thinking] maybe the timeout needs increasing, retrying"),
    card("s_fail", "r3:3", T, "assistant",
         "[tool_result] $ pytest tests/test_cli.py\n"
         "FAILED tests/test_cli.py::test_flags - SystemExit: 2\nExit code 1"),
]

# 第二成功轨迹(KeyError·config.py·标题直说config=非误导→无COMPR)
SUCCESS_TRAJECTORY_2 = [
    card("s_cfg", "r4:0", T, "user", "config loader raises KeyError on settings"),
    card("s_cfg", "r4:1", T, "assistant",
         "[tool_result] $ pytest tests/test_config.py\n"
         "FAILED tests/test_config.py::test_load - KeyError: 'settings'\nExit code 1"),
    card("s_cfg", "r4:2", T, "assistant",
         "[tool_result] diff --git a/app/config.py b/app/config.py\n"
         "@@ -12,6 +12,7 @@ def load_config\n+    data.setdefault('settings', {})\n"),
    card("s_cfg", "r4:3", T, "assistant",
         "[tool_result] $ pytest tests/test_config.py\n2 passed in 0.03s\nExit code 0"),
]

TN = "u_test"


def stored(tenant=TN):
    conn, cc = expd._db()
    with cc._DB_LOCK:
        return conn.execute(
            "SELECT kind, text, errsig FROM exp_cards WHERE tenant=? ORDER BY seq, dia",
            (tenant,)).fetchall()


print("== G1 合成成功轨迹 → FIX 字段正确 ==")
n = expd.observe_session_end(SUCCESS_TRAJECTORY, TN)
rows = stored()
fix_rows = [r for r in rows if r[0] == "expfix"]
check("g1_stored_fix", len(fix_rows) == 1 and n >= 1, f"rows={rows}")
if fix_rows:
    txt = fix_rows[0][1]
    check("g1_title_prefix", txt.startswith("FIX[ValueError"), txt[:60])
    check("g1_title_one_sentence", "\n" in txt and "fix ValueError in parser.py" in txt.split("\n")[0], txt.split("\n")[0])
    check("g1_symptom", "SYMPTOM: FAILED tests/test_header.py::test_header_delim" in txt, txt[:200])
    check("g1_root", "ROOT: root cause is the delimiter hardcoded to comma" in txt, txt[:300])
    check("g1_changed", "CHANGED: src/parser.py" in txt and "parser.py::parse_header" in txt, txt[:400])
    check("g1_verify", "VERIFY: pytest tests/test_header.py" in txt, txt[:500])
    check("g1_token_budget", len(txt) <= 880, str(len(txt)))
    check("g1_errsig_col", fix_rows[0][2].startswith("ValueError-"), fix_rows[0][2])

print("== G2 对抗: 中途 All tests pass 后又失败 → 必不产 FIX ==")
nfix_before = len([r for r in stored() if r[0] == "expfix"])
nanti_before = len([r for r in stored() if r[0] == "expanti"])
expd.observe_session_end(ADVERSARIAL_TRAJECTORY, TN)
rows = stored()
check("g2_no_fix", len([r for r in rows if r[0] == "expfix"]) == nfix_before,
      f"kinds={[r[0] for r in rows]}")
anti_rows = [r for r in rows if r[0] == "expanti"][nanti_before:]
check("g2_anti_instead", len(anti_rows) == 1 and "test_header_delim still failing" in anti_rows[0][1]
      and "Exit 1" in anti_rows[0][1], f"anti={anti_rows}")

print("== G3 失败轨迹(两次Exit 1同测试) → ANTI ==")
expd.observe_session_end(FAIL_TRAJECTORY, TN)
rows = stored()
anti = [r for r in rows if r[0] == "expanti" and "test_cli" in r[1]]
check("g3_anti_produced", len(anti) == 1, f"rows={[r[0] for r in rows]}")
if anti:
    check("g3_format", anti[0][1].startswith("ANTI[SystemExit] tried pytest tests/test_cli.py, "
                                             "Exit 1, tests/test_cli.py::test_flags still failing"),
          anti[0][1])

print("== G4 issue标题误导 → COMPR 从 diff 归属抽模块 ==")
rows = stored()
compr = [r for r in rows if r[0] == "expcmpr"]
check("g4_compr_produced", len(compr) == 1, f"kinds={[r[0] for r in rows]}")
if compr:
    check("g4_module_attribution",
          compr[0][1] == "ISSUE-MAP: 'Fix the header parsing bug in the report generator' "
          "was actually src/parser.py", compr[0][1])
# 对照: 标题直说config·diff改app/config.py → 词面相交 → 不产COMPR
expd.observe_session_end(SUCCESS_TRAJECTORY_2, TN)
rows = stored()
check("g4_no_false_compr", all("config" not in (r[1] or "") or r[0] != "expcmpr" for r in rows),
      f"kinds={[r[0] for r in rows]}")

print("== G5 hero_cards 排序与兜底 ==")
# errsig精确命中优先(压过纯词面命中的其他卡)
out = expd.hero_cards("ValueError: could not convert string to float when parsing rows", TN)
check("g5_errsig_first", out and out[0]["content"].startswith("FIX[ValueError"),
      str([o["content"][:40] for o in out]))
# 无代码特征query: 语义兜底(词面)不空返回不崩
out2 = expd.hero_cards("how did we fix the parser problem", TN)
check("g5_lexical_fallback", isinstance(out2, list) and len(out2) >= 1
      and all(o.get("content") for o in out2),
      str(out2)[:200])
check("g5_fallback_relevant", any("parser" in o["content"] for o in out2), str(out2)[:200])
# 失败语境+SystemExit → ANTI提权到首位
out3 = expd.hero_cards("running the cli test_flags keeps failing with SystemExit", TN)
check("g5_anti_promoted", out3 and out3[0]["content"].startswith("ANTI[SystemExit"),
      str([o["content"][:40] for o in out3]))
check("g5_max3", len(out3) <= 3 and len(out2) <= 3)
# 完全无关query → 空窗(宁缺毋滥·不硬塞)
out4 = expd.hero_cards("zanzibar sunset photographs", TN)
check("g5_irrelevant_empty", out4 == [], str(out4)[:100])
# knife13 fingerprint契约: 显式dict参数被消费(files命中)
out5 = expd.hero_cards("strange failure in the loader", TN,
                       fingerprint={"files": ["app/config.py"], "symbols": [], "errsig": ""})
check("g5_fingerprint_files", out5 and out5[0]["content"].startswith("FIX[KeyError"),
      str([o["content"][:40] for o in out5]))
# fingerprint缺省→尝试import knife13_repo(不存在)→fail-soft不崩
check("g5_no_knife13_nocrash", isinstance(expd.hero_cards("anything failing", TN), list))

print("== G9 源归因标记(SRC: [SBCTX task=]·轨道A记分板记功锚) ==")
out6 = expd.hero_cards("ValueError: could not convert string to float when parsing rows", TN)
fix_out = next((o for o in out6 if o["content"].startswith("FIX[")), None)
check("g9_src_line_present", fix_out is not None
      and fix_out["content"].endswith("\nSRC: [SBCTX task=s_ok]"), str(fix_out)[-120:] if fix_out else None)
check("g9_marker_parses_evaluator_regex",
      fix_out is not None and re.search(r"\[SBCTX task=([^\]\s]+)\]", fix_out["content"]).group(1) == "s_ok")
# sess带租户前缀形态(memory_service conv侧惯例) → 剥前缀取纯instance_id
expd.observe_session_end([
    card("u_test:s_pref", "r9:0", T, "user", "fix the timezone conversion bug"),
    card("u_test:s_pref", "r9:1", T, "assistant",
         "[tool_result] $ pytest tests/test_tz.py\n"
         "FAILED tests/test_tz.py::test_utc - OverflowError: date value out of range\nExit code 1"),
    card("u_test:s_pref", "r9:2", T, "assistant",
         "[tool_result] diff --git a/lib/tz.py b/lib/tz.py\n"
         "@@ -8,6 +8,7 @@ def to_utc\n+    if year > 9999: return None\n"),
    card("u_test:s_pref", "r9:3", T, "assistant",
         "[tool_result] $ pytest tests/test_tz.py\n1 passed in 0.01s\nExit code 0"),
], TN)
out7 = expd.hero_cards("OverflowError date value out of range failing", TN)
pref_hero = next((o for o in out7 if "OverflowError" in o["content"]), None)
check("g9_tenant_prefix_stripped", pref_hero is not None
      and pref_hero["content"].endswith("\nSRC: [SBCTX task=s_pref]"),
      str(pref_hero)[-120:] if pref_hero else str(out7)[:200])
check("g9_id_stable_16hex", fix_out is not None
      and re.fullmatch(r"[0-9a-f]{16}", fix_out["id"]) is not None
      and expd.hero_cards("ValueError: could not convert string to float when parsing rows", TN)[0]["id"]
      == fix_out["id"], fix_out["id"] if fix_out else None)

print("== G6 幂等: 同轨迹重复observe不重复产卡 ==")
before = stored()
n1 = expd.observe_session_end(SUCCESS_TRAJECTORY, TN)
expd.observe_session_end(FAIL_TRAJECTORY, TN)
after = stored()
check("g6_rowcount_stable", len(before) == len(after),
      f"before={len(before)} after={len(after)}")
check("g6_content_stable", sorted(r[1] for r in before) == sorted(r[1] for r in after))

print("== G7 终态判定边界(无法判定→不产卡) ==")
cnt_before = len(stored())
# 纯文本会话(无任何代码痕迹) → 不产卡
n_ = expd.observe_session_end([
    card("s_plain", "r5:0", T, "user", "hello world plain chat"),
], TN)
check("g7_plain_session_nocard", n_ == 0 and len(stored()) == cnt_before, f"n={n_}")
# 末位Exit 0但无diff且无失败 → 不产FIX(宁缺毋滥)
n_ = expd.observe_session_end([
    card("s_nodiff", "r6:0", T, "user", "check"),
    card("s_nodiff", "r6:1", T, "assistant", "[tool_result] $ pytest x.py\n1 passed\nExit code 0"),
], TN)
check("g7_nodiff_nofix", n_ == 0 and len(stored()) == cnt_before, f"n={n_}")
# 单次失败无diff无二次尝试 → 不产ANTI
n_ = expd.observe_session_end([
    card("s_one", "r7:0", T, "user", "run"),
    card("s_one", "r7:1", T, "assistant",
         "[tool_result] $ pytest tests/test_a.py\nFAILED tests/test_a.py::test_x - AssertionError: boom\nExit code 1"),
], TN)
check("g7_single_fail_noanti", n_ == 0 and len(stored()) == cnt_before, f"n={n_}")
# 乱序输入(倒序+步进dt) → 按dt稳定排序后判定一致(FIX仍产出)
ord_cards = [
    card("s_ord", "r8:0", "2026-10-25T11:00:00", "user", "fix the cache eviction error"),
    card("s_ord", "r8:1", "2026-10-25T11:01:00", "assistant",
         "[tool_result] $ pytest tests/test_cache.py\n"
         "FAILED tests/test_cache.py::test_evict - RuntimeError: dictionary changed size during iteration\nExit code 1"),
    card("s_ord", "r8:2", "2026-10-25T11:02:00", "assistant",
         "[tool_result] diff --git a/core/cache.py b/core/cache.py\n"
         "@@ -30,6 +30,7 @@ def evict\n+    for k in list(keys):\n"),
    card("s_ord", "r8:3", "2026-10-25T11:03:00", "assistant",
         "[tool_result] $ pytest tests/test_cache.py\n1 passed in 0.01s\nExit code 0"),
]
n_ = expd.observe_session_end(list(reversed(ord_cards)), TN)
rows_ = stored()
check("g7_shuffled_dt_sort_fix", n_ >= 1 and any(
    r[0] == "expfix" and "RuntimeError" in r[1] for r in rows_), f"n={n_}")

print("== G8 契约: id/content/平台标记/门关/租户隔离 ==")
allout = expd.hero_cards("ValueError parser failing SystemExit KeyError config", TN)
ids = [o["id"] for o in allout]
check("g8_id_unique", len(ids) == len(set(ids)) and all(ids))
check("g8_content_nonempty", all(o["content"] and o["content"].strip() for o in allout))
plat_marks = ("[tool_result]", "[tool_use", "[thinking]", "[Time:", "clbench", "swebench",
              "locomo", "$ ")
check("g8_no_platform_leak", all(not any(m in o["content"] for m in plat_marks) for o in allout),
      str(allout)[:300])
o_a = expd.hero_cards("ValueError failing", TN)
o_b = expd.hero_cards("ValueError failing", TN)
check("g8_deterministic", o_a == o_b)
check("g8_created_at", all(isinstance(o.get("created_at"), str) for o in allout))
# 租户隔离: 异租户零卡
check("g8_tenant_isolated", expd.hero_cards("ValueError failing", "u_other") == [])
# 门关: observe no-op·hero空(不改DB)
os.environ["BRAINOS_EXP_DISTILL"] = "0"
rows_before = stored()
check("g8_gate_off_observe", expd.observe_session_end(SUCCESS_TRAJECTORY, TN) == 0)
check("g8_gate_off_hero", expd.hero_cards("ValueError failing", TN) == [])
check("g8_gate_off_db_untouched", stored() == rows_before)
os.environ["BRAINOS_EXP_DISTILL"] = "1"

fails = [n for n in FAIL]
print(f"== {len(PASS)}/{len(PASS) + len(fails)} pass ==")
sys.exit(1 if fails else 0)
