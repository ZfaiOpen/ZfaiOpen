#!/usr/bin/env python3
"""test_f1.py — deep_f1跨请求聚合摘要master卡·本地全链测试(九组断言·ALL_GREEN)
场景: 同tenant三add请求(R1 Paris/R2 engineer/R3 Bob搬到伦敦) + 15请求12条截断
+ 单请求降级 + overlay注入浮顶 + 原SESSION DIGEST回归。
注: R3输入取"Bob计划搬到伦敦"(无空格) — 实测baseline CN封闭文法 name须紧邻
关系动词、宾语不得以空格开头(带空格变体0抽取, 见交付报告), 抽取="Bob 搬到 伦敦"。
跑法: python3 test_f1.py (sys.path指向本目录=补丁后card_construct+deep_digest)。"""
import os
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
os.environ["BRAINOS_CC_DB"] = str(HERE / "test_f1.db")
shutil.rmtree(HERE / "__pycache__", ignore_errors=True)  # 防陈旧字节码
for suf in ("", "-wal", "-shm"):  # 每跑全新库=确定性
    Path(str(HERE / "test_f1.db") + suf).unlink(missing_ok=True)

import card_construct as cc  # noqa: E402 — 补丁后副本

PASS: list[str] = []
FAIL: list[str] = []


def check(tag: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(tag if cond else f"{tag} <- {detail}")
    print(("  ok  " if cond else "  FAIL ") + tag + ("" if cond else f"  <- {detail}"))


def run_req(tenant: str, rid: str, sess: str, text: str, dt: str) -> None:
    """模拟一次add: derive_all直调(overlay缺省·k3=None)·附原文卡直通回归。"""
    out = cc.derive_all([{"conv": sess, "sess": sess, "speaker": "",
                          "text": text, "dt": dt}], rid, None, tenant)
    assert out and out[0].get("text") == text, "原文卡1:1直通被破坏"


def rows(tenant: str, dia_like: str) -> list[tuple]:
    return cc._db().execute(
        "SELECT dia, text, kind FROM derived WHERE tenant=? AND dia LIKE ?",
        (tenant, dia_like)).fetchall()


MASTER_LIKE, DIGEST_LIKE = "%:master", "%:digest"

# ── 主场景 R1→R2→R3(同tenant·三add请求) ─────────────────────────────────────
run_req("t_f1", "req-1", "s1", "Alice lives in Paris", "2025-01-01T00:00:00")
run_req("t_f1", "req-2", "s2", "Alice works as software engineer", "2025-02-01T00:00:00")
run_req("t_f1", "req-3", "s3", "Bob计划搬到伦敦", "2025-03-01T00:00:00")

masters = rows("t_f1", MASTER_LIKE)
mtext = masters[0][1] if masters else ""
check("① R3后存在:master digest行", len(masters) == 1 and masters[0][2] == "digest",
      f"rows={masters!r}")
check("② master含三请求事实", all(w in mtext for w in
      ("lives in Paris", "works as", "搬到")), f"m={mtext[:120]!r}")
check("③ 前缀KNOWLEDGE DIGEST (all sessions): ",
      mtext.startswith("KNOWLEDGE DIGEST (all sessions): "), f"m={mtext[:60]!r}")
check("④ 旧master已清除(同tenant仅1条)+时序正序",
      len(masters) == 1 and mtext.index("lives in Paris")
      < mtext.index("works as") < mtext.index("搬到"))
hist_n = cc._db().execute(
    "SELECT COUNT(*) FROM digest_hist WHERE tenant='t_f1'").fetchone()[0]
check("④b digest_hist=3条", hist_n == 3, f"hist={hist_n}")

# ── ⑤/⑥ overlay注入+浮顶(master走通kind=digest直通+浮顶路径) ────────────────
FILL = [{"id": f"organ-{i}", "content": f"filler small talk {i}", "created_at": ""}
        for i in range(3)]


def inject(query: str) -> tuple[list, int, int]:
    out, n = cc.overlay_inject("t_f1", query, [dict(f) for f in FILL], 8)
    idx = next((i for i, d in enumerate(out)
                if d.get("content", "").startswith("KNOWLEDGE DIGEST")), -1)
    return out, n, idx


out5, n5, idx5 = inject("Summarize our conversations")
check("⑤ summarize注入且浮顶位1-2", n5 > 0 and 1 <= idx5 <= 2, f"n={n5} idx={idx5}")
out5b, n5b, idx5b = inject("Summarize what you know about Alice")
check("⑤b 官方口径注入且浮顶位1-2", n5b > 0 and 1 <= idx5b <= 2, f"n={n5b} idx={idx5b}")
out0, n0 = cc.overlay_inject("t_f1", "Summarize", [], 8)
check("⑤c 空窗守卫(overlay空data直通)", n0 == 0 and out0 == [], f"n={n0}")
out6, n6, idx6 = inject("总结一下我们聊过的内容")
check("⑥ 中文总结注入", n6 > 0 and idx6 >= 1, f"n={n6} idx={idx6}")

# ── ⑦ 12条窗口截断: 15个小请求 ──────────────────────────────────────────────
for i in range(1, 16):
    run_req("t_f1_many", f"many-{i:02d}", f"m{i}", f"User{i} lives in City{i}",
            f"2025-02-{i:02d}T00:00:00")
many = rows("t_f1_many", MASTER_LIKE)
mtext7 = many[0][1] if many else ""
check("⑦a 15请求master≤1400字符(v091时间线预算)", len(many) == 1 and len(mtext7) <= 1400,
      f"n={len(many)} len={len(mtext7)}")
check("⑦b 含最近事实·最旧3条出窗",
      "User15 lives in City15" in mtext7 and "User4 lives in City4" in mtext7
      and "User3 lives in City3" not in mtext7, f"m={mtext7[:120]!r}")

# ── ⑧ 单请求场景不劣化(独立tenant·仅R1) ─────────────────────────────────────
run_req("t_f1_single", "req-x", "sx", "Alice lives in Paris", "2025-01-01T00:00:00")
check("⑧ 单请求无master(零增益不落)", len(rows("t_f1_single", MASTER_LIKE)) == 0)
sd8 = rows("t_f1_single", DIGEST_LIKE)
check("⑧b session digest照常", len(sd8) == 1
      and sd8[0][1].startswith("SESSION DIGEST: 2025-01-01 Alice lives in Paris"), f"sd={sd8!r}")

# ── ⑨ 原有SESSION DIGEST行为不变 ────────────────────────────────────────────
sds = rows("t_f1", DIGEST_LIKE)
fact3 = cc._db().execute("SELECT text FROM derived WHERE tenant='t_f1' "
                         "AND kind='fact' AND dt LIKE '2025-03-01%'").fetchall()
check("⑨ 三张session digest照常+R3digest=SESSION DIGEST+fact原文",
      len(sds) == 3 and all(t[1].startswith("SESSION DIGEST: ") for t in sds)
      and len(fact3) == 1 and any(t[1] == "SESSION DIGEST: 2025-03-01 " + fact3[0][0] for t in sds),
      f"sds={[(t[0], t[1][:40]) for t in sds]} fact3={fact3!r}")

print()
if FAIL:
    print(f"RED: {len(FAIL)}/{len(PASS) + len(FAIL)} checks failed")
    for f in FAIL:
        print("  !!", f)
    sys.exit(1)
print(f"ALL_GREEN ({len(PASS)} checks)")
