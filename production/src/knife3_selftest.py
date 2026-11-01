#!/usr/bin/env python3
"""knife3_selftest.py — C1时间锚定离线自检(scratch DB·不碰服务DB/不碰服务·零网络)
覆盖: 解析器(中英相对/绝对/日历算术/防毒闸/fail-soft) + 索引落盘 + SEARCH相
(duration双卡前插+日期差·when归一注记+浮前排·order位次·none直通·history零重排·
粒度对表) + 范围检索API + backfill幂等 + 重开持久 + 探针租户purge。
用法: knife3_selftest.py [scratch_dir缺省=同目录.selftest_scratch]
输出: 逐案PASS/FAIL+终判行; 退出码0=全过。"""
import json
import shutil
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import knife3_time as k3  # noqa: E402

SCRATCH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / ".selftest_scratch"
if SCRATCH.exists():
    shutil.rmtree(SCRATCH)
SCRATCH.mkdir(parents=True)
DB = SCRATCH / "selftest_knife3.db"
results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {detail}" if detail else ""))


def dates_of(text, dt):
    return [(a["gstart"], a["gend"], a["granularity"]) for a in k3.parse_anchors(text, dt)]


def fresh_K():
    return k3.Knife3(DB)


def cards_of(cards):
    return [{"id": k3.card_id_of(c["sess"], c["dt"], c["text"]),
             "content": c["text"], "created_at": c["dt"]} for c in cards]


# ── 1. 解析器: 英文相对 ──────────────────────────────────────────────────────
check("P1 three days ago", dates_of("I visited the dentist three days ago.", "2026-10-07T10:00:00")
      == [("2026-10-04", None, "day")])
check("P2 last Friday(严格之前)", dates_of("I started yoga last Friday.", "2026-10-07T10:00:00")
      == [("2026-10-02", None, "day")])
check("P3 next Monday(严格将来)", dates_of("next Monday we sail", "2026-10-07T10:00:00")
      == [("2026-10-12", None, "day")])
check("P4 in three days", dates_of("In three days we leave.", "2026-10-07T10:00:00")
      == [("2026-10-10", None, "day")])
check("P5 two days from now", dates_of("submit two days from now.", "2026-10-07T10:00:00")
      == [("2026-10-09", None, "day")])
check("P6 yesterday/tomorrow", dates_of("yesterday was wild, tomorrow better", "2026-10-07T10:00:00")
      == [("2026-10-06", None, "day"), ("2026-10-08", None, "day")])
check("P7 a few days ago", dates_of("a few days ago it rained", "2026-10-07T10:00:00")
      == [("2026-10-04", None, "day")])
check("P8 last week区间", dates_of("last week we hiked", "2026-10-07T10:00:00")
      == [("2026-09-28", "2026-10-04", "week")])
check("P9 next month区间", dates_of("next month we move", "2026-10-07T10:00:00")
      == [("2026-11-01", "2026-11-30", "month")])
check("P10 last year区间", dates_of("born last year", "2026-10-07T10:00:00")
      == [("2025-01-01", "2025-12-31", "year")])

# ── 2. 解析器: 英文绝对 ──────────────────────────────────────────────────────
check("P11 March 3, 2025", dates_of("Signed the lease on March 3, 2025.", "2025-06-27T18:05:00")
      == [("2025-03-03", None, "day")])
check("P12 May 15, 2025", dates_of("Move-in is May 15, 2025.", "2025-06-27T18:09:00")
      == [("2025-05-15", None, "day")])
check("P13 裸April 2(短横守卫)", dates_of("kickoff was April 2 — retro.", "2025-06-27T18:05:00")
      == [("2025-04-02", None, "day")])
check("P14 裸August 19(逗号守卫)", dates_of("landed August 19, one day late.", "2025-06-27T18:09:00")
      == [("2025-08-19", None, "day")])
check("P15 无年回退一年", dates_of("the March 3 signing feels far", "2025-09-05T18:07:00")
      == [("2025-03-03", None, "day")])
check("P16 单卡双日期(间隔载体)", len(dates_of("From the April 2 kickoff to the August 19 freeze.", "2025-09-05T18:05:00")) == 2)
check("P17 DMY不双触发MY", dates_of("Started on 12 January 2024.", "2024-09-30T10:00:00")
      == [("2024-01-12", None, "day")])
check("P18 Month YYYY月粒度", dates_of("March 2025 was busy", "2025-06-27T10:00:00")
      == [("2025-03-01", "2025-03-31", "month")])
check("P19 ISO日期", dates_of("since 2024-01-01 all good", "2025-06-27T10:00:00")
      == [("2024-01-01", None, "day")])
check("P20 may情态守卫(may 3 people≠日期)", dates_of("may 3 people joined", "2025-06-27T10:00:00") == [])
check("P21 may强年份(may 15, 2025)", dates_of("may 15, 2025 was the day", "2025-06-27T10:00:00")
      == [("2025-05-15", None, "day")])
check("P22 3 May.(守卫尾点)", dates_of("leave on 3 May.", "2025-04-01T10:00:00")
      == [("2025-05-03", None, "day")])

# ── 3. 解析器: 中文 ──────────────────────────────────────────────────────────
check("P23 两天前", dates_of("我两天前去了北京。", "2026-10-07T10:00:00") == [("2026-10-05", None, "day")])
check("P24 三天后", dates_of("我三天后出发。", "2026-10-07T10:00:00") == [("2026-10-10", None, "day")])
check("P25 上周五", dates_of("上周五签的合同", "2026-10-07T10:00:00") == [("2026-10-02", None, "day")])
check("P26 下周三", dates_of("下周三开会", "2026-10-07T10:00:00") == [("2026-10-14", None, "day")])
check("P27 上周区间", dates_of("上周我们去爬山了", "2026-10-07T10:00:00")
      == [("2026-09-28", "2026-10-04", "week")])
check("P28 下个月区间", dates_of("下个月搬家", "2026-10-07T10:00:00")
      == [("2026-11-01", "2026-11-30", "month")])
check("P29 去年区间", dates_of("去年冬天很冷", "2026-10-07T10:00:00")
      == [("2025-01-01", "2025-12-31", "year")])
check("P30 昨天/明天", dates_of("昨天签约，明天入住", "2026-10-07T10:00:00")
      == [("2026-10-06", None, "day"), ("2026-10-08", None, "day")])
check("P31 三个月前(日历月)", dates_of("三个月前入职", "2026-10-07T10:00:00")
      == [("2026-07-07", None, "month")])

# ── 4. 防毒闸与fail-soft ────────────────────────────────────────────────────
check("P32 出界弃锚(a thousand years ago)", dates_of("a thousand years ago", "2026-10-07T10:00:00") == [])
check("P33 空文本→[]", dates_of("", "2026-10-07T10:00:00") == [])
check("P34 无dt→[](fail-soft)", dates_of("three days ago it worked", "") == [])
check("P35 脏dt→[](fail-soft)", dates_of("three days ago it worked", "not-a-date") == [])
check("P36 Feb29钳位不炸", isinstance(dates_of("Feb 29 party", "2025-03-01T10:00:00"), list))

# ── 5. query档位检测 ─────────────────────────────────────────────────────────
check("P37 duration(how long between)", k3.detect_tier("How long was it between Rosa signing and moving in?") == "duration")
check("P38 duration(how many months)", k3.detect_tier("How many months passed between X and Y?") == "duration")
check("P39 when(什么时候)", k3.detect_tier("小明什么时候去的北京？") == "when")
check("P40 when(When did...)", k3.detect_tier("When did Maya visit the dentist?") == "when")
check("P41 order(in order)", k3.detect_tier("List the companies in order.") == "order")
check("P42 泛化直通(Tell me about)", k3.detect_tier("Tell me about Alice.") == "none")
check("P43 E3守护不误触发(health)", k3.detect_tier("How is Alice's health?") == "none")
check("P44 D1当前态不误触发(where does)", k3.detect_tier("Where does Alice live?") == "none")

# ── 6. DB+SEARCH相机制 ──────────────────────────────────────────────────────
K = fresh_K()
cards = [
    {"sess": "s1", "dt": "2025-06-27T18:05:00", "text": "Ren: I signed the lease on March 3, 2025."},
    {"sess": "s1", "dt": "2025-06-27T18:09:00", "text": "Ren: I moved into the apartment on May 15, 2025."},
    {"sess": "s2", "dt": "2026-10-07T10:00:00", "text": "Maya: I visited the dentist three days ago."},
]
K.observe("t1", cards)
st = K.stats()
check("P45 observe落盘(3卡3锚)", st["cards"] == 3 and st["anchor_rows"] == 3, json.dumps(st["by_granularity"]))
check("P46 observe幂等(重放零增)", (K.observe("t1", cards) or K.stats())["anchor_rows"] == 3)

data = cards_of(cards)
noise = [{"id": "x1", "content": "noise filler", "created_at": "2026-01-01T00:00:00"},
         {"id": "x2", "content": "noise2", "created_at": "2026-01-02T00:00:00"}]
full = noise[:1] + data + noise[1:]  # x1 + 3真卡 + x2
out, tier, n = K.time_annotate("t1", "How long was it between Ren signing the lease and moving in?", full)
iv = [d.get("metadata", {}).get("time", {}).get("interval") for d in out[:2]]
check("P47 duration双卡前插", tier == "duration" and out[0]["content"].startswith("Ren: I signed")
      and out[1]["content"].startswith("Ren: I moved"))
check("P48 日期差注记(73天)", iv[0] and iv[0]["days"] == 73 and iv[0]["from"] == "2025-03-03"
      and iv[0]["to"] == "2025-05-15")
check("P49 content原文零改写", out[0]["content"] == cards[0]["text"]
      and all("[=" not in d["content"] for d in out))
out6, _, _ = K.time_annotate("t1", "How many months passed between signing and moving in?", cards_of(cards))
check("P50 粒度对表(问月给月数附天数)", "months" in out6[0]["metadata"]["time"]["interval"]["note"])

out2, tier2, n2 = K.time_annotate("t1", "When did Maya visit the dentist?", cards_of(cards))
md = [d["metadata"]["time"] for d in out2 if d["id"] == cards_of(cards)[2]["id"] and d.get("metadata")]
check("P51 when归一注记(双存)", tier2 == "when" and md
      and md[0]["normalized_dates"][0]["date"] == "2026-10-04"
      and md[0]["normalized_dates"][0]["said_as"] == "three days ago")
check("P52 when浮前排(钉top3外命中)", out2.index(next(d for d in out2 if d["id"] == cards_of(cards)[2]["id"])) < 4)

out3, tier3, _ = K.time_annotate("t1", "List the events in order.", cards_of(cards))
ranks = [(d["id"], d.get("metadata", {}).get("time", {}).get("session_time_rank")) for d in out3 if d.get("metadata")]
check("P53 order位次注记(C2红利)", tier3 == "order"
      and ranks and any(r and r["rank"] == 1 and r["of"] == 2 for _, r in ranks))
out4, tier4, _ = K.time_annotate("t1", "Tell me about Ren.", cards_of(cards))
check("P54 none直通零metadata", tier4 == "none" and all("metadata" not in d for d in out4))
out5, tier5, _ = K.time_annotate("t1", "How long between the two events?", cards_of(cards), k1_intent="history")
check("P55 history意图零重排(knife1 C3不打架)", [d["id"] for d in out5] == [d["id"] for d in cards_of(cards)])
check("P56 范围检索API", len(K.cards_in_range("t1", "2025-03-01", "2025-06-01")) == 2
      and K.cards_in_range("t1", "2030-01-01", "2030-12-31") == [])

n_c, n_a, ms_ = K.backfill([{"conv": "t1:s1", "sess": "s1", "dt": "2025-06-27T18:05:00",
                             "text": cards[0]["text"]}])
check("P57 backfill幂等(存量零增)", n_a == 0 and n_c == 1, f"{ms_:.1f}ms")
n_c2, n_a2, _ = K.backfill([{"conv": "t9:s9", "sess": "s9", "dt": "2026-10-07T09:00:00",
                             "text": "Zed: started two days ago"}])
check("P58 backfill新卡入索引", n_c2 == 1 and n_a2 == 1)
K.observe("t1", [{"sess": None, "dt": "bad", "text": None}, {"bad": "dict"}, "junk", 42])
check("P59 脏输入fail-soft(不炸)", True)

K2 = fresh_K()
check("P60 重开持久", K2.stats()["anchor_rows"] == K.stats()["anchor_rows"])
purged = K.purge_tenant("t9")
check("P61 purge租户", purged >= 2 and K.stats()["cards"] == 3)

print(f"\nSELFTEST-VERDICT: {'ALL PASS' if all(results) else 'HAS FAIL'} ({sum(results)}/{len(results)})")
shutil.rmtree(SCRATCH, ignore_errors=True)
sys.exit(0 if all(results) else 1)
