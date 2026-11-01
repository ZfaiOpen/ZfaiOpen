#!/usr/bin/env python3
"""knife6_selftest.py — S7刀离线自检(scratch DB·零网络·零服务依赖·knife1/2/3同款纪律)
用法: knife6_selftest.py [calib_questions.jsonl路径]   (缺省=同目录或~/exam/pipeline/calib_bed/)
覆盖: S1词典装载 S2句检测(正反例) S3主体/对象 S4数值抽取 S5趋势聚簇 S6意图门与置顶加成
      S7全床50query零扰动机械门(仅4条G题允许意图命中) S8敏感卡H2放行门 S9去重移动置顶
      S10幂等 S11脏输入fail-soft S12租户purge S13 k2缺席fail-closed S14模块API S15总闸
输出: 逐案PASS/FAIL + 终判行; 退出码0=全过。"""
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
SCRATCH = HERE / "selftest_knife6.db"
if SCRATCH.exists():
    SCRATCH.unlink()
for suf in ("-wal", "-shm"):
    p = Path(str(SCRATCH) + suf)
    if p.exists():
        p.unlink()
os.environ["KNIFE6_DB"] = str(SCRATCH)

import knife6_cards as k6  # noqa: E402

results = []


def check(name, ok, evidence=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {evidence}" if evidence else ""))


def fresh():
    if SCRATCH.exists():
        SCRATCH.unlink()
    return k6.Knife6(SCRATCH)


def card(text, sess="s1", dt="2026-01-01T10:00:00", speaker="user"):
    return {"conv": sess, "sess": sess, "dia": "r:0", "speaker": speaker,
            "text": text, "dt": dt}


# ── S1 词典装载 ──
check("S1 dict loaded", len(k6._RULE_RES) > 5 and len(k6._EXP_RES) > 3
      and len(k6._METRICS) >= 4 and len(k6._RULE_Q_RES) > 3,
      f"rule_rx={len(k6._RULE_RES)} exp_rx={len(k6._EXP_RES)} metrics={len(k6._METRICS)}")

# ── S2 句检测 ──
cases_rule = [
    "Wen: Makerspace orientation: no food or drink near the lathes, ever — and label everything you store in the bins.",
    "Sable: House rules for game night: the loser of the first round brings snacks next time, and no phones at the table.",
    "以后每周都去健身房三次，雷打不动。",
    "You must take your medication every morning at 8.",
    "Remember to label your bin before Friday.",
]
cases_exp = [
    "Jun: Hard-won lesson: in winter, proof the sourdough twice as long or the crumb turns dense.",
    "Petra: Booking tip from my Larchmere route trips: reserve the campsite ferry crossings three months ahead or summer weekends sell out.",
    "Tried the cold pack method and turns out it halves the proof time.",
    "这招是我总结出的经验：冬天要发面两次。",
]
cases_neg = [
    "Alice lives in Paris.",
    "I had coffee at 9 with Dana.",
    "The meeting is on Monday at noon.",
    "Reef tank log: holding at 25 Celsius, salinity 1.025.",
]
ok = all(k6._classify_sent(s)[0] for s in cases_rule)
check("S2a rule句检测(5正例)", ok)
ok = all(k6._classify_sent(s)[1] for s in cases_exp)
check("S2b exp句检测(4正例)", ok)
ok = not any(k6._classify_sent(s) != (False, False) for s in cases_neg)
check("S2c 反例零误报(4句)", ok)

# ── S3 主体/对象 ──
check("S3a 主体=name前缀", k6._subject_of(cases_exp[0], "") == "Jun",
      k6._subject_of(cases_exp[0], ""))
check("S3b 主体=第一人称→speaker", k6._subject_of("I must go to the gym weekly.", "Rik") == "Rik")
check("S3c 对象元数据非空", len(k6._object_of(cases_rule[0])) > 3)

# ── S4 数值抽取 ──
p1 = k6.extract_points("I weigh 70.5 kg today")
p2 = k6.extract_points("slept 7 hours last night")
p3 = k6.extract_points("salinity 1.025 and 25 celsius")
check("S4a weight抽取", p1 == [("weight", 70.5, "kg")], str(p1))
check("S4b sleep抽取", len(p2) == 1 and p2[0][0] == "sleep_hours" and p2[0][1] == 7.0, str(p2))
check("S4c 无单位数值不误抽", p3 == [], str(p3))

# ── S5 趋势聚簇 ──
k = fresh()
k.observe("t_trend", [card("Day1: I weigh 70 kg", sess="a", dt="2026-03-01T09:00:00"),
                      card("Day2: I weigh 72 kg", sess="a", dt="2026-03-02T09:00:00")])
n0 = k.stats()["trends"]
k.observe("t_trend", [card("Day3: I weigh 74 kg", sess="a", dt="2026-03-03T09:00:00")])
st = k.stats()
trend = k._conn.execute("SELECT content, n, vmin, vmax, direction FROM trends").fetchone()
check("S5a <3点无趋势卡", n0 == 0, f"trends={n0}")
check("S5b ≥3点成趋势卡·rising·区间", trend is not None and trend[1] == 3
      and trend[2] == 70.0 and trend[3] == 74.0 and trend[4] == "rising",
      trend[0] if trend else "none")
check("S5c 趋势卡文案含range", "range [70, 74]" in (trend[0] or ""), trend[0] if trend else "")
fall = fresh()
fall.observe("t_fall", [card("w 90 kg", dt="2026-03-01T09:00:00"),
                        card("w 85 kg", dt="2026-03-02T09:00:00"),
                        card("w 80 kg", dt="2026-03-03T09:00:00")])
d = fall._conn.execute("SELECT direction FROM trends").fetchone()
check("S5d falling方向", d and d[0] == "falling", str(d))

# ── S6 意图门与置顶加成 ──
k = fresh()
uid_cards = [card(cases_rule[0], sess="m1", dt="2026-08-05T18:05:00", speaker="Wen"),
             card(cases_exp[0], sess="m2", dt="2026-01-09T18:05:00", speaker="Jun"),
             card(cases_exp[1], sess="m3", dt="2026-02-25T18:05:00", speaker="Petra")]
k.observe("t_boost", uid_cards)
fact_data = [{"id": f"f{i}", "content": f"fact card {i}", "created_at": "2026-08-01T10:00:00"}
             for i in range(12)]
out, n = k.boost("t_boost", "What did Jun learn about winter sourdough?", list(fact_data))
hit = out and "winter" in str(out[0].get("content", "")).lower() and "proof" in str(out[0].get("content", "")).lower()
check("S6a exp意图置顶(G3-01同构)", n >= 1 and hit,
      f"n={n} top={out[0].get('content', '')[:40] if out else ''}")
out2, n2 = k.boost("t_boost", "What are the makerspace rules about food and storage?", list(fact_data))
check("S6b rule意图置顶(G2-01同构)", n2 >= 1 and "lathes" in str(out2[0].get("content", "")),
      f"n={n2} top={out2[0].get('content', '')[:40] if out2 else ''}")
out3, n3 = k.boost("t_boost", "他周一做了什么？", list(fact_data))
check("S6c 事实query零注入(死护栏)", n3 == 0 and out3 == fact_data, f"n={n3}")
out3b, n3b = k.boost("t_boost", "What did he do on Monday?", list(fact_data))
check("S6d EN事实query零注入", n3b == 0 and out3b == fact_data, f"n={n3b}")
out4, n4 = k.boost("t_boost", "List every rule Jun has followed, in order?",
                   list(fact_data), k1_intent="history")
check("S6e 强时间线意图让位(C3资产)", n4 == 0, f"n={n4}")
out4b, n4b = k.boost("t_boost", "What did Jun learn about winter sourdough?",
                     list(fact_data), k1_intent="history")
check("S6e2 k1弱history代理(did)不劫持经验query", n4b >= 1, f"n={n4b}")
out5, n5 = k.boost("t_boost", "How long between his trips?", list(fact_data), k3_tier="duration")
check("S6f k3时间档让位", n5 == 0, f"n={n5}")
src6 = list(fact_data)
out6, n6 = k.boost("t_boost", "random query", src6)
check("S6g 无信号直通零拷贝", n6 == 0 and out6 is src6, f"n={n6}")
check("S6h 加成不灭事实卡", len(out) == len(fact_data) + n
      and all(f["id"] in {d["id"] for d in out} for f in fact_data),
      f"len={len(out)} fact+pin={len(fact_data) + n}")

# ── S7 全床50query零扰动机械门 ──
qpath = Path(sys.argv[1]) if len(sys.argv) > 1 else None
if not (qpath and qpath.exists()):
    for cand in (HERE / "calib_questions.jsonl", Path.home() / "exam/pipeline/calib_bed/calib_questions.jsonl"):
        if cand.exists():
            qpath = cand
            break
expect = {"G2-01": ["rule"], "G2-02": ["rule"], "G3-01": ["exp"], "G3-02": ["exp"]}
if qpath and qpath.exists():
    bad, got_ok = [], 0
    n_q = 0
    for line in open(qpath, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        if o.get("meta"):
            continue
        n_q += 1
        kinds = k6.query_kinds(o["query"])
        if o["qid"] in expect:
            if kinds == expect[o["qid"]]:
                got_ok += 1
            else:
                bad.append(f"{o['qid']} kinds={kinds}")
        elif kinds:
            bad.append(f"{o['qid']} 意外意图{kinds}")
    check(f"S7 全床{n_q}query零扰动门(仅4G题意图·{got_ok}/4命中)", not bad and got_ok == 4,
          "; ".join(bad[:4]))
    qs_all = {}
    for line in open(qpath, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        if not o.get("meta"):
            qs_all[o["qid"]] = o["query"]
    g_strong = [qid for qid in expect
                if any(rx.search(qs_all.get(qid, "")) for rx in k6._STRONG_RES)]
    check("S7b 4G题零强时间线命中(置顶可放行)", not g_strong, str(g_strong))
    c_strong = [qid for qid in ("C2-01", "C3-01")
                if any(rx.search(qs_all.get(qid, "")) for rx in k6._STRONG_RES)]
    check("S7c 时间线题被强名单拦截", len(c_strong) == 2, str(c_strong))
else:
    check("S7 全床query零扰动门", False, "calib_questions.jsonl未找到")

# ── S8 敏感卡H2放行门 ──
k = fresh()
k.observe("t_h2", [card("Maria was diagnosed with diabetes, so she must take her medication every morning at 8.",
                        sess="h1", dt="2026-05-01T08:00:00", speaker="Maria")])
row = k._conn.execute("SELECT kind, cats FROM cards").fetchone()
cats = set(json.loads(row[1] or "[]")) if row else set()
check("S8a 敏感规则卡带类目", bool(cats), str(cats))
outb, nb = k.boost("t_h2", "What are Maria's rules?", [])
check("S8b 泛化query敏感卡不注入", nb == 0, f"n={nb}")
outc, nc = k.boost("t_h2", "What are the rules about her diabetes medication?", [])
check("S8c 显式健康域放行注入", nc >= 1, f"n={nc}")

# ── S9 去重移动置顶(非重复注入) ──
k = fresh()
src = card(cases_rule[1], sess="g1", dt="2025-09-30T18:05:00", speaker="Sable")
k.observe("t_dedup", [src])
eid = k6.card_id_of("g1", "2025-09-30T18:05:00", src["text"])
data = [{"id": f"x{i}", "content": f"c{i}", "created_at": "2025-10-01T00:00:00"} for i in range(20)]
data.append({"id": eid, "content": src["text"], "created_at": "2025-09-30T18:05:00"})
outd, nd = k.boost("t_dedup", "What are the house rules for game night?", data)
ids = [d["id"] for d in outd]
check("S9a 原卡移动置顶不重复", nd >= 1 and outd[0]["id"] == eid and ids.count(eid) == 1
      and len(outd) == len(data), f"n={nd} top={outd[0]['id'][:8]} cnt={ids.count(eid)}")

# ── S9b 同卡多规则句: 一句一行全保留·置顶每卡只出最佳一句 ──
k = fresh()
multi = card("Rule one: no drinks near the saws, ever. Rule two: label everything you store.",
             sess="m9", dt="2026-06-01T10:00:00", speaker="Wen")
k.observe("t_multi", [multi])
nrows = k._conn.execute("SELECT COUNT(*) FROM cards WHERE tenant='t_multi'").fetchone()[0]
check("S9b-1 多规则句全保留(一句一行)", nrows == 2, f"rows={nrows}")
datam = [{"id": f"m{i}", "content": f"c{i}", "created_at": "2026-06-02T00:00:00"}
         for i in range(15)]
outm, nm = k.boost("t_multi", "What are the workshop rules about storage?", datam)
pins = [d for d in outm if (d.get("metadata") or {}).get("knife6")]
eids = [d["id"] for d in pins]
check("S9b-2 置顶含句子且每卡一句", nm >= 1 and len(eids) == len(set(eids))
      and any("label everything" in d["content"] or "no drinks" in d["content"] for d in pins),
      f"n={nm} pins={[(d['content'][:24]) for d in pins]}")

# ── S10 幂等(observe两遍计数稳定) ──
k = fresh()
k.observe("t_idem", uid_cards)
s1_ = k.stats()
k.observe("t_idem", uid_cards)
s2_ = k.stats()
check("S10 重复observe幂等", s1_["rule_cards"] == s2_["rule_cards"]
      and s1_["exp_cards"] == s2_["exp_cards"] and s1_["points"] == s2_["points"],
      f"{s1_} vs {s2_}")

# ── S11 脏输入fail-soft ──
try:
    k.observe("t_dirty", [{"text": None}, {}, {"text": ""}, {"sess": "x", "dt": "t"}])
    kb = k6.Knife6(SCRATCH.parent / "selftest_k6_b.db")
    kb.boost("nope", "", [])
    kb.boost("nope", "rules?", [{"id": "", "content": "", "created_at": ""}])
    check("S11 脏输入不炸", True)
except Exception as e:  # noqa: BLE001
    check("S11 脏输入不炸", False, repr(e))
finally:
    for p in (SCRATCH.parent / "selftest_k6_b.db",
              SCRATCH.parent / "selftest_k6_b.db-wal",
              SCRATCH.parent / "selftest_k6_b.db-shm"):
        if p.exists():
            p.unlink()

# ── S12 租户purge ──
before = k.stats()["rule_cards"] + k.stats()["exp_cards"]
kn = k.purge_tenant("t_idem")
after = k.stats()["rule_cards"] + k.stats()["exp_cards"]
check("S12 purge_tenant清行", kn > 0 and after < before, f"purged={kn}")

# ── S13 k2缺席fail-closed ──
saved = {m: sys.modules.get(m) for m in ("knife2_sensitive",)}
sys.modules["knife2_sensitive"] = None  # import → ImportError → cats=∅(fail-closed不标)
try:
    k2free = fresh()
    k2free.observe("t_nok2", [card("She must take insulin every day.", speaker="A")])
    row = k2free._conn.execute("SELECT cats FROM cards WHERE tenant='t_nok2'").fetchone()
    check("S13 k2缺席=零类目不误标", row is not None and json.loads(row[0] or "[]") == [],
          str(row))
finally:
    for m, v in saved.items():
        if v is None:
            sys.modules.pop(m, None)
        else:
            sys.modules[m] = v

# ── S14 模块级API ──
try:
    k6.get()
    k6.stats()
    k6.observe("t_api", [card("we always label the bins", speaker="Z")])
    k6.backfill([{"conv": "t_api:s9", "sess": "s9", "dt": "2026-01-02T08:00:00",
                  "speaker": "Z", "text": "Booking tip: reserve early."}])
    k6.boost("t_api", "any tips?", [])
    k6.purge_tenant("t_api")
    check("S14 模块API五件套", True)
except Exception as e:  # noqa: BLE001
    check("S14 模块API五件套", False, repr(e))

# ── S15 总闸 ──
os.environ["BRAINOS_K6"] = "0"
try:
    outz, nz = k6.get().boost("t_h2", "any rules?", fact_data)
    check("S15 BRAINOS_K6=0关门直通", nz == 0 and outz is fact_data)
finally:
    os.environ["BRAINOS_K6"] = "1"

# ── 清场 ──
for suf in ("", "-wal", "-shm"):
    for p in (Path(str(SCRATCH) + suf),
              SCRATCH.parent / ("selftest_k6_b.db" + suf)):
        if p.exists():
            p.unlink()

npass = sum(results)
print(f"\n== knife6_selftest 终判: {npass}/{len(results)} PASS ==")
sys.exit(0 if npass == len(results) else 1)
