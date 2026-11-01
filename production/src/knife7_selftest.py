# knife7_selftest.py — S4抽取式摘要刀自检(离线·零HTTP·stdlib only·20261009)
# 用法: python3 knife7_selftest.py   (在app目录跑·需knife1_supersede同目录可导入)
# 覆盖: 词典门/聚类确定性/会话触发/滚动版本superseded/进结果门守卫/实体触发/幂等重放/清理
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import knife7_digest as k7  # noqa: E402

RESULTS: list[bool] = []


def check(name: str, ok: bool, ev: str = "") -> None:
    RESULTS.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f" | {ev}" if ev else ""))


def card(sess: str, i: int, text: str, day: int, speaker: str = "user") -> dict:
    return {"sess": sess, "dt": f"2026-09-{day:02d}T10:{i % 60:02d}:00",
            "speaker": speaker, "text": text}


TMP = Path(tempfile.mkdtemp(prefix="k7self"))
DB = TMP / "knife7.db"
K = k7.Knife7(DB)

# ── T1 词典门(白名单阳性/阴性·清单纪律) ──
for q in ("总结一下这段时间", "他所有行李都带了吗", "一共花了多少钱", "Summarize the trip",
          "Give me a summary of our chats", "How has Ming been overall?",
          "what happened all time", "概括一下", "long-term memory recap"):
    check(f"T1a 聚合白名单命中[{q[:18]}]", k7.is_aggregate(q))
for q in ("他周二做了什么", "What did he buy on Tuesday?", "陈明的姐姐叫什么",
          "Where does Chen work?", "What was decided at the product meeting?",
          "Who gave demos at the weekly sync?", "Which project did Alice choose?",
          "他上周三去没去图书馆"):
    check(f"T1b 事实query不触发[{q[:18]}]", not k7.is_aggregate(q))

# ── T2 聚类确定性(两主题不串簇) ──
cards2 = [card("s", 0, "周二去了图书馆看书借书", 1), card("s", 1, "周三在图书馆继续借书", 2),
          card("s", 2, "瑜伽课学了新体式拉伸", 3), card("s", 3, "晚上瑜伽课拉伸练习", 4)]
cl = k7._cluster(cards2)
check("T2 聚类分出独立主题簇", len(cl) >= 2 and
      {len(c) for c in cl} >= {2}, f"clusters={[len(c) for c in cl]}")

# ── T3 会话触发@20 → digest v1 ──
S1 = "probe_sess"
for i in range(19):
    K.observe("t3", [card(S1, i, f"周二陈明去图书馆第{i}次借书, 并整理笔记", i + 1)])
st = K.stats()
check("T3a 19卡未触发", st["digests"] == 0, f"digests={st['digests']}")
K.observe("t3", [card(S1, 20, "周二下午陈明在图书馆还了全部旧书", 21)])
st = K.stats()
check("T3b 20卡触发digest v1", st["digests"] == 1 and st["digests_current"] == 1,
      f"stats={st['digests']}/{st['digests_current']}")
with sqlite3.connect(DB) as cx:
    row = cx.execute("SELECT card_id, content, covered, dt_start, dt_end, superseded "
                     "FROM k7_digests").fetchone()
cid_v1, content_v1 = row[0], row[1]
check("T3c digest内容=覆盖计数+跨度+原文引用",
      row[0].startswith("k7s") and row[2] == 20 and row[5] == 0
      and "覆盖20卡" in content_v1 and "时间跨度" in content_v1
      and "«" in content_v1 and "图书馆" in content_v1,
      f"id={row[0][:8]} covered={row[2]} span={row[3]}~{row[4]}")
T3_ORIG = [f"周二陈明去图书馆第{i}次借书, 并整理笔记" for i in range(19)] + \
          ["周二下午陈明在图书馆还了全部旧书"]
import re as _re
quotes = _re.findall(r"«([^»]+)»", content_v1)
check("T3d 代表句原文逐字保真(每条引用均为某原文子串·抽取非生成)",
      len(quotes) >= 1 and all(
          any(q.split("…")[0] in orig for orig in T3_ORIG) for q in quotes),
      f"quotes={quotes}")

# ── T4 滚动更新@40 → v2·v1标superseded(knife1双时相语义) ──
for i in range(18):
    K.observe("t3", [card(S1, 30 + i, f"周三陈明练瑜伽第{i}课, 拉伸与平衡", 22 + i)])
st = K.stats()
check("T4a 38卡仍v1", st["digests"] == 1)
K.observe("t3", [card(S1, 50, "周五陈明参加瑜伽结业考", 40),
                 card(S1, 51, "周六陈明回图书馆办年卡", 41)])
st = K.stats()
check("T4b 42卡触发v2(v1被superseded)",
      st["digests"] == 2 and st["digests_current"] == 1, f"stats={st['digests']}/{st['digests_current']}")
with sqlite3.connect(DB) as cx:
    rows = cx.execute("SELECT version, superseded FROM k7_digests ORDER BY version").fetchall()
check("T4c v1=superseded·v2=current", rows == [(1, 1), (2, 0)], f"rows={rows}")

# ── T5 进结果门守卫 ──
base = [{"id": f"r{i}", "content": f"card{i}", "created_at": "2026-09-01"} for i in range(5)]
out, n, hb = K.inject("t3", "他周二做了什么", list(base), 100)
check("T5a 事实query零突变(命中数守卫)", out == base and n == 0 and hb == 5)
out, n, hb = K.inject("t3", "总结一下这段时间", list(base), 100)
check("T5b 聚合query注入v2且只追加",
      n == 1 and out[:5] == base and out[5]["id"].startswith("k7")
      and "[digest v2]" in out[5]["content"], f"n={n} id={out[5]['id'][:10]}")
out2, n2, _ = K.inject("t3", "总结一下这段时间", list(base), 5)
check("T5c 空位不足(top_k=5)放弃注入=绝不夺位", out2 == base and n2 == 0)
dupid = [{"id": cid_v1 and "k7sx", "content": "x", "created_at": ""}]
out3, n3, _ = K.inject("t3", "总结一下这段时间", base + [], 100)
out4, n4, _ = K.inject("t3", "总结一下这段时间", out3, 100)
check("T5d 幂等(已注入的digest不重复)", n4 == 0 and out4 == out3)

# ── T6 实体触发@8事实 → 实体digest(复用knife1抽取) ──
for i in range(4):
    K.observe("t6", [card("e1", i, f"I live in Ardenvale block {i}.", 1 + i)])
st = K.stats()
check("T6a 4事实未触发", st["digests_current"] == 1, f"current={st['digests_current']}")
for i in range(4):
    K.observe("t6", [card("e2", i, f"I work at Northlake lab {i} on Tuesdays.", 10 + i)])
st = K.stats()
cur = [r for r in K.stats().items()]
with sqlite3.connect(DB) as cx:
    ent = cx.execute("SELECT kind, dkey, version, superseded FROM k7_digests "
                     "WHERE kind='ent' AND tenant='t6'").fetchall()
check("T6b 8同实体事实触发实体digest", len(ent) == 1 and ent[0][3] == 0, f"ent={ent}")

# ── T7 幂等重放(同卡零增量) ──
before = K.stats()["digests"]
K.observe("t6", [card("e2", 0, "I work at Northlake lab 0 on Tuesdays.", 10)])
check("T7 同卡重放零增量", K.stats()["digests"] == before)

# ── T8 purge ──
n = K.purge_tenant("t6")
check("T8 purge_tenant清空本租户", n > 0 and
      sqlite3.connect(DB).execute(
          "SELECT COUNT(*) FROM k7_cards WHERE tenant='t6'").fetchone()[0] == 0)

shutil.rmtree(TMP, ignore_errors=True)
npass = sum(RESULTS)
print(f"\n== knife7_selftest 终判: {npass}/{len(RESULTS)} PASS ==")
sys.exit(0 if npass == len(RESULTS) else 1)
