#!/usr/bin/env python3
"""knife2_selftest.py — H2隐私闸离线自检(部署线·零内容依赖官方评分·不碰服务DB)
用法: KNIFE2_DB=<scratch路径> knife2_selftest.py   (退出码0=全过)
覆盖: A类标注(五类目各案+普通卡零误标) / B类放行门(泛化滤除·显式域放行·ANY-release·E3守护)
      / C类持久化(重开DB不丢) / D类fail-soft(脏输入不炸) / E类超采补偿计数
纪律: scratch DB用完即删; 与服务knife2.db隔离(KNIFE2_DB env), 绝不清真实标注。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))

SCRATCH = os.environ.get("KNIFE2_DB") or str(
    Path.home() / "knife2_selftest_scratch.db")
if Path(SCRATCH).exists():
    os.remove(SCRATCH)
os.environ["KNIFE2_DB"] = SCRATCH

import knife2_sensitive as k2  # noqa: E402

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(("PASS " if ok else "FAIL ") + name + ((" | " + detail) if detail else ""))


# ── A类: ADD相标注 ──
A_CASES = [
    ("id_ssn", "Alice's social security number is 123-45-6789, keep it private.", "id_document"),
    ("id_cn18", "她的身份证号是110101199003078515。", "id_document"),
    ("id_passport", "Bob said his passport number is X1234567.", "id_document"),
    ("fin_salary", "Alice earns $120,000 a year at the firm.", "finance_precise"),
    ("fin_monthly", "He makes about 2,500 dollars per month from rent.", "finance_precise"),
    ("fin_cn", "她月薪3万2千, 别告诉别人。", "finance_precise"),
    ("health_diag", "Alice was diagnosed with type 2 diabetes last spring.", "health"),
    ("health_med", "She takes metformin every morning.", "health"),
    ("health_cn", "父亲确诊了高血压, 在长期服药。", "health"),
    ("family_div", "Alice is divorcing her husband after ten years.", "family_conflict"),
    ("family_fight", "He keeps fighting with his wife about money.", "family_conflict"),
    ("addr_full", "Alice lives at 42 Maple Street in Springfield.", "address_full"),
    ("addr_apt", "Her address is 221B Baker Road, apartment 12.", "address_full"),
]
for name, text, want_cat in A_CASES:
    cats = k2.classify_text(text)
    check(f"A_{name}", want_cat in cats, f"cats={sorted(cats)}")

BENIGN = [
    "Alice lives in London.",  # 城市级 ≠ 门牌全址(D1资产不得误标)
    "Alice likes sushi and jazz.",
    "Bob works at the hospital as an accountant.",
    "她住在杭州, 最喜欢西湖醋鱼。",
    "Alice moved to a new city recently.",
    "The meeting is at 3 pm on Friday.",
]
for i, text in enumerate(BENIGN):
    cats = k2.classify_text(text)
    check(f"A_benign_{i}", not cats, f"cats={sorted(cats)}")

# ── B类: 放行门 ──
GEN_Q = ["Tell me about Alice.", "What has happened recently?",
         "What has Alice been up to?", "Summarize the conversation.",
         "最近有什么事?", "和我讲讲Alice"]
for q in GEN_Q:
    rel = k2.release_domains(q)
    check(f"B_gen_release_empty[{q[:24]}...]", len(rel) == 0, f"release={sorted(rel)}")

REL_Q = [
    ("How is Alice's health?", "health"),
    ("Does Alice have any health conditions?", "health"),
    ("What medication does Alice take?", "health"),
    ("Alice的身体状况怎么样?", "health"),
    ("What is Alice's salary?", "finance_precise"),
    ("How much does she earn?", "finance_precise"),
    ("她的工资是多少?", "finance_precise"),
    ("Is Alice having problems with her husband?", "family_conflict"),
    ("What is Alice's passport number?", "id_document"),
    ("Where does Alice live exactly? What's her address?", "address_full"),
]
for q, want in REL_Q:
    rel = k2.release_domains(q)
    check(f"B_rel_{want}[{q[:28]}...]", want in rel, f"release={sorted(rel)}")

# ── C类: 端到端(observe→filter_gate) + 持久化 ──
TEN = "selftest_t2"
K = k2.get()
cards = [
    {"sess": f"{TEN}-s1", "dt": "2026-10-08T09:00:00", "speaker": "user",
     "text": "Alice's social security number is 123-45-6789.", "conv": f"{TEN}-s1"},
    {"sess": f"{TEN}-s2", "dt": "2026-10-08T09:01:00", "speaker": "user",
     "text": "Alice was diagnosed with diabetes.", "conv": f"{TEN}-s2"},
    {"sess": f"{TEN}-s3", "dt": "2026-10-08T09:02:00", "speaker": "user",
     "text": "Alice lives in London.", "conv": f"{TEN}-s3"},
    {"sess": f"{TEN}-s4", "dt": "2026-10-08T09:03:00", "speaker": "user",
     "text": "Alice earns $120,000 a year.", "conv": f"{TEN}-s4"},
]
K.observe(TEN, cards)


def evid(c):
    return k2.card_id_of(c["sess"], c["dt"], c["text"])


data = [{"id": evid(c), "content": c["text"], "created_at": c["dt"]} for c in cards]
# C1 泛化query → 敏感全滤, 仅London留
kept, dropped = K.filter_gate(TEN, "Tell me about Alice.", list(data))
check("C1_gen_filters_all_sensitive", dropped == 3 and len(kept) == 1
      and kept[0]["content"] == "Alice lives in London.",
      f"dropped={dropped} kept={[x['content'][:22] for x in kept]}")
# C2 E3守护: 健康query → 健康卡照常返回
kept, dropped = K.filter_gate(TEN, "How is Alice's health?", list(data))
kept_txt = " || ".join(x["content"] for x in kept)
check("C2_e3_health_kept", any("diabetes" in t for t in kept_txt.split(" || ")),
      f"kept={len(kept)} dropped={dropped}")
# C2b 健康query不得带出证件号/财务(混合卡防御面·ANY-release实证: 纯单类卡不跨泄)
check("C2b_health_no_leak_id_fin", "123-45-6789" not in kept_txt and "$120,000" not in kept_txt,
      f"kept_txt={kept_txt[:80]}")
# C3 显式财务query → 财务卡放行(London本就非敏感同留), 证件/健康不回流
kept, dropped = K.filter_gate(TEN, "What is Alice's salary?", list(data))
kept_txt = " || ".join(x["content"] for x in kept)
check("C3_finance_only", any("$120,000" in t for t in kept_txt.split(" || "))
      and "123-45-6789" not in kept_txt and "diabetes" not in kept_txt,
      f"kept={[x['content'][:24] for x in kept]}")
# C4 持久化: 置空单例重开DB → 标注不丢(禁del模块全局 — 会打爆get()的global引用)
k2._K2 = None
K2 = k2.get()
cats = K2.sensitive_cats(TEN, [evid(c) for c in cards])
check("C4_persist_reopen", len(cats) == 3 and
      all(cats.get(evid(c)) for c in cards[:2] + [cards[3]]),
      f"flagged={ {k[:8]: sorted(v) for k, v in cats.items()} }")
# C5 超采补偿计数
check("C5_extra_fetch", K2.extra_fetch(TEN) == 3, f"extra={K2.extra_fetch(TEN)}")

# ── D类: fail-soft ──
K2.observe(TEN, [{"sess": "x", "dt": "", "speaker": "user", "text": 12345, "conv": "x"}])
K2.observe(TEN, [None, {}, {"sess": "", "dt": "", "text": "", "speaker": ""}])
kept, dropped = K2.filter_gate(TEN, "Tell me.", [{"id": "", "content": "x", "created_at": ""}])
check("D_failsoft_dirty_input", kept == [{"id": "", "content": "x", "created_at": ""}] and dropped == 0,
      f"kept={kept} dropped={dropped}")
empty, dropped = K2.filter_gate(TEN, "anything", [])
check("D_empty_data_passthrough", empty == [] and dropped == 0)

n_fail = sum(1 for _, ok, _ in results if not ok)
print(f"\nSELFTEST: {len(results) - n_fail}/{len(results)} PASS, {n_fail} FAIL")
if Path(SCRATCH).exists():
    os.remove(SCRATCH)
sys.exit(0 if n_fail == 0 else 1)
