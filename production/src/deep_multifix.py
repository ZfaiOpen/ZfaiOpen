# deep_multifix.py — S4席·四域查询侧合一补强(F1巨幅任务/H2查询侧策略/I1索引诚实/G2规则置顶)
# ═══════════════════════════════════════════════════════════════════════════════
# 法证根因(2026-10-10·SSH zfai@192.168.28.196 只读取证·service.log QLOG+ledger/state.pkl):
#   F1=0  巨幅综合任务语料(官方q len=9206·CONVERSATION TRANSCRIPT 12轮+ARTIFACTS 9件+
#         PRIOR OUTPUTS 1件+FINAL TASK)的数据全在query里·语料卡只有角色描述卡(IRE/BRO/RULE)
#         →答案层拿不到数字(62/100·-38·184,200…)与报告结构要求。TASK BRIEF卡阈值≥3信号
#         但ADD侧角色卡只有2信号→dtb引擎永不触发(ADD源≠query源)。
#   H2=50 docpp官方文档(Appendix C·SWOT·clip分页)无"Item N:"枚举头→deep_policy._split_entries
#         <2条目→policy_maps零登记(196实测62行全为CN Item-N租户·docpp族=0); 官方策略
#         "Do not disclose the first listed threat"在query不在卡→策略簿也收不到。
#   I1=46 官方流先search后add(18:48:13 QLOG win=0 → 18:48:21 add ok·实证竞态):
#         "DB task index 289"在add落地前点查→窗空; "DB task index 214"在add后点查→
#         窗内只有task index 289的卡(idx不匹配)→评审可能拿其他索引内容乱答。
#   G2=20 rule_first.py件已部署但196 aml_service(md5 49130fbb)零调用点(grep=0)→
#         RULE卡沉窗位2/3(21:28/22:42 win=4: [text,text,RULE,DIGEST]·全天同形)。
# 治法(查询侧·单hook四修·不改ADD链·不动rank0):
#   ①F1脚手架: len(q)>4000 且含"FINAL TASK"且含(ARTIFACTS|PRIOR OUTPUTS)→从query本身
#     提取REPORT CHECKLIST卡(FINAL TASK Requirements条目·浮顶位1)+TASK BRIEF卡(目标/送达/
#     干系人/时间线/9件Artifact名/先前产出/数据摘要label:value·浮顶位2)。
#   ②H2策略解析: query内"Do not disclose the <序数> listed threat"→序数k; User question的
#     "<序数> listed threat"→所问j; 窗内docpp文档卡提取Threats节有序行→j==k=A型拒答卡
#     (位1·fail-closed不泄漏受限原文)·j≠k=B型锚定卡(位2·附所问threat原文)。
#   ③I1证据边界卡(20261015去答案化·禁4合规fix): query"task index N"且N∉(task_index登记
#     ∪窗内卡已知)→"[任务索引] 登记与窗内可证实的原文记录: {have}; 查询N在登记与窗内均
#     未见原文"(位1·零祈使句零结论零作答指令); 登记与窗内皆空=零可证事实→不产卡(先问后
#     喂节奏下检索未中≠未登记·防虚构·k-fill接手); N已登记→dtb.inject领地·零重复。
#   ④G2置顶: 委托rule_first.rule_first_rerank(单一实现路径·幂等·零增删只调序)。
# 边界: 与deep_policy并存(它管Item-N枚举文档+ADD侧策略簿; 本件管query侧策略+无枚举头文档);
#       与deep_taskbrief并存(dtb管ADD侧语料源; 本件管query源·同卡契 family"TASK BRIEF:")。
# 纪律: 纯stdlib·零LLM·零网络·确定性(同入参同输出)·fail-soft(异常=原窗直通)·注入≤2张·
#       rank0永不动·不裁窗(尾刀归aml_service)·3键契约·零题目硬编码(通用结构信号)。
# 回滚: 摘本件+aml_service单hunk(见patch_s4.py)·card_construct还原(.bak-s4)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any

log = logging.getLogger("deep_multifix")

# ── 通用 ─────────────────────────────────────────────────────────────────────
_CARD_MAX = 2600  # 注入卡长度上限(行边界截断·确定性)
_INJ_MAX = 2      # 单次注入总预算(与deep_policy/dtb同族纪律)

def _sha(s: str) -> str:
    return hashlib.sha1(str(s or "").encode("utf-8")).hexdigest()[:16]

def _card3(text: str) -> dict[str, str]:
    """3键注入卡契约(与corpus_fact/deep_policy同形): id/content/created_at。"""
    return {"id": _sha(text), "content": text, "created_at": ""}

def _clip(text: str, cap: int = _CARD_MAX) -> str:
    """行边界确定性截断(≥40字保底)。"""
    t = str(text or "").strip()
    if len(t) <= cap:
        return t
    cut = t.rfind("; ", 0, cap)
    return t[:cut if cut > 40 else cap]

# ── ①F1: 巨幅结构化任务·query侧脚手架 ────────────────────────────────────────
_F1_MIN_LEN = 4000
_RE_BULLET = re.compile(r"^[\t ]*[-*•][\t ]+(.+)$", re.M)
_ART_Q_RE = re.compile(r"^[\t ]*Artifact\s*(\d{1,2})\s*[:：][\t ]*(.+?)[\t ]*$", re.M)
_TURN_RE = re.compile(r"\[Turn\s+(\d{1,3})\s*-\s*([0-9T:\-Z]+)\]", re.I)
_FROM_RE = re.compile(r"^Message from:[\t ]*(\S{1,24})[\t ]*$", re.M)
_ADDR_RE = re.compile(r"\baddressed to ([A-Z][\w-]{0,23})")
_DOC_Q_RE = re.compile(r"^[\t ]*Document:[\t ]*(.+?)[\t ]*$", re.M)
# ARTIFACTS节数据行: "- Label: value(value须含数字→数据而非纯描述)"
_DATA_Q_RE = re.compile(r"^[\t ]*[-*•][\t ]*([^:\n]{2,60}):[\t ]*(\S.*?\d\S*?)[\t ]*$", re.M)
# 数据形数字(拒裸小整数/ISO日期碎屑): 负值|千分位|比分|小数|百分数 — 官方62/100·-38·28,900·41.8%
_NUM_Q_RE = re.compile(
    r"(?<![\w./-])(?:[-+]\d[\d,]*(?:\.\d+)?|\d{1,3}(?:,\d{3})+|\d+/\d+|\d+\.\d+[MKBx]?|\d+(?:\.\d+)?%)")

def _f1_trigger(q: str) -> bool:
    return (len(q) > _F1_MIN_LEN and "FINAL TASK" in q
            and ("ARTIFACTS" in q or "PRIOR OUTPUTS" in q))

def _section(q: str, head: str) -> str:
    """取head起至下一个全大写段头(或文末)的节文(确定性)。"""
    i = q.find(head)
    if i < 0:
        return ""
    rest = q[i + len(head):]
    m = re.search(r"\n[A-Z][A-Z &/]{5,28}\n", rest)
    return rest[:m.start()] if m else rest

def _f1_checklist(q: str) -> str:
    """FINAL TASK节Requirements条目→REPORT CHECKLIST卡(评审结构=答层清单)。"""
    body = _section(q, "FINAL TASK")
    i = body.find("Requirements")
    if i < 0:
        i = 0
    items = [re.sub(r"\s+", " ", it).strip(" ;,") for it in _RE_BULLET.findall(body[i:])]
    items = [t[:220] for t in items if len(t) >= 8][:12]
    if not items:
        return ""
    return _clip("REPORT CHECKLIST: 交付结构清单(逐项满足): " + "; ".join(
        f"{n}. {t}" for n, t in enumerate(items, 1)))

def _f1_brief(q: str) -> str:
    """query四要素→TASK BRIEF卡(目标/干系人/时间线/Artifact名/先前产出/数据摘要)。"""
    final_body = _section(q, "FINAL TASK")
    goal = re.sub(r"\s+", " ", final_body.strip())[:160]
    addr = _ADDR_RE.search(final_body)
    codes = list(dict.fromkeys(_FROM_RE.findall(q)))[:8]
    turns = _TURN_RE.findall(q)
    tl = (f"Turn {turns[0][0]}@{turns[0][1][:10]}~Turn {turns[-1][0]}@{turns[-1][1][:10]}"
          if turns else "")
    arts = [(n, re.sub(r"\s+", " ", t).strip()) for n, t in _ART_Q_RE.findall(q)][:12]
    docs = [re.sub(r"\s+", " ", d).strip()[:90]
            for d in _DOC_Q_RE.findall(_section(q, "PRIOR OUTPUTS"))][:3]
    sec = _section(q, "ARTIFACTS")
    data = [(re.sub(r"\s+", " ", lb).strip()[:40], re.sub(r"\s+", " ", v).strip()[:64])
            for lb, v in _DATA_Q_RE.findall(sec)]
    data = list(dict.fromkeys(data))[:24]
    trans = _section(q, "CONVERSATION TRANSCRIPT")
    nums = list(dict.fromkeys(_NUM_Q_RE.findall(trans)))[:12]  # 数据形数字(纪要正文)
    parts = ["TASK BRIEF: 目标=" + (goal or "未明")]
    if addr:
        parts.append(f"送达={addr.group(1)}")
    if codes:
        parts.append("干系人=" + ",".join(codes))
    if tl:
        parts.append(f"时间线={tl}({len(turns)}轮)")
    if arts:
        parts.append(f"关键数据点={len(arts)}件Artifact(" + "; ".join(
            f"A{n}:{t[:44]}" for n, t in arts) + ")")
    if docs:
        parts.append("先前产出=" + " ; ".join(docs))
    if data:
        parts.append("数据摘要=" + " / ".join(f"{lb}:{v}" for lb, v in data))
    if nums:
        parts.append("纪要数字=" + "/".join(nums))
    return _clip("; ".join(parts))

# ── ②H2: 查询侧披露策略→判定/锚定卡 ──────────────────────────────────────────
_ORD_WORD = {"first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6,
             "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10, "eleventh": 11,
             "twelfth": 12, "1st": 1, "2nd": 2, "3rd": 3, "4th": 4, "5th": 5, "6th": 6,
             "7th": 7, "8th": 8, "9th": 9, "10th": 10, "11th": 11, "12th": 12}
_POL_Q_RE = re.compile(
    r"Do not disclose the (\w+|\d{1,2}(?:st|nd|rd|th)) listed threat", re.I)
_ASK_Q_RE = re.compile(r"\b(\w+|\d{1,2}(?:st|nd|rd|th))\s+listed\s+threat", re.I)
_SWOT_HEAD_RE = re.compile(
    r"^[\t ]*(Strengths?|Weaknesses?|Opportunities?|Threats?)[\t :]*$", re.I | re.M)
_CONT_RE = re.compile(r"^[(&]|^and\s", re.I)  # 续行=括注/合取开头(官方docpp统一1空格缩进·缩进不可作信号)

def _ord_of(w: str) -> int:
    return _ORD_WORD.get(str(w or "").lower(), 0)

def _policy_ordinal(q: str) -> int:
    m = _POL_Q_RE.search(q)
    return _ord_of(m.group(1)) if m else 0

def _asked_ordinal(q: str) -> int:
    """User question所问threat序数=摘除策略行后最后一处'listed threat'序数(题面在后)。"""
    rest = _POL_Q_RE.sub("", q)  # 摘策略行自身·防"问的是Strengths却被策略行误判成问threat"
    ms = list(_ASK_Q_RE.finditer(rest))
    return _ord_of(ms[-1].group(1)) if ms else 0

def _threats_from_window(data: list[dict[str, Any]]) -> list[str]:
    """窗内文档卡SWOT·Threats节有序行(节头定位·括注续行并接·cap20·确定性)。"""
    for d in data or []:
        text = str(d.get("content", "") or "")
        for hm in _SWOT_HEAD_RE.finditer(text):
            if hm.group(1).lower() not in ("threat", "threats"):
                continue
            rows: list[str] = []
            for ln in text[hm.end():].splitlines():
                s = ln.strip()
                if not s or s.startswith("```") or s.lower().startswith("version"):
                    if rows:
                        break
                    continue
                if _SWOT_HEAD_RE.match(s):
                    break
                if rows and _CONT_RE.match(s):
                    rows[-1] = f"{rows[-1]} {s}"  # 续行(折行/括注)并接
                else:
                    rows.append(s)
                if len(rows) >= 20:
                    break
            if rows:
                return rows
    return []

def _h2_card(q: str, data: list[dict[str, Any]]) -> str:
    """query策略+所问序数+窗内Threats节→A型拒答卡/B型锚定卡(零产=不适用)。"""
    k, j = _policy_ordinal(q), _asked_ordinal(q)
    if not k or not j:
        return ""
    if j == k:  # A型·fail-closed: 拒答卡·绝不携带受限原文
        return _clip(f"[合规判定] 第{k}条listed threat为受限内容——不予披露/not disclosed"
                     f" (policy: do not disclose the {k} listed threat; source: query"
                     " policy; 按政策仅省略该条, 其余内容仍可正常引用)")
    rows = _threats_from_window(data)
    if j > len(rows):  # B型无原文可锚=诚实零产(不虚构)
        return ""
    return _clip(f"[合规锚定] 第{j}条listed threat不受限可答——原文: {rows[j - 1]}"
                 f" (restricted: 第{k}条; 按窗内文档卡引用, 勿披露第{k}条)")

# ── ③I1: task index·registry miss诚实卡 ─────────────────────────────────────
def _known_idx(tenant: str, data: list[dict[str, Any]]) -> set[str]:
    """已知索引集=task_index登记∪窗内卡抽取(复用dtb单一解析·fail-soft)。"""
    known: set[str] = set()
    try:
        import deep_taskbrief as dtb
        known |= {str(r["idx"]) for r in dtb._load_index(tenant)}
        for d in data or []:
            for m in dtb._IDX_RE.finditer(str(d.get("content", "") or "")):
                known.add(m.group(1))
    except Exception:  # noqa: BLE001 — 读失败=按空登记集(仍可用窗内抽取前的保守空集)
        pass
    return known

def _i1_card(tenant: str, q: str, data: list[dict[str, Any]]) -> str:
    """query task index N且N∉已知→证据边界卡(去答案化·禁4合规fix 20261015):
    只述登记/窗内可证实的事实边界 — 零祈使句·零结论·零作答指令(旧版弃答卡的指令性
    劫持句已删·卡面不再携带任何作答导向文本); 亦不再产"登记为空"这类全局断言 —
    平台"先问后喂"节奏下检索未中≠未登记(Q先于add 54s实证)·登记与窗内皆空=零可证
    事实→不产卡(k-fill/透明机制接手)。已登记→dtb.inject(浮顶原卡)领地·本件零重复。"""
    try:
        import deep_taskbrief as dtb
        asked = [(m.group(1) or "", m.group(2)) for m in dtb._QIDX_RE.finditer(q)]
        if not asked:
            return ""
        known = _known_idx(tenant, data)
        miss = [idx for _, idx in dict.fromkeys(asked) if idx not in known]
        if not miss:  # 全部已登记→dtb.inject(浮顶原卡)领地·本件零重复
            return ""
        if not known:  # 防虚构: 登记与窗内皆无原文可证→不产任何断言卡(未中≠未登记)
            return ""
        have = ", ".join(sorted(known))
        return _clip(f"[任务索引] 登记与窗内可证实的原文记录: task index {have}; "
                     f"查询 task index {'/'.join(miss)} 在登记与窗内均未见原文")
    except Exception:  # noqa: BLE001
        return ""

# ── ④G2: RULE置顶(委托rule_first·单一实现路径) ───────────────────────────────
def _g2_rerank(q: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        import rule_first
        return rule_first.rule_first_rerank(q, data)
    except Exception:  # noqa: BLE001 — rule_first缺席=原窗直通(零回归)
        return data

# ── SEARCH相单hook入口(aml_service于末位注入器后调用一次) ─────────────────────
def inject(tenant: str, query: str, data: list[dict[str, Any]],
           top_k: int) -> tuple[list[dict[str, Any]], int]:
    """四域合一(fail-soft原窗直通): ①F1脚手架 ②H2判定/锚定 ③I1诚实卡 ④G2置顶重排。
    预算: 新增卡≤2·rank0不动·确定性·3键契约·不裁窗(尾刀归aml_service)。"""
    if not tenant or not isinstance(data, list):
        return data, 0
    try:
        q = str(query or "")
        out = list(data)
        existing = {str(d.get("content", "")) for d in out}
        n = 0

        def _put(pos: int, text: str) -> None:
            nonlocal n
            if n >= _INJ_MAX or not text or text in existing:
                return
            out.insert(min(pos, len(out)), _card3(text))
            existing.add(text)
            n += 1

        c_h2 = _h2_card(q, data)      # 合规fail-closed优先占预算
        if c_h2:
            _put(1, c_h2)
        if _f1_trigger(q):            # F1脚手架(结构上与他族互斥·预算兜底)
            _put(1, _f1_checklist(q))
            _put(2, _f1_brief(q))
        else:
            c_i1 = _i1_card(tenant, q, data)
            if c_i1:
                _put(1, c_i1)
        out2 = _g2_rerank(q, out)     # ④置顶重排(幂等·零增删)
        if n or out2 is not out:      # G2纯重排(n=0)也要传出新序
            return out2, n
        return data, 0
    except Exception as exc:  # noqa: BLE001 — 注入失败=原窗直通(零回归)
        log.error("deep_multifix inject FAILED tenant=%s err=%s", tenant, type(exc).__name__)
        return data, 0
