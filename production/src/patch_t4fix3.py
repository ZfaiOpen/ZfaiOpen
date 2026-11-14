#!/usr/bin/env python3
"""patch_t4fix3.py — 策略行句级化+机密扫描限表头(治整文档误判为策略行→全条目被禁)
①_policy_lines: 行级→句级(。.!？?!\n切分)·策略动词收紧为显式披露动词
  (不得披露/不予披露/禁止披露/do not disclose/not to disclose·机密是条目标签非策略语句)
②build_map: 条目FORBIDDEN的CONF扫描限表头前20字(官方格式标签在表头"条目3(渠道·机密)"
  ·正文尾部复述的策略句不再污染条目4)"""
import hashlib, shutil, sys
from pathlib import Path
T = Path("/home/zfai/zo_deploy/app/deep_policy.py")
src = T.read_text()
subs = [
    ('''def _policy_lines(text: str) -> list[str]:
    """策略行=同时含披露策略词与定位词(条目N/threat)的行(ptext序·确定性)。"""
    return sorted({ln.strip() for ln in str(text or "").splitlines() if ln.strip() and _POLICY_RE.search(ln) and _LOCATE_RE.search(ln)})''',
     '''_POLICY_VERB_RE = re.compile(r"不得披露|不予披露|禁止披露|do not disclose|not to disclose", re.I)
_POLICY_SENT_SPLIT = re.compile(r"[。.!！？?\\n]+")


def _policy_lines(text: str) -> list[str]:
    """策略行=句级提取(20261010修正): 原行级+机密词命中会把整篇文档误判为策略行
    (文档任何条目带机密标签→全文档=1行→其内全部条目N引用入禁集→全条目被禁)。
    修: 句级切分·策略动词收紧为显式披露语(不得/不予/禁止披露/do not disclose)·
    同时含定位词(条目N/threat)才收。机密/保密/confidential归条目标签扫描(_CONF_RE)。"""
    out = set()
    for ln in str(text or "").splitlines():
        if not ln.strip():
            continue
        for sent in _POLICY_SENT_SPLIT.split(ln):
            s = sent.strip()
            if s and _POLICY_VERB_RE.search(s) and _LOCATE_RE.search(s):
                out.add(s[:120])
    return sorted(out)'''),
    ('''    forb = {n for n, b in entries if _CONF_RE.search(b)}''',
     '''    forb = {n for n, b in entries if _CONF_RE.search(b[:20])}  # t4fix3: 机密标签在表头(官方格式"条目N(·机密)")·扫表头防正文尾部策略句污染'''),
]
bak = T.with_suffix(".py.bak-t4f3")
shutil.copy2(T, bak)
cur = src
for i, (o, n) in enumerate(subs):
    if cur.count(o) != 1:
        print(f"ANCHOR#{i} FAIL count={cur.count(o)}"); sys.exit(1)
    cur = cur.replace(o, n, 1)
T.write_text(cur)
print("[OK] t4fix3 ->", hashlib.md5(T.read_bytes()).hexdigest()[:8])
