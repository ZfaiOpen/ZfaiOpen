#!/usr/bin/env python3
"""patch_f1.py — deep_f1跨请求聚合摘要master卡·card_construct三处手术
①import区: "def DT_HONEST" 行前插 ddig import(刻意避开"import deep_fact"行锚=防他席合流冲突)
②derive_all尾部digest块: session digest落库后追加master聚合(append_and_build→prune→store)
③overlay_inject零改动: 仅验证 kind=digest 直通+浮顶+摘要问句词锚在位(master确实走通该路径)
纪律: 每锚点先验count==1, 失配=exit1拒改(不落盘); .bak-f1备份; 纯stdlib; 已打过=幂等exit0。"""
import hashlib
import shutil
import sys
from pathlib import Path

CC = Path(__file__).resolve().parent / "card_construct.py"
MARKER = "deep_f1"
FAILS: list[str] = []

# ── ③验证锚(必须存在且唯一·只验不改: master走通overlay直通+浮顶的路径证据) ──
CHECKS = (
    # 准入: digest问句词直通(summarize/总结类query零词面重叠也放行)
    '            key = a["dia"] if "dia" in a else a["text"][:16]\n'
    '            kind_special = a.get("kind") in ("rule", "digest")',
    # 浮顶: digest与fact/rule同争FLOAT位(插位1-2)
    'if a.get("kind") in ("fact", "rule", "digest") and float_ok and fpos <= fmax:',
    # 问句词表: EN摘要词
    '"summar", "digest", "overview",',
    # 问句词表: CN摘要词+summariz词干
    '"总结", "汇总", "概要", "causal", "becaus", "因为", "由于", "summariz"}',
)

# ── 手术① import区(DT_HONEST前·避开他席的dfact行锚) ─────────────────────────
SUB1 = (
    "def DT_HONEST() -> bool:",
    "import deep_digest as ddig  # deep_f1: 跨请求聚合摘要\n\n\n"
    "def DT_HONEST() -> bool:",
)

# ── 手术② derive_all尾部digest块: session digest之后追加master聚合 ──────────
SUB2_OLD = '''            try:
                seg_src = (rules_all + facts_all)[:12]
                if seg_src:
                    d0 = seg_src[0]
                    digest_text = ("SESSION DIGEST: "
                                   + "; ".join(d["text"] for d in seg_src))[:700]
                    store_derived(tenant, [{
                        "conv": _sidecar(d0.get("conv", "")),
                        "sess": d0.get("sess", ""),
                        "dia": f"{rid}:digest",
                        "dt": d0.get("dt", ""),
                        "speaker": "",
                        "text": digest_text,
                        "kind": "digest",
                    }], "digest")
            except Exception:
                pass'''
SUB2_NEW = '''            try:
                seg_src = (rules_all + facts_all)[:12]
                if seg_src:
                    d0 = seg_src[0]
                    digest_text = ("SESSION DIGEST: "
                                   + "; ".join(d["text"] for d in seg_src))[:700]
                    store_derived(tenant, [{
                        "conv": _sidecar(d0.get("conv", "")),
                        "sess": d0.get("sess", ""),
                        "dia": f"{rid}:digest",
                        "dt": d0.get("dt", ""),
                        "speaker": "",
                        "text": digest_text,
                        "kind": "digest",
                    }], "digest")
                    # deep_f1: 跨请求聚合master卡 — 本请求digest入历史, 聚合最近
                    # ≤12会话(≤900字符·保最新)成KNOWLEDGE DIGEST; 聚合生效(返回
                    # ≠本请求digest)时prune旧master再落新master(dia=rid:master,
                    # kind=digest→overlay直通+浮顶): master覆盖跨请求视野,
                    # session digest保留本请求细节, 两者并存。fail-soft降级=现状。
                    try:
                        master_text = ddig.append_and_build(
                            tenant, digest_text, d0.get("dt", ""))
                        if master_text != digest_text:
                            ddig.prune_master(tenant)
                            store_derived(tenant, [{
                                "conv": _sidecar(d0.get("conv", "")),
                                "sess": d0.get("sess", ""),
                                "dia": f"{rid}:master",
                                "dt": d0.get("dt", ""),
                                "speaker": "",
                                "text": master_text,
                                "kind": "digest",
                            }], "digest")
                    except Exception:
                        pass
            except Exception:
                pass'''

SUBS = (SUB1, (SUB2_OLD, SUB2_NEW))


def main() -> int:
    if not CC.exists():
        print(f"!! 目标不存在: {CC}")
        return 1
    src = CC.read_text()
    if MARKER in src:
        print(f"[SKIP] {CC.name} 已含 {MARKER} 补丁(幂等·不重复落刀)")
        return 0
    cur = src
    for i, (old, new) in enumerate(SUBS, 1):
        n = cur.count(old)
        if n != 1:
            FAILS.append(f"手术锚#{i} 失配/非唯一(count={n}): {old[:60]!r}")
            continue
        cur = cur.replace(old, new, 1)
    for j, c in enumerate(CHECKS, 1):
        n = src.count(c)
        if n != 1:
            FAILS.append(f"验证锚#{j} 失配/非唯一(count={n}): {c[:60]!r}")
    if FAILS:
        for f in FAILS:
            print("  !!", f)
        print("PATCH REFUSED (拒改·原文件未动)")
        return 1
    bak = CC.with_suffix(CC.suffix + ".bak-f1")
    shutil.copy2(CC, bak)
    CC.write_text(cur)
    try:
        compile(CC.read_text(), str(CC), "exec")  # 语法硬门
    except SyntaxError as exc:
        shutil.copy2(bak, CC)
        print(f"  !! 语法校验失败已回滚: {exc}")
        return 1
    h = hashlib.md5(CC.read_bytes()).hexdigest()[:8]
    print(f"[OK] card_construct-f1 -> {h} (bak={bak.name})")
    print("[OK] 手术① ddig import | 手术② master聚合落库 | 手术③ overlay路径锚4/4验证")
    return 0


if __name__ == "__main__":
    sys.exit(main())
