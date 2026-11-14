#!/usr/bin/env python3
"""patch_jf.py — deep_jf四hunk精确锚点补丁(作用于jf/内副本·锚count==1验证·失配exit1拒改)
①card_construct: import cfact ②card_construct: derive_all尾observe钩子(ADD相)
③aml_service: import cfact ④aml_service: search相overlay_inject后jfact注入
锚点说明: ②任务简报预估锚("except/pass/return out")在derive_all尾部非连续
(digest块后实有dsup.commit行) — 已read实证改为真实尾两行组合锚(各count==1)。"""
import hashlib
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
FAILS: list[str] = []


def patch(path: Path, subs: list[tuple[str, str]], tag: str) -> None:
    src = path.read_text()
    bak = path.with_suffix(path.suffix + ".bak-jf")
    if not bak.exists():  # 首跑快照原件·重跑不覆盖(保pristine回滚点)
        shutil.copy2(path, bak)
    cur = src
    for i, (old, new) in enumerate(subs):
        if new in cur:
            continue  # 已应用(new文本已在=重跑幂等·前缀型锚old⊂new防二重施术)
        if old not in cur or cur.count(old) != 1:
            FAILS.append(f"{tag} 锚#{i} 失配/非唯一: {old[:70]!r}")
            return
        cur = cur.replace(old, new, 1)
    if cur == src:
        print(f"[SKIP] {tag} 已是补丁态(幂等)")
        return
    path.write_text(cur)
    h = hashlib.md5(path.read_bytes()).hexdigest()[:8]
    print(f"[OK] {tag} -> {h} (bak={bak.name})")


CC_SUBS = [
    # ① deep_fact行后: import cfact(消费全部函数内延迟import·防循环)
    (
        "import deep_fact as dfact  # deep_v3: 真事实抽取(结构化知识卡EN+CN)\n",
        "import deep_fact as dfact  # deep_v3: 真事实抽取(结构化知识卡EN+CN)\n"
        "import corpus_fact as cfact  # deep_jf: 官方JSON事实方言(Corpus UPDATE)\n",
    ),
    # ② derive_all尾部digest/d1块之后·return out之前: ADD相方言观察钩子
    (
        "            dsup.commit(tenant, d1_collisions, facts_all)\n        return out\n",
        "            dsup.commit(tenant, d1_collisions, facts_all)\n"
        "        # deep_jf: 官方JSON事实方言观察(Corpus/UPDATE卡→jfacts·fail-soft)\n"
        "        try:\n"
        "            cfact.observe(tenant, cards, rid)\n"
        "        except Exception:\n"
        "            pass\n"
        "        return out\n",
    ),
]

AML_SUBS = [
    # ③ import区: knife8_rank行后(指定锚)
    (
        "import knife8_rank as k8  # noqa: E402\n",
        "import knife8_rank as k8  # noqa: E402\n"
        "import corpus_fact as cfact  # noqa: E402\n",
    ),
    # ④ search相: overlay_inject块except之后·STAGE S3之前: jfact注入(同过k2-k7刀闸)
    (
        "    except Exception as exc:\n"
        '        log.error("card_construct overlay FAILED uid=%s err=%s", uid, type(exc).__name__)\n',
        "    except Exception as exc:\n"
        '        log.error("card_construct overlay FAILED uid=%s err=%s", uid, type(exc).__name__)\n'
        "    # deep_jf: 官方JSON事实方言注入(D1新值浮顶/B2二跳链) — overlay之后同过后续刀闸\n"
        "    try:\n"
        "        data, jf_n = cfact.inject(tenant_of(uid), query, data, top_k)\n"
        "        if jf_n:\n"
        '            log.info("corpus_fact inject uid=%s injected=%d kept=%d", uid, jf_n, len(data))\n'
        "    except Exception as exc:\n"
        '        log.error("corpus_fact inject FAILED uid=%s err=%s", uid, type(exc).__name__)\n',
    ),
]

if __name__ == "__main__":
    patch(HERE / "card_construct.py", CC_SUBS, "card_construct-deep_jf")
    patch(HERE / "aml_service.py", AML_SUBS, "aml_service-deep_jf")
    if FAILS:
        for f in FAILS:
            print("  !!", f)
        sys.exit(1)
    print("ALL PATCHES OK")
