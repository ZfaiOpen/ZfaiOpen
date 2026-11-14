#!/usr/bin/env python3
"""patch_d1.py — D1新值覆盖维度·card_construct四处手术(锚点count==1·失配拒改)
①import: 接线deep_supersede
②derive_all: store_derived(fact)前dsup.prepare / rules+digest store后dsup.commit
③load_derived: SELECT加superseded_by列(out dict同键) — 缓存失效由dsup.commit负责
④overlay_inject: current/history意图检测(EN+CN) + superseded卡
   current→skip零注入 / history→正常注入浮顶放行 / 默认→可尾注禁浮顶"""
import hashlib
import shutil
import sys
from pathlib import Path

D1 = Path(__file__).resolve().parent
CC = D1 / "card_construct.py"
FAILS: list[str] = []


def patch(path: Path, subs: list[tuple[str, str]], tag: str) -> None:
    src = path.read_text()
    bak = path.with_suffix(path.suffix + ".bak-d1")
    shutil.copy2(path, bak)
    cur = src
    for i, (old, new) in enumerate(subs):
        if old not in cur or cur.count(old) != 1:
            FAILS.append(f"{tag} 锚#{i} 失配/非唯一(count={cur.count(old)}): {old[:70]!r}")
            return
        cur = cur.replace(old, new, 1)
    path.write_text(cur)
    h = hashlib.md5(path.read_bytes()).hexdigest()[:8]
    print(f"[OK] {tag} -> {h} (bak={bak.name})")


CC_SUBS = [
    # ① import区: deep_supersede接线
    (
        """import deep_fact as dfact  # deep_v3: 真事实抽取(结构化知识卡EN+CN)""",
        """import deep_fact as dfact  # deep_v3: 真事实抽取(结构化知识卡EN+CN)
import deep_supersede as dsup  # deep_d1: 新旧值对撞(superseded_by)""",
    ),
    # ②a derive_all: store_derived(fact)前 — prepare(改text缀出处·回传对撞清单)
    (
        """            store_derived(tenant, notes_all, "note")
            store_derived(tenant, atoms_all, "atom")
            store_derived(tenant, facts_all, "fact")""",
        """            store_derived(tenant, notes_all, "note")
            store_derived(tenant, atoms_all, "atom")
            # deep_d1: store前置备 — 批内+跨请求新旧值对撞(改text缀出处·算最终cid)
            d1_collisions = dsup.prepare(tenant, facts_all)
            store_derived(tenant, facts_all, "fact")""",
    ),
    # ②b derive_all: rules/digest store之后 — commit(旧行superseded_by·新卡key·缓存失效)
    (
        """            except Exception:
                pass
        return out""",
        """            except Exception:
                pass
            # deep_d1: 对撞落库 — 旧行superseded_by·新卡fact_key/fact_obj·租户缓存失效
            dsup.commit(tenant, d1_collisions, facts_all)
        return out""",
    ),
    # ③ load_derived: superseded_by列(空列读NULL→falsy·旧行为零变化)
    (
        """            rows = _db().execute(
                "SELECT sess, dt, speaker, text, kind FROM derived WHERE tenant=? "
                "ORDER BY seq", (tenant,)).fetchall()
        out = [{"sess": r[0], "dt": r[1], "speaker": r[2], "text": r[3], "kind": r[4]}
               for r in rows]""",
        """            rows = _db().execute(
                "SELECT sess, dt, speaker, text, kind, superseded_by FROM derived WHERE tenant=? "
                "ORDER BY seq", (tenant,)).fetchall()
        out = [{"sess": r[0], "dt": r[1], "speaker": r[2], "text": r[3], "kind": r[4],
                "superseded_by": r[5]}
               for r in rows]""",
    ),
    # ④a 意图检测regex(current=问现在值/history=问过往·EN+CN)
    (
        """    cjk = re.findall(r"[\\u4e00-\\u9fff]", t)
    out.update(a + b for a, b in zip(cjk, cjk[1:]))  # 中文二元
    return out - _STOP


def overlay_inject(""",
        """    cjk = re.findall(r"[\\u4e00-\\u9fff]", t)
    out.update(a + b for a, b in zip(cjk, cjk[1:]))  # 中文二元
    return out - _STOP


# deep_d1: 意图检测 — current(现在值)/history(过往)决定superseded卡skip与浮顶
_D1_CURRENT = re.compile(r"\\b(still|currently|now|these days|nowadays|today)\\b", re.I)
_D1_HISTORY = re.compile(r"\\b(ever|used to|before|in the past|history)\\b", re.I)
_D1_CURRENT_CN = re.compile(r"(现在|如今|目前|现在还)")
_D1_HISTORY_CN = re.compile(r"(曾经|以前|之前|过去)")


def overlay_inject(""",
    ),
    # ④b overlay_inject: 意图布尔计算(qt空早退后·循环前)
    (
        """        qt = _tokens(query)
        if not qt:
            return data, 0
        max_tok = _cap("OVERLAY_MAX", 3)""",
        """        qt = _tokens(query)
        if not qt:
            return data, 0
        # deep_d1: current/history意图(EN+CN) — history优先(问过去→过期卡照常出席)
        q_cur = bool(_D1_CURRENT.search(query)) or bool(_D1_CURRENT_CN.search(query))
        q_hist = bool(_D1_HISTORY.search(query)) or bool(_D1_HISTORY_CN.search(query))
        max_tok = _cap("OVERLAY_MAX", 3)""",
    ),
    # ④c 原子循环顶: superseded卡current意图→直接skip(新值生效旧值退场)
    (
        """        for a in atoms:
            if a["text"] in existing:
                continue  # 已在窗(organ命中)零重复""",
        """        for a in atoms:
            if a["text"] in existing:
                continue  # 已在窗(organ命中)零重复
            if a.get("superseded_by") and q_cur and not q_hist:
                continue  # deep_d1: current意图→过期旧值卡零注入(防评审读到旧值)""",
    ),
    # ④d 浮顶闸: superseded卡默认禁浮顶(history意图除外·可尾注)
    (
        """            if a.get("kind") in ("fact", "rule", "digest") and float_ok and fpos <= fmax:""",
        """            if (a.get("kind") in ("fact", "rule", "digest") and float_ok
                    and fpos <= fmax and (not a.get("superseded_by") or q_hist)):""",
    ),
    # ⑤ store_derived显式列名(必修): ADD COLUMN三列后裸VALUES(9值)对12列
    #    =OperationalError全量落库失败 — 显式列名后新列默认NULL·基线行为逐字节不变
    (
        """                conn.execute("INSERT OR REPLACE INTO derived VALUES(?,?,?,?,?,?,?,?,?)",""",
        """                # deep_d1: 显式列名 — ADD COLUMN后裸9值位置插入失配(12列)·必修
                conn.execute("INSERT OR REPLACE INTO derived"
                             "(tenant,cid,sess,dia,dt,speaker,text,kind,seq)"
                             " VALUES(?,?,?,?,?,?,?,?,?)",""",
    ),
]

if __name__ == "__main__":
    patch(CC, CC_SUBS, "card_construct-d1")
    if FAILS:
        for f in FAILS:
            print("  !!", f)
        sys.exit(1)
    print("ALL PATCHES OK")
