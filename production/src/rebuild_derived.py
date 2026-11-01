"""rebuild_derived.py — PG毒卡清除席·边车派生卡全量重建器(纯stdlib·确定性·fail-soft)
背景: 48.38实战窗实证生产边车DB已有毒卡(RULE假规则/断裂解引卡/废主语事实卡)——
修了deep_fact.py抽取器只治增量, 存量必须全量重建洗掉。

设计(只读organ·边车可写):
  ①清空: derived表全行 + digest_hist全行 + cc._TENANT_CACHE内存缓存全清;
  ②分组: svc.organ._cards 只读遍历, 按 conv 前缀 split(":",1)[0] 分租户;
  ③重建: 每租户 cc_mod.derive_all(cards_copy, rid="rebuild_<tenant[:16]>",
          k3_mod=None, tenant=tenant) — 事实/规则/原子/摘要卡全部用修复版
          抽取器重出(digest随重建); dsup新旧值对撞在批内重建(表已清=无历史锚);
  ④幂等: derive_all的rid决定dia键·落库INSERT OR REPLACE(tenant,cid主键)
          → 重放同结果; cid=sha1(sess|dt|text)确定性。
边界(违者勿动):
  - organ只读(organ._cards遍历, 绝不写organ/绝不finalize);
  - jfacts表不碰(那是JF席管辖区);
  - 重建由主线在服务重启前手动跑(独立脚本模式), 服务运行期勿跑(add相并发写)。

用法:
  A. 独立运行(196部署机): python3 rebuild_derived.py
     — sys.path插/home/zfai/zo_deploy/app后import aml_service(重: 载organ快照
       约60s/2GB内存, 只读), 调rebuild_all并打印统计。
  B. import复用: from rebuild_derived import rebuild_all; rebuild_all(svc, cc_mod)
"""
from __future__ import annotations

from typing import Any

_DEPLOY_APP = "/home/zfai/zo_deploy/app"


def rebuild_all(svc: Any, cc_mod: Any) -> dict[str, int]:
    """边车派生卡全量重建(见模块docstring设计①~④)。返回统计{"tenants","cards","derived"}。
    fail-soft: 任何异常→清库已完成的干净态优先, 返回零统计绝不抛出。"""
    try:
        # ①清空(单事务·原子): 毒存量行全灭; digest_hist不存在=首启→忽略
        with cc_mod._DB_LOCK:
            conn = cc_mod._db()
            conn.execute("DELETE FROM derived")
            try:
                conn.execute("DELETE FROM digest_hist")
            except Exception:  # noqa: BLE001 — 表未建(无digest史)→幂等忽略
                pass
            conn.commit()
        cc_mod._TENANT_CACHE.clear()
        # ②租户分组(organ只读遍历·卡dict浅拷贝防派生层就地写穿透organ)
        groups: dict[str, list[dict[str, Any]]] = {}
        for card in svc.organ._cards.values():
            tenant = str(card.get("conv", "")).split(":", 1)[0]
            if not tenant:
                continue  # 无租户前缀卡=无法归组·诚实跳过
            groups.setdefault(tenant, []).append({
                "conv": card.get("conv", ""), "sess": card.get("sess", ""),
                "dia": card.get("dia", ""), "speaker": card.get("speaker", ""),
                "text": card.get("text", ""), "dt": card.get("dt", ""),
            })
        # ③逐租户重建(键序确定性·digest随derive_all重出)
        n_cards = 0
        for tenant in sorted(groups):
            cards = groups[tenant]
            n_cards += len(cards)
            cc_mod.derive_all([dict(c) for c in cards],
                              rid=f"rebuild_{tenant[:16]}", k3_mod=None, tenant=tenant)
        # ④统计(边车DB行数=落库真相)
        with cc_mod._DB_LOCK:
            n_derived = int(cc_mod._db().execute(
                "SELECT COUNT(*) FROM derived").fetchone()[0])
        return {"tenants": len(groups), "cards": n_cards, "derived": n_derived}
    except Exception as exc:  # noqa: BLE001 — 重建失败不致命(边车可重跑·organ无损)
        print(f"[rebuild_derived] FAILED fail-soft: {type(exc).__name__}: {exc}")
        return {"tenants": 0, "cards": 0, "derived": 0}


if __name__ == "__main__":
    import os
    import sys
    import time
    sys.path.insert(0, _DEPLOY_APP)
    # 重建预算钉高: derive_all缺省每请求48/每卡4上限是add相embed预算门, 全量重建
    # 若沿用会把整租户派生卡截到48张——存量清洗必须全量(此env只影响本进程)。
    os.environ.setdefault("BRAINOS_CC_DERIVED_PER_REQ", "1000000")
    os.environ.setdefault("BRAINOS_CC_DERIVED_PER_MSG", "64")
    t0 = time.time()
    print("[rebuild_derived] importing aml_service (organ snapshot ~60s/2GB, read-only)...")
    import aml_service  # noqa: E402 — 重: 加载organ快照(只读)
    import card_construct as cc_mod  # noqa: E402 — aml_service同源边车引擎
    stats = rebuild_all(aml_service.SVC, cc_mod)
    print(f"[rebuild_derived] done in {time.time() - t0:.1f}s "
          f"tenants={stats['tenants']} cards={stats['cards']} derived={stats['derived']}")
