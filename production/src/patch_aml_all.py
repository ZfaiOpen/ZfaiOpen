#!/usr/bin/env python3
"""patch_aml_all.py — 总装补丁: b366cc6c四刀态 + knife5(S3) + knife6(S7) + knife7(S4) 三刀合一
CAS式: md5核验→精确替换→计数断言; 任何锚点失配=拒改退出1。
用法: patch_aml_all.py   (在app目录内跑)
来源: k5锚=staging_knife5/patch_aml_k5.py逐字 · k6 hunks=shadow_s7 k6_service.py diff逐字 ·
      k7 hunks=shadow_s4 aml_service.py diff逐字。合成顺序: import块 k5→k6→k7;
      startup k4装→k5装→k6回填→k7预初始化; add相 k3观察→k6观察→k7观察;
      search相 k1过滤→k5多跳扩查→k2/k3/k4门→k4重排→k6置顶加成→k7尾部追加→top_k截断。"""
import hashlib
import sys
from pathlib import Path

P = Path(__file__).resolve().parent / "aml_service.py"
EXPECT = "b366cc6cbf687214e2438f4dbfeee668"
raw = P.read_text()
got = hashlib.md5(raw.encode()).hexdigest()
if got != EXPECT:
    print(f"CAS FAIL: md5 {got} != {EXPECT} — 拒改")
    sys.exit(1)

n = 0

# ── 合1: import块 — k4行后挂 k5/k6/k7 三刀注释+import(次序 k5→k6→k7) ─────────────
a1 = "import knife4_streaming as k4  # noqa: E402\n"
b1 = a1 + """
# knife5_1008(部署线第5刀): S3多跳扩查刀 — 官方B2因果链=0最大血洞。影子取证根因:
# ①k1历史意图误报("Why did X..."过去时因果询问命中\\bdid\\b/has+Ved→整窗dt升序→证据卡
#   埋到rank31+·窗召回100%但top10=0) ②关系链二跳事实("A的哥哥是X→X住在Y")单跳词面必漏Y卡。
# 三件套(零LLM·词典knife5_relations.json可增量): R1意图精化(真历史/聚合/时序整体让位·
#   误报才修复=桥卡+二跳新卡前置·k1代码零修改·PK4让位判例对表) + R2实体桥接扩查(query专名
#   →speaker/text桥卡→内容扩查词拼二跳query) + R3合并去重(eid同源·邻插封顶防top10挤位)。
# 触发门=关系链词 或 一跳top1分<阈; 预算硬门BRAINOS_K5_BUDGET_MS=8000(超时降级单跳fail-soft)。
# 回滚 = 还原 pre_knife5_1008/aml_service.py + 摘除 knife5_multihop.py/knife5_relations.json/
#   knife5_probe.py/knife5_selftest.py (S3零状态无DB)。
import knife5_multihop as k5  # noqa: E402

# knife6_1009(部署线S7刀): G2规则卡 + G4经验卡/趋势卡 — 零LLM纯结构化overlay。
# ADD相词典逐句检测(规则句/经验句→一等公民卡: 原句verbatim+主体+对象元数据落盘
# knife6_cards.db; 数值事实正则抽取→同实体同度量≥3点→趋势卡·纯算术) + 启动相存量回填 +
# SEARCH相意图门置顶加成(规则/经验/聚合意图·每类≤4·总计≤6·非意图字节级直通=清单加项
# 即死护栏; k1 history与k3时间档让位; 敏感卡过k2放行门)。四刀路径零修改·纯叠加层。
# 回滚 = 摘除knife6_cards.py/knife6_rules.json + 还原本文件三处hunk(knife6_cards.db可留档)。
import knife6_cards as k6  # noqa: E402

# knife7_1009(S4影子·部署线第7刀): F1抽取式层次摘要刀 — 官方F1(摘要与压缩/长历史全局综合)=16.67
# 死因=长历史散卡无全局综合视图。ADD相: 卡数/实体事实越阈→主题词重叠聚类(零LLM)→digest卡
# (代表句2-3条**原文**+覆盖计数+时间跨度·纯抽取拼装非生成)落盘knife7.db·版本递增旧版superseded
# (复用knife1双时相语义)。SEARCH相: 仅聚合意图query(词典白名单knife7_digest.json·清单加项即死)
# →digest卡尾部追加入结果(只增益不夺位·命中数守卫≤MAX_INJECT); 非聚合query零突变=事实query
# 证据零digest混入。与knife1-4共存叠加(k7在四刀之后·追加在截断前)。
# 回滚 = 还原 pre_knife7_1009/aml_service.py + 摘除 knife7_digest.py/knife7_digest.json
#        (knife7.db可留档)。env总闸=BRAINOS_K7(缺省开·=0关门)。
import knife7_digest as k7  # noqa: E402
"""
assert raw.count(a1) == 1, "anchor1(k4 import) not unique"
raw = raw.replace(a1, b1)
n += 1

# ── 合2: startup — k4 install块后挂 k5 install + k6 backfill + k7 预初始化 ─────────
a2 = """    try:
        _k4_flags = k4.install(SVC, LEDGER, LEDGER_DB)
        log.info("knife4 install: %s", _k4_flags if _k4_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife4 install FAILED err=%s", type(exc).__name__)
"""
b2 = a2 + """    # knife5_1008: S3多跳扩查刀安装(词典装载·无状态零monkey-patch·fail-soft)
    try:
        _k5_flags = k5.install(SVC)
        log.info("knife5 install: %s", _k5_flags if _k5_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife5 install FAILED err=%s", type(exc).__name__)
    # knife6_1009: S7存量卡回填(规则/经验/数值点·幂等OR REPLACE·修前已入库卡亦覆盖); fail-soft
    try:
        b6_cards, b6_sents, b6_ms = k6.backfill(list(SVC.organ._cards.values()))
        log.info("knife6 backfill: cards=%d rule_exp_sents=%d in %.0fms",
                 b6_cards, b6_sents, b6_ms)
    except Exception as exc:
        log.error("knife6 backfill FAILED err=%s", type(exc).__name__)
    # knife7_1009: 摘要刀惰性单例预初始化(可见性·fail-soft)
    try:
        log.info("knife7 init: %s", k7.get().stats())
    except Exception as exc:
        log.error("knife7 init FAILED err=%s", type(exc).__name__)
"""
assert raw.count(a2) == 1, "anchor2(k4 install) not unique"
raw = raw.replace(a2, b2)
n += 1

# ── 合3: add端点 — k3观察后挂 k6观察 + k7观察 ────────────────────────────────────
a3 = """            except Exception as exc:
                log.error("knife3 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
"""
b3 = a3 + """            # knife6_1008·S7: 规则/经验句卡片化+数值事实抽取(落盘knife6_cards.db);
            # fail-soft同knife1/2/3纪律 — 失败只丢S7卡, 绝不影响add成功语义
            try:
                await run_in_threadpool(k6.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife6 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
            # knife7_1009·F1: 摘要代谢观察(会话卡数/实体事实越阈→聚类→digest卡落盘·幂等);
            # fail-soft同knife1/2/3纪律
            try:
                await run_in_threadpool(k7.observe, tenant_of(uid), cards)
            except Exception as exc:
                log.error("knife7 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
"""
assert raw.count(a3) == 1, "anchor3(k3 observe) not unique"
raw = raw.replace(a3, b3)
n += 1

# ── 合4: search端点(k1过滤后·k2门前) — k5多跳扩查合并点(修复分支作用于k1 dt重排后的窗) ──
a4 = """    except Exception as exc:
        log.error("knife1 filter FAILED uid=%s err=%s", uid, type(exc).__name__)
"""
b4 = a4 + """    # knife5_1008·S3: 多跳扩查(触发门+预算硬门·二跳卡并入窗后同过k2/k3/k4门=零旁路)
    try:
        data, out = k5.multihop_expand(SVC, tenant_of(uid), query, data, out, t0)
    except Exception as exc:
        log.error("knife5 multihop FAILED uid=%s err=%s", uid, type(exc).__name__)
"""
assert raw.count(a4) == 1, "anchor4(k1 filter) not unique"
raw = raw.replace(a4, b4)
n += 1

# ── 合5: search端点(k4重排后·top_k截断前) — k6置顶加成 + k7尾部追加 ───────────────
a5 = """    except Exception as exc:
        log.error("knife4 recency FAILED uid=%s err=%s", uid, type(exc).__name__)
"""
b5 = a5 + """    # knife6_1008·S7: 意图门置顶加成(规则/经验/聚合·有限加成·非意图字节级直通=死护栏;
    # k1 history与k3时间档让位; 敏感卡过k2放行门)。fail-soft同前三刀。
    try:
        data, k6_hits = await run_in_threadpool(
            k6.boost, tenant_of(uid), query, data, k1_intent, k3_tier)
        if k6_hits:
            log.info("knife6 boost uid=%s injected=%d kept=%d", uid, k6_hits, len(data))
    except Exception as exc:
        log.error("knife6 boost FAILED uid=%s err=%s", uid, type(exc).__name__)
    # knife7_1009·F1: 聚合意图query(词典白名单)→digest卡追加入结果(只增益不夺位·命中数守卫:
    # 注入≤MAX_INJECT且前缀逐位保留绝不截断真实卡; 非聚合query零突变=证据零digest混入)。
    # fail-soft同前四刀。
    try:
        data, k7_n, k7_before = k7.inject(tenant_of(uid), query, data, top_k)
        if k7_n:
            log.info("knife7 digest uid=%s injected=%d hits %d->%d",
                     uid, k7_n, k7_before, k7_before + k7_n)
    except Exception as exc:
        log.error("knife7 inject FAILED uid=%s err=%s", uid, type(exc).__name__)
"""
assert raw.count(a5) == 1, "anchor5(k4 recency) not unique"
raw = raw.replace(a5, b5)
n += 1

P.write_text(raw)
print(f"patched {n}/5 anchors ok; new md5={hashlib.md5(raw.encode()).hexdigest()}")
