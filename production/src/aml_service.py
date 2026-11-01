# /home/zfai/zo_deploy/app/aml_service.py — BRAIOS AML线协议薄适配层(部署件·非冻结核心)
# ═══════════════════════════════════════════════════════════════════════════════
# 职责(只加不改·零冻结核心修改):
#   评测平台 AML线协议 (agentmemoryleaderboard Add/Search API) → 进程内调用
#   ~/exam/memory_service.py 的 MemoryService(HippocampalRecall检索器官+PersistentRecall持久化)。
#
# 契约(官方API指南+规则逐字对表, 2026-10-06 补丁十条版):
#   POST /add  {"request_id","messages":[{role,timestamp,content(str|[part...])}],"user_id","session_id"}
#              → 200 {"success":true,"request_id","user_id","session_id"} 三ID逐字节回显;
#              持久化完成才回200; request_id幂等(重放同id返回同结果不重复入库);
#              metadata/app_id/agent_id/async_mode 等额外字段一概忽略(发了就当没有)。
#   POST /search {"query","user_id","top_k"(, "options":[...])} → 200 {"data":[{"id","content","created_at"}]}
#              id非空; 返回数 ≤ top_k 严格; 空结果 []; options 容错忽略。
#   GET  /health 免鉴权 2xx。
#   认证: Authorization: Token <KEY> (/add /search 必带; KEY 落盘 zo_deploy/app/.api_key 0600)。
#   失败诚实: 内部错误 5xx, 绝不伪装 200。(平台重试 408/409/425/429/500/502/503/504/524)
#   多模态降级(诚实): image_url 部件→只存 "[图片]" 标记占位, 不识图; 未知type部件跳过。
#
# 持久化双层(均落盘 zo_deploy/data):
#   ① ledger.db  (sqlite WAL+synchronous=NORMAL, 实测commit 0.039ms/op — 本机默认journal
#      模式fsync=538ms/op为WSL2平台病, 故必须WAL) — request_id 幂等账本+启动重放源。
#   ② snapshot/  (PersistentRecall 全量快照, 后台60s周期+停机时) — 启动直恢复。
#   启动恢复 = MemoryService载snapshot + 重放 ledger 中 created_ms > last_snapshot_ms 的行
#   (卡片键 dia="{request_id}:{i}" 确定性 → 重放覆写同键=幂等)。
#
# 合规: 评测内容(query/text)零采集零落盘日志 — 日志只记 request_id/user_id/session_id/
#       状态码/耗时/卡数; KEY 不进日志; 快照属服务运行必需态(30天内可清理, 见DEPLOY.md)。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import asyncio
import collections
import hashlib
import re
import hmac
import json
import sys
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np  # [B-Z boot加速20261019] emb缓存落盘用(venv既有依赖·零新增)
from collections import deque  # [B-Z吞吐刀20261019] 边刀后置队列

# ── 环境隔离(必须在 import memory_service 之前) ─────────────────────────────
os.environ["HF_HUB_OFFLINE"] = "1"       # 实测: ST加载时model_info()出网可卡死90s+; 模型已在本地缓存
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("EXAM_USE_EMBED", "1")   # 三通道RRF含语义臂(本地MiniLM, 离线)
_DEPLOY = Path(__file__).resolve().parent.parent          # /home/zfai/zo_deploy
os.environ.setdefault("BRAINOS_TKG", "0")              # organ v2.6 TKG门: 其sqlite连接线程不安全, 16并发finalize每次抛ProgrammingError+刷栈(实测63MB日志/10min)且fail-soft零贡献 → 关门; 单线程离线评测可自行开
os.environ.setdefault("BRAINOS_LANCEDB", "0")          # organ v2.5 ANN关门: >3000卡每次finalize触发LanceDB同步(实测首建256s) → 暴力点积(评测规模更快)
os.environ.setdefault("BRAINOS_ANN_THRESHOLD", "1000000")  # 双保险: 阈值门抬到百万卡
os.environ["BRAINOS_SNAPSHOT"] = str(_DEPLOY / "data" / "snapshot")
os.environ["BRAINOS_EMB_CACHE"] = str(_DEPLOY / "data" / "emb_cache.npz")  # 隔离~/exam默认缓存

sys_path_brainos = os.environ.get("BRAINOS_HOME", str(Path.home() / "brainos_new"))
sys_path_exam = os.environ.get("EXAM_HOME", str(Path.home() / "exam"))
for _p in (sys_path_brainos, sys_path_exam):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── [fix_f 20261025·重启日期注记累加修复·幂等守卫] 必须在 import memory_service 之前安装 ──
# (时序根因: memory_service.py:312 "SVC = MemoryService()" 在模块import时即构造并载快照
#  → restore→organ.ingest_cards 在本行 import 时就跑; 补丁装在其后=拦不到restore路径。)
# 病灶(8099复现·轨道E集成发现同源): organ卡text由冻结核ingest统一追加"[= 日期]"锚
# (BRAINOS_DATE_ANCHOR=1·hippocampal_recall.ingest_cards "text=x+date_anchor(...)"),
# 而快照(recall_store._dump)存的是已注记text; 重启restore→recall_store._restore→
# organ.ingest_cards对已注记text重跑date_anchor→注记内星期名再点火→每重启整组重注
# (8099实测6卡: 13→28→43组·每轮+15·卡id=sha1(sess|dt|text)随之逐轮漂移)。
# 修法(零冻结核改动·类级包装): ingest前剥除text尾部既有锚注记组→冻结核对本轮text
# 统一重注一次。首add零变化(text无尾注记→剥除no-op); 重启restore=剥旧注→同dt重注→
# 与首轮逐字节同形(幂等不动点); 已污染多组卡一轮重启自愈回首轮规范形(卡id回正)。
# env总闸BRAINOS_ANCHOR_IDEM(缺省开·=0关门回基线); 写侧门BRAINOS_DATE_ANCHOR=0时
# 包装直通(注记writer本身零开火·零剥离·双重对表)。
from brainos.memory.hippocampal_recall import HippocampalRecall as _HR_fix  # noqa: E402

_ANCHOR_GRP_RE = re.compile(
    r"\s*\[= (?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
    r" \d{1,2} (?:January|February|March|April|May|June|July|August|September"
    r"|October|November|December) \d{4}\]$")

def _strip_anchor_tail(text: str) -> str:
    """剥除text尾部连续的date_anchor注记组("[= Tuesday 20 October 2026]"...)。
    只动尾部(run逐组剥)·中部同形文本零触碰; 无匹配原样返回(fail-soft零破坏)。"""
    prev = None
    while prev != text:
        prev = text
        text = _ANCHOR_GRP_RE.sub("", text)
    return text

def _ingest_cards_anchor_idem(self, cards):
    if (os.environ.get("BRAINOS_ANCHOR_IDEM", "1") != "0"
            and os.environ.get("BRAINOS_DATE_ANCHOR", "0") != "0"):
        try:
            for c in cards or []:
                if isinstance(c, dict) and c.get("text"):
                    s = _strip_anchor_tail(str(c["text"]))
                    if s != c["text"]:
                        c["text"] = s
        except Exception:
            pass  # fail-soft: 剥除失败按原text直通(=基线行为·绝不影响add语义)
    return _HR_fix_ingest_orig(self, cards)

if not getattr(_HR_fix.ingest_cards, "_anchor_idem", False):
    _HR_fix_ingest_orig = _HR_fix.ingest_cards
    _HR_fix.ingest_cards = _ingest_cards_anchor_idem
    _ingest_cards_anchor_idem._anchor_idem = True

import memory_service as ms  # noqa: E402  (exam队服务件, 进程内只读复用)

ms.TOP_K_MAX = 256  # AML top_k 上限100; 服务件默认cap64会静默截断 — 模块级常量运行时可抬(读全局)

# knife1_1008(部署线第1刀): D1双时相新值覆盖 + D2矛盾检测 — 纯overlay挂件, 冻结核心/organ/exam考场件零修改。
# ADD相观察(supersede/conflict落盘knife1.db) + SEARCH相当前态排除superseded·时间线全量+dt序(C3禁回退)。
# 回滚 = 还原 pre_knife1_1008/aml_service.py + 摘除 knife1_supersede.py/knife1_predicates.json (knife1.db可留档)。
import knife1_supersede as k1  # noqa: E402

# knife2_1008(部署线第2刀): H2隐私闸 — ADD相敏感标注(词典+模式落盘knife2.db) +
# SEARCH相最小披露放行门(泛化query滤敏感卡·显式域query放行[E3健康关怀守护])。
# 选型=滤除非脱敏(服务契约"只回证据原文"禁改写+滤除保证严格更强), 配超采补偿维持top_k填充。
# 回滚 = 还原 pre_knife2_1008/aml_service.py + 摘除 knife2_sensitive.py/knife2_sensitive.json (knife2.db可留档)。
import knife2_sensitive as k2  # noqa: E402

# knife3_1008(部署线第3刀): C1时间锚定 — ADD相相对/绝对时间归一(以消息timestamp为锚·自写轻量
# 解析器·零新增依赖)落盘knife3_time.db时间索引(卡上双存: 归一化ISO日期+原文措辞+可选区间) +
# 启动相存量全量回填(修前已入库卡亦覆盖) + SEARCH相时间query检测(duration/when/order·中英):
# 归一化日期注记(metadata附注·不改写content原文)·when档命中卡浮前排(钉原top3)·
# duration档同会话双日期卡必回+日期差注记(粒度对表: 问天给天数/问月给月数)·
# order档会话内时间位次标注(C2顺序红利)。k1 history意图只注记零重排(C3时间线资产禁打架)。
# 注记只附metadata不进文本索引 — 与冻结核BRAINOS_DATE_ANCHOR(缺省关·DA4锚入卡回归形态)零重叠。
# 回滚 = 还原 pre_knife3_1008/aml_service.py + 摘除 knife3_time.py/knife3_time_terms.json
#        (knife3_time.db可留档)。
import knife3_time as k3  # noqa: E402

# knife4_1008(部署线第4刀): I1流式延迟刀 — 官方低分格最大死因=队列谋杀(15路search同毫秒
# 涌入→全局RLock串行→队尾68.7s·超30s的12查全落低分域)。M1a encode-memo(每search逐conv
# 重复encode塌缩为1) + M1b 单趟租户取窗(小租户双趟全库重扫→单趟·语义逐位等价) + RW读写分离
# (overlay层读共享写独占·写优先) + M2 带内recency决胜(ledger created_ms·相邻分差<5%带内
# 只换序不改集·k1时间线意图让位)。纯overlay: 冻结核心/organ/exam件零修改。
# 回滚 = 还原 pre_knife4_1008/aml_service.py + 摘除 knife4_streaming.py/knife4_probe.py/
# knife4_selftest.py。env总闸=BRAINOS_K4(缺省开·=0关门回旧行为)。
import knife4_streaming as k4  # noqa: E402

# knife5_1008(部署线第5刀): S3多跳扩查刀 — 官方B2因果链=0最大血洞。影子取证根因:
# ①k1历史意图误报("Why did X..."过去时因果询问命中\bdid\b/has+Ved→整窗dt升序→证据卡
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

# knife11_forget(部署线第11刀·D3删除遗忘抑制·沙箱原型20261022·仅deploy_sb·零考题内容):
# L1删除指令识别(纯正则三槽)→L2 tombstone软删标记(镜像匹配·organ卡本体不动)→L3检索过滤
# (非审计整卡摘除/审计豁免+[DeletionRecord]注入)→L4遗忘衰减接口。四层与词典见
# knife11_forget.py模块头。挂接4处: 本import + _knives_process的k11.observe +
# /search超采k11.extra_fetch + k2闸后filter_gate。env总闸BRAINOS_K11(缺省开·=0全直通)。
# 回滚=摘4处挂接+删本件(knife11.db可留档)。
import knife11_forget as k11  # noqa: E402
import sys as _sys; print("[boot] knife7 module file=%s" % k7.__file__, file=_sys.stderr, flush=True)  # B-E-1修20261017: 原log.info早于log定义(271行)致启动即崩NameError·改stderr直出保调试意图

# knife13_repo(部署线第13刀·M1代码指纹层·20261025轨道B): ADD相逐卡抽代码指纹(路径/符号/
# 错误签名/测试名/构建命令/issue引用)→sqlite倒排 + repo社区聚类(路径前缀种子+稀有符号·
# 防numpy/pandas同域异库误伤·跨社区软降权×0.4绝不硬删) + SEARCH相query同款指纹→重叠度
# 硬加权(路径3.0>errsig2.5>测试1.5>符号1.0>命令0.5)。零LLM零外部API。
# 挂接3处: 本import + _knives_process的k13.observe + search相erank后加权块。
# 回滚=摘3处挂接+删本件(knife13_repo.db可留档)。env总闸BRAINOS_K13(缺省0=关)。
import knife13_repo as k13  # noqa: E402
# citeref(部署线引用图谱刀·M2·20261025轨道B): ADD相建 卡↔#issue/PR引用号 双向边邻接表 +
# SEARCH相query抽引用号→1跳×2.0/2跳×1.4(模拟出题方递归扩展·k13社区联合校验防跨库撞号)。
# 挂接3处: 本import + _knives_process的crf.observe + search相erank后与k13同块加权。
# 回滚=摘3处挂接+删本件(citeref.db可留档)。env总闸BRAINOS_CITEREF(缺省0=关)。
import citeref as crf  # noqa: E402
# coding_window(轨道E·M5编程方言窗口编排器·20261025): query侧方言检测(指纹+代码标记
# 打分≥阈)→编程模式=英雄卡置顶(expd.hero_cards)+自适应窗(字符预算/卡数钳制)+文本刀
# 注入抑制(k6 pattern/k12 struct/k7 digest); 非编程query=apply()原样直通(零改动路径)。
# 挂接点: 本import + /search k13b后_qa_window_trim前apply() + 三处注入守卫(_cw_coding)。
# 回滚=摘挂接+删本件(无独立DB)。env总闸BRAINOS_CODING_WINDOW(缺省0=关)。
import coding_window as cw  # noqa: E402
# deep_expdistill(轨道C·M3经验蒸馏): FIX/ANTI/COMPR英雄卡(纯规则零LLM·终态判定防中途假pass)。
# 挂接: add相observe_session_end(build_cards后) + 编程模式英雄卡经coding_window.apply统一
# 入窗(不设独立search hunk)。env门BRAINOS_EXP_DISTILL(缺省0=关)。
import deep_expdistill as expd  # noqa: E402
# llm_ensemble(轨道I·M8检索期LLM集成器·20261025): expand(query→2变体检索式·k13直召回∪
# _tenant_fetch语义→RRF融合) + hyde(假想记忆卡→语义近邻第三榜·仅描述型query触发) + filter
# (终窗前12卡LLM精滤·keep∈[3,12]护栏越界全保留)。三独立子开关(BRAINOS_ENS_EXPAND/HYDE/
# FILTER)+总门BRAINOS_LLM_ENS(缺省0=关·关=字节直通零外部调用)。合规硬门: 模型名非
# gpt-4o-mini→armed()恒False拒绝启用; 请求必带stream:false; 双端点主备(NewAPI/apimart);
# TTL600s缓存; 全局死线t0+10s超时跳过后续LLM步骤; expand/hyde与主检索并行(先到先用不互等)。
# 红线: 仅检索辅助·零答案生成·假想卡/变体只作检索键永不进响应体。
# 挂接4处: 本import + search相xfetch后SVC.search前fire() + k13b后harvest() + cwin后
# apply_filter()。回滚=摘4处挂接+删本件(无独立DB零状态)。
import llm_ensemble as ens  # noqa: E402
# repo_map(M9·轨道J仓库地图·Aider repo-map跨域移植): ADD相复用k13指纹与社区归属→
# 社区内共现图(节点=文件/符号/测试名·边=同卡共现)→PageRank(纯python幂迭代)→SEARCH相
# 地图卡(第4英雄位·经coding_window.apply heroes构建后追加·≤1张≤420字符)。零LLM。
# 挂接2处: 本import + _knives_process的rmap.observe(k13.observe后) + coding_window.apply
# 内heroes hunk(见repo_map交付文档)。env总闸BRAINOS_REPO_MAP(缺省0=关)。
import repo_map as rmap  # noqa: E402

# spanvec(轨道P·L4多向量分片嵌入SPANVEC·20261025): ADD相逐卡切语义分片(~400-600字符·
# 边界=\n\n/[tool_result]/[tool_use]/diff --git/Exit code·≤12片·短卡不切)→批量嵌入
# (organ._qmodel同源·k4 memo/v4shim缓存自动生效)→sqlite分片向量+父卡全文镜像; SEARCH相
# query×分片批量点积→父卡回溯(score=max片分+0.3×(命中片数-1))→k13b块RRF第三榜直召回
# (与k13.recall_by_fingerprint并列第二直召回源·复用B惯例k=60/rank0钉扎/k11+k2闸同口径)。
# 治"长卡单向量语义稀释"——gold埋长卡中部的query按片命中。
# 挂接3处: 本import + _knives_process的spv.observe(k13.observe后) + search相k13b块内
# spanvec直召回并入。回滚=摘3处挂接+删本件(spanvec.db可留档)。env总闸BRAINOS_SPANVEC
# (缺省0=关=观察零嵌入+召回恒[]=零行为差)。
import spanvec as spv  # noqa: E402

# knife8_1009(wave2·k8_d2): D2余量刀·矛盾显影 — k1已标矛盾对(knife1.db conflicts/superseded)
# 在比较类query下的呈现层: 双卡按时序成对必回+changed from X to Y差异注记。ADD侧零新增(矛盾
# 检测唯一owner=k1·本刀ro只读其db)·SEARCH侧纯排序呈现(比较词典零命中=字节直通·k5聚合让位/
# k3 order档直通/duration重叠跳过)。env总闸=BRAINOS_K8D2(缺省关·=1启用·部署候令)。
import knife8_d2pair as k8d2  # noqa: E402

# knife8_rank(部署线shadow排序刀·20261009·I2x床三MISS修复+wave2 SPEC k11增强件并入):
# ①query专名卡级覆盖boost(I2X-06) ②卡级实体覆盖度重排(I2X-10) ③跨请求同conv缝链插位
# (I2X-09·M3 app层等价·k1/k2闸复检零旁路) ④同实体组带外晋升(k11 M2b:组键=k1.fact_current
# subject·时序=k4._created_ms·MAX_PROMOTE=1·top3铁闸·freshness/kin注记)。
# 全部在 data[:top_k] 截断前最终排序位·fail-soft同前七刀·缺省关(BRAINOS_K8=0)。
# 回滚 = 摘除本import+截断前k8块+install行 + 摘除knife8_rank.py(零DB零状态)。
import knife8_rank as k8  # noqa: E402
import evidence_rerank as erank
import content_sanitizer as csan  # noqa: E402  f2_1012代码证据重排
import corpus_fact as cfact
import knife12_struct as k12  # noqa: E402  # W3·A3结构卡(集合/枚举/计数字段抽取
#  →专卡·observe在add相inject在search相·零organ改动·env总闸BRAINOS_K12)
import deep_qa_graph as dqag  # Q1 QA预计算  # noqa: E402
import deep_policy as dpol  # noqa: E402  # deep_t4: 策略合规地图注入(H域/K6·A型判定/B型地图)
import deep_temporal as dtmp  # noqa: E402  (t3: 时序/成员资格预计算TENURE/SEQUENCE判定卡)
import deep_closure as dcl  # noqa: E402
import deep_timeline as dtl  # noqa: E402
import knife10_a1asof as k10  # noqa: E402  # 刀10·A1时变点查(单点+日期+归属角色→原文卡确定性点查)
import deep_multifix as dmf
import rule_first as rf
import deep_taskbrief as dtb  # noqa: E402  (deep_tf: task-index点查+TASK BRIEF注入)

# [U-R45·检索排序层四修 20261028] UF归因检索侧手术(F1去样板/F2 repo加权/F3高频垃圾卡
# 降权/F4毒卡存在性机检)·全部收口本新件·env门缺省全关=逐字节等价·fail-soft·零LLM。
# 回滚=摘本import+search内5处R45钩子(标记"U-R45")+删r45_rank.py。UR(dev_ur)边界:
# R1英雄社区门/R2 digest剥除/R3遥测全部在deep_expdistill+knife13+coding_window三件,
# 与本件零文件重叠·互不覆盖。
try:
    import r45_rank as r45  # noqa: E402
except Exception:  # noqa: BLE001 — 件缺席=四修整体缺席(服务不死)
    r45 = None

# perf_c1_1011(C1席搜索性能刀): 三级精确memo overlay(organ.search_ex参数级/BM25
# get_scores实例级/SVC.recents租户级·同输入⇒同输出·零语义变化) + knife4配套三hunk
# (n_t ver缓存/_attach eid倒排/ranked_map复用)。失效模型: ver=卡数·gen=finalize代数·
# 脏态旁路。env总闸=BRAINOS_PERF_CACHE(缺省开·=0关门回基线逐位)。
# 回滚 = 摘本import+startup install行 + 还原knife4_streaming三hunk + 摘perf_cache.py。
import perf_cache as pc1  # noqa: E402

# card_construct_1009(基地重构·证据内容层三刀): 官方卡=消息流1:1直通副本(零切块零原子化
# 零日期+毒dt)=冒烟46.10结构性根因。三刀(全部只增不改·原文卡text逐字节不变·派生卡走
# 同一add流过k1-k7门=零旁路): ①毒dt止血(缺timestamp→dt=""不造假·步进dt保留) ②归一
# 日期注记卡[dates: ...](k3锚定归一入text通道·治C1/C2) ③句级原子事实卡(拆句+真名
# speaker+代词解引+时间继承·治D1/A1/B2)。env总闸: BRAINOS_CC_NODTPOISON/BRAINOS_CC_DATES/
# BRAINOS_CC_ATOM(各缺省开·=0关门回官方直通行为)。
# 回滚 = 还原 pre_rebuild_1009/aml_service.py(a4f369b9) + 摘除 card_construct.py/
#   card_construct_selftest.py (无独立DB状态)。
import card_construct as cc  # noqa: E402

# w2_timeline(W2-1·梯队一第一刀·20261014): 时间戳透传修复(card_construct.dt_of ISO直析)
# + 双时间轴字段 + 确定性时间线卡(同session消息按原时间排序·按天分窗·零LLM)。
# ADD相: build_cards后按session产时间线卡(dia="W2TL:{sess}"稳定键=分批幂等覆写)并入同一
# SVC.add批(一等公民入organ索引·任意实体/时间query可召回·随窗携带绝对日期词面);
# 时间线卡正文含会话原句→并入k2.observe同口径敏感标注(隐私闸零旁路); k1/k3/k6/k7观察
# 保持原文卡流零扰动。回滚=env BRAINOS_W2_TIMELINE=0(不产卡) + 摘本import与add相两行
# + 删w2_timeline.py(.bak齐备)。测试副本专用, 生产8090零触碰。
import w2_timeline as w2tl  # noqa: E402

# c1_quad(梯队二C1·四路混合检索·20261014): 语义(现有embed序位次先验)+词面(现有锚
# 词面重叠)+实体词典(新)+时间索引(新·dt吃W2-1透传修复) 线性融合重排, 全SQLite零新依赖。
# ADD相: c1q.observe增量落实体词典/时间索引(对表k1-k7 observe家规); 启动相存量回填。
# SEARCH相: 截断前最后排序位融合重排+实体/时间两路新召回(organ窗外卡拉回竞争·先过
# k2同口径隐私闸=零旁路)。总闸=ZFAIOS_FUSION(缺省0=关·零写零行为字节直通);
# 权重=ZFAIOS_FUSION_W1..W4。回滚=env软关+摘本import与三处hunk+删c1_quad.py
# 与time_norm.py副本(.bak齐备)。测试副本专用, 生产8090零触碰。
import c1_quad as c1q  # noqa: E402

from fastapi import Body, FastAPI, HTTPException, Request  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from starlette.concurrency import run_in_threadpool  # noqa: E402

SVC = ms.SVC
DATA_DIR = _DEPLOY / "data"
LEDGER_DB = DATA_DIR / "ledger.db"
SNAP_DIR = DATA_DIR / "snapshot"
# ── [刀9·治根] 持久化记忆日志: add逐笔落journal·启动相回放重建(重启不再失忆) ──
JOURNAL_PATH = DATA_DIR / "add_journal.jsonl"
_JOURNAL_LOCK = __import__("threading").Lock()

def _journal_append(rid: str, uid: str, sid: str, msgs: list) -> None:
    """[刀9→P0fix_1027] 成功add逐笔追加journal(append+flush+fsync·崩溃安全)+ts毫秒
    (对账/取证用·回放不依赖它=全量幂等重放)。msgs=平台原报文(role/content/timestamp)
    ——回放侧经build_cards按add路径同源重建确定性卡(dia="{rid}:{i}")=内容零丢失。"""
    try:
        with _JOURNAL_LOCK:
            with open(JOURNAL_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps({"rid": rid, "user_id": uid, "session_id": sid,
                                    "ts": int(time.time() * 1000),
                                    "messages": msgs}, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
    except Exception as exc:
        log.error("journal append FAILED rid=%s err=%s", rid, type(exc).__name__)

def _journal_replay() -> tuple[int, int]:
    """[P0fix_1027·治根] 启动相回放: journal逐rid重建真实卡→重启内容零丢失(幂等·rid去重)。
    根因修复(dev_p0实证): 旧实现把平台原报文(role/content/timestamp)按卡键(sess/dt/
    speaker/text)取值→全空_AddItem→器官跳无text卡→回放恒0真卡("N msgs restored"只是
    消息计数假象)=快照成了唯一真恢复源, 快照一丢(损坏/目录被搬)即静默失忆——20261027
    生产2880卡事故链的引擎侧共因。修法=与add路径单实现:
      ·平台形报文 → build_cards(同add)确定性重建 + w2tl面/时间线卡(同env门·同键同文
        =与快照内已面卡逐字节同·覆写零回归);
      ·卡形报文(conv/sess/dia/speaker/text/dt·历史兼容) → 直映_AddItem(空text器官自跳);
      ·租户=tenant_of(uid)(旧实现裸user_id入器官="uid="族幻影租户同族病);
      ·回放尾单批finalize_ingest(同_startup_replay语义)=回放卡立即可检索。
    返回(真实重建卡数, 批数)。"""
    if not JOURNAL_PATH.exists():
        return (0, 0)
    seen_rids: set[str] = set()
    n_card = 0; n_batch = 0
    try:
        with open(JOURNAL_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                rid = str(rec.get("rid") or "")
                if rid in seen_rids: continue
                seen_rids.add(rid)
                uid = str(rec.get("user_id") or "default")
                tenant = tenant_of(uid)
                msgs = [m for m in (rec.get("messages") or []) if isinstance(m, dict)]
                try:
                    if msgs and any(("text" in m or "sess" in m or "dia" in m or "conv" in m)
                                    for m in msgs):
                        items = [ms._AddItem(**{k: m.get(k) for k in
                                 ("conv", "sess", "dia", "speaker", "text", "dt") if k in m})
                                 for m in msgs]
                    else:
                        _body = {"request_id": rid, "user_id": uid,
                                 "session_id": rec.get("session_id") or "", "messages": msgs}
                        _cards = build_cards(_body, tenant)
                        _tl = w2tl.session_cards(_cards, rid) if w2tl.ON() else []
                        _cards = w2tl.face_cards(_cards, rid) \
                            if (w2tl.ON() and os.environ.get("BRAINOS_W2_DTFACE", "1") != "0") \
                            else _cards
                        items = [ms._AddItem(**c) for c in _cards + _tl]
                    items = [it for it in items if str(it.text or "").strip()]
                    if not items: continue
                    SVC.add(items, False, tenant)
                    n_card += len(items); n_batch += 1
                except Exception as exc:
                    log.error("journal replay batch FAILED rid=%s err=%s", rid, type(exc).__name__)
    except FileNotFoundError:
        return (0, 0)
    if n_card:
        try:
            SVC.organ.finalize_ingest()
            SVC._dirty = False
        except Exception as exc:
            log.error("journal replay finalize FAILED err=%s", type(exc).__name__)
    return (n_card, n_batch)

# ── [刀9·治根] 启动相journal回放触发点(修序20261016·二次: 调用必须在def之后——
#    模块自上而下执行, 前置调用=NameError每boot必败·回放自部署以来从未生效)
#    (三次修·20261016: 同步回放→后台daemon线程——大journal回放达40-90min,
#     同步版把uvicorn绑定堵在回放之后=每次重启平台断网小时级·不可接受;
#     后台版=绑定先行(秒级恢复服务)·回放流式重建·卡级幂等(同id覆盖)零重)
#    (P0fix_1027·四次修: Event门控——旧版import即跑①与_on_startup/_startup_replay
#     竞态②前引log/build_cards/tenant_of(定义在后)=潜伏NameError·全赖线程调度延迟
#     未爆。门在_on_startup全部启动相(账本重放/索引自愈/回填)就绪后放行=确定性次序)
_JOURNAL_GO = threading.Event()
try:
    def _journal_replay_bg() -> None:
        try:
            _JOURNAL_GO.wait()  # 门: _on_startup尾放行(纯wait·启动不完成不回放)
            _n, _b = _journal_replay()
            if _n:
                logging.warning("journal replay(background): %d cards / %d batches restored", _n, _b)
            _persist_wipe_check()  # [P0fix_1027] 回放完成后终判(牌vs终态·误报归零)
        except Exception as _exc:
            logging.error("journal replay FAILED err=%s", type(_exc).__name__)
    threading.Thread(target=_journal_replay_bg, name="journal-replay", daemon=True).start()
except Exception as _exc:
    logging.error("journal replay spawn FAILED err=%s", type(_exc).__name__)

# ── [P0fix_1027] 持久化停机牌+启动对账: 数据目录被整目录搬走/清空(mv data data.bak*)
#    =20261027生产2880卡事故第一因——引擎0卡静默起机+0卡快照续写, 全程零告警。
#    牌落logs/(data目录之外=目录被搬也幸存)·停机/每轮快照后刷新; 启动对账
#    "上轮N卡→本轮0卡"即CRITICAL铁证。只告警零行为变更(fail-soft)。
_STATE_MARKER = _DEPLOY / "logs" / "last_state.json"

def _persist_state_marker(where: str) -> None:
    try:
        _STATE_MARKER.parent.mkdir(parents=True, exist_ok=True)
        _STATE_MARKER.write_text(json.dumps({
            "cards": int(SVC._total), "ts": int(time.time() * 1000),
            "pid": os.getpid(), "where": where}), encoding="utf-8")
    except Exception as exc:
        log.error("state marker write(%s) FAILED err=%s", where, type(exc).__name__)

def _persist_boot_audit() -> None:
    """[P0fix_1027] 启动对账(INFO行): 上轮停机牌vs本次启动实载。硬判(WIPE SUSPECTED)
    在journal回放完成之后(_persist_wipe_check)——回放是后台流式, 启动相SVC._total
    尚不含回放卡, 此处硬判=误报(实证dev_p0: 删快照留journal场景)。fail-soft。"""
    try:
        if not _STATE_MARKER.exists():
            return
        prev = json.loads(_STATE_MARKER.read_text(encoding="utf-8"))
        log.info("persistence audit: prev_cards=%s boot_cards=%d (journal replay pending)",
                 prev.get("cards"), int(SVC._total))
    except Exception as exc:
        log.error("persistence audit FAILED err=%s", type(exc).__name__)

def _persist_wipe_check() -> None:
    """[P0fix_1027] journal回放完成后的终判: 牌N卡>0 vs 终态0卡=快照+journal+账本齐空
    =数据目录被搬/清(mv data data.bak*·20261027生产2880卡事故第一因)→CRITICAL铁证。
    只告警零行为变更(fail-soft)。"""
    try:
        if not _STATE_MARKER.exists():
            return
        prev = json.loads(_STATE_MARKER.read_text(encoding="utf-8"))
        prev_cards = int(prev.get("cards") or 0)
        now_cards = int(SVC._total)
        if prev_cards > 0 and now_cards == 0:
            log.error("PERSISTENCE WIPE SUSPECTED: last snapshot cards=%d (ts=%s) but boot+"
                      "journal-replay restored 0 cards — snapshot/journal/ledger all empty; "
                      "data dir moved or wiped? Restore from backup before serving.",
                      prev_cards, prev.get("ts"))
        else:
            log.info("persistence audit ok (post-replay): prev_cards=%d now_cards=%d",
                     prev_cards, now_cards)
    except Exception as exc:
        log.error("persistence wipe check FAILED err=%s", type(exc).__name__)

KEY_FILE = Path(os.environ.get("ZO_KEYFILE", str(_DEPLOY / "app" / ".api_key")))
SNAP_INTERVAL_S = float(os.environ.get("ZO_SNAP_INTERVAL", "60"))
MAX_BODY_BYTES = 32 * 1024 * 1024  # 单add≤30MiB(官方mm上限)+裕量; 超限413

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(_DEPLOY / "logs" / "service.log"), logging.StreamHandler()],
)
log = logging.getLogger("aml")

# 死锁取证器(20261010): SIGUSR1触发全线程堆栈落地(诊断用·零行为影响)
import faulthandler as _fh
import signal as _sig
try:
    _fh.register(_sig.SIGUSR1, file=open("/home/zfai/zo_deploy/logs/faulthandler.log", "a"), all_threads=True)
except Exception:
    pass
# 静音第三方 INFO 噪声
for noisy in ("uvicorn.access", "urllib3", "httpx", "sentence_transformers", "brainos"):
    logging.getLogger(noisy).setLevel(logging.WARNING)  # brainos=organ逐调用INFO刷屏(XMemory每卡两条), 收到WARNING

API_KEY = KEY_FILE.read_text().strip() if KEY_FILE.exists() else ""
if not API_KEY:
    log.warning("API KEY 文件缺失: %s — /add /search 将全拒(401)", KEY_FILE)

app = FastAPI(title="BRAIOS AML Adapter", docs_url=None, redoc_url=None, openapi_url=None)


# ── ledger(sqlite WAL) ──────────────────────────────────────────────────────
class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")   # 实测0.039ms/commit; 进程崩溃安全
        self._conn.execute("""CREATE TABLE IF NOT EXISTS ledger(
            request_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
            response_json TEXT NOT NULL, cards_json TEXT NOT NULL, created_ms INTEGER NOT NULL)""")
        self._conn.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        self._conn.commit()
        self._lock = threading.Lock()

    def get(self, request_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT response_json FROM ledger WHERE request_id=?", (request_id,)).fetchone()
        return row[0] if row else None

    def put(self, request_id: str, user_id: str, response_json: str, cards_json: str) -> bool:
        """True=本进程首次写入; False=已存在(并发重放), 调用方须改返回已存响应"""
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO ledger VALUES(?,?,?,?,?)",
                (request_id, user_id, response_json, cards_json, int(time.time() * 1000)))
            self._conn.commit()
            return cur.rowcount > 0

    def rows_after(self, ms_epoch: int) -> list[tuple[str, str]]:
        with self._lock:
            return self._conn.execute(
                "SELECT user_id, cards_json FROM ledger WHERE created_ms>? ORDER BY created_ms",
                (ms_epoch,)).fetchall()

    def meta_get(self, k: str) -> str | None:
        with self._lock:
            rows = self._conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchall()
        return rows[0][0] if rows else None

    def meta_set(self, k: str, v: str) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, v))
            self._conn.commit()


LEDGER = Ledger(LEDGER_DB)


def tenant_of(user_id: str) -> str:
    """user_id→器官租户命名空间: 禁冒号(服务件_norm_tenant), 替换后非空; 响应仍回显原始user_id
    p0fix_1010(搜索全零同族病·196终端实证): 部分客户端search以"uid=u_..."拼写user_id(与add时
    裸"u_..."同一逻辑用户) — "="不在白名单被换"_"→幻影租户零卡可搜(实证: 10-10 01:16起213条
    uid=uid=u_*查询全hits=0·同query裸租户离线复搜10命中)。归一=剥可选"uid="前缀后再消毒,
    add/search双相同一映射=单实现路径·零题目先验·零跨租户泄漏(剥离后租户串仍逐字消毒)。"""
    u = str(user_id)
    if u[:4].lower() == "uid=":
        u = u[4:]
    t = "".join(c if c.isalnum() or c in "-_." else "_" for c in u)[:128]
    return t or "u_default"


def _dt_from_ts(ts: Any, text: str = "") -> str:
    """消息timestamp(毫秒epoch, 兼容秒)→organ dt(探针实证格式 ISO无时区尾Z)
    card_construct刀1·毒dt止血(终端摸底修正): 缺/脏timestamp→[Time:]前缀真源→
    "unknown"哨兵(organ EG审计#3跳空dt卡=证据蒸发, 故空串不可行·哨兵自述未知
    零伪造: FSRS跳过/时间线排尾/零锚)。合法步进dt路径与原实现逐字节同形。
    关门(BRAINOS_CC_NODTPOISON=0)回旧now()兜底。"""
    v = cc.dt_of(ts)
    if v:
        return v
    if cc.DT_HONEST():
        return cc.honest_dt(text)
    try:
        float(ts)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return v


def _content_to_text(content: Any) -> tuple[str, int]:
    """content(str|[part...])→(正文, 图片数); text拼接·image_url→[图片]标记·未知type跳过(宽松)"""
    if content is None:
        return "", 0
    if isinstance(content, str):
        return content, 0
    if isinstance(content, list):
        parts: list[str] = []
        n_img = 0
        for p in content:
            if not isinstance(p, dict):
                continue
            t = p.get("type")
            if t == "text" and isinstance(p.get("text"), str):
                parts.append(p["text"])
            elif t == "image_url":
                n_img += 1
            # 未知type: 跳过(宽松解析)
        if n_img:
            parts.append("[图片]" if n_img == 1 else f"[图片x{n_img}]")
        return "\n".join(x for x in parts if x), n_img
    return str(content), 0


def build_cards(body: dict[str, Any], tenant: str = "") -> list[dict[str, Any]]:
    """AML messages→器官证据卡六字段; dia="{request_id}:{i}" 确定性键=幂等重放的物理基础"""
    sid = str(body.get("session_id", ""))
    cards: list[dict[str, Any]] = []
    rid = str(body.get("request_id", ""))
    for i, m in enumerate(body.get("messages") or []):
        if not isinstance(m, dict):
            continue
        text, n_img = _content_to_text(m.get("content"))
        if not text:
            continue
        cards.append({
            "conv": sid,
            "sess": sid,
            "dia": f"{rid}:{i}",
            "speaker": str(m.get("role", "user")),
            "text": text,
            "dt": _dt_from_ts(m.get("timestamp"), text),
        })
    # card_construct刀2+刀3: 派生层总装(日期注记卡+原子事实卡) — overlay缺省:
    # organ索引=纯原文卡流(零扰动·零回归由构造保证), 派生卡入边车DB搜索相尾注;
    # 原文卡逐字节不动=回归基线。fail-soft
    return cc.derive_all(cards, rid, k3, tenant=tenant)


def _evidence_id(e: dict[str, Any]) -> str:
    raw = f'{e.get("session", "")}|{e.get("dt", "")}|{e.get("text", "")}'
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


# ── bz刀3·死线两梯降级件(20261015·60.34战报§四刀3): 无锁轻量检索/场景块化recents ──
# 只读+fail-soft+零生成; 通用词类信号零题面触碰; 阈值env化(BRAINOS_SEARCH_T1_MS)。
# 回滚=摘两helper+还原死线块单梯形(见bak-knife3-dl2-20261015)。
_DL_STOP = frozenset((
    "the", "a", "an", "and", "or", "but", "not", "no", "yes", "is", "are", "was", "were",
    "be", "been", "being", "to", "of", "in", "on", "at", "for", "with", "by", "from", "as",
    "that", "this", "these", "those", "it", "its", "his", "her", "their", "my", "your",
    "our", "we", "you", "he", "she", "they", "me", "us", "them", "do", "does", "did",
    "what", "when", "where", "who", "whom", "which", "how", "why", "will", "would", "can",
    "could", "should", "have", "has", "had", "about", "into", "over", "under", "after",
    "before", "between", "during", "的", "了", "吗", "呢", "吧", "啊", "呀", "哦", "是"))


def _query_terms(query: str) -> list[str]:
    # 词面项: ASCII词(≥2字·滤通用停用词) + CJK单字(中文题面·零语言硬编码仅语言粒度)
    words = [w for w in re.findall(r"[a-z][a-z0-9_]+", query.lower()) if w not in _DL_STOP]
    return words + re.findall(r"[\u4e00-\u9fff]", query)


def _tenant_cards_unlocked(tenant: str, retries: int = 2) -> list[dict[str, Any]]:
    # organ原文卡无锁只读快照(同memory_service.recents信任级·不碰organ锁);
    # dict并发变更大小的RuntimeError→有界重试→fail-soft空表(上游落T2)。
    for _ in range(max(1, retries + 1)):
        try:
            return [c for c in list(SVC.organ._cards.values())
                    if str(c.get("conv", "")).split(":", 1)[0] == tenant]
        except RuntimeError:
            continue
        except Exception:
            break
    return []


def _lexical_fastlane(query: str, tenant: str, cap: int) -> dict[str, Any]:
    """第一梯·词面快车道(12-15s): 过载时按'词面相关>时序近因'梯度降级的通用检索容量层。
    organ原文卡无锁只读+query词面重叠过滤·倒排式两键稳定序(命中数desc→dt desc→dia desc)
    ·确定性·零生成。evidence_score=窗内词面覆盖率(诚实标注·只入日志·契约恰好3键)。"""
    terms = _query_terms(query)
    cards = _tenant_cards_unlocked(tenant)
    if not cards or not terms:
        return {"evidence": [], "evidence_score": 0.0}
    tset = set(terms)
    scored: list[tuple[int, str, str, dict[str, Any]]] = []
    for c in cards:
        text = c.get("text", "")
        if not text:
            continue
        low = text.lower()
        hits = sum(1 for t in tset if t in low)
        if hits > 0:
            scored.append((hits, str(c.get("dt", "")), str(c.get("dia", "")), c))
    if not scored:
        return {"evidence": [], "evidence_score": 0.0}
    scored.sort(key=lambda r: (r[1], r[2]), reverse=True)   # dt desc→dia desc(稳定)
    scored.sort(key=lambda r: -r[0])                        # 命中数desc(稳定·近因决胜)
    top = scored[:max(1, min(int(cap), 256))]
    seen: set[str] = set()
    for _h, _d, _di, c in top:
        low = str(c.get("text", "")).lower()
        seen.update(t for t in tset if t in low)
    score = min(1.0, len(seen) / max(1, len(tset)))
    return {"evidence": [{"session": c.get("sess", ""), "dt": c.get("dt", ""),
                          "speaker": c.get("speaker", ""), "text": c.get("text", "")}
                         for _h, _d, _di, c in top],
            "evidence_score": float(score)}


def _recents_blocked(tenant: str, n: int, query: str = "") -> list[dict[str, Any]]:
    """第二梯·recents场景块化(25s): 同session相邻卡整块呈现·禁单卡碎片 —
    块序=块内最新dt降序(近因窗语义不变); 块内dt升序(叙事连续·防H/I2跨段题被散点近卡填窗);
    装不下整块时取连续前缀/后缀段(≥2卡·绝不让块尾留1卡碎片)。无锁只读·确定性·零生成。
    刀6·I2(20261015·60.34战报§二I2钉因): 词面预留切片 — 跨会话事件序列题恰需**老**
    会话卡·recents近因窗与之结构性相克(沙箱实测: 87卡租户降级窗源卡0/8·仅剩最新会话)。
    修法=预留≤n/3槽位(≥4·env BRAINOS_SEARCH_T2_LEX=0关)给query词面命中卡(fastlane同款
    确定性序: 命中数desc→dt desc→dia desc)·块化照旧填余量·跨租户均匀零题面触碰。"""
    cards = _tenant_cards_unlocked(tenant)
    if not cards:
        return []
    n = max(0, int(n))
    rows: list[dict[str, Any]] = []
    have: set[str] = set()
    if query and os.environ.get("BRAINOS_SEARCH_T2_LEX", "1") != "0":
        terms = _query_terms(str(query))
        if terms:
            tset = set(terms)
            lex: list[tuple[int, str, str, dict[str, Any]]] = []
            for c in cards:
                text = c.get("text", "")
                if not text:
                    continue
                low = text.lower()
                hits = sum(1 for t in tset if t in low)
                if hits > 0:
                    lex.append((hits, str(c.get("dt", "")), str(c.get("dia", "")), c))
            if lex:
                cap = min(len(lex), max(4, n // 6), max(1, n // 3))
                lex.sort(key=lambda r: (r[1], r[2]), reverse=True)   # dt desc→dia desc
                lex.sort(key=lambda r: -r[0])                        # 命中数desc(稳定)
                for _h, _d, _di, c in lex[:cap]:
                    t = c.get("text", "")
                    if not t or t in have:
                        continue
                    rows.append({"session": c.get("sess", ""), "dt": c.get("dt", ""),
                                 "speaker": c.get("speaker", ""), "text": t})
                    have.add(t)
    blocks: dict[str, list[dict[str, Any]]] = {}
    for c in cards:
        blocks.setdefault(str(c.get("sess", "")), []).append(c)
    ordered = sorted(blocks.values(),
                     key=lambda b: (max(str(c.get("dt", "")) for c in b),
                                    str(b[0].get("dia", ""))),
                     reverse=True)
    for b in ordered:
        b = sorted(b, key=lambda c: (str(c.get("dt", "")), str(c.get("dia", ""))))
        if not rows and len(b) > n:
            b = b[-n:]  # 首块超预算: 取最新连续段(同session相邻·不碎)
        room = n - len(rows)
        if len(b) > room:
            if room < 2:
                break  # 余量<2: 宁缺勿单卡碎片
            b = b[-room:]
        for c in b:
            t = c.get("text", "")
            if not t or t in have:
                continue
            rows.append({"session": c.get("sess", ""), "dt": c.get("dt", ""),
                         "speaker": c.get("speaker", ""), "text": t})
            have.add(t)
        if len(rows) >= n:
            break
    return rows[:n]


# ── 快照线程 ────────────────────────────────────────────────────────────────
_snap_stop = threading.Event()
_LAST_SERVE = [0.0]  # [刀8.12·快照忙时让路20261024] 最近一次add/search完成时刻(秒)
_last_vacuum = [0.0]


def _ledger_maintain() -> None:
    """疫苗3: WAL checkpoint(TRUNCATE)每拍 + VACUUM每24h(追加快照已compact, 禁O(n²)重写);
    疫苗17: 账本保留期 — 已入快照且超24h的行才删(重放语义不受损, 幂等表不无限涨)"""
    try:
        now_s = time.time()
        with LEDGER._lock:
            LEDGER._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            if now_s - _last_vacuum[0] > 86400 and _last_vacuum[0] > 0:
                LEDGER._conn.execute("VACUUM")
                _last_vacuum[0] = now_s
                log.info("ledger VACUUM done")
            _last_vacuum[0] = _last_vacuum[0] or now_s
        snap_ms = int(LEDGER.meta_get("last_snapshot_ms") or 0)
        cutoff = min(int((now_s - 86400) * 1000), snap_ms)
        with LEDGER._lock:
            cur = LEDGER._conn.execute("DELETE FROM ledger WHERE created_ms < ?", (cutoff,))
            if cur.rowcount:
                LEDGER._conn.commit()
                log.info("ledger purge: %d rows older than cutoff", cur.rowcount)
    except Exception as exc:
        log.error("ledger maintain failed: %s", exc)


_EMB_SAVE_LOCK = threading.Lock()
_EMB_SAVE_STATE = {"keys": 0}


def _emb_cache_save(reason: str, min_delta: int = 0) -> None:
    """[B-Z boot加速20261019·FULL连环判死真因修复] organ增量embed从不落盘(冻结层
    _build_embed增量路径明写"不覆写磁盘缓存")→10-17后全部卡向量每次boot重嵌
    (py-spy三采样钉死MiniLM forward=40分钟boot真身)→平台重试窗耗尽→ADD 502判死×8。
    本函数: organ._emb整体原子落盘(keys+vecs npz·tmp文件写+os.replace·崩不损旧缓存)
    ·min_delta节流防频闪·加锁防并发双写。只读organ私有态·零冻结层修改·
    失败fail-soft(=回退现行为·下次boot照旧重嵌·不比现状差)。"""
    if not _EMB_SAVE_LOCK.acquire(blocking=False):
        return  # 已有写入在途(关机vs周期拍重叠): 跳过本轮·下拍再存
    try:
        from brainos.memory.hippocampal_recall import EMB_CACHE
        emb = getattr(SVC.organ, "_emb", None)
        if not emb:
            return
        n = len(emb)
        if reason != "startup" and n - _EMB_SAVE_STATE["keys"] < min_delta:
            return
        t0 = time.time()
        keys = list(emb.keys())
        vecs = np.stack([np.asarray(emb[k], dtype=np.float32) for k in keys])
        p = Path(EMB_CACHE)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        with open(tmp, "wb") as f:  # 文件对象式savez: 避免 savez对无.npz后缀名追加.npz
            np.savez(f, keys=np.array(keys), vecs=vecs)
        os.replace(tmp, p)
        _EMB_SAVE_STATE["keys"] = n
        log.info("emb_cache saved(%s): keys=%d in %.0fs", reason, n, time.time() - t0)
    except Exception as exc:
        log.error("emb_cache save(%s) FAILED err=%s", reason, type(exc).__name__)
    finally:
        _EMB_SAVE_LOCK.release()


_KNIFE_Q_LOCK = threading.Lock()
_KNIFE_Q: "collections.deque" = deque()


def _knife_enqueue(uid: str, tenant: str, cards: list, tl_cards: list, rid: str) -> None:
    """[B-Z吞吐刀20261019] 7把边刀入队(200回包后后台消化)。失败语义=丢失派生增强索引
    (k1覆盖标记/k3时间锚/k6规则卡/k7摘要/c1q词典/dtb任务表)——卡主数据已200前落库
    (SVC.add+ledger+快照=持久化契约不破); boot期k3/k6/c1q/dtb backfill幂等可重建。"""
    with _KNIFE_Q_LOCK:
        _KNIFE_Q.append((uid, tenant, cards, tl_cards, rid))


def _knives_process(uid: str, tenant: str, cards: list, tl_cards: list, rid: str) -> None:
    """原add处理器内联7刀逐字迁移(fail-soft纪律原样·执行顺序原样)。回滚=bak-bootfix-20261019。"""
    try:
        k1.observe(tenant, cards)
    except Exception as exc:
        log.error("knife1 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        k2.observe(tenant, cards + tl_cards)
    except Exception as exc:
        log.error("knife2 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # W3·A3结构卡observe: 原文卡流→集合/枚举/计数字段抽取(sqlite专表·幂等·
#  fail-soft·时间线卡不吃防转抄重复)
    try:
        k12.observe(tenant, cards)
    except Exception as exc:
        log.error("knife12 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # knife11·D3 L1/L2(ADD相·fail-soft): 删除指令识别+tombstone标记。镜像cards+tl_cards
    # (k2同款闸面同源·时间线卡引文同入镜像防引文通道二次泄漏); 返回受影响会话(本件只报·
    # k7 digest v+1重建调用留v2)。异步刀队列内·add 200关键路径零感知。
    try:
        k11.observe(tenant, cards + tl_cards)
    except Exception as exc:
        log.error("knife11 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # knife13·M1指纹(ADD相·fail-soft·异步刀队列): 原文卡流代码指纹→倒排+社区(只吃原文
    # cards·派生/时间线卡不吃防转抄重复)。env BRAINOS_K13缺省0=关。
    try:
        k13.observe(tenant, cards)
    except Exception as exc:
        log.error("knife13 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # spanvec·L4分片嵌入(ADD相·fail-soft·异步刀队列·k13块后): 原文卡流切语义分片→批量
    # 嵌入(SVC.organ._qmodel同源)→sqlite分片向量+父卡全文镜像。幂等(card_id去重)。
    # 环境总闸BRAINOS_SPANVEC缺省0=关(零嵌入零写=零行为差); fail-soft: 嵌入失败退整卡
    # 单片·再败跳过该卡(organ自有索引不受影响)。
    try:
        spv.observe(tenant, cards, getattr(getattr(SVC, "organ", None), "_qmodel", None))
    except Exception as exc:
        log.error("spanvec observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # citeref·M2引用图谱(ADD相·fail-soft): 卡↔#issue/PR号双向边(原文卡流)。
    # env BRAINOS_CITEREF缺省0=关。
    try:
        crf.observe(tenant, cards)
    except Exception as exc:
        log.error("citeref observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    # repo_map·M9仓库地图(ADD相·fail-soft): k13社区→共现图(节点=文件/符号/测试名·
    # 边=同卡共现)+errsig近账·只标dirty(PageRank首次query懒构建)。集成契约: 本调用须在
    # k13.observe之后(社区归属由k13本批先写)。env BRAINOS_REPO_MAP缺省0=关。
    try:
        rmap.observe(tenant, cards)
    except Exception as exc:
        log.error("repo_map observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        k3.observe(tenant, cards)
    except Exception as exc:
        log.error("knife3 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        k6.observe(tenant, cards)
    except Exception as exc:
        log.error("knife6 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        k7.observe(tenant, cards)
    except Exception as exc:
        log.error("knife7 observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        c1q.observe(tenant, cards + tl_cards)
    except Exception as exc:
        log.error("c1_quad observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
    try:
        dtb.observe(tenant, cards, rid)
    except Exception as exc:
        log.error("deep_taskbrief observe FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)


def _knife_worker() -> None:
    """[B-Z吞吐刀20261019] 单worker串行消化(GIL友好·sqlite写天然串行化·零新锁竞争面)。"""
    while True:
        try:
            with _KNIFE_Q_LOCK:
                item = _KNIFE_Q.popleft() if _KNIFE_Q else None
            if item is None:
                time.sleep(0.25)
                continue
            _knives_process(*item)
        except Exception as exc:
            log.error("knife worker err=%s", type(exc).__name__)
            time.sleep(1)


def _snapshot_loop() -> None:
    _last_vacuum[0] = time.time()
    while not _snap_stop.wait(SNAP_INTERVAL_S):
        try:
            # [刀8.12·快照忙时让路20261024]: 29万卡dump纯Python序列化15-28分钟≈GIL常驻,
            # 是search持锁车队/add排队17分钟的放大器(P0法证20261024)。有近期流量即推迟
            # 本轮(下一拍再判)——快照只在静默窗口跑。BRAINOS_SNAP_IDLE_S缺省120秒,0=关。
            try:
                _idle_s = float(os.environ.get("BRAINOS_SNAP_IDLE_S", "120") or "0")
            except ValueError:
                _idle_s = 120.0
            if _idle_s > 0 and (time.time() - _LAST_SERVE[0]) < _idle_s:
                continue
            man = SVC.snapshot(str(SNAP_DIR))
            LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))
            _persist_state_marker("periodic")  # [P0fix_1027] 牌随每轮快照刷新(崩溃场景也对账有据)
            _ledger_maintain()
            log.info("snapshot ok cards=%s convs=%s", man.get("cards"), man.get("convs"))
            _emb_cache_save("periodic", min_delta=20000)  # [B-Z 20261019] 增量≥2万才存·防频闪
        except Exception as exc:  # 快照失败不致命(ledger兜底重放), 下轮再试
            log.error("snapshot failed: %s", exc)


def _startup_replay() -> int:
    """恢复=MemoryService已载snapshot; 重放 last_snapshot_ms 之后的ledger行(幂等覆写)
    (疫苗4: 重放时长落log, >60s WARN; organ增量索引=无全表扫描, 卡键确定性=幂等覆写)"""
    t0 = time.time()
    snap_ms = int(LEDGER.meta_get("last_snapshot_ms") or 0)
    rows = LEDGER.rows_after(snap_ms)
    by_user: dict[str, list[dict[str, Any]]] = {}
    for user_id, cards_json in rows:
        by_user.setdefault(user_id, []).extend(json.loads(cards_json))
    n = 0
    for user_id, cards in by_user.items():
        if cards:
            SVC.add([ms._AddItem(**c) for c in cards], finalize=False, tenant=tenant_of(user_id))
            n += len(cards)
    if n:
        try:
            SVC.organ.finalize_ingest()  # replay-fix: 全量重放后单批嵌入(原逐租户finalize=O(N²)重复编码)
            SVC._dirty = False
        except Exception as exc:
            log.error("startup replay finalize failed: %s", exc)
        try:
            SVC.snapshot(str(SNAP_DIR))
            LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))  # 推进水位→下次重启近零重放
        except Exception as exc:
            log.error("startup replay snapshot failed: %s", exc)
        msg = "startup replay: %d cards from %d users in %.1fs (snap_ms=%d)"
        if time.time() - t0 > 60:
            log.warning(msg + " — SLOW(>60s WARN)", n, len(by_user), time.time() - t0, snap_ms)
        else:
            log.info(msg, n, len(by_user), time.time() - t0, snap_ms)
    return n


def _warmup() -> None:
    """预热: 把MiniLM嵌入模型加载进内存(首次请求不再扛冷启动~70s)。
    疫苗6: 零探针/哑数据入库 — 只加载模型权重, organ状态零污染。"""
    try:
        t0 = time.time()
        SVC.organ._qmodel = SVC.organ._embed_model()
        log.info("warmup: embed model loaded in %.1fs (no data injected)", time.time() - t0)
    except Exception as exc:
        log.warning("warmup failed (embed通道将fail-soft降级词面): %s", exc)


# [刀8.11·FULL加固20261017·py-spy实锤: anyio线程池默认40tokens——96k库检索3-8s×
# 16并发+add观察链+finalize全在抢→add排队78s] 启动即扩容线程池(仅容量·零语义变化)。
@app.on_event("startup")
async def _bump_threadpool() -> None:
    try:
        import anyio.to_thread as _tt
        _n = int(os.environ.get("BRAINOS_THREADPOOL", "160"))
        _tt.current_default_thread_limiter().total_tokens = _n
        logging.getLogger("uvicorn").info("threadpool tokens=%d (FULL加固)", _n)
    except Exception as _e:
        logging.getLogger("uvicorn").error("threadpool bump FAILED err=%s", type(_e).__name__)


@app.on_event("startup")
def _on_startup() -> None:
    t0 = time.time()
    _warmup()
    n = _startup_replay()
    # p0fix_1010(snapshot载入索引自愈·"organ snapshot载入后搜索全零"同族病·196 P0):
    # PersistentRecall.load 契约="状态直恢复(非索引重建)" — _idx(BM25双通道)/_emb(向量)完整性
    # 只靠"save前已finalize"的约定(未强制), 任何一环缺口(半建索引入快照/旧版快照/外部产物)
    # = 载入卡对搜索不可见→全零且重启不自愈。启动相全量校验+精准修复: conv覆盖缺口→逐conv
    # _rebuild_conv(与finalize全量共用同一实现路径·无功能重复); emb键缺口→_build_embed增量补
    # (缺失向量才encode·npz缓存命中零成本)。健康态=两次集合运算零开销·缺口态=自愈·幂等。
    try:
        _t_heal = time.time()
        _organ = SVC.organ
        _conv_gap = {c.get("conv") for c in _organ._cards.values()} - set(_organ._idx)
        for _cv in _conv_gap:
            _organ._rebuild_conv(_cv)
        _emb_gap: list[str] = []
        if _organ.use_embed:
            if _organ._emb is not None:
                _emb_gap = [k for k in _organ._cards if k not in _organ._emb]
            elif _organ._cards:
                _emb_gap = list(_organ._cards)  # emb整体缺失: only_keys路径先npz缓存补再encode缺量
        if _emb_gap:
            _organ._build_embed(only_keys=_emb_gap)
        if _conv_gap or _emb_gap:
            log.warning("startup index heal: cards=%d conv_gap=%d emb_gap=%d — repaired in %.1fs",
                        len(_organ._cards), len(_conv_gap), len(_emb_gap), time.time() - _t_heal)
        else:
            log.info("startup index check ok: cards=%d idx_convs=%d emb=%d (no gap, %.0fms)",
                     len(_organ._cards), len(_organ._idx),
                     len(_organ._emb) if _organ._emb is not None else 0,
                     (time.time() - _t_heal) * 1000)
    except Exception as exc:
        log.error("startup index heal FAILED err=%s", type(exc).__name__)
    # [B-Z 20261019] heal后emb全量在RAM→立即原子落盘: 本boot之后任何重启的emb缺口≈0
    # (boot从40分钟级→分钟级·FULL写入期任何崩溃不再连环判死)。同步存(~20-40s·一次性)。
    _emb_cache_save("startup")
    # knife3_1008: 存量卡时间索引全量回填(幂等OR IGNORE·修前已入库卡亦被C1归一覆盖); fail-soft
    try:
        b_cards, b_anchors, b_ms = k3.backfill(list(SVC.organ._cards.values()))
        log.info("knife3 backfill: cards=%d new_anchors=%d in %.0fms", b_cards, b_anchors, b_ms)
    except Exception as exc:
        log.error("knife3 backfill FAILED err=%s", type(exc).__name__)
    # knife4_1008: I1延迟刀安装(M1a memo/M1b onepass+RW/M2 recency·fail-soft) — 须在_warmup后
    try:
        _k4_flags = k4.install(SVC, LEDGER, LEDGER_DB)
        k8.install(SVC)  # knife8: 只存svc引用(organ ro消费)·零patch零线程
        log.info("knife4 install: %s", _k4_flags if _k4_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife4 install FAILED err=%s", type(exc).__name__)
    # [v4嵌入合规件20261024]: BRAINOS_EMBED_PROVIDER=dashscope_v4 → organ._qmodel 换装
    # text-embedding-v4(1024维·备案v0.10.0-aml承诺组件)。读侧API+md5缓存·新卡add相
    # 走同shim。缺省不启用=MiniLM原路。
    try:
        import v4shim as _v4
        _v4.install(SVC)
    except Exception as exc:
        log.error("v4shim install FAILED err=%s", type(exc).__name__)
    # [P·SPANVEC·可选启动回填20261025]: 门后开(池先于门建)场景的存量卡分片补嵌 —
    # 双门BRAINOS_SPANVEC=1∧BRAINOS_SV_BACKFILL=1才触发(缺省0=零boot成本零行为差);
    # 幂等(cards表card_id去重)·v4shim install之后跑保证维度与生产Provider一致。fail-soft。
    try:
        if (os.environ.get("BRAINOS_SPANVEC", "0") == "1"
                and os.environ.get("BRAINOS_SV_BACKFILL", "0") == "1"):
            _qm = getattr(getattr(SVC, "organ", None), "_qmodel", None)
            _svb_t = time.time()
            _sv_cards, _sv_new = 0, 0
            _sv_by_t: dict[str, list] = {}
            for _c in list(SVC.organ._cards.values()):
                _sv_by_t.setdefault(str(_c.get("conv") or "").split(":", 1)[0] or "u_default",
                                    []).append(_c)
            for _tn in sorted(_sv_by_t):
                _sv_new += spv.observe(_tn, _sv_by_t[_tn], _qm)
                _sv_cards += len(_sv_by_t[_tn])
            log.info("spanvec backfill: cards=%d new=%d in %.0fs", _sv_cards, _sv_new,
                     time.time() - _svb_t)
    except Exception as exc:
        log.error("spanvec backfill FAILED err=%s", type(exc).__name__)
    # knife5_1008: S3多跳扩查刀安装(词典装载·无状态零monkey-patch·fail-soft)
    try:
        _k5_flags = k5.install(SVC)
        log.info("knife5 install: %s", _k5_flags if _k5_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("knife5 install FAILED err=%s", type(exc).__name__)
    # perf_c1: organ内层profiling(env门BRAINOS_PROF_ORGAN=1·零行为变化·纯log)
    if os.environ.get("BRAINOS_PROF_ORGAN", "0") == "1":
        try:
            _orig_sex = SVC.organ.search_ex

            def _sex_prof(query, conv_scope=None, topk=10, __o=_orig_sex):
                _t = time.perf_counter()
                r = __o(query, conv_scope=conv_scope, topk=topk)
                log.info("PROF organ.search_ex topk=%s dt=%.1fms nconv=%d ncard=%d",
                         topk, (time.perf_counter() - _t) * 1000,
                         len(SVC.organ._idx), len(SVC.organ._cards))
                return r

            SVC.organ.search_ex = _sex_prof
            log.info("prof organ.search_ex installed")
        except Exception as exc:
            log.error("prof organ install FAILED err=%s", type(exc).__name__)
    # perf_c1(20261011): 三级精确memo安装(须在k4之后·ranked stash复放依赖其_tls)
    try:
        _pc_flags = pc1.install(SVC)
        log.info("perf_cache install: %s", _pc_flags if _pc_flags else "OFF(master gate)")
    except Exception as exc:
        log.error("perf_cache install FAILED err=%s", type(exc).__name__)
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
    # c1_quad·C1四路检索: 存量卡实体词典+时间索引全量回填(对表k3/k6家规·幂等跳过);
    # 总闸ZFAIOS_FUSION关=跳过零写。fail-soft
    try:
        c1_cards, c1_rows, c1_ms = c1q.backfill(list(SVC.organ._cards.values()))
        log.info("c1_quad backfill: cards=%d rows=%d in %.0fms (gate=%s)",
                 c1_cards, c1_rows, c1_ms, c1q.ON())
    except Exception as exc:
        log.error("c1_quad backfill FAILED err=%s", type(exc).__name__)
    # deep_tf·G4(20261015·存量回填·修"表空因断线非无源"): organ存量卡逐租户跑一次
    # dtb.observe — 修前已入库卡(先问后喂竞态受害者·生产task_index/tbrief_src 0行)一并
    # 建表。幂等: task_index同(tenant,idx)后写胜·tbrief_src OR IGNORE去重(二轮启动零
    # 重建·确定性); 逐租户对账落log。fail-soft同前刀纪律。回滚=摘本块。
    try:
        _bt = time.time()
        _dtb_tenants: dict[str, list[dict[str, Any]]] = {}
        for _c in SVC.organ._cards.values():
            if isinstance(_c, dict) and str(_c.get("text", "")):
                _cv = str(_c.get("conv", ""))
                _tn = _cv.split(":", 1)[0] if ":" in _cv else "default"
                _dtb_tenants.setdefault(_tn, []).append(_c)
        _dtb_touched = 0
        for _tn, _tcs in _dtb_tenants.items():
            _dtb_touched += dtb.observe(_tn, _tcs, "dtb-backfill")
        log.info("deep_taskbrief backfill: tenants=%d cards=%d touched=%d in %.0fms",
                 len(_dtb_tenants), sum(len(v) for v in _dtb_tenants.values()),
                 _dtb_touched, (time.time() - _bt) * 1000)
    except Exception as exc:
        log.error("deep_taskbrief backfill FAILED err=%s", type(exc).__name__)
    threading.Thread(target=_snapshot_loop, name="snap", daemon=True).start()
    threading.Thread(target=_knife_worker, name="knifeworker", daemon=True).start()  # [B-Z吞吐刀20261019]
    _persist_boot_audit()  # [P0fix_1027] 启动对账: 上轮停机牌vs实载(数据目录被搬=CRITICAL铁证)
    _JOURNAL_GO.set()      # [P0fix_1027] journal回放放行(一切启动相就绪后·确定性次序)
    log.info("startup done: cards=%s replayed=%d in %.1fs", SVC._total, n, time.time() - t0)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _snap_stop.set()
    try:
        SVC.snapshot(str(SNAP_DIR))
        LEDGER.meta_set("last_snapshot_ms", str(int(time.time() * 1000)))
        log.info("shutdown snapshot ok")
    except Exception as exc:
        log.error("shutdown snapshot failed: %s", exc)
    _persist_state_marker("shutdown")  # [P0fix_1027] 停机牌(下次启动对账基准)
    _emb_cache_save("shutdown")  # [B-Z 20261019] 优雅关机时emb缓存落盘(崩溃场景由startup/periodic兜)


# ── 鉴权 ────────────────────────────────────────────────────────────────────
def _auth(request: Request) -> None:
    got = request.headers.get("authorization", "")
    want = f"Token {API_KEY}"
    if not API_KEY or not hmac.compare_digest(got, want):
        raise HTTPException(status_code=401, detail="unauthorized")


# ── 端点 ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict[str, Any]:
    # 疫苗10: 自标识字段 — 公网侧验证到达的是BRAIOS进程本体, 禁假设端口没人占
    # [M6-1·版本统一20261025] version=env BRAINOS_VERSION缺省"v0.9.1-aml"(health/材料/
    # git tag三处一致·JSON形状不变)。
    out = {"status": "ok", "service": "braios-aml",
           "version": os.environ.get("BRAINOS_VERSION", "v0.9.1-aml"),
           "cards": SVC._total, "pid": os.getpid()}
    # [M6-2·v4零向量监控20261025] 可选字段embed_zero_count(v4shim模块计数器·新增字段
    # 不破坏2xx JSON契约; v4未启用时恒0)。fail-soft: 取不到就不加。
    try:
        import v4shim as _v4h  # noqa: PLC0415
        out["embed_zero_count"] = int(getattr(_v4h, "ZERO_EMBED_COUNT", 0))
    except Exception:
        pass
    return out


# ── [刀8.12·journal-queue v0.5 20261027·FULL#2写入499风暴终结] ──
# add毫秒级200即回(journal先落fsync+pending入队)·ingest后台串行消化(无GIL thrash)
# search前置按租户flush(秒级·证据完整)·BRAINOS_JQ=0一键回退同步旧路径。
JQ_ON = lambda: os.environ.get("BRAINOS_JQ", "1") == "1"
_jq_queue: "collections.deque" = collections.deque()
_jq_lock = threading.Lock()
_jq_status: dict[str, str] = {}


def _jq_enqueue(rid: str, uid: str, sid: str, payload: dict[str, Any]) -> None:
    with _jq_lock:
        _jq_queue.append((rid, uid, sid, payload))
        _jq_status[rid] = "pending"


def _jq_take_tenant(tenant: str) -> list:
    out, rest = [], []
    with _jq_lock:
        while _jq_queue:
            it = _jq_queue.popleft()
            (out if tenant_of(it[1]) == tenant else rest).append(it)
        for it in rest:
            _jq_queue.append(it)
    return out


def _jq_take_all(max_n: int = 32) -> list:
    out = []
    with _jq_lock:
        while _jq_queue and len(out) < max_n:
            out.append(_jq_queue.popleft())
    return out


class _FakeReq:
    def __init__(self, token: str):
        self.headers = {"authorization": f"Token {token}"}


async def _jq_flush_tenant_async(tenant: str) -> None:
    """search前置flush: 同步消化该租户全部pending(证据完整性保证·秒级)。"""
    items = _jq_take_tenant(tenant)
    for _rid, _uid, _sid, _payload in items:
        try:
            await add(_FakeReq(API_KEY), _payload)
        except Exception as exc:
            log.error("jq flush FAILED rid=%s err=%s", _rid, type(exc).__name__)


async def _jq_worker() -> None:
    """JQ worker: 串行消化全租户pending(单worker无GIL thrash)·每拍让路事件循环。"""
    while True:
        try:
            items = _jq_take_all(32)
            for _rid, _uid, _sid, _payload in items:
                await add(_FakeReq(API_KEY), _payload)
            await asyncio.sleep(0.05)
        except Exception as exc:
            log.error("jq worker err=%s", type(exc).__name__)
            await asyncio.sleep(1)


@app.post("/add")
async def add(request: Request, payload: dict[str, Any] = Body(...)):
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="payload too large")
    _auth(request)
    t0 = time.time()
    rid = payload.get("request_id")
    uid = payload.get("user_id")
    sid = payload.get("session_id")
    if not isinstance(rid, str) or not rid or not isinstance(uid, str) or not isinstance(sid, str):
        raise HTTPException(status_code=400, detail="request_id/user_id/session_id required")
    # [刀8.12·journal-queue] journal先落(fsync崩溃安全)+pending入队+200即回(毫秒级)
    # ·ingest由后台worker串行消化·BRAINOS_JQ=0回退同步旧路径。
    if JQ_ON():
        if _jq_status.get(rid) != "pending":
            _journal_append(rid, uid, sid, payload.get("messages") or [])
            _jq_enqueue(rid, uid, sid, payload)
        resp = {"success": True, "request_id": rid, "user_id": uid, "session_id": sid}
        log.info("add JQ-QUEUED rid=%s uid=%s dt=%.1fms", rid, uid, (time.time() - t0) * 1000)
        return JSONResponse(content=resp)
    # 幂等: 同request_id重放→返回已存结果(不重复入库)
    stored = await run_in_threadpool(LEDGER.get, rid)
    if stored is not None:
        log.info("add replay rid=%s uid=%s sid=%s status=200 dt=%.1fms", rid, uid, sid,
                 (time.time() - t0) * 1000)
        return JSONResponse(content=json.loads(stored))
    try:
        cards = await run_in_threadpool(build_cards, payload, tenant_of(uid))
        # ── M3·经验蒸馏(deep_expdistill·轨道C·2026-10-25) ─────────────────────
        # 会话终态判定(防中途假pass=erank v3教训)→FIX/ANTI/COMPR英雄卡落边车
        # (card_construct.db exp_trace/exp_cards独立表·overlay_inject零感知)。
        # env门BRAINOS_EXP_DISTILL缺省关·fail-soft(任何失败只丢英雄卡绝不影响add)。
        try:
            _nexp = await run_in_threadpool(expd.observe_session_end, cards, tenant_of(uid))
            if _nexp:
                log.info("expdistill observe uid=%s hero_cards=%d", uid, _nexp)
        except Exception as exc:
            log.error("expdistill observe FAILED uid=%s err=%s", uid, type(exc).__name__)
        # 深度知识卡唯一路径=card_construct边车overlay: build_cards→derive_all
        # (注记+原子+deep_v3结构化事实)→搜索相overlay_inject注入浮顶。原deep_v2/
        # deep_construct两死块已摘(五禁令·实现唯一·20261009)。
        if cards:
            # W2-1时间线卡(确定性·零LLM·env BRAINOS_W2_TIMELINE=0关门): 会话事件链摘要卡
            # 并入同一add批; 失败只丢时间线卡绝不影响add语义(fail-soft)
            tl_cards = await run_in_threadpool(w2tl.session_cards, cards,
                                               str(payload.get("request_id", ""))) \
                if w2tl.ON() else []
            # W2-1双时间轴·事实时间面(BRAINOS_W2_DTFACE=0关门): 原文卡text前置
            # "[fact-time ISO | 人读]"单行=日期入卡面三通道; k1/k3/k6/k7观察仍吃
            # 未加面cards(零扰动), k2观察吃加面+时间线(内容面=闸面同源)。
            add_cards = await run_in_threadpool(w2tl.face_cards, cards,
                                                str(payload.get("request_id", ""))) \
                if (w2tl.ON() and os.environ.get("BRAINOS_W2_DTFACE", "1") != "0") else cards
            await run_in_threadpool(
                SVC.add, [ms._AddItem(**c) for c in add_cards + tl_cards], True, tenant_of(uid))
            # [B-Z吞吐刀20261019·499风暴根治] 原7把内联边刀(k1/k2/k3/k6/k7/c1q/dtb·
            # 各带sqlite写)整体移出200回包关键路径→_knife_worker后台串行消化。卡主数据
            # (SVC.add+ledger+快照)仍200前同步=持久化契约不破; 刀=派生增强索引, 延迟落地
            # 只降增强不丢卡(boot期backfill幂等可重建)。单笔add脱离GIL大部队→平台30s
            # 预算内必回→499/重试风暴终止。回滚=还原内联7刀(bak-bootfix-20261019)。
            _knife_enqueue(uid, str(tenant_of(uid)), cards, tl_cards, rid)
        resp = {"success": True, "request_id": rid, "user_id": uid, "session_id": sid}
        first = await run_in_threadpool(
            LEDGER.put, rid, uid, json.dumps(resp, ensure_ascii=False),
            json.dumps(cards, ensure_ascii=False))
        if not first:  # 并发同rid: 改回已存响应(幂等)
            resp = json.loads(await run_in_threadpool(LEDGER.get, rid) or "{}")
    except Exception as exc:
        log.error("add FAILED rid=%s uid=%s err=%s", rid, uid, type(exc).__name__)
        raise HTTPException(status_code=500, detail="internal error") from exc
    # 疫苗9: 响应自检断言 — success布尔/三ID逐字节回显; 不过=回5xx诚实失败, 绝不假200
    if not (isinstance(resp, dict) and resp.get("success") is True
            and resp.get("request_id") == rid and resp.get("user_id") == uid
            and resp.get("session_id") == sid):
        log.error("add SELF-CHECK FAILED rid=%s resp=%r", rid, resp)
        raise HTTPException(status_code=500, detail="response self-check failed")
    _journal_append(rid, uid, sid, payload.get("messages") or [])
    _LAST_SERVE[0] = time.time()
    log.info("add ok rid=%s uid=%s sid=%s cards=%d status=200 dt=%.1fms",
             rid, uid, sid, len(cards), (time.time() - t0) * 1000)
    return JSONResponse(content=resp)


# ── [刀8.2·A1单点路由] 词面形态判定+密度重排(纯函数·零题面内容·fail-soft) ──
_A1_SINGLE_RE = re.compile(
    r"\b\w+['\u2019]s\b"
    r"|\bwhat\s+(?:is|are|was|were)\b"
    r"|\bwhich\s+(?:city|country|color|colour|food|drink|job|company|school|university|book|movie|pet|car|phone|team|language)\b"
    r"|\bwhere\s+(?:does|do|did|is|are|was|were)\b"
    r"|的(?:是什么|在哪)", re.I)
_A1_HOP_GUARD_RE = re.compile(
    r"\b(?:timeline|order|sequence|chronolog\w*|chang\w*|both|between|summar\w*|impact|because|reason|why|"
    r"when\b|which\s+year|what\s+year|how\s+long|how\s+old|what\s+date|what\s+time|before|after|first|last)\b"
    r"|时间线|顺序|变化|为什么|因为|什么时候|什么时间", re.I)
_A1_STOP = frozenset({"what", "which", "where", "is", "are", "was", "were", "does", "do", "did",
                      "the", "a", "an", "of", "his", "her", "their", "its", "s", "and", "to", "in"})

def _is_single_point_query(query: str) -> bool:
    """单点属性query判定(形态信号): 命中单点形态 ∧ ≤14词 ∧ 无多跳/聚合/时序守卫词。"""
    q = str(query or "")
    if not q or len(q.split()) > 14:
        return False
    if _A1_HOP_GUARD_RE.search(q):
        return False
    return bool(_A1_SINGLE_RE.search(q))

def _a1_precision_rerank(query: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """判别词覆盖密度重排+缩窗: query内容词在卡面的覆盖数降序(同分保原序=stable)·
    截断至BRAINOS_A1_TOPK(缺省20·契约len≤top_k仍成立)。窗口集合只缩不增(禁4✓零增删)。"""
    cap = max(1, int(os.environ.get("BRAINOS_A1_TOPK", "20")))
    if len(data) <= cap:
        return data
    qt = set(re.findall(r"[a-z0-9']+|[\u4e00-\u9fff]", str(query or "").lower())) - _A1_STOP
    possessors = {m.group(1).lower() for m in re.finditer(r"\b(\w+)['\u2019]s\b", str(query or ""))}
    def _density(d: dict[str, Any]) -> tuple[int, int]:
        toks = set(re.findall(r"[a-z0-9']+|[\u4e00-\u9fff]", str(d.get("content", "")).lower()))
        ov = qt & toks
        rare = len(ov & possessors)  # [刀8.2b] 属主词命中=判别性稀有词·双倍权重
        return (len(ov) + rare, -len(str(d.get("content", ""))))
    return sorted(data, key=_density, reverse=True)[:cap]

# ── [U1·SVFEED 20261026·dev_u1] SPANVEC 重定位纯函数 ─────────────────────────
# 病灶(verdict_r2 实证): H3-v1 让 spanvec 分以 RRF 第三榜直接参与排序→挤占 organ 精确卡
#   (e8093 R@10 0.7171 → av2 0.5700; solve bugFix −3.1pp 全显著)。重定位=召回扩展器:
#   spanvec 命中卡不参与 RRF 融合, 按 span 分降序 append 到候选池尾部(低先验), 原
#   organ+直召回卡序逐位不变; 最终排序权 100% 交下游 rerank(BRAINOS_RERANK=1 时 0.6B
#   重排决定去留·建议配套 BRAINOS_RERANK_TOPN 覆盖池尾使重排可见), rerank 关闭时
#   spanvec 卡按 span 分降序沉尾=窗编排尾部淘汰先弃(对基线序零扰动)。
# 门: BRAINOS_SPANVEC=1(召回闸·沿用) ∧ BRAINOS_SVFEED=1(语义闸·缺省0=v1 第三榜原语义
#   逐位保留)。cap: BRAINOS_SVFEED_CAP(缺省15)·小 top_k 再按 top_k//4 收缩防整窗替换。
# 回滚=unset BRAINOS_SVFEED(退回 H3-v1 逐位原语义)+本两纯函数成死代码。

def _svfeed_on() -> bool:
    """SVFEED 语义闸: SPANVEC=1(召回总闸沿用) ∧ SVFEED=1。缺省/异常=False=原语义。"""
    try:
        return (os.environ.get("BRAINOS_SPANVEC", "0") == "1"
                and os.environ.get("BRAINOS_SVFEED", "0") == "1")
    except Exception:  # noqa: BLE001
        return False

def _svfeed_cap_items(sv_items: list[dict[str, Any]],
                      top_k: int) -> list[dict[str, Any]]:
    """SVFEED 有效补充卡: 入参保持 span 分降序(recall_by_spanvec 出序), 截
    BRAINOS_SVFEED_CAP(缺省15)·再按 top_k//4 收缩(小 top_k 防整窗替换·top_k≥4 时
    cap≥1)。纯函数·确定性。"""
    try:
        cap = max(1, int(os.environ.get("BRAINOS_SVFEED_CAP", "15") or 15))
    except (TypeError, ValueError):
        cap = 15
    try:
        tk = max(1, int(top_k))
    except (TypeError, ValueError):
        tk = 1
    return list(sv_items or [])[:min(cap, max(1, tk // 4))]

def _svfeed_tail_join(data: list[dict[str, Any]], sv_items: list[dict[str, Any]],
                      top_k: int) -> tuple[list[dict[str, Any]], int]:
    """沉尾入池(尾部槽位预留): data=原 organ+直召回卡序(fusion 后)·sv_items=span 分降序
    (调用方先经 _svfeed_cap_items 截额)。data[:top_k-n_sv] + sv_items → len≤top_k 契约
    不变, 原卡前缀序逐位不动, 被让位的仅是原序 rank≥top_k-n_sv 的尾段卡。
    返回 (joined, sv_n)。纯函数·确定性。"""
    try:
        tk = max(1, int(top_k))
    except (TypeError, ValueError):
        tk = 1
    sv = list(sv_items or [])[:tk]
    if not sv:
        return (list(data), 0)  # 空 sv=恒等(引擎侧 _sv_hold 空时本就不触发)
    keep = max(0, min(len(data), tk - len(sv)))
    return (list(data[:keep]) + sv, len(sv))

# [刀9·QA窗额20261023]: scriptmem流式QA回执卡("Streaming online QA feedback:"前缀)内嵌
# 题面文本, 短疑问句query在嵌入空间天然更贴近"问题格式卡"而非语料fact卡(格式偏置)。
# 库重建后A1实测10/10全QA窗=单点证据清零(v9→v10 A1=75→50主钉因)。刀: 终窗组装位对QA卡
# 配额(BRAINOS_QA_WIN_QUOTA·缺省0=关字节级旧路), 超额QA卡沉到全部非QA之后被预算裁掉,
# 非QA卡按原序回填; digest钉扎预算语义不变; 配套超采(ik抬到FETCHMULT×top_k)保证回填
# 弹药。门: BRAINOS_QA_WIN_GATE=1时仅单点形态query生效(其余query字节级旧路=结构性零
# 回归面), 缺省1。回滚=QUOTA=0。
_QA_CARD_PREFIX = "Streaming online QA feedback"

def _qa_window_trim(data: list[dict[str, Any]], top_k: int, query: str) -> list[dict[str, Any]]:
    """终窗QA配额裁剪(替代裸[:top_k]/钉扎预算裁剪位)。quota=0或门未命中→逐位等价旧路。"""
    _dm = lambda d: str(d.get("content", "")).startswith(("[LongMemSummary", "[digest"))
    try:
        quota = int(os.environ.get("BRAINOS_QA_WIN_QUOTA", "0") or "0")
    except ValueError:
        quota = 0
    if quota > 0:
        if os.environ.get("BRAINOS_QA_WIN_GATE", "1") == "1" \
                and not _is_single_point_query(query):
            quota = 0  # 非单点query=零突变(结构护栏)
        elif quota > 0:
            # [刀9.1·淹没门20261023·第二波v2]: 仅真淹没窗才触发 — data前top_k里QA卡
            # ≥FLOOD(缺省7)才算淹没; 轻度混窗(E2/D4/H2型)零触碰。wave2实测门形态过宽
            # (短问句维全中)致E2足迹, 收紧后只剩A1型病窗受益。
            try:
                _flood_n = int(os.environ.get("BRAINOS_QA_WIN_FLOOD", "7") or "7")
            except ValueError:
                _flood_n = 7
            _isqa0 = lambda d: str(d.get("content", "")).startswith(_QA_CARD_PREFIX)
            if sum(1 for d in data[:top_k] if _isqa0(d)) < _flood_n:
                quota = 0
    if quota <= 0:
        _pin_cnt = sum(1 for d in data if _dm(d))
        if _pin_cnt:
            _bud = max(top_k - _pin_cnt, 0)
            _o2 = []
            for d in data:
                if _dm(d):
                    _o2.append(d)
                elif _bud > 0:
                    _o2.append(d); _bud -= 1
            return _o2
        return data[:top_k]
    _isqa = lambda d: str(d.get("content", "")).startswith(_QA_CARD_PREFIX)
    _pin_cnt = sum(1 for d in data if _dm(d))
    _bud = max(top_k - _pin_cnt, 0)
    _o2: list[dict[str, Any]] = []
    _qa_kept = 0
    for d in data:
        if _dm(d):
            _o2.append(d)
        elif _isqa(d):
            if _qa_kept < quota and _bud > 0:
                _o2.append(d); _qa_kept += 1; _bud -= 1
            # [硬配额] 超额QA卡直接弃(不回填): 配额=硬顶, 非QA不足时允许窗<top_k(契约≤成立)
        elif _bud > 0:
            _o2.append(d); _bud -= 1
    return _o2


@app.post("/search")
async def search(request: Request, payload: dict[str, Any] = Body(...)):
    _auth(request)
    t0 = time.time()
    # [B-Z吞吐刀20261019·search相护栏] 刀队列排空(≤15s)再检索: 防C1时间锚/G4任务表等
    # 派生数据滞后于search伤分(写→检相交接处)。空队列=零等待逐位旧路。
    for _ in range(15):
        with _KNIFE_Q_LOCK:
            if not _KNIFE_Q:
                break
        await asyncio.sleep(1)
    # perf_c1 STAGE计时(20261011·只读插桩零行为变化): 每级perf_counter间隔ms·末尾单行落log
    _st: list[str] = []
    _pc = time.perf_counter
    _t_prev = [_pc()]
    _ens_h = None  # [M8] LLM集成器句柄(门关/fire失败=None=后续harvest/filter直通)

    def _mark(name: str) -> None:
        _now = _pc()
        _st.append(f"{name}={(_now - _t_prev[0]) * 1000:.1f}")
        _t_prev[0] = _now

    query = payload.get("query")
    uid = payload.get("user_id")
    if not isinstance(query, str) or not query or not isinstance(uid, str):
        raise HTTPException(status_code=400, detail="query/user_id required")
    try:
        top_k = int(payload.get("top_k") or 100)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="top_k must be int")
    top_k = max(1, min(top_k, ms.TOP_K_MAX))
    # [bzprobe·20261028] 官方T4功能探针(零记忆语义)毫秒级直返空数组:
    # 外部LLM故障期每条探针白烧ens超时×2+T1 20s全链(670条≈3.8h·拖爆总时长)
    # 双保险: startswith预筛+regex确认; 零模块级改动·单点插入
    if query.startswith(("ALF ", "DB ")):
        import re as _re_probe
        if _re_probe.match(r"^(ALF|DB) task index \d+$", query.strip()):
            return {"data": []}
    # [U-R45·F1 检索query去样板 20261028] qA/qB模板词(issue模板header行/"fix the bug
    # in"类样板前缀)在检索前剥离——样板词稀释语义信号(UF归因§3.1: qB开头"Describe the
    # bug"样板与垃圾卡词面同布=排序失配)。挡在一切检索之前=organ/BM25/指纹全链路受益。
    # 纯函数·零LLM·护栏=剥后<max(30,30%原长)即原样。env门BRAINOS_R45_QSTRIP缺省0=关
    # (逐字节原query·零日志)。fail-soft: 异常原query直通。
    if r45 is not None:
        try:
            query, _r45ns = r45.strip_boilerplate_ex(query)
            if _r45ns:
                log.info("R45 qstrip uid=%s removed=%d kept_chars=%d", uid, _r45ns, len(query))
        except Exception as _r45e:
            log.error("R45 qstrip FAILED uid=%s err=%s", uid, type(_r45e).__name__)
    _mark("r45q")
    # [M5·编程方言请求级判定(轨道E·20261025)] query方言检测(指纹+代码标记打分·env总闸
    # BRAINOS_CODING_WINDOW缺省0=关→恒False)→编程模式时抑制文本刀注入(k6 pattern/k12
    # struct/k7 digest三处守卫)·终窗编排归k13b后cw.apply()。fail-soft=恒False走原路。
    try:
        _cw_coding = cw.is_coding(query)
    except Exception:
        _cw_coding = False
    try:
        # knife1_1008: 超采补偿 — 内部多取该租户superseded数, 排除后仍按top_k填满(排除非降权)
        try:
            k1_extra = k1.extra_fetch(tenant_of(uid))
        except Exception:
            k1_extra = 0
        # knife2_1008: 超采补偿同款 — 多取该租户敏感卡数, 隐私闸滤除后仍按top_k填满
        try:
            k2_extra = k2.extra_fetch(tenant_of(uid))
        except Exception:
            k2_extra = 0
        # knife11·D3超采补偿(knife1/2同款): tombstoned卡滤除后仍按top_k填满
        try:
            k11_extra = k11.extra_fetch(tenant_of(uid))
        except Exception:
            k11_extra = 0
        ik = min(top_k + k1_extra + k2_extra + k11_extra, ms.TOP_K_MAX)
        # 刀9·QA窗额配套超采: 配额开启时多取FETCHMULT×top_k候选, 终窗配额裁剪位才有
        # 非QA回填弹药(否则QA淹没窗无米可回)。TOP_K_MAX=64仍为硬上限。
        try:
            _qaq = int(os.environ.get("BRAINOS_QA_WIN_QUOTA", "0") or "0")
        except ValueError:
            _qaq = 0
        if _qaq > 0:
            try:
                _qam = int(os.environ.get("BRAINOS_QA_WIN_FETCHMULT", "3") or "3")
            except ValueError:
                _qam = 3
            ik = min(max(ik, top_k * max(_qam, 1)), ms.TOP_K_MAX)
        _mark("xfetch")
        # [M8·轨道I·LLM集成器fire 20261025] expand/hyde两个LLM线程此处点火(SVC.search前)
        # ·与下方organ主检索完全并行(净增时延=收割等待·非LLM全程·先到先用不互等)。总门关
        # =armed()False=fire返回None=零开销直通。fail-soft: 点火失败不影响主检索。
        try:
            if ens.armed():
                _ens_h = ens.fire(query, tenant_of(uid), top_k, t0,
                                  svc=SVC, eid_fn=_evidence_id)
        except Exception as exc:
            log.error("ens fire FAILED uid=%s err=%s", uid, type(exc).__name__)
        # bzperf刀A(20261013·编程榜0分修复): deadline降级 — 16并发锁排队雪崩下平台60s超时
        # 空手是0分主因; 25s到期即用无锁recents原文卡回填次优证据, 绝不让平台空手回。
        # 纯性能/可用性刀, 检索语义零变更; 回滚=BRAINOS_SEARCH_DEADLINE_MS=0。
        # bz刀3两梯降级(20261015·60.34战报§四刀3): 单梯25s→梯度两梯。
        #   T1(BRAINOS_SEARCH_T1_MS·缺省12000·0=关): 主检索超T1未回→词面快车道
        #   (_lexical_fastlane·organ原文卡无锁只读·不碰organ锁)·标注degraded=lexical;
        #   wait_for超时语义对未起跑的排队future=撤销(锁工作量净减)·在跑者自然完成放锁。
        #   词面零命中→落T2。T2(DEADLINE_MS·25s不变): recents兜底升级场景块化
        #   (_recents_blocked·同session相邻卡整块呈现·禁单卡碎片)。
        #   degraded/evidence_score只入日志不入body(官方契约恰好3键·cfix2守卫同源)。
        #   回滚=BRAINOS_SEARCH_T1_MS=0(回单梯)+DEADLINE_MS=0(全关)+还原bak-knife3-dl2。
        _dl_ms = float(os.environ.get("BRAINOS_SEARCH_DEADLINE_MS", "25000"))
        _t1_ms = float(os.environ.get("BRAINOS_SEARCH_T1_MS", "12000"))
        if _t1_ms <= 0 or _t1_ms >= _dl_ms:
            _t1_ms = 0.0
        if _dl_ms > 0:
            import asyncio as _aio
            import concurrent.futures as _cf
            _pool = getattr(app.state, "_bz_sx_pool", None)
            if _pool is None:
                _pool = _cf.ThreadPoolExecutor(max_workers=24, thread_name_prefix="bzsx")
                app.state._bz_sx_pool = _pool
            try:
                out = await _aio.wait_for(
                    _aio.get_event_loop().run_in_executor(
                        _pool, lambda: SVC.search(query, None, ik, tenant_of(uid))),
                    timeout=(_t1_ms if _t1_ms > 0 else _dl_ms) / 1000.0)
            except _aio.TimeoutError:
                if _t1_ms > 0:  # 第一梯: 词面快车道(无锁轻量检索·零生成)
                    _mark("t1_lex")
                    out = _lexical_fastlane(query, tenant_of(uid), max(ik, top_k))
                    log.warning("search T1 %.0fms uid=%s topk=%d degraded=lexical "
                                "score=%.3f n=%d", _t1_ms, uid, top_k,
                                float(out.get("evidence_score", 0.0)),
                                len(out.get("evidence") or []))
                    if not out.get("evidence"):
                        _rec = _recents_blocked(tenant_of(uid), max(top_k, 24), query)
                        out = {"evidence": _rec, "evidence_score": 0.0}
                        log.warning("search T1 empty uid=%s degraded=blocked n=%d",
                                    uid, len(_rec))
                else:  # 第二梯: recents场景块化兜底(T1关/直达)
                    _mark("deadline_fb")
                    _rec = _recents_blocked(tenant_of(uid), max(top_k, 24), query)
                    log.warning("search DEADLINE %.0fms uid=%s topk=%d "
                                "fallback=recents degraded=blocked n=%d",
                                _dl_ms, uid, top_k, len(_rec))
                    out = {"evidence": _rec, "evidence_score": 0.0}
        else:
            out = await run_in_threadpool(SVC.search, query, None, ik, tenant_of(uid))
        _mark("organ")
    except Exception as exc:
        log.error("search FAILED uid=%s err=%s", uid, type(exc).__name__)
        raise HTTPException(status_code=500, detail="internal error") from exc
    # 合规①: 只回证据原文; id非空·content非空·created_at可溯源; ≤top_k 严格
    data = [{"id": _evidence_id(e), "content": e.get("text", ""), "created_at": e.get("dt", "")}
            for e in out.get("evidence", []) if e.get("text")]
    log.info("STAGE S1_after_organ n=%d", len(data))
    # [刀9·QA窗额·S1前置位20261023]: k5等中段刀会把候选裁回top_k=终窗配额位回填弹药蒸发
    # (AB实测qa_in=10全QA到终窗)。故quota开启时在organ出口先裁一次(下游各刀所见窗已无QA
    # 淹没), 终窗组装位再收口一次(契约保证)。fail-soft。
    try:
        if int(os.environ.get("BRAINOS_QA_WIN_QUOTA", "0") or "0") > 0:
            _s1_pre = len(data)
            data = _qa_window_trim(data, top_k, query)
            if len(data) != _s1_pre:
                log.info("qa window s1 trim uid=%s %d->%d", uid, _s1_pre, len(data))
    except Exception as exc:
        log.error("qa s1 trim FAILED uid=%s err=%s", uid, type(exc).__name__)
    # k-fill薄窗回填(20261009点火48.38法证): 零分维(C1 20/D1 0/F1 0/B2 0/G4 0)直接
    # 病灶=小租户查询词面不相交→organ命中0~3张→overlay空窗闸弃注入→评审0~3卡盲判。
    # 窗<FILL_MIN时以租户原文卡dt降序回填至FILL_N(命中在前·回填在后·确定性·零查询
    # 依赖=零题目先验只回所存证据)。回滚=摘本块+SVC.recents。
    try:
        _fmin = int(os.environ.get("BRAINOS_FILL_MIN", "8"))
        _fn = int(os.environ.get("BRAINOS_FILL_N", "24"))
        if os.environ.get("BRAINOS_FILL_MODE", "full") == "full":
            _fmin = max(_fmin, top_k)  # 满窗模式: 一直补到top_k(评审读全量·k2闸下游照滤)
        if len(data) < _fmin:
            _have = {d.get("content", "") for d in data}
            _rec = await run_in_threadpool(SVC.recents, tenant_of(uid), _fn)
            _added = 0
            for _e in _rec:
                _t = _e.get("text", "")
                if not _t or _t in _have:
                    continue
                data.append({"id": _evidence_id(_e), "content": _t,
                             "created_at": _e.get("dt", "")})
                _have.add(_t)
                _added += 1
            if _added:
                log.info("kfill thin-window uid=%s before=%d filled=%d now=%d",
                         uid, len(data) - _added, _added, len(data))
    except Exception as exc:
        log.error("kfill FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("kfill")
    # knife1_1008: 当前态意图(含无信号默认)→排除superseded卡; 历史/时间线意图→全量+dt升序(C3禁回退)
    # [刀8.10·v0.4.1] 零样本意图分类(OTHER直通零开销·F1/G4强制拉起对应管线)
    _intent = "OTHER"
    try:
        import knife_intent as _ki  # noqa: PLC0415
        _intent = _ki.classify(query, SVC)
    except Exception as _e:
        log.error("intent classify FAILED err=%s", type(_e).__name__)
    _mark("intent")
    k1_intent = "current"  # knife3消费(意图兜底: filter异常时按current=允许knife3排序)
    try:
        data, k1_intent, k1_drop = await run_in_threadpool(
            k1.filter_evidence, tenant_of(uid), query, data, top_k)
        if k1_drop:
            log.info("knife1 filter uid=%s intent=%s dropped=%d kept=%d",
                     uid, k1_intent, k1_drop, len(data))
    except Exception as exc:
        log.error("knife1 filter FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k1")
    log.info("STAGE S2_after_k1 n=%d", len(data))
    # knife5_1008·S3: 多跳扩查(触发门+预算硬门·二跳卡并入窗后同过k2/k3/k4门=零旁路)
    # perf_c1(20261011): 预算门时序冻结 — organ memo命中时按memoized organ实耗把k5的
    # t0虚拟回推, 预算决策="若无缓存基线会怎么判"(治暖态12ms<900ms门限翻转基线冷态
    # 1377ms>900ms的degrade决策→窗口差3卡·纯时序竞态非语义)。脏态/无memo=不回推=基线。
    try:
        _t0k5 = t0 - (out.get("_pc1_organ_ms") or 0.0) / 1000.0
        data, out = k5.multihop_expand(SVC, tenant_of(uid), query, data, out, _t0k5)
    except Exception as exc:
        log.error("knife5 multihop FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k5")
    # card_construct刀2/3·overlay: 边车派生卡(注记/原子)词面强匹配尾注 — 注入位
    # =knife5后knife2闸前→同过k2-k7全门=零旁路; organ索引零扰动=零回归由构造保证。
    # fail-soft: 注入失败=原窗直通。[M5守卫·扩展] 编程模式抑制struct/atom/CAUSAL
    # 结构族注入(验收"pattern/struct/digest注入为零"的家族成员)。
    try:
        if _cw_coding:
            cc_n = 0
        else:
            data, cc_n = cc.overlay_inject(tenant_of(uid), query, data, top_k)
        if cc_n:
            log.info("card_construct overlay uid=%s injected=%d kept=%d", uid, cc_n, len(data))
    except Exception as exc:
        log.error("card_construct overlay FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("cc")
    # W2-1·搜索相时间线注入(query显式日期→session确定性路由·≤2张插位1·rank0不动;
    # 注入后同过k2/k3等后续全门=零旁路·无日期信号query字节级直通=零回归面)。fail-soft。
    try:
        if w2tl.ON():
            data, w2_n = w2tl.timeline_inject(SVC, tenant_of(uid), query, data, top_k)
            if w2_n:
                log.info("w2 timeline inject uid=%s injected=%d kept=%d", uid, w2_n, len(data))
    except Exception as exc:
        log.error("w2 timeline inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("w2tl")
    # v091_f1b·摘要路由: 聚合型query(k7.is_aggregate白名单)→digest卡浮前位1-2
    # (F1/C2: master/session digest含全历史视野+时间线, 原始卡挤窗时沉底——此处提位·rank0不动·fail-soft)
    # 刀5·F1(20261015·60.34战报§二F1钉因): promote前加会话相关性gate — digest卡时间线
    # (年份/年月词面集)与query时间线集相交或内容词面重叠达阈才顶; 双方时间线互不相交
    # =异会话摘要不顶(fail-soft原序直通·跨会话digest降权·相关者按重叠降序优先)。
    # 全库均匀零定点(通用词类信号·阈值env化BRAINOS_K7_GATE_OV)。
    # [M5守卫·扩展] 编程模式抑制digest浮前路由(含纯聚合latest_digests直取)——与k7.inject
    # 尾注同族(digest家族)·验收"digest注入为零"的家族成员。
    try:
        if k7.is_aggregate(query) and not _cw_coding:
            _dig = [d for d in data
                    if str(d.get("content", "")).startswith(("SESSION DIGEST:", "KNOWLEDGE DIGEST"))]
            # [刀8.11·纯聚合直通20261024·F1探针4/18实锤]: 纯元query(无年份·无专名)时
            # 相关性gate的词面重叠恒0→digest全被否→F1空窗。纯聚合下digest全部同域相关:
            # 窗内无digest→k7.latest_digests直取; 相关性gate旁路按原序取2张直通。env门
            # BRAINOS_K811缺省1(开)。fail-soft。
            _pure_agg = False
            try:
                import re as _re811
                _caps = {w for w in _re811.findall(r"\b[A-Z][a-z]{2,}\b", str(query))} - {
                    "Give", "Summarize", "Provide", "Tell", "What", "How", "Describe",
                    "Everything", "The", "All", "Please", "Write", "Make", "List"}
                _pure_agg = (os.environ.get("BRAINOS_K811", "1") == "1" and not _caps
                             and not _re811.search(r"\b(?:19|20)\d{2}\b", str(query)))
            except Exception:
                _pure_agg = False
            if not _dig and _pure_agg:
                try:
                    _dig = await run_in_threadpool(k7.latest_digests, tenant_of(uid), 2)
                    if _dig:
                        log.info("digest route pure-agg fetch uid=%s fetched=%d", uid, len(_dig))
                except Exception as exc:
                    log.error("digest pure-agg fetch FAILED uid=%s err=%s", uid, type(exc).__name__)
            if _dig:
                if _pure_agg:
                    _rel = _dig[:2]
                else:
                    _rel = k7.related_digests(query, _dig)
                if _rel:
                    _rel_ids = {id(d) for d in _rel[:2]}
                    data = _rel[:2] + [d for d in data if id(d) not in _rel_ids]
                    log.info("digest route uid=%s promoted=%d gated=%d pure=%d",
                             uid, min(2, len(_rel)), len(_dig) - min(2, len(_rel)),
                             1 if _pure_agg else 0)
    except Exception as exc:
        log.error("digest route FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dgrt")
    # deep_jf: 官方JSON事实方言注入(D1新值浮顶/B2二跳链) — overlay之后同过后续刀闸
    try:
        data, jf_n = cfact.inject(tenant_of(uid), query, data, top_k)
        if jf_n:
            log.info("corpus_fact inject uid=%s injected=%d kept=%d", uid, jf_n, len(data))
    except Exception as exc:
        log.error("corpus_fact inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("cfact")

    # deep_tf: task-index点查浮顶位1 + TASK BRIEF摘要信号浮顶位2(I域/F1·rank0不动·fail-soft)
    try:
        data, dtb_n = dtb.inject(tenant_of(uid), query, data, top_k)
        if dtb_n:
            log.info("deep_taskbrief inject uid=%s injected=%d kept=%d", uid, dtb_n, len(data))
    except Exception as exc:
        log.error("deep_taskbrief inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dtb1")
    # deep_t4: 策略合规地图注入(A型禁题判定卡插位1/B型邻题地图卡插位2) — cfact后同过后续刀闸
    try:
        data, dp_n = dpol.inject(tenant_of(uid), query, data, top_k)
        if dp_n:
            log.info("deep_policy inject uid=%s injected=%d kept=%d", uid, dp_n, len(data))
    except Exception as exc:
        log.error("deep_policy inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dpol")
    # t3: 时序/成员资格预计算注入(AS-OF VERDICT判定卡/SEQUENCE聚合卡) — cfact之后同过后续刀闸
    try:
        data, dt_n = dtmp.inject(tenant_of(uid), query, data, top_k)
        if dt_n:
            log.info("deep_temporal inject uid=%s injected=%d kept=%d", uid, dt_n, len(data))
    except Exception as exc:
        log.error("deep_temporal inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dtmp")

    # deep_t2: 链闭包终值卡注入(B2多跳·评审直读终值·rank0不动) — cfact之后同过后续刀闸
    try:
        data, dcl_n = dcl.inject(tenant_of(uid), query, data, top_k)
        if dcl_n:
            log.info("deep_closure inject uid=%s injected=%d kept=%d", uid, dcl_n, len(data))
    except Exception as exc:
        log.error("deep_closure inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dcl")
    # deep_t1: as-of时间线判定卡注入(D1/C1预计算)
    try:
        data, tl_n = dtl.inject(tenant_of(uid), query, data, top_k)
        if tl_n:
            log.info("deep_timeline inject uid=%s injected=%d kept=%d", uid, tl_n, len(data))
    except Exception as exc:
        log.error("deep_timeline inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dtl")
    # [刀10·A1时变点查20261023]: 单点+显式日期+归属角色query→租户原文卡确定性词面
    # 点查(卡内日期≤query日期取最近=as-of)·最佳卡插位1/提位1·免疫实例嵌入漂移。
    # env门BRAINOS_A1_ASOF缺省0(关·字节级直通)。fail-soft。
    try:
        if _cw_coding:  # [M5守卫·扩展] 编程模式抑制as-of判定卡注入
            k10_n = 0
        else:
            data, k10_n = k10.inject(SVC, tenant_of(uid), query, data, top_k)
        if k10_n:
            log.info("knife10 a1asof uid=%s injected=%d kept=%d", uid, k10_n, len(data))
    except Exception as exc:
        log.error("knife10 a1asof FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k10")

    # Q1: QA预计算直答引擎
    try:
        data, qa_n = dqag.inject(tenant_of(uid), query, data, top_k)
        if qa_n:
            log.info("deep_qa_graph inject uid=%s injected=%d kept=%d", uid, qa_n, len(data))
    except Exception as exc:
        log.error("deep_qa_graph inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dqag")
    # T-F: TASK BRIEF + task index 注入
    try:
        data, tf_n = dtb.inject(tenant_of(uid), query, data, top_k)
        if tf_n:
            log.info("deep_taskbrief inject uid=%s injected=%d kept=%d", uid, tf_n, len(data))
    except Exception as exc:
        log.error("deep_taskbrief inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dtb2")

    # 刀4·G2(20261015·60.34战报§四刀4): RULE卡保底进窗 — rule-need query(选项列表/
    # 建议形/规则意图)且租户knife6规则卡在库→词面重叠top卡注入位1-2(rank0不动·≤2张·
    # fail-soft·既有三键契约·满窗仅顶替零词面尾卡=禁超窗)。病灶=医疗窗top5零RULE卡,
    # rule_first只重排不增删·RULE不在窗无从置顶——保底进窗是其生效前置。
    # [M5守卫] 编程模式抑制文本pattern注入(窗位让给英雄卡/证据原子卡)。
    try:
        if _cw_coding:
            rw_n = 0
        else:
            data, rw_n = k6.window_inject(tenant_of(uid), query, data, top_k)
        if rw_n:
            log.info("knife6 window uid=%s injected=%d kept=%d", uid, rw_n, len(data))
    except Exception as exc:
        log.error("knife6 window inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k6w")

    # T-E: RULE置顶(knife47修: 原 call 漏传 query 致 TypeError 被吞=静默 no-op,
    # 本刀恢复两参签名 — 重排语义零改动·仅让既有件真正生效)
    try:
        data = rf.rule_first_rerank(query, data)
    except Exception:
        pass
    _mark("rf")

    # S4: 四域补强(F1/H2/I域/G2)
    try:
        data, dmf_n = dmf.inject(tenant_of(uid), query, data, top_k)
        if dmf_n:
            log.info("deep_multifix inject uid=%s injected=%d kept=%d", uid, dmf_n, len(data))
    except Exception as exc:
        log.error("deep_multifix inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("dmf")

    log.info("STAGE S3_after_k5 n=%d", len(data))
    # knife2_1008·H2隐私闸: 泛化query→敏感卡滤除; 显式域query→该域放行(E3健康关怀守护);
    # fail-soft同knife1 — 失败只降级回"不过闸", 绝不影响search成功语义
    try:
        data, k2_drop = (await run_in_threadpool(k2.filter_gate, tenant_of(uid), query, data)) \
            if os.environ.get("BRAINOS_K2GATE", "0") == "1" else (data, 0)
        if k2_drop:
            log.info("knife2 gate uid=%s dropped=%d kept=%d", uid, k2_drop, len(data))
    except Exception as exc:
        log.error("knife2 gate FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k2g")
    log.info("STAGE S4_after_k2 n=%d", len(data))
    # knife11·D3 L3检索过滤闸(沙箱原型20261022·fail-soft同k2): 非审计意图→tombstoned卡+
    # 删除指令卡整卡摘除(指令卡span引被删原文=二次泄漏面·烟测P3实捉); 审计意图(query问
    # 删除事件本身)→全窗豁免+[DeletionRecord]指令原句卡位1注入(≤_MAX_INJECT·总量守top_k·
    # 用户原句摘录=纯抽取·禁4合规)。日志零内容只记计数(k2同款纪律)。
    try:
        data, k11_drop, k11_audit = await run_in_threadpool(
            k11.filter_gate, tenant_of(uid), query, data)
        if k11_audit:
            for _ac in k11_audit[:max(0, top_k - len(data))]:
                data.insert(1 if len(data) > 1 else len(data), _ac)
        if k11_drop:
            log.info("knife11 gate uid=%s dropped=%d kept=%d", uid, k11_drop, len(data))
    except Exception as exc:
        log.error("knife11 gate FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k11g")
    # knife3_1008·C1: 时间query检测→归一化日期注记(metadata附注·不改写content原文)+时距排序;
    # duration档双日期卡必回+日期差注记; order档会话时间位次标注(C2红利)。
    # k1 history意图(dt升序=C3资产)→knife3只注记零重排(禁打架)。fail-soft同前两刀。
    try:
        if _cw_coding:  # [M5守卫·扩展] 编程模式抑制k3时间注记/时序重排
            k3_tier, k3_hits = "none", 0
        else:
            data, k3_tier, k3_hits = await run_in_threadpool(
                k3.time_annotate, tenant_of(uid), query, data, k1_intent)
        if k3_hits:
            log.info("knife3 time uid=%s tier=%s anchored=%d kept=%d",
                     uid, k3_tier, k3_hits, len(data))
    except Exception as exc:
        log.error("knife3 time FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k3")
    log.info("STAGE S5_after_k3 n=%d", len(data))
    # knife4_1008·M2: 带内recency决胜(ledger created_ms新者在前·相邻分差<5%带内只换序
    # 不改集); k1 history时间线意图→让位(C3禁回退)。fail-soft同前三刀。
    try:
        data = k4.recency_reorder(tenant_of(uid), query, data, k1_intent, out, k3_tier)
    except Exception as exc:
        log.error("knife4 recency FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k4r")
    # knife6_1008·S7: 意图门置顶加成(规则/经验/聚合·有限加成·非意图字节级直通=死护栏;
    # k1 history与k3时间档让位; 敏感卡过k2放行门)。fail-soft同前三刀。
    try:
        data, k6_hits = await run_in_threadpool(
            k6.boost, tenant_of(uid), query, data, k1_intent, k3_tier)
        if k6_hits:
            log.info("knife6 boost uid=%s injected=%d kept=%d", uid, k6_hits, len(data))
    except Exception as exc:
        log.error("knife6 boost FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k6")
    # knife8_1009·D2: 矛盾显影(比较类query→k1已标矛盾对双卡成对必回+差异注记;
    # k5聚合让位/k3 order直通/duration重叠跳过·fail-soft同前刀)。
    # [刀8.4·k8d2熔断20261016·军报钉因: 索引heal退化窗内k8d2单次62-105s→3s预算
    #  asyncio.wait_for超时=带原窗先行(僵尸线程自终·fail-soft)·防单相拖死全请求]
    try:
        _k8_budget = float(os.environ.get("BRAINOS_K8D2_BUDGET_S", "3.0"))
        data, k8_pairs = await asyncio.wait_for(
            run_in_threadpool(
                k8d2.d2pair, SVC, tenant_of(uid), query, data, k1_intent, k3_tier),
            timeout=_k8_budget)
        if k8_pairs:
            log.info("knife8 d2pair uid=%s pairs=%d kept=%d", uid, k8_pairs, len(data))
    except asyncio.TimeoutError:
        log.warning("knife8 d2pair BUDGET %.1fs EXCEEDED uid=%s (proceed pre-k8d2 window)",
                    _k8_budget, uid)
    except Exception as exc:
        log.error("knife8 d2pair FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k8d2")
    # knife7_1009·F1: 聚合意图query(词典白名单)→digest卡追加入结果(只增益不夺位·命中数守卫:
    # 注入≤MAX_INJECT且前缀逐位保留绝不截断真实卡; 非聚合query零突变=证据零digest混入)。
    # fail-soft同前四刀。[M5守卫] 编程模式抑制digest尾注注入。
    try:
        if _cw_coding:
            k7_n, k7_before = 0, len(data)
        else:
            data, k7_n, k7_before = k7.inject(tenant_of(uid), query, data, top_k,
                                          force_agg=(_intent == "F1"))
        if k7_n:
            log.info("knife7 digest uid=%s injected=%d hits %d->%d",
                     uid, k7_n, k7_before, k7_before + k7_n)
    except Exception as exc:
        import traceback as _tb
        log.error("knife7 inject FAILED uid=%s err=%s stack=%s",
                  uid, type(exc).__name__, _tb.format_exc()[-500:])
    _mark("k7")
    # knife8_rank·最终排序位: 四机制(专名boost/覆盖重排/缝链插位/组带外晋升)·
    # 缺省关(BRAINOS_K8=0直通)·fail-soft。
    try:
        if k3_tier == "order":
            log.info("knife8 yield to k10 order-tier uid=%s", uid)
        else:
            data = k8.rerank(tenant_of(uid), query, data, k1_intent, k3_tier, out, top_k)
        if os.environ.get("BRAINOS_K8", "0") == "1":
            log.info("knife8 rank uid=%s n=%d stats=%s", uid, len(data), k8.stats())
    except Exception as exc:
        log.error("knife8 rank FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k8rk")
    # c1_quad·C1四路融合重排(截断前最后排序位): 语义位次+词面重叠+实体词典+时间索引
    # 线性融合; 实体/时间两路新召回卡(organ窗外)先过k2同口径隐私闸再入竞争。总闸
    # ZFAIOS_FUSION缺省0=关字节直通; rank0不动(ZFAIOS_FUSION_KEEP_TOP1)。fail-soft。
    try:
        data = c1q.fusion_rerank(SVC, tenant_of(uid), query, data, top_k)
    except Exception as exc:
        log.error("c1 fusion FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("c1q")
    # [刀8.6d·digest钉扎20261017]: c1q融合按词面把低交摘要卡沉尾→[:top_k]裁掉=F1弹药
    # 蒸发(实测inject后10→erank前8)。digest卡(前缀钉扎)不参与截断淘汰·非digest裁至
    # (top_k-digest数)·总量仍≤top_k(契约✓)·序保持融合序。
    # [刀9·QA窗额20261023]: 裁剪位统一收口到_qa_window_trim — quota=0/门未命中=逐位
    # 等价旧路(钉扎预算分支内联复制); 配额开=QA超额卡沉尾·非QA原序回填。
    _pre_qa_n = sum(1 for d in data
                    if str(d.get("content", "")).startswith(_QA_CARD_PREFIX))
    data = _qa_window_trim(data, top_k, query)
    if _pre_qa_n:
        log.info("qa window trim uid=%s before=%d qa_in=%d n=%d topk=%d",
                 uid, len(data), _pre_qa_n, len(data), top_k)
    # [U-R45·F2/F3/F4 应用层加权 20261028] organ融合分之后·重排(erank/k13b/0.6B)之前:
    #  F2 query命中repo标识(路径首段/包名/GitHub slug)→同repo卡×1.5(UF归因§4.4);
    #  F3 跨query高频垃圾卡×0.5(全局入窗计数·跨租户聚合·ridge样板卡23/67窗主修);
    #  F4 蒸馏/英雄体例卡声称路径在租户卡文本全无→×0.3(C3毒卡存在性机检)。
    #  weight=(repo_w×hf_w×path_w)·key=(n-i)×w稳定降序=只动序不动集不改内容·
    #  全权1.0=输出序=输入序(逐位等价)。corpus懒取(仅F4开才读租户卡文本·无锁只读快照)。
    #  env门BRAINOS_R45_REPO/R45_HFCUT/R45_PATHCHK各自缺省0=关。fail-soft=异常原窗直通。
    if r45 is not None:
        try:
            data, _r45mv = r45.apply_weights(
                data, query,
                lambda: _tenant_cards_unlocked(tenant_of(uid)),
                tenant=str(tenant_of(uid)))
            if _r45mv:
                log.info("R45 weights uid=%s moved=%d n=%d", uid, _r45mv, len(data))
        except Exception as _r45e:
            log.error("R45 weights FAILED uid=%s err=%s", uid, type(_r45e).__name__)
    _mark("r45w")
    # f2_1012·代码证据重排: 代码语境query→[tool_result]/diff卡浮前·[thinking]叙述卡让位
    # (冒烟0分结构根因修复·只动序不动内容·非代码query原序逐位=文本赛道零回归·fail-soft)。
    try:
        data = erank.rerank(data, query)
    except Exception as exc:
        log.error("evidence rerank FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("erank")
    # [M1/M2·轨道B指纹+图谱加权20261025] query同款指纹→窗卡重叠度硬加权(k13: 路径3.0>
    # errsig2.5>测试1.5>符号1.0>命令0.5·跨社区×0.4软降权绝不硬删) + 引用号1跳×2.0/
    # 2跳×1.4(crf·社区联合校验)。位形=erank后a1r前(过滤闸全过·只动序不动集·rank0钉扎
    # ·零分卡稳定保原序·fail-soft=失败原窗直通)。cand_texts给k13/crf做窗id漂移解引用。
    # [M1.5·直召回∪organ候选20261025] 加权前先k13.recall_by_fingerprint直召回(≤40·
    # env BRAINOS_K13_RECALL缺省1随K13总闸)——治"gold不在organ候选池=boost无效"结构病。
    # 合并排序=加权RRF(直召回序×W_REC + 基窗序×W_BASE·k=60·rank-based无尺度病): 直召回
    # 强卡浮前·基窗前排卡不被弱指纹卡(符号1.0级)整卡挤出(记分板PhaseB实证: 纯boost分数
    # 堆叠使强基窗gold被噪声直召回卡挤出窗·never-hit 99→197)。k13/crf boost分仅作微
    # tie-break(×0.0005)。直召回卡过k11墓碑闸+k2隐私闸同口径·fail-soft·裁回top_k。
    try:
        _rec_n = 0
        _rec_order: dict[str, int] = {}
        _sv_order: dict[str, int] = {}  # [P·SPANVEC] 第三榜: 分片直召回序(独立BRAINOS_SPANVEC门)
        # [U1·SVFEED 20261026] 语义闸: SPANVEC=1∧SVFEED=1 → spanvec 命中卡=召回扩展器
        # (沉尾喂重排·不参与RRF); 缺省 SVFEED=0 → H3-v1 第三榜 RRF 原语义逐位保留。
        _svfeed = _svfeed_on()
        _sv_hold: list[dict[str, Any]] = []
        if os.environ.get("BRAINOS_K13_RECALL", "1") == "1":
            try:
                _rec = k13.recall_by_fingerprint(tenant_of(uid), query, 40)
            except Exception:
                _rec = []
            if _rec:
                _have_ids = {str(d.get("id", "")) for d in data}
                _have_txt = {str(d.get("content", "")) for d in data}
                _rec_items = []
                for _ri, r in enumerate(_rec):
                    _rid, _rcontent = str(r.get("id", "")), str(r.get("content", ""))
                    if not _rid or not _rcontent or _rcontent in _have_txt:
                        continue
                    if _rid in _have_ids:
                        _rec_order[_rid] = _ri  # 已在窗的直召回卡=双榜佐证·记其直召回序
                        continue
                    _rec_items.append({"id": _rid, "content": _rcontent,
                                       "created_at": str(r.get("created_at")
                                                         or r.get("dt") or "")})
                    _rec_order[_rid] = _ri
                if _rec_items:
                    try:  # 墓碑闸同口径(k11.filter_gate·审计分支返回注入卡忽略)
                        _rec_items, _r11d, _ = k11.filter_gate(
                            tenant_of(uid), query, _rec_items)
                    except Exception:
                        pass
                    if os.environ.get("BRAINOS_K2GATE", "0") == "1":
                        try:
                            _rec_items, _ = k2.filter_gate(
                                tenant_of(uid), query, _rec_items)
                        except Exception:
                            pass
                if _rec_items:
                    data = data + _rec_items
                    _rec_n = len(_rec_items)
        # [P·SPANVEC·L4分片直召回20261025] 与k13.recall_by_fingerprint并列第二直召回源:
        # query×分片批量点积→父卡回溯(score=max片分+0.3×(命中片数-1))→完整父卡入RRF第三榜。
        # 治"长卡整卡向量语义稀释"(gold埋长卡中部query按片直接命中)。去重同k13口径(id+
        # content精确+剥fact-time面content_hash回查·organ卡text带date_anchor尾时仍可对齐);
        # k11墓碑闸+k2隐私闸同口径。env BRAINOS_SPANVEC缺省0=关=本块整体跳过(字节等价)。
        try:
            if os.environ.get("BRAINOS_SPANVEC", "0") == "1":
                _sv = spv.recall_by_spanvec(
                    tenant_of(uid), query, 25,
                    qmodel=getattr(getattr(SVC, "organ", None), "_qmodel", None))
            else:
                _sv = []
        except Exception:
            _sv = []
        if _sv:
            _have_ids = {str(d.get("id", "")) for d in data}
            _have_txt = {str(d.get("content", "")) for d in data}
            _have_hash = {k13.content_hash_of(str(d.get("content", ""))) for d in data}
            _sv_items = []
            for _si, r in enumerate(_sv):
                _sid, _scontent = str(r.get("id", "")), str(r.get("content", ""))
                if not _sid or not _scontent or _scontent in _have_txt:
                    continue
                if _sid in _have_ids or k13.content_hash_of(_scontent) in _have_hash:
                    _sv_order[_sid] = _si  # 已在窗(k13/organ同卡)=双/三榜佐证·记其分片序
                    continue
                _sv_items.append({"id": _sid, "content": _scontent,
                                  "created_at": str(r.get("created_at") or r.get("dt") or "")})
                _sv_order[_sid] = _si
            if _sv_items:
                try:  # 墓碑闸同口径(k11.filter_gate·审计分支返回注入卡忽略)
                    _sv_items, _s11d, _ = k11.filter_gate(tenant_of(uid), query, _sv_items)
                except Exception:
                    pass
                if os.environ.get("BRAINOS_K2GATE", "0") == "1":
                    try:
                        _sv_items, _ = k2.filter_gate(tenant_of(uid), query, _sv_items)
                    except Exception:
                        pass
            if _sv_items:
                if _svfeed:  # [U1·SVFEED] 不并入不扰动·暂扣待融合后沉尾(候选池底部=低先验)
                    _sv_hold = _sv_items
                    log.info("svfeed hold uid=%s cards=%d board=%d (tail-feed·no rrf)",
                             uid, len(_sv_items), len(_sv_order))
                else:
                    data = data + _sv_items
                    log.info("spanvec recall uid=%s +cards=%d board=%d", uid,
                             len(_sv_items), len(_sv_order))
        _b_ids = [str(d.get("id", "")) for d in data]
        _b_texts = {str(d.get("id", "")): str(d.get("content", "")) for d in data}
        _k13b = k13.search_boost(tenant_of(uid), query, _b_ids, _b_texts)
        _crb = crf.boost_by_refs(tenant_of(uid), query, _b_ids, _b_texts)
        _bscore = {c: float(_k13b.get(c, 0.0)) + float(_crb.get(c, 0.0))
                   for c in set(_k13b) | set(_crb)}
        if len(data) > 1 and (_bscore or _rec_order
                              or (_sv_order and not _svfeed)):  # [U1·SVFEED] sv不触发融合
            if _rec_order or (_sv_order and not _svfeed):
                _rrf_k = max(1.0, float(os.environ.get("BRAINOS_K13_RRF_K", "60") or 60))
                _w_rec = max(0.0, float(os.environ.get("BRAINOS_K13_RRF_WREC", "1.0") or 1.0))
                _w_base = max(0.0, float(os.environ.get("BRAINOS_K13_RRF_WBASE", "0.5") or 0.5))
                _w_sv = max(0.0, float(os.environ.get("BRAINOS_SV_RRF_W", "1.0") or 1.0))
                if _svfeed:  # [U1·SVFEED] 基窗序只排 organ+直召回·spanvec 卡不入融合榜
                    _base_rank13 = {str(d.get("id", "")): i for i, d in enumerate(data)
                                    if str(d.get("id", "")) not in _rec_order}
                else:
                    _base_rank13 = {str(d.get("id", "")): i for i, d in enumerate(data)
                                    if str(d.get("id", "")) not in _rec_order
                                    and str(d.get("id", "")) not in _sv_order}
                _key13 = {}
                for d in data:
                    _i = str(d.get("id", ""))
                    _s = 0.0
                    if _i in _rec_order:
                        _s += _w_rec / (_rrf_k + _rec_order[_i] + 1)
                    if _i in _base_rank13:
                        _s += _w_base / (_rrf_k + _base_rank13[_i] + 1)
                    if _i in _sv_order and not _svfeed:  # [P·SPANVEC] 第三榜(v1·SVFEED=1停用)
                        _s += _w_sv / (_rrf_k + _sv_order[_i] + 1)
                    _key13[_i] = _s + 0.0005 * _bscore.get(_i, 0.0)  # boost分微tie-break
                _h13, _r13 = data[:1], data[1:]  # rank0钉扎(家规·top1不动)
                _r13.sort(key=lambda d: -_key13.get(str(d.get("id", "")), 0.0))  # stable
                data = (_h13 + _r13)[:top_k] if len(data) > top_k else _h13 + _r13
                log.info("k13/crf rrf uid=%s recall=+%d rec=%d base=%d hit=%d sv=%d",
                         uid, _rec_n, len(_rec_order), len(_base_rank13), len(_bscore),
                         len(_sv_order))
            elif _bscore:
                _h13, _r13 = data[:1], data[1:]  # rank0钉扎(家规·top1不动)
                _r13.sort(key=lambda d: -_bscore.get(str(d.get("id", "")), 0.0))  # stable
                data = _h13 + _r13
                log.info("k13/crf boost uid=%s recall=+%d hit=%d top=%.2f",
                         uid, _rec_n, len(_bscore), max(_bscore.values()))
        # [U1·SVFEED] 融合/排序完成后沉尾入池(尾部槽位预留·span分降序·len≤top_k):
        # 原卡前缀序逐位不动; 被让位的仅是原序 rank≥top_k-n_sv 尾段卡。BRAINOS_RERANK=1
        # 时下游 0.6B 重排对全池(含 sv 尾)决定去留; rerank 关闭/未触发=sv 卡按 span 分
        # 降序沉尾, 窗编排尾部淘汰先弃=对基线窗零扰动。
        if _svfeed and _sv_hold:
            _pre_sv = len(data)
            data, _sv_n = _svfeed_tail_join(
                data, _svfeed_cap_items(_sv_hold, top_k), top_k)
            log.info("svfeed tail uid=%s base=%d->%d sv=%d topk=%d",
                     uid, _pre_sv, len(data), _sv_n, top_k)
        if len(data) > top_k:  # 直召回并入后安全裁剪(契约≤top_k·零分卡稳定沉尾)
            data = data[:top_k]
    except Exception as exc:
        log.error("k13/crf boost FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k13b")
    # [M8·轨道I·LLM集成器harvest 20261025] 收割expand/hyde先到者(50ms轮询·共享等待预算
    # BRAINOS_ENS_WAIT_S·全局死线t0+10s超时跳过·先到先用不互等)→变体/假想卡检索榜与原窗
    # RRF融合(k=60·rank0钉扎·新卡过k11墓碑闸(+k2可选)与k13b同口径·裁回top_k)。
    # fail-soft: 收割失败=原窗直通。_ens_h=None(门关)=逐字节直通。
    try:
        if _ens_h is not None:
            _pre_ens = len(data)
            data = ens.harvest(_ens_h, data, query, tenant_of(uid), top_k)
            if len(data) != _pre_ens:
                log.info("ens harvest uid=%s %d->%d", uid, _pre_ens, len(data))
    except Exception as exc:
        log.error("ens harvest FAILED uid=%s err=%s", uid, type(exc).__name__)
    # ── M5·编程方言窗口编排器(轨道E·20261025) ─────────────────────────────────
    # 编程query(方言检测≥阈)→终窗编排: ①expd英雄卡置顶位1-3(id+content去重prepend)
    # ②轨道D重排器接线预留(rerank_hook·env BRAINOS_RERANK_URL可达才调·失败原序)
    # ③自适应窗k∈[CW_KMIN,CW_KMAX]+字符预算CW_BUDGET尾部淘汰(英雄卡豁免)·硬契约
    # len≤top_k·id/content非空。非编程query=apply原样返回(零改动路径·字节级等价)。
    # fail-soft: 任何异常=原窗直通。env总闸BRAINOS_CODING_WINDOW缺省0=关。
    try:
        _cw_pre = len(data)
        data = cw.apply(data, query, tenant_of(uid), top_k)
        if len(data) != _cw_pre:
            log.info("coding window uid=%s %d->%d (hero+adapt)", uid, _cw_pre, len(data))
    except Exception as exc:
        log.error("coding window FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("cwin")
    # [U-R45·F4b 毒英雄卡沉底 20261028] cw.apply把英雄卡置顶位1-3后收口: 窗头(前6位)
    # 中蒸馏/英雄体例 ∧ 声称路径在租户卡文本全不存在 → stable移至窗尾(UF归因§3.3:
    # 蒸馏毒卡可读性更好排名更前被照抄=C3锚定10题主通道; 英雄位下沉=不再锚定补丁)。
    # 只动序不动集·窗成员零变化。env门随F4(BRAINOS_R45_PATHCHK)·关=零操作零日志。
    if r45 is not None:
        try:
            data, _r45hm = r45.demote_poison_heroes(
                data, lambda: _tenant_cards_unlocked(tenant_of(uid)),
                tenant=str(tenant_of(uid)))
            if _r45hm:
                log.info("R45 poison hero demote uid=%s moved=%d", uid, _r45hm)
        except Exception as _r45e:
            log.error("R45 hero demote FAILED uid=%s err=%s", uid, type(_r45e).__name__)
    _mark("r45h")
    # [M8·轨道I·LLM集成器filter 20261025] 终窗LLM精滤: query+前12卡各前200字符→
    # {"keep":[id]}。护栏keep∈[3,12]·越界/解析失败/超时/死线过期→全部保留(SWEContextBench:
    # 选错上下文=负收益·误删风险用保守护栏兜)。只删不增不重排(契约len≤top_k维持)。
    # fail-soft: 精滤失败=原窗直通。_ens_h=None(门关)=逐字节直通。
    try:
        if _ens_h is not None:
            _pre_f = len(data)
            data = ens.apply_filter(_ens_h, data, query)
            if len(data) != _pre_f:
                log.info("ens filter uid=%s %d->%d", uid, _pre_f, len(data))
    except Exception as exc:
        log.error("ens filter FAILED uid=%s err=%s", uid, type(exc).__name__)
    # [刀8.2·A1单点路由20261016·62.09战报A1=33.33钉因库3.6倍膨胀语义粗排噪声稀释单点取值]:
    # 单点属性query(词面形态信号·零题面内容·禁2✓)→判别词覆盖密度重排+缩窗(100→A1_TOPK)。
    # 三重护栏零触碰: 聚合意图(k7)/时序意图(dtl._TEMPORAL_INTENT·C1新刀域)/规则意图(k6)
    # →B=100/E=100/C3=100路径字节级不变。env门BRAINOS_A1_ROUTE缺省0(AB对照期关·验后开)。
    try:
        if os.environ.get("BRAINOS_A1_ROUTE", "0") == "1" and _is_single_point_query(query):
            _a1_skip = False
            try:
                import knife7_digest as _k7r  # noqa: PLC0415
                _a1_skip = _k7r.is_aggregate(query)
            except Exception:
                pass
            try:
                _a1_skip = _a1_skip or bool(k6.query_kinds(query))
            except Exception:
                pass
            if not _a1_skip:
                _a1_pre = len(data)
                data = _a1_precision_rerank(query, data)
                log.info("a1 route uid=%s %d->%d (single-point shrink)", uid, _a1_pre, len(data))
    except Exception as exc:
        log.error("a1 route FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("a1r")
    # [刀8.3·D2版本门+冲突注记20261016]: 旧值卡沉尾+双卡冲突对注记(引用原文·禁4✓)。
    # env门BRAINOS_D2_VER缺省0(AB对照期关)。D1=66.67既有链路零触碰(单一实现路径分立)。
    try:
        if os.environ.get("BRAINOS_D2_VER", "0") == "1":
            _d2_pre = len(data)
            data = k1.d2_window_gate(tenant_of(uid), query, data, top_k)
            if len(data) != _d2_pre:
                log.info("d2 gate uid=%s %d->%d (note injected)", uid, _d2_pre, len(data))
    except Exception as exc:
        log.error("d2 gate FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("d2")
    # [刀8.9·C2真重排20261017·C域军报: order档metadata被3键契约剥除=死负载] 顺序意图
    # query→窗卡按created_at升序稳定重排(位0不动·dt并列保原序)·env门BRAINOS_K3_ORDER。
    try:
        if k3_tier == "order" and os.environ.get("BRAINOS_K3_ORDER", "1") == "1" and len(data) > 2:
            head, rest = data[:1], data[1:]
            rest = sorted(rest, key=lambda d: (not str(d.get("created_at", "")),
                                               str(d.get("created_at", ""))))  # unknown沉尾·有dt升序
            data = head + rest
            log.info("c2 resort uid=%s n=%d (order tier)", uid, len(data))
    except Exception as exc:
        log.error("c2 resort FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("c2r")
    # [刀8.7·G4枚举卡]: G4形query→数字事实线按时间枚举位1注入(纯摘录·条件触发)
    try:
        if os.environ.get("BRAINOS_G4_ENUM", "1") == "1":
            import knife9_g4 as _k9  # noqa: PLC0415
            if _cw_coding:  # [M5守卫·扩展] 编程模式抑制G4枚举/保守汇总卡注入
                data = data
            else:
                data = _k9.g4_enum_card(query, data, top_k, force=(_intent == "G4"))
    except Exception as exc:
        log.error("g4 enum FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("g4")
    # W3·A3结构卡inject(位形=knife9_g4同位家族·csan前): 非A3 query字节级直通,
    #  列出所有/how many/字段值query→[StructSet]/[StructCount]/[StructField]专卡位1注入
    #  [M5守卫] 编程模式抑制struct结构卡注入。
    try:
        if _cw_coding:
            k12_n = 0
        else:
            data, k12_n = k12.inject(tenant_of(uid), query, data, top_k)
        if k12_n:
            log.info("knife12 struct uid=%s injected=%d kept=%d", uid, k12_n, len(data))
    except Exception as exc:
        log.error("knife12 struct inject FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("k12")
    # [修20261025-3e] 尾注清扫: k9/k12末端注入的结构卡在k11门之后入窗会绕过墓碑
    # (实测"phone = 139…(as of …)"谓词卡漏出)·csan前再扫一遍被删字面量。
    try:
        data = k11.sweep_literals(tenant_of(uid), data)
    except Exception as exc:
        log.error("tomb sweep FAILED uid=%s err=%s", uid, type(exc).__name__)
    # sanitize_1012·出口content消毒(剥内部标记链·mm同款病修复·fail-soft)
    try:
        data = csan.sanitize(data)
    except Exception as exc:
        log.error("content sanitize FAILED uid=%s err=%s", uid, type(exc).__name__)
    _mark("csan")
    # cfix2_1009·契约守卫: 官方契约恰好3键{id,content,created_at}——k10/k8注记只入留痕
    # 不入响应(metadata为内部排序用·客户端多键=SEARCH_API_CONTRACT_MISMATCH)。置于自检前。
    # cfix3_1009·契约时间戳守卫: created_at必须ISO合法(官方校验点)——诚实哨兵"unknown"/
    # 空值→入库收据时刻(ISO)·事件时间语义诚实保留在content的[Time:]标记; 排序链已用
    # ledger时序/obs_seq(墙钟免疫)不依赖本字段=零回归。
    log.info("STAGE S11_boundary n=%d", len(data))
    # qlog: 官方题目可见性(法证基建) — query全文+前5窗摘要
    try:
        _prev = ["%s:%s" % (str(d.get("id", ""))[:8], str(d.get("content", ""))[:60].replace("\n", " "))
                 for d in data[:5]]
        log.info("QLOG q=%r win=%d top5=%s", query, len(data), " | ".join(_prev))
    except Exception:
        pass
    _mark("qlog")
    data = [{"id": d["id"], "content": d["content"],
             "created_at": (d["created_at"] if re.match(r"^\d{4}-\d{2}-\d{2}T", str(d.get("created_at", ""))) else time.strftime("%Y-%m-%dT%H:%M:%S"))}
            for d in data]
    # 疫苗9: 响应自检断言 — data键/id非空/content非空/len≤top_k严格; 不过=5xx诚实失败
    if len(data) > top_k or not all(d["id"] and d["content"] for d in data):
        log.error("search SELF-CHECK FAILED uid=%s n=%d top_k=%d", uid, len(data), top_k)
        raise HTTPException(status_code=500, detail="response self-check failed")
    # "options" 等额外字段: 有意忽略(容错不炸)
    # kfix_1009·查询留痕: 官方评测取证开窗(query+返回证据·我方运维遥测)
    if os.environ.get("BRAINOS_TRACE", "1") != "0":
        try:
            with open("/home/zfai/zo_deploy/logs/query_trace.log", "a", encoding="utf-8") as _tf:
                _tf.write(json.dumps({"ts": time.strftime("%F %T"), "uid": uid[:16], "q": query,
                                      "top_k": top_k,
                                      "ret": [{"i": d["id"], "c": d["content"][:80]} for d in data[:10]]},
                                     ensure_ascii=False) + "\n")
        except Exception:
            pass
    _mark("trace")
    _LAST_SERVE[0] = time.time()
    log.info("STAGES uid=%s %s total=%.1fms", str(uid)[:24], " ".join(_st),
             (time.time() - t0) * 1000)
    log.info("search ok uid=%s top_k=%d hits=%d status=200 dt=%.1fms",
             uid, top_k, len(data), (time.time() - t0) * 1000)
        # cfix4_1015·契约id唯一守卫: 官方校验id唯一性(重复id=SEARCH_API_CONTRACT_MISMATCH实锤20261015
    # 被拒体取证/rejected_190839_38870 id=1d295a7a双卡)——保首去重·零语义损失
    try:
        _seen_ids = set()
        _dedup = []
        for _d in data:
            _k = str(_d.get("id", ""))
            if _k in _seen_ids:
                continue
            _seen_ids.add(_k)
            _dedup.append(_d)
        if len(_dedup) != len(data):
            log.warning("id dedup %d->%d (duplicate id guard)", len(data), len(_dedup))
            data = _dedup
        # [刀8.5·内容级去重20261016]: 同文本异id多版本(平台重试/journal重放异dt)——
        # id守卫拦不住→同卡2-3次占窗(实测1窗丢2-3位=多样性证据损失)。保首去重·零语义损失。
        try:
            _seen_c = set()
            _cdedup = []
            for _d in data:
                _ck = str(_d.get("content", ""))
                if _ck in _seen_c:
                    continue
                _seen_c.add(_ck)
                _cdedup.append(_d)
            if len(_cdedup) != len(data):
                log.warning("content dedup %d->%d (duplicate content guard)",
                            len(data), len(_cdedup))
                data = _cdedup
        except Exception as exc:
            log.error("content dedup FAILED err=%s", type(exc).__name__)
    except Exception as exc:
        log.error("id dedup FAILED err=%s", type(exc).__name__)
    # [U-R45·F3 入窗计数 20261028] 终窗卡全局入窗计数(跨query跨租户聚合·进程内计数器
    # ·高频垃圾卡F3降权的弹药)。只写内存·零响应面·门关(BRAINOS_R45_HFCUT=0)=零开销。
    if r45 is not None:
        try:
            r45.note_served(data)
        except Exception:  # noqa: BLE001 — 计数绝不伤响应
            pass
    return JSONResponse(content={"data": data})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("ZO_HOST", "127.0.0.1"),
                port=int(os.environ.get("ZO_PORT", "8090")), workers=1,
                timeout_keep_alive=int(os.environ.get("ZO_KEEPALIVE", "65")),
                log_level="warning")
