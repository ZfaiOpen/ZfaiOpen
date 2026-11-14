#!/bin/bash
# BRAIOS arm-v5 升级版·VPS 引擎启动脚本 (staging 20261028 · 基于在役 start_engine.sh arm-final 冻结配置)
# 变更=纯增(在役 arm-final 行为逐行保留):
#   ① PYTHONPATH 前置 /www/braios_coding/pylibs —— U7 AST 依赖 tree-sitter; 在役脚本未含此路径,
#     而 venv 无 tree_sitter(实测 import 失败)。不加则 BRAINOS_AST=1 会【静默 fail-soft 关闭】。
#   ② arm-v5 门块(E2-E5 波: SPAN/SPANVEC/SVFEED/U2密度/U3 RELAX/AST/R三防线/R45四修)。
#   ③ CW2_KMAX=30 显式化(KFIX 终裁; 与在役 CW_KMAX=30 同值双保险) + CW_KMIN=15(KFIX 固化)。
#   ④ RERANK_TOPN 30→120(arm-v5 交付值; SVFEED 生效前提=TOPN≥候选池100+sv尾15)。
#     生产紧张回 30: RERANK_TOPN=30 bash start_engine.sh (或直接改下行缺省)。
# 合规不变: LLM=NewAPI gpt-4o-mini(env 文件·已实证 fp_83bfdef07b 上游 apimart)
#           Embedding=dashscope text-embedding-v4(v4shim·1024维)  Reranker=qwen3-0.6b(规则不限)
# 回滚: /www/braios_coding/start_engine.sh.armfinal(切换脚本自动留档)·见 rollback_armfinal.sh
set -a; [ -f /www/braios_coding/env ] && . /www/braios_coding/env; set +a
cd /www/braios_coding/src || exit 1
export PYTHONPATH=/www/braios_coding/pylibs:/www/braios_coding/brainos_frozen
export BRAINOS_EMBED_MODEL=/www/braios_coding/models/models--sentence-transformers--all-MiniLM-L6-v2/snapshots/1110a243fdf4706b3f48f1d95db1a4f5529b4d41
export BRAINOS_HOME=/www/braios_coding
export ZO_KEYFILE=/www/braios_coding/src/.api_key
export ZO_PORT=8090
export ZO_HOST=127.0.0.1
# ── arm-final 冻结清单 (原样保留·20261025 部署智能体M) ──
export BRAINOS_VERSION=v0.10.0-aml
export BRAINOS_EMBED_PROVIDER=dashscope_v4
export BRAINOS_K13=1 BRAINOS_CITEREF=1 BRAINOS_MS_TENANT_FETCH=1 BRAINOS_K13_RECALL=1
export BRAINOS_K4_RECENCY=0
export BRAINOS_CODING_WINDOW=1 BRAINOS_CW_BUDGET=48000 BRAINOS_CW_KMAX=30 BRAINOS_CW_V2=1
export BRAINOS_REPO_MAP=1 BRAINOS_EXP_DISTILL=1
export BRAINOS_LLM_ENS=1 BRAINOS_ENS_EXPAND=1 BRAINOS_ENS_HYDE=1 BRAINOS_ENS_FILTER=0
export BRAINOS_ENS_MODEL=gpt-4o-mini
export BRAINOS_RERANK=1 BRAINOS_RERANK_URL=http://127.0.0.1:8098
export BRAINOS_RERANK_TOPN=30 BRAINOS_RERANK_TIMEOUT_MS=8000
# (BRAINOS_ENS_URL / BRAINOS_ENS_KEY 由 /www/braios_coding/env 提供·600)
# ── 刀管线基线 (原样保留) ──
export BRAINOS_QA_WIN_QUOTA=3 BRAINOS_A1_ASOF=1 BRAINOS_K811=1 BRAINOS_SNAP_IDLE_S=120
export BRAINOS_SEARCH_T1_MS=20000 BRAINOS_A1_TOPK=50 BRAINOS_A1_ROUTE=0
export BRAINOS_K7_FORCE_SWAP=1 BRAINOS_K7_MIN_COVER=3 BRAINOS_K7_BUILD_CAP=400
export BRAINOS_K7_GLOBAL=1 BRAINOS_K7_BACKFILL=1 BRAINOS_K12=1
export BRAINOS_XMEM_HEBBIAN=0 BRAINOS_DREAM=0 BRAINOS_BM25_FAST=1
export BRAINOS_INTENT_COOLDOWN_S=60 OMP_NUM_THREADS=4 BRAINOS_ORGAN_QV=1
export BRAINOS_DATE_ANCHOR=1 BRAINOS_CHOSENIMMUNE=1 BRAINOS_FINALIZE_CHUNK=25
export BRAINOS_SUPE_EXPANDED=1 BRAINOS_CC_ENTITY_IDX=1 BRAINOS_CC_MAX_INJECT=6
export BRAINOS_LLM_F1=1 BRAINOS_TRACE=1
export BRAINOS_F1_LLM_URL=http://127.0.0.1:3000/v1/chat/completions
export BRAINOS_F1_LLM_MODEL=gpt-4o-mini
# ══ [arm-v5 升级块 20261026-28 · staging·纯增] ══
# E2/E3 波(L-SPAN/SPANVEC/U1 SVFEED/U2 密度/U3 RELAX/U7 AST)
export BRAINOS_SPAN=1 BRAINOS_SPAN_CARD=1400
export BRAINOS_SPANVEC=1
export BRAINOS_SV_BACKFILL="${SV_BACKFILL:-0}"    # 存量卡 spanvec 回填=一次性·决策见 STAGING_README §5
export BRAINOS_CW2_KMAX=30                        # KFIX 终裁(显式化)
export BRAINOS_CW_KMIN=15                         # KFIX 窗下限(固化·VPS env 修复实证)
export BRAINOS_SVFEED=1 BRAINOS_SVFEED_CAP=15
export BRAINOS_CW_DEDUP=1 BRAINOS_CW_DEDUP_TH=0.85
export BRAINOS_RR_TAILCUT=0
export BRAINOS_RR_TAIL=0.15 BRAINOS_RR_KEEP_MIN=30
export BRAINOS_EXP_RELAX=1
export BRAINOS_AST=1 BRAINOS_AST_SPAN=1 BRAINOS_AST_SYM=1
# E4 波(UR 三防线)
export BRAINOS_HERO_COMM=1 BRAINOS_CW_NODIGEST=1 BRAINOS_CW_TELEM=1
# E5 波(U-R45 四修; 逐门回滚: 任一置 0 即单门摘除·test_r45 T6 钉死)
export BRAINOS_R45_QSTRIP=1 BRAINOS_R45_REPO=1 BRAINOS_R45_HFCUT=1 BRAINOS_R45_PATHCHK=1
export BRAINOS_R45_HF_N=6 BRAINOS_R45_HF_RATE=0.15
# 重排可见性(SVFEED 生效前提): 120=arm-v5 交付值; 生产紧张透传 RERANK_TOPN=30 回旧值
export BRAINOS_RERANK_TOPN="${RERANK_TOPN:-120}"
export BRAINOS_JQ=0
export BRAINOS_ENS_EXPAND=0 BRAINOS_ENS_HYDE=0  # 上游双通道失效(3000 TimeoutError+apimart URLError)·扩展贡献=0纯收15s超时税(20261029冒烟取证)  # 平台同步契约: add返回前必须立即可检索(规则原文: persisted and immediately searchable)·JQ worker孤儿+自echo双缺陷(20261029)
exec /www/braios_coding/venv/bin/python aml_service.py
