# BRAIOS v0.10.0-aml 模块地图·未开放模块说明(对应组委会四件之四)

本文件对应组委会 2026-11-13 来函要求④:"说明未公开模块是否用于本次正式评测及其具体作用"。
结论先行: **原声明未开放的调度与调优模块,本次正式评测【全部启用并实际生效】**,源码已随
tag v0.10.0-aml(commit b2250b3a)全量公开于 production/src/(73 模块)。逐模块作用见下表。

## 一、调度模块(原声明未开放·本次评测用于·已开源)

| 模块 | 作用(官方评测语境) | 对应 10-20 登记项 |
|---|---|---|
| coding_window.py | 编程窗口:预算调度(CW_BUDGET=48000 字节)、候选窗 KMAX=30、去重 0.85——官方 add/search 全程生效 | 摘要全量生效(登记③) |
| cw_signals.py / win_density.py | 窗口信号密度与占窗密度计算(窗口准入/尾截断依据) | 同上 |
| span_cut.py / spanvec.py | 证据跨度切割与向量化管理(检索证据入窗的跨度调优) | 检索线性重排(登记②) |
| r45_rank.py | R45 四修排序(查询.strip/仓库映射/HF截断/路径检查四防线) | 时间推理强化(登记⑤)族 |
| knife1-13_*.py | 13 族分域探针/自检(上线前自证·评测期旁路监控) | 质量保障(非评分路径) |
| deep_processor.py | 主处理流水线(摄取→消化→检索编排) | 全部登记项的载体 |
| memory_service.py | 记忆服务核心(add/search/forget/lifecycle 端点实现) | 引擎本体 |
| llm_ensemble.py | LLM 集成(扩展/HyDE/过滤三通道——评测期上游失效,扩展贡献=0,fail-soft 降级原序·如实申报) | —(评测期实际未产生增益) |

## 二、调优模块(原声明未开放·本次评测用于·已开源)

| 模块 | 作用 | 对应登记项 |
|---|---|---|
| evidence_rerank.py | 证据重排(证据卡打分入窗) | 检索线性重排(登记②) |
| v4shim.py | 嵌入通道换装包装件: organ._qmodel ← text-embedding-v4(1024 维 DashScope API·maas.key) | 嵌入换装(登记①) |
| rerank_client.py | 重排客户端(→127.0.0.1:8098 qwen3-0.6b 重排服务·fail-soft) | 检索线性重排(登记②) |
| deep_supersede.py / deep_policy.py | D3 删除/遗忘/抑制语义实现 | D3 能力补齐(登记④) |
| deep_temporal.py / time_norm.py | 时间推理与多日历时间归一(含混基修复) | 时间推理强化(登记⑤) |
| deep_digest.py | 摘要压缩全量(规模门解除=登记③;LLM gpt-4o-mini 摘要) | 摘要全量(登记③) |
| deep_fact/deep_timeline/deep_qa_graph/deep_taskbrief/deep_expdistill/deep_multifix/deep_closure/deep_construct | 记忆抽取深度家族(事实/时间线/问答图/任务简报/经验蒸馏/多修/闭包/构造) | 时间推理强化(登记⑤)族 |

## 三、基础与冻结件

| 组件 | 说明 |
|---|---|
| brainos/memory/(43 模块) | 记忆核心:存储/持久化/情景-语义-程序-工作记忆/巩固/遗忘曲线/CRDT/时态图 v1+v2/锚定/编织/完整性守卫 |
| brainos/api/ | AML wire 契约 + 多通道召回基线(BM25+char-trigram+recency·RRF) |
| brainos/kernel/ | 安全执行/异步兼容/重试熔断/协议接口 |
| brainos/observability/ | auto_log |
| | models/（配置骨架已入仓 production/models/） | all-MiniLM-L6-v2 配置/词表骨架；运行期嵌入被 v4shim 替换为 text-embedding-v4(DashScope API)——本地权重未参与运行,如实申报 | |
| pylibs/（已入仓 production/pylibs/） | tree-sitter(python/go/java/javascript)供应依赖·U7 AST启用前提 |

## 四、外部服务依赖(依赖清单之运行时部分·密钥自备不入库)

| 服务 | 用途 | 配置 |
|---|---|---|
| gpt-4o-mini(经 NewAPI 自建中转) | 摘要压缩(deep_digest)+LLM 集成 | env: BRAINOS_F1_LLM_URL/KEY/MODEL |
| text-embedding-v4(DashScope 官方 API·maas.key) | 嵌入通道(v4shim) | src/maas.key 部署时放置 |
| qwen3-0.6b 重排服务(127.0.0.1:8098) | 检索重排 | BRAINOS_RERANK_URL |

以上与 2026-10-20 报备邮件登记的 v0.10.0-aml 变更逐项对应;本次正式评测全程使用同一登记实现。

## 四、开发期补丁与自测脚本（已一并入仓·非运行时路径）

src/ 内 patch_*.py（18件·一次性迁移补丁工具）与 test_*.py（3件·本地自测）为开发期工具，正式评测运行时路径不加载（运行时引用核验=零import）；一并入仓以保源码完整可核验。
