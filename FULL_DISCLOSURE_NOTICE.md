# FULL DISCLOSURE NOTICE — brainos v0.10.0-aml(本文件取代 v0.9 期 CARVE_NOTICE.md)

2026-11-01 提交 b2250b3a(= tag v0.10.0-aml)已将 v0.9 期声明"未开放"的调度与调优模块
全量补入本仓库 production/src/(73 模块)——它们全部用于 2026-11-02 起的正式评测
(teval_e566217ef1617a43·coding track FULL·150/150·61.00),逐模块作用见 MODULE_MAP.md。

## v0.9 期 carve 声明的现状更正

| v0.9 期声明未开放 | v0.10.0-aml 现状 |
|---|---|
| Battle-tuned scheduling layer(tuned parameters) | **已全量开源**: production/src/coding_window.py 等逐行在库,tuned 参数值见 start_engine.sh env 门(BRAINOS_CW_BUDGET=48000/CW_KMAX=30 等) |
| 检索 boosting/fidelity/surprise 调优 | **已全量开源**: spanvec.py/span_cut.py/cw_signals.py/win_density.py/evidence_rerank.py/r45_rank.py |
| Cross-domain combo engine | 同前: kernel 内 no-op stub 不变(combo 引擎属商业版另一实现·BRAIOS 编程部署未使用) |
| Governance engine | 同前: NoopConstitution 不变(governance 完整实现属商业版·BRAIOS 部署未使用) |
| Agentic meta-architecture(agent_loop/amcc/autonomous/ans) | **维持商业版专属**: 这些模块不属于 BRAIOS 编程部署(部署内无此代码·已核查),元架构本体亦不在本仓库 |

## 依赖与入口补充(v0.10.0-aml 起新增文件)

| 文件 | 说明 |
|---|---|
| start_engine.sh | 正式运行入口(环境门全清单·生产启动命令) |
| env.template | 配置模板(密钥占位·真实密钥部署时注入·不入库) |
| requirements.txt | 生产 venv 全量 freeze(75 包) |
| MODULE_MAP.md | 全模块地图与官方评测用途说明 |

## 维持商业版专属(本仓库永不包含)

- zfaios 商业版 Rust 实现(54 crates·独立代码库·独立参赛申报 request_6f1c0b69)
- 多席位舰队运维体系与实战经验
- 商业版持续演化的 tuned 参数(后续版本继续领先)
