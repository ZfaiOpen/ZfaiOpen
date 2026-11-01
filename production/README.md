# BRAIOS v0.10.0-aml · 生产部署源码

本目录包含 AML 编程赛道完整评测（teval_e566217ef1617a43·150/150·61.00 分）实际运行的生产部署源码。

## 与 brainos/ 开源包的关系
- brainos/ = 模块化开源包（整理架构）
- production/src/ = 生产部署扁平结构（实际运行代码）
- 两者实现同一记忆系统·共享算法与逻辑

## 模型组件
- 嵌入：text-embedding-v4（DashScope API）
- LLM：gpt-4o-mini（F1 摘要/记忆结构化）
- 重排：Qwen3-Reranker-0.6B（本地）
