# v0.10.0-aml 全模块公开清单(95模块·书面披露)

> 版本: BRAIOS v0.10.0-aml · 固定Commit: b2250b3a50a81e463ccb061e3887dc001d6bc8a4(tag v0.10.0-aml)
> 任务: teval_e566217ef1617a43 · 正式评测运行入口: src/aml_service.py(start_engine.sh启动·端口8090)
> 用途说明: 以下全部模块均属正式评测运行版本·**无任何'未公开但用于正式评测'的模块**

| # | 模块文件 | 大小B | 作用(源码头注) |
|---|---|---|---|
| 1 | aml_import_probe.py | 951 |  |
| 2 | aml_service.py | 142413 |  |
| 3 | ast_supply.py | 35515 |  |
| 4 | c1_quad.py | 28346 |  |
| 5 | card_construct.py | 55515 |  |
| 6 | citeref.py | 18447 |  |
| 7 | coding_window.py | 27204 |  |
| 8 | content_sanitizer.py | 2383 |  |
| 9 | corpus_fact.py | 13498 |  |
| 10 | cw_signals.py | 15832 |  |
| 11 | deep_closure.py | 13280 |  |
| 12 | deep_construct.py | 6289 |  |
| 13 | deep_digest.py | 11750 |  |
| 14 | deep_expdistill.py | 42269 |  |
| 15 | deep_fact.py | 22702 |  |
| 16 | deep_multifix.py | 16370 |  |
| 17 | deep_policy.py | 13790 |  |
| 18 | deep_processor.py | 7625 |  |
| 19 | deep_qa_graph.py | 31467 |  |
| 20 | deep_supersede.py | 25262 |  |
| 21 | deep_taskbrief.py | 12453 |  |
| 22 | deep_temporal.py | 29179 |  |
| 23 | deep_timeline.py | 28555 |  |
| 24 | evidence_rerank.py | 3905 |  |
| 25 | k4_profile_once.py | 1478 |  |
| 26 | kbase_entry.py | 34275 |  |
| 27 | knife10_a1asof.py | 6099 |  |
| 28 | knife10_order.py | 16601 |  |
| 29 | knife10_order_probe.py | 8838 |  |
| 30 | knife10_order_selftest.py | 7140 |  |
| 31 | knife11_forget.py | 32748 |  |
| 32 | knife12_struct.py | 58039 |  |
| 33 | knife13_repo.py | 48933 |  |
| 34 | knife1_selftest.py | 8495 |  |
| 35 | knife1_supersede.py | 22684 |  |
| 36 | knife2_probe.py | 4439 |  |
| 37 | knife2_selftest.py | 7352 |  |
| 38 | knife2_sensitive.py | 11510 |  |
| 39 | knife3_probe.py | 6280 |  |
| 40 | knife3_profile.py | 4829 |  |
| 41 | knife3_selftest.py | 12056 |  |
| 42 | knife3_time.py | 33678 |  |
| 43 | knife4_probe.py | 4568 |  |
| 44 | knife4_selftest.py | 17817 |  |
| 45 | knife4_streaming.py | 21645 |  |
| 46 | knife5_multihop.py | 15926 |  |
| 47 | knife5_probe.py | 8584 |  |
| 48 | knife5_selftest.py | 11298 |  |
| 49 | knife6_cards.py | 29109 |  |
| 50 | knife6_probe.py | 5773 |  |
| 51 | knife6_selftest.py | 14502 |  |
| 52 | knife7_digest.py | 43240 |  |
| 53 | knife7_f1_probe.py | 7827 |  |
| 54 | knife7_selftest.py | 6851 |  |
| 55 | knife8_d2pair.py | 16065 |  |
| 56 | knife8_rank.py | 20301 |  |
| 57 | knife9_g4.py | 4453 |  |
| 58 | knife_intent.py | 3891 |  |
| 59 | llm_ensemble.py | 32144 |  |
| 60 | memory_service.py | 44915 |  |
| 61 | np_bm25.py | 3340 |  |
| 62 | patch_aml_all.py | 9002 |  |
| 63 | patch_aml_k4.py | 3065 |  |
| 64 | patch_aml_k4b.py | 759 |  |
| 65 | patch_comply2.py | 2250 |  |
| 66 | patch_d1.py | 6575 |  |
| 67 | patch_dlfix.py | 1788 |  |
| 68 | patch_dlfix2.py | 1341 |  |
| 69 | patch_f1.py | 6085 |  |
| 70 | patch_f2_rerank.py | 1659 |  |
| 71 | patch_fh.py | 1057 |  |
| 72 | patch_jf.py | 3825 |  |
| 73 | patch_pg.py | 3660 |  |
| 74 | patch_replay.py | 1820 |  |
| 75 | patch_t4fix.py | 836 |  |
| 76 | patch_t4fix2.py | 806 |  |
| 77 | patch_t4fix3.py | 2491 |  |
| 78 | patch_t4fix4.py | 1234 |  |
| 79 | patch_wcap.py | 1859 |  |
| 80 | perf_cache.py | 7947 |  |
| 81 | r45_rank.py | 20083 |  |
| 82 | rebuild_derived.py | 4762 |  |
| 83 | repo_map.py | 28143 |  |
| 84 | rerank_client.py | 6365 |  |
| 85 | rule_first.py | 4239 |  |
| 86 | span_cut.py | 11705 |  |
| 87 | spanvec.py | 25406 |  |
| 88 | test_d1.py | 8259 |  |
| 89 | test_deep_expdistill.py | 15547 |  |
| 90 | test_evidence_rerank.py | 3272 |  |
| 91 | test_f1.py | 6444 |  |
| 92 | time_norm.py | 36455 |  |
| 93 | v4shim.py | 7406 |  |
| 94 | w2_timeline.py | 17622 |  |
| 95 | win_density.py | 14329 |  |

## 合规运行外联(非仓库内容·env注入)
- LLM: NewAPI gpt-4o-mini(经apimart·key在env不公开)
- Embedding: dashscope text-embedding-v4(1024维·合规接口)
- Reranker: qwen3-0.6b 本地(规则不限)

## 与委员会沟通对照
- 调度与调优模块: 上表cw_signals/spanvec/win_density/telemetry族=调优链·现已全部公开
- 运行入口: aml_service.py+start_engine.sh(本仓库production/与docs/RUNBOOK.md)
- 依赖: docs/DEPENDENCIES.md(pip freeze精确版本)

披露生成: 2026-11-17 03:20:03 · ZfaiOpen团队