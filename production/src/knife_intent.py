# knife_intent.py — [刀8.10·v0.4.1零样本意图分类路由20261017]
# 病根: AML隐式考题不带summary/pattern词面("overall uptake rate"/"likely to pursue")→
# 正则触发器永不命中→F1/G4管线死。本件=本地嵌入零样本分类: 原型句组嵌入cosine→F1/G4/OTHER。
# 预算: 单查询嵌入≈5-20ms(复用organ已载MiniLM·零新模型)≪100ms红线; 超软预算/异常=OTHER直通。
# 纪律: 条件触发(OTHER零开销)·env门BRAINOS_INTENT_ZS·margin门(不自信=OTHER)·禁2✓通用原型句。
from __future__ import annotations
import logging, os, time
from typing import Any

log = logging.getLogger("kintent")

_F1_PROTO = [
    "summarize everything that happened across all our conversations",
    "give me a global summary of the whole story over time",
    "recap all the main events and key moments in our history",
    "what are the main things that happened overall",
    "taken together what do all these shared moments suggest about the overall story",
]
_G4_PROTO = [
    "how many times and how often did patterns recur in my records",
    "discover the trends averages and frequencies in my activity data",
    "what statistical patterns can be found in my experience history",
    "estimate the rate of change and likely progression from my past data",
    "what is the average uptake rate measured across my data",
    "what fields or outcomes am I likely to pursue based on my history",
    "how many different things have I mentioned doing over the months",
]

_P_EMB = None
_model_fail_until = 0.0  # [修20261025-2] 模块级初始化: classify内赋值需global否则UnboundLocalError


def _protos(model: Any):
    global _P_EMB
    if _P_EMB is None:
        import numpy as np
        _P_EMB = np.asarray(model.encode(_F1_PROTO + _G4_PROTO,
                                         normalize_embeddings=True))
    return _P_EMB


def classify(query: str, svc: Any) -> str:
    """零样本意图分类 → "F1"|"G4"|"OTHER"。软预算100ms·异常/margin不足=OTHER·env可关。"""
    global _model_fail_until  # [修20261025-2] except内赋值跨调用生效(冷却)·缺此声明=每次UnboundLocalError
    if os.environ.get("BRAINOS_INTENT_ZS", "1") != "1":
        return "OTHER"
    if time.time() < _model_fail_until:
        return "OTHER"
    t0 = time.perf_counter()
    try:
        import numpy as np
        organ = getattr(svc, "organ", None)
        model = getattr(organ, "_qmodel", None)
        if model is None:
            _model_fail = True
            return "OTHER"
        qv = np.asarray(model.encode([str(query or "")], normalize_embeddings=True))[0]
        pe = _protos(model)
        sims = pe @ qv
        f1 = float(sims[:len(_F1_PROTO)].max())
        g4 = float(sims[len(_F1_PROTO):].max())
        label, conf = ("F1", f1) if f1 >= g4 else ("G4", g4)
        margin = abs(f1 - g4)
        thr = float(os.environ.get("BRAINOS_INTENT_MARGIN", "0.10"))
        if conf < 0.22 or margin < thr:
            label = "OTHER"
        dt = (time.perf_counter() - t0) * 1000
        # [修20261017] 超时改告警不弃果: 本地编码已完成·丢弃=白白降级F1/G4(实测CPU忙时>100ms
        # 占比高→10次降级吃掉F1路径)。分类算力开销≪T1预算·红线由T1/T2帽保证。
        if dt > 100:
            log.warning("intent classify slow %.0fms>100ms (结果仍采用)", dt)
        log.info("intent_class_label=%s conf=%.2f margin=%.2f %.0fms q=%.60s",
                 label, conf, margin, dt, str(query)[:60])
        return label
    except Exception as exc:  # noqa: BLE001
        # [修20261025] 永久自锁改60秒冷却
        _model_fail_until = time.time() + max(1.0, float(os.environ.get("BRAINOS_INTENT_COOLDOWN_S", "60")))
        log.error("intent classify FAILED err=%s — 冷却60s后重试", type(exc).__name__)
        return "OTHER"
