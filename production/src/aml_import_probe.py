#!/usr/bin/env python3
"""aml_import_probe.py — card_construct_selftest专用轻量导入探针。
以EXAM_USE_EMBED=0(单测口径·服务件原生支持)导入真实aml_service,
暴露毒dt止血真身函数——单测与生产同一实现=零复刻(PENTA-UNIQ·五禁令④)。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = Path(__file__).parent
_DEPLOY = _HERE.parent
os.environ.setdefault("BRAINOS_HOME", str(_DEPLOY / "brainos_frozen"))
os.environ.setdefault("EXAM_HOME", str(_DEPLOY / "exam"))
os.environ.setdefault("EXAM_USE_EMBED", "0")  # 单测口径: 零语义模型加载(服务件原生开关)
for p in (str(_HERE), str(_DEPLOY / "exam")):
    if p not in sys.path:
        sys.path.insert(0, p)

import aml_service as _aml  # noqa: E402


def dt(ts, text: str = "") -> str:
    """真身直通: aml_service._dt_from_ts(生产同源·非复刻)"""
    return _aml._dt_from_ts(ts, text)
