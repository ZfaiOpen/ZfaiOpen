# brainos/memory/recall_store.py — 影子重建件(披露: 196冻结版真身不在本机, 此为API同形的
# 影子替代实现) — PersistentRecall 快照持久化包装器(R1-PERSIST)。
# ═══════════════════════════════════════════════════════════════════════════════
# 契约(对表两处调用点逐字反推):
#   memory_service.py:93   PersistentRecall(organ=self.organ).load(snap) -> manifest|None
#   memory_service.py:170  PersistentRecall(organ=self.organ).save(path) -> manifest
#   closure_battery P4:    PersistentRecall(d, organ=o); with ... as pr2 (进出=load/save)
# 影子格式: <dir>/cards.jsonl(六字段原文)+manifest.json — 纯影子自用, 与196快照格式零互通。
# 语义保真: load=全量re-ingest+finalize重建索引(检索行为等价·启动耗时换实现简单);
#   全程fail-soft(损坏→None零卡起), 与memory_service注释"绝不因坏快照拒启动"一致。
# ═══════════════════════════════════════════════════════════════════════════════
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger("brainos.recall_store")

_FIELDS = ("conv", "sess", "dia", "speaker", "text", "dt")


class PersistentRecall:
    def __init__(self, path: str | os.PathLike = "", organ: Any = None) -> None:
        self.path = Path(str(path)) if str(path) else None
        self.organ = organ

    # ── 内部: 卡片读写(六字段规范化·确定性) ─────────────────────────────────
    def _cards_file(self, d: Path) -> Path:
        return d / "cards.jsonl"

    def _dump(self, d: Path) -> dict[str, Any]:
        d.mkdir(parents=True, exist_ok=True)
        cards = []
        for c in getattr(self.organ, "_cards", {}).values():
            cards.append({k: str(c.get(k, "") or "") for k in _FIELDS})
        tmp = None
        with tempfile.NamedTemporaryFile("w", dir=str(d), suffix=".tmp",
                                         delete=False, encoding="utf-8") as f:
            tmp = f.name
            for c in cards:
                f.write(json.dumps(c, ensure_ascii=False) + "\n")
        os.replace(tmp, str(self._cards_file(d)))
        convs = len({c.get("conv", "") for c in cards})
        return {"version": 1, "cards": len(cards), "convs": convs, "emb": False}

    def _restore(self, d: Path) -> dict[str, Any] | None:
        cf = self._cards_file(d)
        if not cf.exists():
            return None
        cards: list[dict[str, Any]] = []
        with open(cf, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    cards.append(json.loads(line))
                except Exception:
                    continue  # 坏行fail-soft跳过
        if not cards:
            return None
        self.organ.ingest_cards(cards)
        self.organ.finalize_ingest()
        convs = len({c.get("conv", "") for c in cards})
        return {"version": 1, "cards": len(cards), "convs": convs, "emb": False}

    # ── 公开契约 ────────────────────────────────────────────────────────────
    def save(self, path: str | os.PathLike | None = None) -> dict[str, Any]:
        d = Path(str(path)) if path else self.path
        if d is None or self.organ is None:
            return {}
        return self._dump(d)

    def load(self, path: str | os.PathLike | None = None) -> dict[str, Any] | None:
        d = Path(str(path)) if path else self.path
        if d is None or self.organ is None:
            return None
        try:
            return self._restore(d)
        except Exception as exc:  # fail-soft: 坏快照零卡起
            log.warning("snapshot load failed: %s", type(exc).__name__)
            return None

    def close(self) -> None:  # 显式no-op(上下文协议出相用save)
        pass

    def __enter__(self) -> "PersistentRecall":
        if self.path is not None:
            self.load()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.path is not None:
            try:
                self.save()
            except Exception as exc:
                log.warning("snapshot save failed: %s", type(exc).__name__)
        return False
