import json
import os
import pickle
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _apply_payload_timestamps(
    payload: dict[str, Any],
    existing: Mapping[str, Any] | None = None,
) -> None:
    now = _now_iso()
    created_at = None if existing is None else existing.get("created_at")
    if created_at is None:
        created_at = now
    payload["created_at"] = created_at
    payload["updated_at"] = now


def _atomic_write_pickle(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
        ) as handle:
            tmp_path = Path(handle.name)
            pickle.dump(payload, handle, protocol=5)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            encoding="utf-8",
        ) as handle:
            tmp_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
