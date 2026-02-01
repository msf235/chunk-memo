import json
from pathlib import Path
from typing import Any, Mapping, Tuple

from .cache_utils import _atomic_write_json, _now_iso

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


def _build_chunk_index_entry(
    existing_entry: Mapping[str, Any] | None,
    chunk_key: ChunkKey,
) -> dict[str, Any]:
    created_at = None if existing_entry is None else existing_entry.get("created_at")
    if created_at is None:
        created_at = _now_iso()
    return {
        "created_at": created_at,
        "updated_at": _now_iso(),
        "chunk_key": chunk_key,
    }


def chunk_index_path(memo_root: Path) -> Path:
    return memo_root / "chunks_index.json"


def load_chunk_index(memo_root: Path) -> dict[str, Any]:
    path = chunk_index_path(memo_root)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def update_chunk_index(
    memo_root: Path,
    chunk_hash: str,
    chunk_key: ChunkKey,
) -> None:
    index = load_chunk_index(memo_root)
    entry = _build_chunk_index_entry(index.get(chunk_hash), chunk_key)
    index[chunk_hash] = entry
    _atomic_write_json(chunk_index_path(memo_root), index)
