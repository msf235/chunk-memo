import json
from pathlib import Path
from typing import Any, Tuple

from .data_write_utils import _apply_payload_timestamps, _atomic_write_json

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


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
    existing_entry = index.get(chunk_hash)
    entry = {
        "chunk_key": chunk_key,
    }
    _apply_payload_timestamps(entry, existing=existing_entry)
    index[chunk_hash] = entry
    _atomic_write_json(chunk_index_path(memo_root), index)
