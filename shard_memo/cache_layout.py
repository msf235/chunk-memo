from pathlib import Path
from typing import Any, Callable, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


def memo_root(cache_root: Path, memo_hash: str) -> Path:
    return cache_root / memo_hash


def resolve_cache_path(
    *,
    memo_root_path: Path,
    cache_path_fn: Callable[[dict[str, Any], ChunkKey, str, str], Path | str] | None,
    params: dict[str, Any],
    chunk_key: ChunkKey,
    cache_version: str,
    chunk_hash: str,
) -> Path:
    if cache_path_fn is None:
        return memo_root_path / "chunks" / f"{chunk_hash}.pkl"
    path = cache_path_fn(params, chunk_key, cache_version, chunk_hash)
    path_obj = Path(path)
    if path_obj.is_absolute():
        return path_obj
    return memo_root_path / path_obj
