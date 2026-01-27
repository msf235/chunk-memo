import dataclasses
import functools
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Sequence, Tuple

from .memo import ChunkKey, ChunkMemo, Diagnostics


@dataclasses.dataclass
class BridgeDiagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    executed_points: int = 0


def _default_map_fn(
    func: Callable[..., Any],
    points: Iterable[Any],
    **kwargs: Any,
) -> List[Any]:
    max_workers = kwargs.get("max_workers")
    chunksize = kwargs.get("chunksize")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if chunksize is None:
            return list(executor.map(func, points))
        return list(executor.map(func, points, chunksize=chunksize))


def _normalize_point(point: Any) -> Tuple[Any, ...]:
    if isinstance(point, tuple):
        return point
    if isinstance(point, list):
        return tuple(point)
    return (point,)


def _ensure_iterable(points: Iterable[Any]) -> List[Any]:
    if isinstance(points, list):
        return points
    return list(points)


def _chunk_key_matches_point(point: Tuple[Any, ...], chunk_key: ChunkKey) -> bool:
    for (_, values), value in zip(chunk_key, point):
        if value not in values:
            return False
    return True


def _expand_points_to_chunks(
    points: Sequence[Any],
    chunk_keys: Sequence[ChunkKey],
) -> List[List[Any]]:
    chunked: List[List[Any]] = [[] for _ in chunk_keys]
    for point in points:
        normalized = _normalize_point(point)
        for index, chunk_key in enumerate(chunk_keys):
            if _chunk_key_matches_point(normalized, chunk_key):
                chunked[index].append(point)
                break
    return chunked


def memo_parallel_run(
    memo: ChunkMemo,
    params: dict[str, Any],
    points: Iterable[Any],
    *,
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> tuple[Any, Diagnostics]:
    if map_fn is None:
        map_fn = _default_map_fn

    cached_chunks: List[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: List[ChunkKey] = list(cache_status.get("missing_chunks", []))

    outputs: List[Any] = []
    exec_outputs: List[Any] = []
    exec_outputs = list(exec_outputs)
    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))
    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda items: items
    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    for chunk_key in cached_chunks:
        chunk_hash = memo.chunk_hash_fn(params, chunk_key, memo.cache_version)
        path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
        if not path.exists():
            continue
        diagnostics.cached_chunks += 1
        with open(path, "rb") as handle:
            outputs.append(pickle.load(handle)["output"])

    point_list = _ensure_iterable(points)
    missing_points = [
        point
        for point in point_list
        if any(
            _chunk_key_matches_point(_normalize_point(point), key)
            for key in missing_chunks
        )
    ]
    if not missing_points and missing_chunks:
        missing_points = point_list

    if missing_points:
        diagnostics.executed_chunks = len(missing_chunks)
        exec_fn = functools.partial(memo.exec_fn, params)
        exec_outputs = list(
            map_fn(
                exec_fn,
                missing_points,
                **map_fn_kwargs,
            )
        )

        chunked_points = _expand_points_to_chunks(missing_points, missing_chunks)
        cursor = 0
        for chunk_key, chunk_points in zip(missing_chunks, chunked_points):
            chunk_size = len(chunk_points)
            chunk_output = collate_fn(exec_outputs[cursor : cursor + chunk_size])
            chunk_hash = memo.chunk_hash_fn(params, chunk_key, memo.cache_version)
            path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
            with open(path, "wb") as handle:
                pickle.dump({"output": chunk_output}, handle, protocol=5)
            outputs.append(chunk_output)
            cursor += chunk_size

    diagnostics.merges += 1
    if memo.merge_fn is not None:
        merged = memo.merge_fn(outputs)
    else:
        merged = outputs
    if not merged and point_list:
        merged = exec_outputs if missing_points else []
    return merged, diagnostics
