from __future__ import annotations

import dataclasses
import functools
import hashlib
import itertools
import pickle
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


@dataclasses.dataclass
class Diagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    executed_shards: int = 0
    merges: int = 0
    stream_flushes: int = 0


def _stable_serialize(value: Any) -> str:
    if isinstance(value, dict):
        items = ((k, _stable_serialize(value[k])) for k in sorted(value))
        return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        inner = ",".join(_stable_serialize(v) for v in value)
        return "[" + inner + "]"
    return repr(value)


def default_chunk_hash(
    params: dict[str, Any], chunk_key: ChunkKey, version: str
) -> str:
    payload = {
        "params": params,
        "chunk_key": chunk_key,
        "version": version,
    }
    data = _stable_serialize(payload)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _chunk_values(values: Sequence[Any], size: int) -> List[Tuple[Any, ...]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    chunks: List[Tuple[Any, ...]] = []
    for start in range(0, len(values), size):
        end = min(start + size, len(values))
        chunks.append(tuple(values[start:end]))
    return chunks


def _iter_exec_shards(points: Sequence[Any], shard_size: int) -> Iterable[List[Any]]:
    for start in range(0, len(points), shard_size):
        end = min(start + shard_size, len(points))
        yield list(points[start:end])


class SwarmMemo:
    def __init__(
        self,
        cache_root: str | Path,
        memo_chunk_spec: dict[str, Any],
        exec_chunk_size: int,
        enumerate_points: Callable[[dict[str, Any], dict[str, Any]], List[Any]],
        exec_fn: Callable[[dict[str, Any], Any], Any],
        collate_fn: Callable[[List[Any]], Any],
        merge_fn: Callable[[List[Any]], Any],
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_version: str = "v1",
        max_workers: int | None = None,
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
    ) -> None:
        """Initialize a SwarmMemo runner.

        Args:
            cache_root: Directory for chunk cache files.
            memo_chunk_spec: Per-axis chunk sizes (e.g., {"strat": 1, "s": 3}).
            exec_chunk_size: Executor batching size for points.
            enumerate_points: Builds ordered points for a split_spec.
            exec_fn: Executes one point with full params, signature (params, point).
            collate_fn: Combines per-point outputs into a chunk output.
            merge_fn: Combines chunk outputs into the final output.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_version: Cache namespace/version tag.
            max_workers: Max process workers for execution.
            axis_order: Axis iteration order (defaults to lexicographic).
            verbose: Verbosity flag.
        """
        if exec_chunk_size <= 0:
            raise ValueError("exec_chunk_size must be > 0")
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.memo_chunk_spec = memo_chunk_spec
        self.exec_chunk_size = exec_chunk_size
        self.enumerate_points = enumerate_points
        self.exec_fn = exec_fn
        self.collate_fn = collate_fn
        self.merge_fn = merge_fn
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_version = cache_version
        self.max_workers = max_workers
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose

    def run(
        self, params: dict[str, Any], split_spec: dict[str, Any]
    ) -> Tuple[Any, Diagnostics]:
        axis_order = self._resolve_axis_order(split_spec)
        chunk_keys = self._build_chunk_keys(split_spec, axis_order)
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))

        outputs: List[Any] = []
        all_points: List[Any] = []
        point_ranges: List[Tuple[ChunkKey, int, int, Path]] = []

        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self.cache_root / f"{chunk_hash}.pkl"
            if path.exists():
                diagnostics.cached_chunks += 1
                with open(path, "rb") as handle:
                    outputs.append(pickle.load(handle)["output"])
                continue

            diagnostics.executed_chunks += 1
            chunk_split_spec = {axis: list(values) for axis, values in chunk_key}
            points = self.enumerate_points(params, chunk_split_spec)
            start = len(all_points)
            all_points.extend(points)
            end = len(all_points)
            point_ranges.append((chunk_key, start, end, path))

        if all_points:
            exec_outputs = self._execute_points(params, all_points, diagnostics)
            for chunk_key, start, end, path in point_ranges:
                chunk_output = self.collate_fn(exec_outputs[start:end])
                with open(path, "wb") as handle:
                    pickle.dump({"output": chunk_output}, handle, protocol=5)
                outputs.append(chunk_output)

        diagnostics.merges += 1
        merged = self.merge_fn(outputs)
        return merged, diagnostics

    def _resolve_axis_order(self, split_spec: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(split_spec))

    def _build_chunk_keys(
        self, split_spec: dict[str, Any], axis_order: Sequence[str]
    ) -> List[ChunkKey]:
        axis_chunks: List[List[Tuple[Any, ...]]] = []
        for axis in axis_order:
            values = split_spec.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in split_spec")
            size = self._resolve_axis_chunk_size(axis)
            axis_chunks.append(_chunk_values(values, size))

        chunk_keys: List[ChunkKey] = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def _resolve_axis_chunk_size(self, axis: str) -> int:
        spec = self.memo_chunk_spec.get(axis)
        if spec is None:
            raise KeyError(f"Missing memo_chunk_spec for axis '{axis}'")
        if isinstance(spec, dict):
            size = spec.get("size")
            if size is None:
                raise KeyError(f"Missing size for axis '{axis}'")
            return int(size)
        return int(spec)

    def _execute_points(
        self, params: dict[str, Any], points: Sequence[Any], diagnostics: Diagnostics
    ) -> List[Any]:
        if not points:
            return []
        results: List[Any] = []
        # breakpoint()
        shard_count = (len(points) + self.exec_chunk_size - 1) // self.exec_chunk_size
        diagnostics.executed_shards += shard_count
        exec_fn = functools.partial(self.exec_fn, params)
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            results.extend(
                executor.map(exec_fn, points, chunksize=self.exec_chunk_size)
            )
        return results

    def run_streaming(
        self, params: dict[str, Any], split_spec: dict[str, Any]
    ) -> Diagnostics:
        """Execute missing chunks and flush outputs to disk only."""
        axis_order = self._resolve_axis_order(split_spec)
        chunk_keys = self._build_chunk_keys(split_spec, axis_order)
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))

        stream_points: List[Any] = []
        chunk_sizes: List[int] = []
        chunk_paths: List[Path] = []

        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self.cache_root / f"{chunk_hash}.pkl"
            if path.exists():
                diagnostics.cached_chunks += 1
                continue

            diagnostics.executed_chunks += 1
            chunk_split_spec = {axis: list(values) for axis, values in chunk_key}
            points = self.enumerate_points(params, chunk_split_spec)
            if not points:
                continue
            stream_points.extend(points)
            chunk_sizes.append(len(points))
            chunk_paths.append(path)

        if not stream_points:
            return diagnostics

        buffer: List[Any] = []
        chunk_index = 0
        exec_fn = functools.partial(self.exec_fn, params)
        next_point_index = 0
        total_points = len(stream_points)
        pending: Dict[Any, int] = {}
        results_by_index: Dict[int, Any] = {}
        next_flush_index = 0

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            while (
                next_point_index < total_points and len(pending) < self.exec_chunk_size
            ):
                future = executor.submit(exec_fn, stream_points[next_point_index])
                pending[future] = next_point_index
                next_point_index += 1

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    results_by_index[index] = future.result()

                while next_flush_index in results_by_index:
                    buffer.append(results_by_index.pop(next_flush_index))
                    next_flush_index += 1
                    while (
                        chunk_index < len(chunk_sizes)
                        and len(buffer) >= chunk_sizes[chunk_index]
                    ):
                        size = chunk_sizes[chunk_index]
                        chunk_output = self.collate_fn(buffer[:size])
                        print(
                            "writing chunk_output to file: ", chunk_paths[chunk_index]
                        )
                        print("buffer was: ", buffer)
                        with open(chunk_paths[chunk_index], "wb") as handle:
                            pickle.dump({"output": chunk_output}, handle, protocol=5)
                        diagnostics.stream_flushes += 1
                        buffer = buffer[size:]
                        chunk_index += 1

                while (
                    next_point_index < total_points
                    and len(pending) < self.exec_chunk_size
                ):
                    future = executor.submit(exec_fn, stream_points[next_point_index])
                    pending[future] = next_point_index
                    next_point_index += 1

        diagnostics.executed_shards += (
            total_points + self.exec_chunk_size - 1
        ) // self.exec_chunk_size
        return diagnostics


def example_enumerate_points(
    params: dict[str, Any], split_spec: dict[str, Any]
) -> List[Tuple[str, int]]:
    points: List[Tuple[str, int]] = []
    for strat in split_spec["strat"]:
        for s in split_spec["s"]:
            points.append((strat, s))
    return points


def example_exec_fn(params: dict[str, Any], point: Tuple[str, int]) -> dict[str, Any]:
    strat, s = point
    print("function execution with", point, params)
    time.sleep(5)
    return {"strat": strat, "s": s, "value": len(strat) + s}


def example_collate_fn(outputs: List[dict[str, Any]]) -> List[dict[str, Any]]:
    return outputs


def example_merge_fn(chunks: List[List[dict[str, Any]]]) -> List[dict[str, Any]]:
    merged: List[dict[str, Any]] = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


if __name__ == "__main__":
    memo = SwarmMemo(
        cache_root="./memo_cache",
        memo_chunk_spec={"strat": 1, "s": 4},
        exec_chunk_size=3,
        enumerate_points=example_enumerate_points,
        exec_fn=example_exec_fn,
        collate_fn=example_collate_fn,
        merge_fn=example_merge_fn,
        max_workers=4,
    )

    params = {"alpha": 0.4}
    split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
    output, diag = memo.run(params, split_spec)
    print("Output:", output)
    # diag = memo.run_streaming(params, split_spec)
    print("Diagnostics:", dataclasses.asdict(diag))
