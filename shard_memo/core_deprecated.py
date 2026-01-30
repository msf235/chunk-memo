from __future__ import annotations

import dataclasses
import functools
import hashlib
import inspect
import itertools
import pickle
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
Chunk = Sequence[Any]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[Chunk]]
PointEnumerator = Callable[[dict[str, Any], dict[str, Any]], Sequence[Any]]


@dataclasses.dataclass
class Diagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    executed_shards: int = 0
    merges: int = 0
    stream_flushes: int = 0
    max_stream_buffer: int = 0
    max_out_of_order: int = 0
    max_in_memory_results: int = 0


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


class SwarmMemo:
    def __init__(
        self,
        cache_root: str | Path,
        memo_chunk_spec: dict[str, Any],
        exec_chunk_size: int,
        exec_fn: Callable[[dict[str, Any], Any], Any],
        collate_fn: Callable[[List[Any]], Any],
        merge_fn: Callable[[List[Any]], Any],
        point_enumerator: PointEnumerator | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
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
            exec_fn: Executes one point with full params, signature (params, point).
            collate_fn: Combines per-point outputs into a chunk output.
            merge_fn: Combines chunk outputs into the final output.
            point_enumerator: Optional point enumerator that defines ordering
                within each memo chunk.
            memo_chunk_enumerator: Optional chunk enumerator that defines the
                memo chunk order. This also defines the ordering of points that
                will be used when building and running execution chunks.
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
        self.point_enumerator = point_enumerator
        self.memo_chunk_enumerator = memo_chunk_enumerator
        self.exec_fn = exec_fn
        self.collate_fn = collate_fn
        self.merge_fn = merge_fn
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_version = cache_version
        self.max_workers = max_workers
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose

    def run_wrap(
        self, *, params_arg: str = "params", split_arg: str = "axis_values"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output."""
        return self._build_wrapper(
            params_arg=params_arg, split_arg=split_arg, streaming=False
        )

    def streaming_wrap(
        self, *, params_arg: str = "params", split_arg: str = "axis_values"
    ) -> Callable[[Callable[..., Any]], Callable[..., Diagnostics]]:
        """Decorator for streaming memoized execution to disk only."""
        return self._build_wrapper(
            params_arg=params_arg, split_arg=split_arg, streaming=True
        )

    def _build_wrapper(
        self, *, params_arg: str, split_arg: str, streaming: bool
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            signature = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if split_arg not in kwargs:
                    raise ValueError(f"Missing required keyword argument '{split_arg}'")
                axis_values = kwargs.pop(split_arg)

                if params_arg in kwargs and args:
                    raise ValueError(
                        f"'{params_arg}' passed as both positional and keyword"
                    )

                bound = signature.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                if params_arg not in bound.arguments:
                    raise ValueError(f"Missing required argument '{params_arg}'")

                params = bound.arguments[params_arg]
                if not isinstance(params, dict):
                    raise ValueError(f"'{params_arg}' must be a dict")
                extras = {k: v for k, v in bound.arguments.items() if k != params_arg}
                merged_params = dict(params)
                merged_params.update(extras)

                try:
                    pickle.dumps(func)
                except Exception as exc:
                    raise ValueError(
                        "exec_fn must be a top-level function to work with "
                        "ProcessPoolExecutor"
                    ) from exc

                exec_fn = functools.partial(func, **extras)
                runner = self._clone_with_exec_fn(exec_fn)
                if streaming:
                    return runner.run_streaming(merged_params, axis_values)
                return runner.run(merged_params, axis_values)

            return wrapper

        return decorator

    def _clone_with_exec_fn(
        self, exec_fn: Callable[[dict[str, Any], Any], Any]
    ) -> "SwarmMemo":
        return SwarmMemo(
            cache_root=self.cache_root,
            memo_chunk_spec=self.memo_chunk_spec,
            exec_chunk_size=self.exec_chunk_size,
            exec_fn=exec_fn,
            collate_fn=self.collate_fn,
            merge_fn=self.merge_fn,
            point_enumerator=self.point_enumerator,
            memo_chunk_enumerator=self.memo_chunk_enumerator,
            chunk_hash_fn=self.chunk_hash_fn,
            cache_version=self.cache_version,
            max_workers=self.max_workers,
            axis_order=self.axis_order,
            verbose=self.verbose,
        )

    def run(
        self, params: dict[str, Any], axis_values: dict[str, Any]
    ) -> Tuple[Any, Diagnostics]:
        outputs: List[Any] = []
        chunk_paths: List[Path] = []
        chunk_sizes: List[int] = []
        all_points: List[Any] = []

        chunks = self._build_memo_chunks(params, axis_values)
        diagnostics = Diagnostics(total_chunks=len(chunks))

        for chunk_key, chunk_points in zip(
            self._chunk_keys_iter(chunks, axis_values), chunks
        ):
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self.cache_root / f"{chunk_hash}.pkl"
            if path.exists():
                diagnostics.cached_chunks += 1
                with open(path, "rb") as handle:
                    outputs.append(pickle.load(handle)["output"])
                continue

            diagnostics.executed_chunks += 1
            chunk_paths.append(path)
            chunk_sizes.append(len(chunk_points))
            all_points.extend(chunk_points)

        if all_points:
            exec_outputs = self._execute_points(params, all_points, diagnostics)
            cursor = 0
            for size, path in zip(chunk_sizes, chunk_paths):
                chunk_output = self.collate_fn(exec_outputs[cursor : cursor + size])
                with open(path, "wb") as handle:
                    pickle.dump({"output": chunk_output}, handle, protocol=5)
                outputs.append(chunk_output)
                cursor += size

        diagnostics.merges += 1
        merged = self.merge_fn(outputs)
        return merged, diagnostics

    def run_streaming(
        self, params: dict[str, Any], axis_values: dict[str, Any]
    ) -> Diagnostics:
        """Execute missing chunks and flush outputs to disk only."""
        stream_points: List[Any] = []
        chunk_sizes: List[int] = []
        chunk_paths: List[Path] = []

        chunks = self._build_memo_chunks(params, axis_values)
        diagnostics = Diagnostics(total_chunks=len(chunks))

        for chunk_key, chunk_points in zip(
            self._chunk_keys_iter(chunks, axis_values), chunks
        ):
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self.cache_root / f"{chunk_hash}.pkl"
            if path.exists():
                diagnostics.cached_chunks += 1
                continue

            diagnostics.executed_chunks += 1
            if not chunk_points:
                continue
            stream_points.extend(chunk_points)
            chunk_sizes.append(len(chunk_points))
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
                    print(
                        "appending to size "
                        f"results_by_index={len(results_by_index)} "
                        f"buffer={len(buffer)}"
                    )
                diagnostics.max_out_of_order = max(
                    diagnostics.max_out_of_order, len(results_by_index)
                )

                while next_flush_index in results_by_index:
                    buffer.append(results_by_index.pop(next_flush_index))
                    print(
                        "flushing to size "
                        f"results_by_index={len(results_by_index)} "
                        f"buffer={len(buffer)}"
                    )
                    diagnostics.max_stream_buffer = max(
                        diagnostics.max_stream_buffer, len(buffer)
                    )
                    diagnostics.max_in_memory_results = max(
                        diagnostics.max_in_memory_results,
                        len(buffer) + len(results_by_index),
                    )
                    next_flush_index += 1

                    while (
                        chunk_index < len(chunk_sizes)
                        and len(buffer) >= chunk_sizes[chunk_index]
                    ):
                        size = chunk_sizes[chunk_index]
                        chunk_output = self.collate_fn(buffer[:size])
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

    def _resolve_axis_order(self, axis_values: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(axis_values))

    def _build_chunk_keys(
        self, axis_values: dict[str, Any], axis_order: Sequence[str]
    ) -> List[ChunkKey]:
        axis_chunks: List[List[Tuple[Any, ...]]] = []
        for axis in axis_order:
            values = axis_values.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            size = self._resolve_axis_chunk_size(axis)
            axis_chunks.append(_chunk_values(values, size))

        chunk_keys: List[ChunkKey] = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def _default_point_enumerator(
        self, params: dict[str, Any], axis_values: dict[str, Any]
    ) -> List[Any]:
        axis_order = self._resolve_axis_order(axis_values)
        values = [axis_values[axis] for axis in axis_order]
        return [tuple(point) for point in itertools.product(*values)]

    def _build_memo_chunks(
        self, params: dict[str, Any], axis_values: dict[str, Any]
    ) -> List[List[Any]]:
        if self.memo_chunk_enumerator is not None:
            return [list(chunk) for chunk in self.memo_chunk_enumerator(axis_values)]

        axis_order = self._resolve_axis_order(axis_values)
        chunk_keys = self._build_chunk_keys(axis_values, axis_order)
        enumerator = self.point_enumerator or self._default_point_enumerator
        chunks: List[List[Any]] = []
        for chunk_key in chunk_keys:
            chunk_axis_values = {axis: list(values) for axis, values in chunk_key}
            points = list(enumerator(params, chunk_axis_values))
            chunks.append(points)
        return chunks

    def _chunk_keys_iter(
        self, chunks: Sequence[Sequence[Any]], axis_values: dict[str, Any]
    ) -> Iterable[ChunkKey]:
        if self.memo_chunk_enumerator is None:
            axis_order = self._resolve_axis_order(axis_values)
            return iter(self._build_chunk_keys(axis_values, axis_order))

        return iter(
            self._chunk_key_from_default_chunk(points, axis_values) for points in chunks
        )

    def _chunk_key_from_default_chunk(
        self, points: Sequence[Any], axis_values: dict[str, Any]
    ) -> ChunkKey:
        axis_order = self._resolve_axis_order(axis_values)
        axes: Dict[str, List[Any]] = {axis: [] for axis in axis_order}
        for point in points:
            for axis, value in zip(axis_order, point):
                axes[axis].append(value)
        return tuple((axis, tuple(axes[axis])) for axis in axis_order)

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
        shard_count = (len(points) + self.exec_chunk_size - 1) // self.exec_chunk_size
        diagnostics.executed_shards += shard_count
        exec_fn = functools.partial(self.exec_fn, params)
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            results.extend(
                executor.map(exec_fn, points, chunksize=self.exec_chunk_size)
            )
        return results
