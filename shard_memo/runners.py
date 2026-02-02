import dataclasses
import functools
import inspect
import itertools
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence, Tuple, cast

from ._format import (
    build_plan_lines,
    chunk_key_size,
    format_rate_eta,
    prepare_progress,
    print_chunk_summary,
    print_detail,
    print_progress,
)
from .cache_utils import _apply_payload_timestamps, _atomic_write_pickle
from .runner_protocol import CacheStatus, MemoRunnerBackend

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MergeFn = Callable[[list[Any]], Any]
EXEC_REPORT_INTERVAL_SECONDS = 2.0


@dataclasses.dataclass
class Diagnostics:
    """Execution diagnostics for run and parallel runners."""

    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    merges: int = 0
    max_stream_items: int = 0
    stream_flushes: int = 0
    max_parallel_items: int = 0


@dataclasses.dataclass
class _ParallelSetup:
    map_fn: Callable[..., Iterable[Any]]
    params_dict: dict[str, Any]
    cached_chunks: list[ChunkKey]
    missing_chunks: list[ChunkKey]
    diagnostics: Diagnostics
    collate_fn: Callable[[list[Any]], Any]
    map_fn_kwargs: Mapping[str, Any]


@dataclasses.dataclass
class _MissingTracker:
    missing_items: list[Any] = dataclasses.field(default_factory=list)
    missing_items_by_chunk: dict[ChunkKey, list[Any]] = dataclasses.field(
        default_factory=dict
    )
    missing_chunk_order: list[ChunkKey] = dataclasses.field(default_factory=list)
    missing_item_keys: list[ChunkKey] | None = None

    @classmethod
    def create(cls, *, track_item_keys: bool) -> "_MissingTracker":
        missing_item_keys = [] if track_item_keys else None
        return cls(missing_item_keys=missing_item_keys)

    def register(self, chunk_key: ChunkKey, items_to_add: list[Any]) -> None:
        if not items_to_add:
            return
        if chunk_key not in self.missing_items_by_chunk:
            self.missing_items_by_chunk[chunk_key] = []
            self.missing_chunk_order.append(chunk_key)
        self.missing_items_by_chunk[chunk_key].extend(items_to_add)
        self.missing_items.extend(items_to_add)
        if self.missing_item_keys is not None:
            self.missing_item_keys.extend([chunk_key] * len(items_to_add))


def _stream_item_count(output: Any) -> int:
    if isinstance(output, (list, tuple, dict)):
        return len(output)
    return 1


def _payload_item_map(
    memo: MemoRunnerBackend,
    chunk_key: ChunkKey,
    payload: dict[str, Any],
    *,
    path: Path | None = None,
    write_back: bool = False,
) -> dict[str, Any] | None:
    item_map = payload.get("items")
    if item_map is None:
        item_map, item_axis_vals = memo.build_item_maps_from_chunk_output(
            chunk_key,
            chunk_output=payload.get("output"),
        )
        if item_map is not None:
            payload["items"] = item_map
            if item_axis_vals is not None:
                payload["axis_vals"] = item_axis_vals
            if write_back and path is not None:
                _atomic_write_pickle(path, payload)
    return item_map


def _save_chunk_payload(
    memo: MemoRunnerBackend,
    params_dict: dict[str, Any],
    chunk_key: ChunkKey,
    chunk_output: Any,
    item_map: dict[str, Any] | None,
    cached_payloads: dict[ChunkKey, Mapping[str, Any]],
    diagnostics: Diagnostics,
    chunk_hash: str,
    path: Path,
    missing_chunks: list[ChunkKey],
    spec_fn: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if chunk_key in missing_chunks:
        payload: dict[str, Any] = {}
        if item_map is not None:
            payload["items"] = item_map
        else:
            payload["output"] = chunk_output
    else:
        existing = cached_payloads.get(chunk_key, {})
        payload = dict(existing)
        existing_items = existing.get("items", {})
        merged_items = dict(existing_items)
        if item_map is not None:
            merged_items.update(item_map)
        payload["items"] = merged_items
        if "output" not in payload and chunk_output is not None and item_map is None:
            payload["output"] = chunk_output
    _apply_payload_timestamps(
        payload,
        existing=cached_payloads.get(chunk_key),
    )
    if spec_fn is not None:
        payload["axis_vals"] = spec_fn()
    _atomic_write_pickle(path, payload)
    memo.update_chunk_index(params_dict, chunk_hash, chunk_key)
    return payload


def _exec_with_item(
    exec_fn: Callable[..., Any],
    params: dict[str, Any],
    item: Any,
) -> Any:
    if isinstance(item, Mapping):
        return exec_fn(params, **item)
    return exec_fn(params, item)


def _map_executor(
    func: Callable[..., Any],
    items: Iterable[Any],
    **kwargs: Any,
) -> Iterable[Any]:
    max_workers = kwargs.get("max_workers")
    chunksize = kwargs.get("chunksize")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if chunksize is None:
            yield from executor.map(func, items)
            return
        yield from executor.map(func, items, chunksize=chunksize)


def _require_params(cache_status: CacheStatus) -> dict[str, Any]:
    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    return dict(params)


def _resolve_cache_status(
    memo: MemoRunnerBackend,
    cache_status: CacheStatus | None,
    params: dict[str, Any] | None,
    axis_indices: Mapping[str, Any] | None,
    axes: Mapping[str, Any],
) -> CacheStatus:
    if cache_status is not None:
        if params is not None or axis_indices is not None or axes:
            raise ValueError(
                "cache_status cannot be combined with params or axis selections"
            )
        return cache_status
    if params is None:
        raise ValueError("params is required when cache_status is not provided")
    return memo.cache_status(params, axis_indices=axis_indices, **axes)


def _prepare_parallel_setup(
    memo: MemoRunnerBackend,
    cache_status: CacheStatus,
    *,
    map_fn: Callable[..., Iterable[Any]] | None,
    map_fn_kwargs: Mapping[str, Any] | None,
    collate_fn: Callable[[list[Any]], Any] | None,
    params_dict: dict[str, Any] | None = None,
    write_metadata: bool = True,
) -> _ParallelSetup:
    if map_fn is None:
        if memo.verbose == 1:
            map_fn = _map_executor
        else:
            map_fn = lambda func, items, **kwargs: list(
                _map_executor(func, items, **kwargs)
            )
    map_fn = cast(Callable[..., Iterable[Any]], map_fn)

    if params_dict is None:
        params_dict = _require_params(cache_status)
    if write_metadata:
        memo.write_metadata(params_dict)

    cached_chunks: list[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: list[ChunkKey] = list(cache_status.get("missing_chunks", []))

    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))

    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda chunk: chunk
    collate_fn = cast(Callable[[list[Any]], Any], collate_fn)

    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    if memo.verbose == 1:
        axis_values = cache_status.get("axis_values")
        if isinstance(axis_values, Mapping):
            _, axis_order, _ = memo.expand_cache_status(cache_status)
            lines = build_plan_lines(
                params_dict,
                axis_values,
                axis_order,
                len(cached_chunks),
                len(missing_chunks),
            )
            print("\n".join(lines))

    return _ParallelSetup(
        map_fn=map_fn,
        params_dict=params_dict,
        cached_chunks=cached_chunks,
        missing_chunks=missing_chunks,
        diagnostics=diagnostics,
        collate_fn=collate_fn,
        map_fn_kwargs=map_fn_kwargs,
    )


def _prepare_parallel_items(
    memo: MemoRunnerBackend,
    cache_status: CacheStatus,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cached_chunks: Sequence[ChunkKey],
    missing_chunks: Sequence[ChunkKey],
) -> tuple[
    list[Any],
    Callable[[Any], Tuple[Any, ...]] | None,
    list[list[Any]],
    list[list[Any]],
]:
    item_list = items if isinstance(items, list) else list(items)
    if not item_list:
        return item_list, None, [], []
    axis_extractor = _build_item_axis_extractor(
        memo,
        cache_status,
        item_list,
        exec_fn,
    )
    cached_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        missing_chunks,
        axis_extractor,
    )
    return item_list, axis_extractor, cached_chunk_items, missing_chunk_items


def _scan_cached_chunk_items(
    memo: MemoRunnerBackend,
    params_dict: dict[str, Any],
    cached_chunks: Sequence[ChunkKey],
    cached_chunk_items: Sequence[Sequence[Any]],
    *,
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
    report_progress: Callable[[int, bool], None],
    update_processed: Callable[[int], None],
    total_chunks: int,
    tracker: _MissingTracker,
    diagnostics: Diagnostics,
    cached_payloads: dict[ChunkKey, Mapping[str, Any]],
    chunk_exists: Callable[[str, Path], bool],
    load_full_chunk_payload: bool,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    outputs: list[Any] | None = None,
) -> None:
    for processed, (chunk_key, chunk_items) in enumerate(
        zip(cached_chunks, cached_chunk_items), start=1
    ):
        if not chunk_items:
            report_progress(processed, processed == total_chunks)
            continue
        update_processed(len(chunk_items))
        full_chunk = len(chunk_items) == chunk_key_size(chunk_key)
        chunk_hash = memo.chunk_hash(params_dict, chunk_key)
        path = memo.resolve_cache_path(params_dict, chunk_key, chunk_hash)
        if not chunk_exists(chunk_hash, path):
            tracker.register(chunk_key, list(chunk_items))
            continue
        if full_chunk and not load_full_chunk_payload:
            diagnostics.cached_chunks += 1
            if memo.verbose >= 2:
                print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
            report_progress(processed, processed == total_chunks)
            continue
        payload = memo.load_payload(path)
        if payload is None:
            tracker.register(chunk_key, list(chunk_items))
            continue
        if full_chunk:
            diagnostics.cached_chunks += 1
            if memo.verbose >= 2:
                print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
            if outputs is not None and collate_fn is not None:
                chunk_output = payload.get("output")
                if chunk_output is None:
                    items_payload = payload.get("items")
                    if items_payload is not None and isinstance(items_payload, Mapping):
                        chunk_output = memo.reconstruct_output_from_items(
                            chunk_key, items_payload
                        )
                if chunk_output is None:
                    raise ValueError("Cache payload missing required data")
                outputs.append(chunk_output)
            report_progress(processed, processed == total_chunks)
            continue
        item_map = _payload_item_map(memo, chunk_key, payload)
        if item_map is None:
            cached_payloads[chunk_key] = payload
            tracker.register(chunk_key, list(chunk_items))
            continue
        item_outputs: list[Any] = []
        missing = False
        for item in chunk_items:
            axis_values = axis_extractor(item)
            item_key = memo.item_hash(chunk_key, axis_values)
            if item_key not in item_map:
                cached_payloads[chunk_key] = payload
                tracker.register(chunk_key, list(chunk_items))
                missing = True
                item_outputs = []
                break
            if outputs is not None and collate_fn is not None:
                item_outputs.append(item_map[item_key])
        if not missing:
            diagnostics.cached_chunks += 1
            if memo.verbose >= 2:
                print_detail(
                    f"[ShardMemo] load chunk={chunk_key} items={len(chunk_items)}"
                )
            if outputs is not None and collate_fn is not None and item_outputs:
                outputs.append(collate_fn(item_outputs))
        report_progress(processed, processed == total_chunks)


def _exec_iter_with_progress(
    memo: MemoRunnerBackend,
    exec_iter: Iterable[Any],
    *,
    cached_items_total: int,
    total_items_all: int,
    total_missing: int,
) -> Iterable[tuple[int, Any]]:
    exec_start = time.monotonic()
    last_report = exec_start
    report_interval = EXEC_REPORT_INTERVAL_SECONDS
    for index, result in enumerate(exec_iter, start=1):
        now = time.monotonic()
        if memo.verbose == 1 and (now - last_report) >= report_interval:
            processed_items = cached_items_total + index
            message = format_rate_eta(
                "exec_items",
                processed_items,
                total_items_all,
                exec_start,
                rate_processed=index,
                rate_total=total_missing,
            )
            print_progress(message, final=False)
            last_report = now
        yield index, result
    if memo.verbose == 1:
        processed_items = cached_items_total + total_missing
        message = format_rate_eta(
            "exec_items",
            processed_items,
            total_items_all,
            exec_start,
            rate_processed=total_missing,
            rate_total=total_missing,
        )
        print_progress(message, final=True)


def _register_missing_chunk_items(
    missing_chunks: Sequence[ChunkKey],
    missing_chunk_items: Sequence[Sequence[Any]],
    tracker: _MissingTracker,
    update_processed: Callable[[int], None],
    report_progress: Callable[[int, bool], None],
    *,
    base_index: int,
    total_chunks: int,
) -> None:
    for offset, (chunk_key, chunk_items) in enumerate(
        zip(missing_chunks, missing_chunk_items), start=1
    ):
        if chunk_items:
            tracker.register(chunk_key, list(chunk_items))
        update_processed(len(chunk_items))
        report_progress(
            base_index + offset,
            (base_index + offset) == total_chunks,
        )


def _finalize_missing_items(
    tracker: _MissingTracker,
    missing_chunks: Sequence[ChunkKey],
    item_list: Sequence[Any],
) -> list[Any]:
    if not tracker.missing_items and missing_chunks:
        return list(item_list)
    return tracker.missing_items


def _build_item_axis_extractor(
    memo: MemoRunnerBackend,
    cache_status: CacheStatus,
    items: Sequence[Any],
    exec_fn: Callable[..., Any],
) -> Callable[[Any], Tuple[Any, ...]]:
    _, axis_order, _ = memo.expand_cache_status(cache_status)

    if items:
        sample = items[0]
        if isinstance(sample, Mapping):
            axis_names = list(axis_order)

            def extract(item: Any) -> Tuple[Any, ...]:
                if not isinstance(item, Mapping):
                    raise ValueError("Mixed item types: expected mapping items")
                return tuple(item[name] for name in axis_names)

            return extract
        if isinstance(sample, (tuple, list)):
            if len(sample) == len(axis_order):
                axis_count = len(axis_order)

                def extract(item: Any) -> Tuple[Any, ...]:
                    if isinstance(item, Mapping):
                        raise ValueError("Mixed item types: expected positional items")
                    if isinstance(item, tuple):
                        values = item
                    elif isinstance(item, list):
                        values = tuple(item)
                    else:
                        values = (item,)
                    if len(values) != axis_count:
                        raise ValueError(
                            "Item does not match axis order length for memo axes"
                        )
                    return tuple(values)

                return extract
        if len(axis_order) == 1:

            def extract(item: Any) -> Tuple[Any, ...]:
                if isinstance(item, Mapping):
                    raise ValueError("Mixed item types: expected positional items")
                return (item,)

            return extract

    signature = inspect.signature(exec_fn)
    param_names = list(signature.parameters)
    if not param_names or param_names[0] != "params":
        raise ValueError("exec_fn must accept 'params' as the first argument")
    axis_names = [name for name in param_names[1:] if name in axis_order]
    if not axis_names:
        raise ValueError(
            "Positional items require exec_fn to list axis arguments in its signature"
        )
    axis_positions = {name: index for index, name in enumerate(axis_names)}

    def extract(item: Any) -> Tuple[Any, ...]:
        if isinstance(item, Mapping):
            raise ValueError("Mixed item types: expected positional items")
        if isinstance(item, tuple):
            values = item
        elif isinstance(item, list):
            values = tuple(item)
        else:
            values = (item,)
        if len(values) < len(axis_names):
            raise ValueError("Item does not provide enough positional values")
        values_by_axis = {
            axis_name: values[index] for axis_name, index in axis_positions.items()
        }
        missing = [name for name in axis_order if name not in values_by_axis]
        if missing:
            raise ValueError(
                f"Item does not provide positional values for axes: {missing}"
            )
        return tuple(values_by_axis[name] for name in axis_order)

    return extract


def _expand_items_to_chunks_fast(
    memo: MemoRunnerBackend,
    cache_status: CacheStatus,
    items: Sequence[Any],
    chunk_keys: Sequence[ChunkKey],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
) -> list[list[Any]]:
    if not chunk_keys:
        return [[] for _ in chunk_keys]
    _, axis_order, axis_chunk_maps = memo.expand_cache_status(cache_status)
    chunk_index_map: dict[Tuple[int, ...], int] = {}
    for index, chunk_key in enumerate(chunk_keys):
        chunk_ids: list[int] = []
        for axis, values in chunk_key:
            chunk_ids.append(axis_chunk_maps[axis][values[0]])
        chunk_index_map[tuple(chunk_ids)] = index

    chunked: list[list[Any]] = [[] for _ in chunk_keys]
    for item in items:
        axis_vals = axis_extractor(item)
        if len(axis_vals) != len(axis_order):
            raise ValueError("Item axis values do not match axis order")
        chunk_ids = []
        for axis, value in zip(axis_order, axis_vals):
            if value not in axis_chunk_maps[axis]:
                raise KeyError(
                    f"Value '{value}' not found in axis_values for axis '{axis}'"
                )
            chunk_ids.append(axis_chunk_maps[axis][value])
        chunk_index = chunk_index_map.get(tuple(chunk_ids))
        if chunk_index is None:
            continue
        chunked[chunk_index].append(item)
    return chunked


def _extract_axis_values(
    items: Sequence[Any], axis_extractor: Callable[[Any], Tuple[Any, ...]]
) -> list[Tuple[Any, ...]]:
    return [axis_extractor(item) for item in items]


def _load_chunk_payload(
    cache: MemoRunnerBackend,
    params: dict[str, Any],
    chunk_key: ChunkKey,
) -> tuple[str, Path, dict[str, Any] | None]:
    chunk_hash = cache.chunk_hash(params, chunk_key)
    path = cache.resolve_cache_path(params, chunk_key, chunk_hash)
    payload = cache.load_payload(path)
    return chunk_hash, path, payload


def prepare_chunk_run(
    cache: MemoRunnerBackend, chunk_keys: Sequence[ChunkKey]
) -> tuple[Diagnostics, Callable[[int, bool], None], Callable[[int], None], int]:
    """Prepare progress tracking for a chunk run."""
    diagnostics = Diagnostics(total_chunks=len(chunk_keys))
    total_chunks = len(chunk_keys)
    total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
    report_progress, update_processed = prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=cache.verbose,
        label="planning",
    )
    return diagnostics, report_progress, update_processed, total_chunks


def run(
    memo: MemoRunnerBackend,
    params: dict[str, Any],
    exec_fn: Callable[..., Any],
    *,
    axis_indices: Mapping[str, Any] | None = None,
    **axes: Any,
) -> Tuple[Any, Diagnostics]:
    """Run memoized execution with output via the cache runner."""
    axis_values, chunk_keys, requested_items = memo.prepare_run(
        params, axis_indices, **axes
    )
    return run_chunks(
        memo,
        params,
        chunk_keys,
        exec_fn,
        requested_items_by_chunk=requested_items,
    )


def run_streaming(
    memo: MemoRunnerBackend,
    params: dict[str, Any],
    exec_fn: Callable[..., Any],
    *,
    axis_indices: Mapping[str, Any] | None = None,
    **axes: Any,
) -> Diagnostics:
    """Run memoized execution without returning outputs."""
    axis_values, chunk_keys, requested_items = memo.prepare_run(
        params, axis_indices, **axes
    )
    return run_chunks_streaming(
        memo,
        params,
        chunk_keys,
        exec_fn,
        requested_items_by_chunk=requested_items,
    )


def execute_and_save_chunk(
    cache: MemoRunnerBackend,
    params: dict[str, Any],
    chunk_key: ChunkKey,
    exec_fn: Callable[..., Any],
    path: Path,
    chunk_hash: str,
    diagnostics: Diagnostics,
    existing_payload: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any] | None]:
    """Execute a chunk, persist payload, and update index."""
    diagnostics.executed_chunks += 1
    chunk_axes = {axis: list(values) for axis, values in chunk_key}
    chunk_output = exec_fn(params, **chunk_axes)
    diagnostics.max_stream_items = max(
        diagnostics.max_stream_items,
        _stream_item_count(chunk_output),
    )
    payload: dict[str, Any] = {}
    item_map, item_axis_vals = cache.build_item_maps_from_chunk_output(
        chunk_key,
        chunk_output=chunk_output,
    )
    if item_map is not None:
        payload["items"] = item_map
        if item_axis_vals is not None:
            payload["axis_vals"] = item_axis_vals
    else:
        payload["output"] = chunk_output
    _apply_payload_timestamps(payload, existing=existing_payload)
    _atomic_write_pickle(path, payload)
    cache.update_chunk_index(params, chunk_hash, chunk_key)
    return chunk_output, item_map


def run_chunks(
    cache: MemoRunnerBackend,
    params: dict[str, Any],
    chunk_keys: Sequence[ChunkKey],
    exec_fn: Callable[..., Any],
    *,
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None,
) -> Tuple[Any, Diagnostics]:
    """Run a list of chunk keys and return merged output."""
    collate_fn: MergeFn = (
        cache.merge_fn if cache.merge_fn is not None else lambda chunk: chunk
    )
    diagnostics, report_progress, update_processed, total_chunks = prepare_chunk_run(
        cache, chunk_keys
    )

    def process_chunk(chunk_key: ChunkKey, processed: int) -> tuple[Any, bool, bool]:
        chunk_hash, path, payload = _load_chunk_payload(cache, params, chunk_key)
        existing_payload = payload
        requested_items = None
        if requested_items_by_chunk is not None:
            requested_items = requested_items_by_chunk.get(chunk_key)
        if payload is not None:
            cached_output = cache.load_cached_output(
                payload,
                chunk_key,
                requested_items,
                collate_fn,
            )
            if cached_output is not None:
                if cache.verbose >= 2:
                    if requested_items is None:
                        item_count = "all"
                    else:
                        item_count = len(requested_items)
                    print_detail(
                        f"[ShardMemo] load chunk={chunk_key} items={item_count}"
                    )
                return cached_output, True, False

        chunk_output, item_map = execute_and_save_chunk(
            cache,
            params,
            chunk_key,
            exec_fn,
            path,
            chunk_hash,
            diagnostics,
            existing_payload,
        )

        if requested_items_by_chunk is None:
            if cache.verbose >= 2:
                print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
            return chunk_output, False, False
        requested_items = requested_items_by_chunk.get(chunk_key)
        if requested_items is None:
            if cache.verbose >= 2:
                print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
            return chunk_output, False, False
        if cache.verbose >= 2:
            print_detail(
                f"[ShardMemo] run chunk={chunk_key} items={len(requested_items)}"
            )
        extracted = cache.extract_items_from_map(
            item_map,
            chunk_key,
            requested_items,
        )
        result = collate_fn([extracted]) if extracted is not None else chunk_output
        return result, False, False

    outputs: list[Any] = []
    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        output, cached, _ = process_chunk(chunk_key, processed)
        if cached:
            diagnostics.cached_chunks += 1
        outputs.append(output)
        report_progress(processed, processed == total_chunks)

    diagnostics.merges += 1
    if cache.merge_fn is not None:
        merged = cache.merge_fn(outputs)
    else:
        merged = outputs
    print_chunk_summary(diagnostics, cache.verbose)
    return merged, diagnostics


def run_chunks_streaming(
    cache: MemoRunnerBackend,
    params: dict[str, Any],
    chunk_keys: Sequence[ChunkKey],
    exec_fn: Callable[..., Any],
    *,
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None,
) -> Diagnostics:
    """Run chunks and flush payloads to disk only."""
    diagnostics, report_progress, update_processed, total_chunks = prepare_chunk_run(
        cache, chunk_keys
    )

    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        chunk_hash, path, payload = _load_chunk_payload(cache, params, chunk_key)
        if payload is not None:
            if requested_items_by_chunk is None:
                diagnostics.cached_chunks += 1
                if cache.verbose >= 2:
                    print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
                report_progress(processed, processed == total_chunks)
                continue
            item_map = _payload_item_map(
                cache,
                chunk_key,
                payload,
                path=path,
                write_back=True,
            )
            if item_map is not None:
                diagnostics.cached_chunks += 1
                if cache.verbose >= 2:
                    requested_items = requested_items_by_chunk.get(chunk_key)
                    item_count = (
                        "all" if requested_items is None else len(requested_items)
                    )
                    print_detail(
                        f"[ShardMemo] load chunk={chunk_key} items={item_count}"
                    )
                report_progress(processed, processed == total_chunks)
                continue

        execute_and_save_chunk(
            cache,
            params,
            chunk_key,
            exec_fn,
            path,
            chunk_hash,
            diagnostics,
            None,
        )

        if cache.verbose >= 2:
            if requested_items_by_chunk is None:
                print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
            else:
                requested_items = requested_items_by_chunk.get(chunk_key)
                item_count = "all" if requested_items is None else len(requested_items)
                print_detail(f"[ShardMemo] run chunk={chunk_key} items={item_count}")

        report_progress(processed, processed == total_chunks)

    print_chunk_summary(diagnostics, cache.verbose)
    return diagnostics


def memo_parallel_run(
    memo: MemoRunnerBackend,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: CacheStatus | None = None,
    params: dict[str, Any] | None = None,
    axis_indices: Mapping[str, Any] | None = None,
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    **axes: Any,
) -> tuple[Any, Diagnostics]:
    """Execute items in parallel, reusing cached chunk data.

    Provide cache_status directly, or supply params/axis selections to build it.
    """
    cache_status = _resolve_cache_status(
        memo,
        cache_status,
        params,
        axis_indices,
        axes,
    )
    setup = _prepare_parallel_setup(
        memo,
        cache_status,
        map_fn=map_fn,
        map_fn_kwargs=map_fn_kwargs,
        collate_fn=collate_fn,
    )
    params_dict = setup.params_dict
    cached_chunks = setup.cached_chunks
    missing_chunks = setup.missing_chunks
    diagnostics = setup.diagnostics
    collate_fn = setup.collate_fn
    map_fn = setup.map_fn
    map_fn_kwargs = setup.map_fn_kwargs

    outputs: list[Any] = []
    exec_outputs: list[Any] = []
    total_chunks = diagnostics.total_chunks

    item_list, axis_extractor, cached_chunk_items, missing_chunk_items = (
        _prepare_parallel_items(
            memo,
            cache_status,
            items,
            exec_fn=exec_fn,
            cached_chunks=cached_chunks,
            missing_chunks=missing_chunks,
        )
    )
    if not item_list or axis_extractor is None:
        return [], diagnostics
    total_items = len(item_list)
    report_progress_main, update_processed = prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=memo.verbose,
        label="planning",
    )

    tracker = _MissingTracker.create(track_item_keys=False)
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}

    _scan_cached_chunk_items(
        memo,
        params_dict,
        cached_chunks,
        cached_chunk_items,
        axis_extractor=axis_extractor,
        report_progress=report_progress_main,
        update_processed=update_processed,
        total_chunks=total_chunks,
        tracker=tracker,
        diagnostics=diagnostics,
        cached_payloads=cached_payloads,
        chunk_exists=lambda _chunk_hash, path: path.exists(),
        load_full_chunk_payload=True,
        collate_fn=collate_fn,
        outputs=outputs,
    )

    base_index = len(cached_chunks)
    _register_missing_chunk_items(
        missing_chunks,
        missing_chunk_items,
        tracker,
        update_processed,
        report_progress_main,
        base_index=base_index,
        total_chunks=total_chunks,
    )

    missing_items = _finalize_missing_items(tracker, missing_chunks, item_list)

    if missing_items:
        diagnostics.executed_chunks = len(tracker.missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_iter = map_fn(
            exec_fn,
            missing_items,
            **map_fn_kwargs,
        )
        exec_outputs = []
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        for _, result in _exec_iter_with_progress(
            memo,
            exec_iter,
            cached_items_total=cached_items_total,
            total_items_all=total_items_all,
            total_missing=len(missing_items),
        ):
            exec_outputs.append(result)

        cursor = 0
        for chunk_key in tracker.missing_chunk_order:
            chunk_items = tracker.missing_items_by_chunk.get(chunk_key, [])
            chunk_size = len(chunk_items)
            if chunk_size == 0:
                continue
            chunk_outputs = exec_outputs[cursor : cursor + chunk_size]
            chunk_output = collate_fn(chunk_outputs)
            axis_values = _extract_axis_values(chunk_items, axis_extractor)
            item_map, item_axis_vals = memo.build_item_maps_from_axis_values(
                chunk_key,
                axis_values,
                chunk_outputs,
            )
            chunk_hash = memo.chunk_hash(params_dict, chunk_key)
            path = memo.resolve_cache_path(params_dict, chunk_key, chunk_hash)
            _save_chunk_payload(
                memo,
                params_dict,
                chunk_key,
                chunk_output,
                item_map,
                cached_payloads,
                diagnostics,
                chunk_hash,
                path,
                missing_chunks,
                lambda: item_axis_vals,
            )
            if memo.verbose >= 2:
                print_detail(f"[ShardMemo] run chunk={chunk_key} items={chunk_size}")
            outputs.append(chunk_output)
            cursor += chunk_size
            report_progress_main(
                base_index + len(missing_chunks),
                (base_index + len(missing_chunks)) == total_chunks,
            )
    diagnostics.merges += 1
    if memo.merge_fn is not None:
        merged = memo.merge_fn(outputs)
    else:
        merged = outputs
    if not merged and item_list:
        merged = exec_outputs if missing_items else []
    print_chunk_summary(diagnostics, memo.verbose)
    report_progress_main(total_chunks, True)
    return merged, diagnostics


def memo_parallel_run_streaming(
    memo: MemoRunnerBackend,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: CacheStatus | None = None,
    params: dict[str, Any] | None = None,
    axis_indices: Mapping[str, Any] | None = None,
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    **axes: Any,
) -> Diagnostics:
    """Parallel streaming run that flushes chunk payloads as ready.

    Provide cache_status directly, or supply params/axis selections to build it.
    """
    cache_status = _resolve_cache_status(
        memo,
        cache_status,
        params,
        axis_indices,
        axes,
    )
    profile_start = time.monotonic() if memo.profile else None
    params_dict = _require_params(cache_status)
    memo.write_metadata(params_dict)
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(f"[ShardMemo] profile metadata_s={time.monotonic() - profile_start:0.3f}")

    setup = _prepare_parallel_setup(
        memo,
        cache_status,
        map_fn=map_fn,
        map_fn_kwargs=map_fn_kwargs,
        collate_fn=collate_fn,
        params_dict=params_dict,
        write_metadata=False,
    )
    cached_chunks = setup.cached_chunks
    missing_chunks = setup.missing_chunks
    diagnostics = setup.diagnostics
    collate_fn = setup.collate_fn
    map_fn = setup.map_fn
    map_fn_kwargs = setup.map_fn_kwargs
    total_chunks = diagnostics.total_chunks

    chunk_index = memo.load_chunk_index(params_dict) or {}
    use_index = bool(chunk_index)
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile cache_status_s={time.monotonic() - profile_start:0.3f}"
        )

    item_list, axis_extractor, cached_chunk_items, missing_chunk_items = (
        _prepare_parallel_items(
            memo,
            cache_status,
            items,
            exec_fn=exec_fn,
            cached_chunks=cached_chunks,
            missing_chunks=missing_chunks,
        )
    )
    if not item_list or axis_extractor is None:
        return diagnostics
    total_items = len(item_list)
    report_progress_streaming, update_processed = prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=memo.verbose,
        label="planning",
    )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile item_plan_s={time.monotonic() - profile_start:0.3f}"
        )

    tracker = _MissingTracker.create(track_item_keys=True)
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}

    def chunk_exists(chunk_hash: str, path: Path) -> bool:
        if use_index:
            return chunk_hash in chunk_index
        return path.exists()

    _scan_cached_chunk_items(
        memo,
        params_dict,
        cached_chunks,
        cached_chunk_items,
        axis_extractor=axis_extractor,
        report_progress=report_progress_streaming,
        update_processed=update_processed,
        total_chunks=total_chunks,
        tracker=tracker,
        diagnostics=diagnostics,
        cached_payloads=cached_payloads,
        chunk_exists=chunk_exists,
        load_full_chunk_payload=False,
    )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile cached_scan_s={time.monotonic() - profile_start:0.3f}"
        )

    base_index = len(cached_chunks)
    _register_missing_chunk_items(
        missing_chunks,
        missing_chunk_items,
        tracker,
        update_processed,
        report_progress_streaming,
        base_index=base_index,
        total_chunks=total_chunks,
    )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile missing_map_s={time.monotonic() - profile_start:0.3f}"
        )

    missing_items = _finalize_missing_items(tracker, missing_chunks, item_list)
    missing_item_keys = tracker.missing_item_keys
    if missing_item_keys is None:
        raise ValueError("Missing item keys for streaming run")

    if missing_items:
        diagnostics.executed_chunks = len(tracker.missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_iter = map_fn(
            exec_fn,
            missing_items,
            **map_fn_kwargs,
        )
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        expected_counts = {
            chunk_key: len(items)
            for chunk_key, items in tracker.missing_items_by_chunk.items()
        }
        buffers: dict[ChunkKey, dict[str, list[Any]]] = {}
        current_buffer_items = 0
        for index, result in _exec_iter_with_progress(
            memo,
            exec_iter,
            cached_items_total=cached_items_total,
            total_items_all=total_items_all,
            total_missing=len(missing_items),
        ):
            chunk_key = missing_item_keys[index - 1]
            buffer = buffers.setdefault(chunk_key, {"items": [], "outputs": []})
            buffer["items"].append(missing_items[index - 1])
            buffer["outputs"].append(result)
            current_buffer_items += 1
            diagnostics.max_parallel_items = max(
                diagnostics.max_parallel_items, current_buffer_items
            )
            if len(buffer["outputs"]) < expected_counts[chunk_key]:
                continue

            chunk_outputs = buffer["outputs"]
            chunk_items = buffer["items"]
            chunk_output = collate_fn(chunk_outputs)
            axis_values = _extract_axis_values(chunk_items, axis_extractor)
            item_map, item_axis_vals = memo.build_item_maps_from_axis_values(
                chunk_key,
                axis_values,
                chunk_outputs,
            )
            chunk_hash = memo.chunk_hash(params_dict, chunk_key)
            path = memo.resolve_cache_path(params_dict, chunk_key, chunk_hash)
            _save_chunk_payload(
                memo,
                params_dict,
                chunk_key,
                chunk_output,
                item_map,
                cached_payloads,
                diagnostics,
                chunk_hash,
                path,
                missing_chunks,
                lambda: item_axis_vals,
            )
            diagnostics.stream_flushes += 1
            if memo.verbose >= 2:
                print_detail(
                    f"[ShardMemo] run chunk={chunk_key} items={len(chunk_items)}"
                )
            current_buffer_items -= len(chunk_items)
            buffers.pop(chunk_key, None)

    if memo.verbose >= 1:
        print_detail(
            f"[ShardMemo] stream_mem_max items={diagnostics.max_parallel_items}"
        )
    print_chunk_summary(diagnostics, memo.verbose)
    report_progress_streaming(total_chunks, True)

    return diagnostics
