from __future__ import annotations

import dataclasses
import functools
import inspect
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence, Tuple, cast

from ._format import (
    build_plan_lines,
    chunk_key_size,
    format_rate_eta,
    print_chunk_summary,
    print_progress,
)
from .runner_protocol import (
    BuildItemMapsFromAxisValuesFn,
    BuildItemMapsFromChunkOutputFn,
    CacheProtocol,
    CacheStatus,
    ChunkHashFn,
    CollectChunkDataFn,
    ItemHashFn,
    LoadChunkIndexFn,
    LoadPayloadFn,
    ReconstructOutputFromItemsFn,
    ResolveCachePathFn,
    RunnerContext,
    UpdateChunkIndexFn,
    WriteChunkPayloadFn,
    WriteMetadataFn,
)
from .runners_common import (
    ChunkKey,
    Diagnostics,
    _log_chunk,
    _merge_outputs,
    _payload_item_map,
    _require_axis_info,
    _require_params,
    _save_chunk_payload,
    prepare_progress_callbacks,
    resolve_chunk_path,
)

EXEC_REPORT_INTERVAL_SECONDS = 2.0


@dataclasses.dataclass
class _RunnerDeps:
    write_metadata: WriteMetadataFn
    chunk_hash: ChunkHashFn
    resolve_cache_path: ResolveCachePathFn
    load_payload: LoadPayloadFn
    write_chunk_payload: WriteChunkPayloadFn
    update_chunk_index: UpdateChunkIndexFn
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn
    reconstruct_output_from_items: ReconstructOutputFromItemsFn
    item_hash: ItemHashFn
    context: RunnerContext
    collect_chunk_data: CollectChunkDataFn | None = None
    load_chunk_index: LoadChunkIndexFn | None = None


def _resolve_runner_deps(
    *,
    cache: CacheProtocol | None,
    context: RunnerContext | None,
    write_metadata: WriteMetadataFn | None,
    chunk_hash: ChunkHashFn | None,
    resolve_cache_path: ResolveCachePathFn | None,
    load_payload: LoadPayloadFn | None,
    write_chunk_payload: WriteChunkPayloadFn | None,
    update_chunk_index: UpdateChunkIndexFn | None,
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn | None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None,
    reconstruct_output_from_items: ReconstructOutputFromItemsFn | None,
    collect_chunk_data: CollectChunkDataFn | None,
    item_hash: ItemHashFn | None,
    load_chunk_index: LoadChunkIndexFn | None,
    require_collect_chunk_data: bool,
    require_load_chunk_index: bool,
) -> _RunnerDeps:
    missing: list[str] = []

    def resolve(name: str, value: Any) -> Any:
        if value is not None:
            return value
        if cache is None:
            missing.append(name)
            return None
        if not hasattr(cache, name):
            missing.append(name)
            return None
        return getattr(cache, name)

    context_resolved = context if context is not None else cache
    if context_resolved is None:
        missing.append("context")

    write_metadata_resolved = resolve("write_metadata", write_metadata)
    chunk_hash_resolved = resolve("chunk_hash", chunk_hash)
    resolve_cache_path_resolved = resolve("resolve_cache_path", resolve_cache_path)
    load_payload_resolved = resolve("load_payload", load_payload)
    write_chunk_payload_resolved = resolve("write_chunk_payload", write_chunk_payload)
    update_chunk_index_resolved = resolve("update_chunk_index", update_chunk_index)
    build_item_maps_from_axis_values_resolved = resolve(
        "build_item_maps_from_axis_values", build_item_maps_from_axis_values
    )
    build_item_maps_from_chunk_output_resolved = resolve(
        "build_item_maps_from_chunk_output", build_item_maps_from_chunk_output
    )
    reconstruct_output_from_items_resolved = resolve(
        "reconstruct_output_from_items", reconstruct_output_from_items
    )
    item_hash_resolved = resolve("item_hash", item_hash)

    collect_chunk_data_resolved = None
    if require_collect_chunk_data:
        collect_chunk_data_resolved = resolve("collect_chunk_data", collect_chunk_data)
    elif collect_chunk_data is not None:
        collect_chunk_data_resolved = collect_chunk_data
    elif cache is not None and hasattr(cache, "collect_chunk_data"):
        collect_chunk_data_resolved = cache.collect_chunk_data

    load_chunk_index_resolved = None
    if require_load_chunk_index:
        load_chunk_index_resolved = resolve("load_chunk_index", load_chunk_index)
    elif load_chunk_index is not None:
        load_chunk_index_resolved = load_chunk_index
    elif cache is not None and hasattr(cache, "load_chunk_index"):
        load_chunk_index_resolved = cache.load_chunk_index

    if missing:
        missing = list(dict.fromkeys(missing))
        raise ValueError(
            "Missing runner dependencies: "
            + ", ".join(missing)
            + ". Provide them explicitly or pass cache=..."
        )

    return _RunnerDeps(
        write_metadata=cast(WriteMetadataFn, write_metadata_resolved),
        chunk_hash=cast(ChunkHashFn, chunk_hash_resolved),
        resolve_cache_path=cast(ResolveCachePathFn, resolve_cache_path_resolved),
        load_payload=cast(LoadPayloadFn, load_payload_resolved),
        write_chunk_payload=cast(WriteChunkPayloadFn, write_chunk_payload_resolved),
        update_chunk_index=cast(UpdateChunkIndexFn, update_chunk_index_resolved),
        build_item_maps_from_axis_values=cast(
            BuildItemMapsFromAxisValuesFn, build_item_maps_from_axis_values_resolved
        ),
        build_item_maps_from_chunk_output=cast(
            BuildItemMapsFromChunkOutputFn, build_item_maps_from_chunk_output_resolved
        ),
        reconstruct_output_from_items=cast(
            ReconstructOutputFromItemsFn, reconstruct_output_from_items_resolved
        ),
        item_hash=cast(ItemHashFn, item_hash_resolved),
        context=cast(RunnerContext, context_resolved),
        collect_chunk_data=cast(CollectChunkDataFn | None, collect_chunk_data_resolved),
        load_chunk_index=cast(LoadChunkIndexFn | None, load_chunk_index_resolved),
    )


@dataclasses.dataclass
class _ParallelSetup:
    map_fn: Callable[..., Iterable[Any]]
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


def _exec_with_item(
    exec_fn: Callable[..., Any],
    item: Any,
) -> Any:
    if isinstance(item, Mapping):
        return exec_fn(**item)
    return exec_fn(item)


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


def _prepare_parallel_setup(
    context: RunnerContext,
    write_metadata_fn: WriteMetadataFn,
    cache_status: CacheStatus,
    *,
    map_fn: Callable[..., Iterable[Any]] | None,
    map_fn_kwargs: Mapping[str, Any] | None,
    collate_fn: Callable[[list[Any]], Any] | None,
    params_dict: dict[str, Any] | None = None,
    write_metadata: bool = True,
) -> _ParallelSetup:
    if map_fn is None:
        if context.verbose == 1:
            map_fn = _map_executor
        else:
            map_fn = lambda func, items, **kwargs: list(
                _map_executor(func, items, **kwargs)
            )
    map_fn = cast(Callable[..., Iterable[Any]], map_fn)

    if params_dict is None:
        params_dict = _require_params(cache_status)
    if write_metadata:
        write_metadata_fn()

    cached_chunks: list[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: list[ChunkKey] = list(cache_status.get("missing_chunks", []))

    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))

    if collate_fn is None:
        collate_fn = (
            context.merge_fn if context.merge_fn is not None else lambda chunk: chunk
        )
    collate_fn = cast(Callable[[list[Any]], Any], collate_fn)

    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    if context.verbose == 1:
        axis_values = cache_status.get("axis_values")
        if isinstance(axis_values, Mapping):
            axis_order, _ = _require_axis_info(cache_status)
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
        cached_chunks=cached_chunks,
        missing_chunks=missing_chunks,
        diagnostics=diagnostics,
        collate_fn=collate_fn,
        map_fn_kwargs=map_fn_kwargs,
    )


def _prepare_parallel_items(
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
        cache_status,
        item_list,
        exec_fn,
    )
    cached_chunk_items = _expand_items_to_chunks_fast(
        cache_status,
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks_fast(
        cache_status,
        item_list,
        missing_chunks,
        axis_extractor,
    )
    return item_list, axis_extractor, cached_chunk_items, missing_chunk_items


def _scan_cached_chunk_items(
    context: RunnerContext,
    chunk_hash: ChunkHashFn,
    resolve_cache_path: ResolveCachePathFn,
    load_payload: LoadPayloadFn,
    reconstruct_output_from_items: ReconstructOutputFromItemsFn,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn,
    write_chunk_payload: WriteChunkPayloadFn,
    item_hash: ItemHashFn,
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
        chunk_hash_value, path = resolve_chunk_path(
            chunk_hash,
            resolve_cache_path,
            chunk_key,
        )
        if not chunk_exists(chunk_hash_value, path):
            tracker.register(chunk_key, list(chunk_items))
            continue
        if full_chunk and not load_full_chunk_payload:
            diagnostics.cached_chunks += 1
            _log_chunk(context, "load", chunk_key, None)
            report_progress(processed, processed == total_chunks)
            continue
        payload = load_payload(path)
        if payload is None:
            tracker.register(chunk_key, list(chunk_items))
            continue
        if full_chunk:
            diagnostics.cached_chunks += 1
            _log_chunk(context, "load", chunk_key, None)
            if outputs is not None and collate_fn is not None:
                chunk_output = payload.get("output")
                if chunk_output is None:
                    items_payload = payload.get("items")
                    if items_payload is not None and isinstance(items_payload, Mapping):
                        chunk_output = reconstruct_output_from_items(
                            chunk_key, items_payload
                        )
                if chunk_output is None:
                    raise ValueError("Cache payload missing required data")
                outputs.append(chunk_output)
            report_progress(processed, processed == total_chunks)
            continue
        item_map = _payload_item_map(
            build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
            resolve_cache_path=resolve_cache_path,
            write_chunk_payload=write_chunk_payload,
            chunk_key=chunk_key,
            payload=payload,
        )
        if item_map is None:
            cached_payloads[chunk_key] = payload
            tracker.register(chunk_key, list(chunk_items))
            continue
        item_outputs: list[Any] = []
        missing = False
        for item in chunk_items:
            axis_values = axis_extractor(item)
            item_key = item_hash(chunk_key, axis_values)
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
            _log_chunk(context, "load", chunk_key, len(chunk_items))
            if outputs is not None and collate_fn is not None and item_outputs:
                outputs.append(collate_fn(item_outputs))
        report_progress(processed, processed == total_chunks)


def _exec_iter_with_progress(
    context: RunnerContext,
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
        if context.verbose == 1 and (now - last_report) >= report_interval:
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
    if context.verbose == 1:
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
    cache_status: CacheStatus,
    items: Sequence[Any],
    exec_fn: Callable[..., Any],
) -> Callable[[Any], Tuple[Any, ...]]:
    axis_order, _ = _require_axis_info(cache_status)

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
    cache_status: CacheStatus,
    items: Sequence[Any],
    chunk_keys: Sequence[ChunkKey],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
) -> list[list[Any]]:
    if not chunk_keys:
        return [[] for _ in chunk_keys]
    axis_order, axis_chunk_maps = _require_axis_info(cache_status)
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


def memo_parallel_run(
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache: CacheProtocol,
    write_metadata: WriteMetadataFn | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    reconstruct_output_from_items: ReconstructOutputFromItemsFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    item_hash: ItemHashFn | None = None,
    context: RunnerContext | None = None,
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[list[Any]], Any] | None = None,
) -> tuple[Any, Diagnostics]:
    """Execute items in parallel, reusing cached chunk data.

    cache must already represent the desired axis subset.
    """
    cache_status = cache.cache_status()
    deps = _resolve_runner_deps(
        cache=cache,
        context=context,
        write_metadata=write_metadata,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_axis_values=build_item_maps_from_axis_values,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        reconstruct_output_from_items=reconstruct_output_from_items,
        collect_chunk_data=collect_chunk_data,
        item_hash=item_hash,
        load_chunk_index=None,
        require_collect_chunk_data=False,
        require_load_chunk_index=False,
    )
    context = cast(RunnerContext, deps.context)
    setup = _prepare_parallel_setup(
        deps.context,
        deps.write_metadata,
        cache_status,
        map_fn=map_fn,
        map_fn_kwargs=map_fn_kwargs,
        collate_fn=collate_fn,
    )
    cached_chunks = setup.cached_chunks
    missing_chunks = setup.missing_chunks
    diagnostics = setup.diagnostics
    collate_fn_resolved = setup.collate_fn
    map_fn_resolved = setup.map_fn
    map_fn_kwargs_resolved = setup.map_fn_kwargs

    outputs: list[Any] = []
    exec_outputs: list[Any] = []
    total_chunks = diagnostics.total_chunks

    item_list, axis_extractor, cached_chunk_items, missing_chunk_items = (
        _prepare_parallel_items(
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
    report_progress_main, update_processed = prepare_progress_callbacks(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=context.verbose,
        label="planning",
    )

    tracker = _MissingTracker.create(track_item_keys=False)
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}

    _scan_cached_chunk_items(
        context,
        deps.chunk_hash,
        deps.resolve_cache_path,
        deps.load_payload,
        deps.reconstruct_output_from_items,
        deps.build_item_maps_from_chunk_output,
        deps.write_chunk_payload,
        deps.item_hash,
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
        collate_fn=collate_fn_resolved,
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
        exec_fn = functools.partial(_exec_with_item, exec_fn)
        exec_iter = map_fn_resolved(
            exec_fn,
            missing_items,
            **map_fn_kwargs_resolved,
        )
        exec_outputs = []
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        for _, result in _exec_iter_with_progress(
            context,
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
            chunk_output = collate_fn_resolved(chunk_outputs)
            axis_values = _extract_axis_values(chunk_items, axis_extractor)
            item_map, item_axis_vals = deps.build_item_maps_from_axis_values(
                chunk_key,
                axis_values,
                chunk_outputs,
            )
            chunk_hash_value = deps.chunk_hash(chunk_key)
            _save_chunk_payload(
                resolve_cache_path=deps.resolve_cache_path,
                write_chunk_payload=deps.write_chunk_payload,
                update_chunk_index=deps.update_chunk_index,
                chunk_key=chunk_key,
                chunk_output=chunk_output,
                item_map=item_map,
                cached_payloads=cached_payloads,
                chunk_hash=chunk_hash_value,
                missing_chunks=missing_chunks,
                spec_fn=lambda: item_axis_vals,
            )
            _log_chunk(context, "run", chunk_key, chunk_size)
            outputs.append(chunk_output)
            cursor += chunk_size
            report_progress_main(
                base_index + len(missing_chunks),
                (base_index + len(missing_chunks)) == total_chunks,
            )
    merged = _merge_outputs(context, outputs, diagnostics)
    if not merged and item_list:
        merged = exec_outputs if missing_items else []
    print_chunk_summary(diagnostics, context.verbose)
    report_progress_main(total_chunks, True)
    return merged, diagnostics


def memo_parallel_run_streaming(
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache: CacheProtocol,
    write_metadata: WriteMetadataFn | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    load_chunk_index: LoadChunkIndexFn | None = None,
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    reconstruct_output_from_items: ReconstructOutputFromItemsFn | None = None,
    item_hash: ItemHashFn | None = None,
    context: RunnerContext | None = None,
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[list[Any]], Any] | None = None,
) -> Diagnostics:
    """Parallel streaming run that flushes chunk payloads as ready.

    cache must already represent the desired axis subset.
    """
    cache_status = cache.cache_status()
    deps = _resolve_runner_deps(
        cache=cache,
        context=context,
        write_metadata=write_metadata,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_axis_values=build_item_maps_from_axis_values,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        reconstruct_output_from_items=reconstruct_output_from_items,
        collect_chunk_data=None,
        item_hash=item_hash,
        load_chunk_index=load_chunk_index,
        require_collect_chunk_data=False,
        require_load_chunk_index=True,
    )
    context = cast(RunnerContext, deps.context)
    profile_start = time.monotonic() if context.profile else None
    params_dict = _require_params(cache_status)
    deps.write_metadata()
    if context.profile and context.verbose >= 1 and profile_start is not None:
        print(f"[ShardMemo] profile metadata_s={time.monotonic() - profile_start:0.3f}")

    setup = _prepare_parallel_setup(
        deps.context,
        deps.write_metadata,
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
    collate_fn_resolved = setup.collate_fn
    map_fn_resolved = setup.map_fn
    map_fn_kwargs_resolved = setup.map_fn_kwargs
    total_chunks = diagnostics.total_chunks

    load_chunk_index = cast(LoadChunkIndexFn, deps.load_chunk_index)
    chunk_index = load_chunk_index() or {}
    use_index = bool(chunk_index)
    if context.profile and context.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile cache_status_s={time.monotonic() - profile_start:0.3f}"
        )

    item_list, axis_extractor, cached_chunk_items, missing_chunk_items = (
        _prepare_parallel_items(
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
    report_progress_streaming, update_processed = prepare_progress_callbacks(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=context.verbose,
        label="planning",
    )
    if context.profile and context.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile item_plan_s={time.monotonic() - profile_start:0.3f}"
        )

    tracker = _MissingTracker.create(track_item_keys=True)
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}
    queued_chunks: dict[ChunkKey, list[Any]] = {}
    queued_outputs: dict[ChunkKey, list[Any]] = {}

    _scan_cached_chunk_items(
        context,
        deps.chunk_hash,
        deps.resolve_cache_path,
        deps.load_payload,
        deps.reconstruct_output_from_items,
        deps.build_item_maps_from_chunk_output,
        deps.write_chunk_payload,
        deps.item_hash,
        cached_chunks,
        cached_chunk_items,
        axis_extractor=axis_extractor,
        report_progress=report_progress_streaming,
        update_processed=update_processed,
        total_chunks=total_chunks,
        tracker=tracker,
        diagnostics=diagnostics,
        cached_payloads=cached_payloads,
        chunk_exists=lambda _chunk_hash, path: (
            _chunk_hash in chunk_index if use_index else path.exists()
        ),
        load_full_chunk_payload=False,
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

    missing_items = _finalize_missing_items(tracker, missing_chunks, item_list)
    missing_item_keys = tracker.missing_item_keys or []

    if missing_items:
        diagnostics.executed_chunks = len(tracker.missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn)
        exec_iter = map_fn_resolved(
            exec_fn,
            missing_items,
            **map_fn_kwargs_resolved,
        )
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        for item_idx, result in _exec_iter_with_progress(
            context,
            exec_iter,
            cached_items_total=cached_items_total,
            total_items_all=total_items_all,
            total_missing=len(missing_items),
        ):
            chunk_key = missing_item_keys[item_idx - 1]
            queued_chunks.setdefault(chunk_key, []).append(missing_items[item_idx - 1])
            queued_outputs.setdefault(chunk_key, []).append(result)
            if len(queued_outputs[chunk_key]) < chunk_key_size(chunk_key):
                continue
            chunk_items = queued_chunks.pop(chunk_key, [])
            chunk_outputs = queued_outputs.pop(chunk_key, [])
            chunk_output = collate_fn_resolved(chunk_outputs)
            axis_values = _extract_axis_values(chunk_items, axis_extractor)
            item_map, item_axis_vals = deps.build_item_maps_from_axis_values(
                chunk_key,
                axis_values,
                chunk_outputs,
            )
            chunk_hash_value = deps.chunk_hash(chunk_key)
            _save_chunk_payload(
                resolve_cache_path=deps.resolve_cache_path,
                write_chunk_payload=deps.write_chunk_payload,
                update_chunk_index=deps.update_chunk_index,
                chunk_key=chunk_key,
                chunk_output=chunk_output,
                item_map=item_map,
                cached_payloads=cached_payloads,
                chunk_hash=chunk_hash_value,
                missing_chunks=missing_chunks,
                spec_fn=lambda: item_axis_vals,
            )
            _log_chunk(context, "run", chunk_key, len(chunk_items))
            diagnostics.max_parallel_items = max(
                diagnostics.max_parallel_items, len(chunk_items)
            )
            diagnostics.stream_flushes += 1

        if queued_outputs:
            for chunk_key, chunk_outputs in queued_outputs.items():
                chunk_items = queued_chunks.get(chunk_key, [])
                chunk_output = collate_fn_resolved(chunk_outputs)
                axis_values = _extract_axis_values(chunk_items, axis_extractor)
                item_map, item_axis_vals = deps.build_item_maps_from_axis_values(
                    chunk_key,
                    axis_values,
                    chunk_outputs,
                )
                chunk_hash_value = deps.chunk_hash(chunk_key)
                _save_chunk_payload(
                    resolve_cache_path=deps.resolve_cache_path,
                    write_chunk_payload=deps.write_chunk_payload,
                    update_chunk_index=deps.update_chunk_index,
                    chunk_key=chunk_key,
                    chunk_output=chunk_output,
                    item_map=item_map,
                    cached_payloads=cached_payloads,
                    chunk_hash=chunk_hash_value,
                    missing_chunks=missing_chunks,
                    spec_fn=lambda: item_axis_vals,
                )
                _log_chunk(context, "run", chunk_key, len(chunk_items))
                diagnostics.max_parallel_items = max(
                    diagnostics.max_parallel_items, len(chunk_items)
                )
                diagnostics.stream_flushes += 1

    print_chunk_summary(diagnostics, context.verbose)
    report_progress_streaming(total_chunks, True)
    return diagnostics
