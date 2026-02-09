from __future__ import annotations

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from ._format import chunk_key_size, prepare_progress, print_chunk_summary
from .runner_protocol import (
    BuildItemMapsFromChunkOutputFn,
    CacheProtocol,
    ChunkHashFn,
    CollectChunkDataFn,
    ExtractItemsFromMapFn,
    LoadPayloadFn,
    ResolveCachePathFn,
    RunnerContext,
    UpdateChunkIndexFn,
    WriteChunkPayloadFn,
)
from .runners_common import (
    ChunkKey,
    Diagnostics,
    _log_chunk,
    _merge_outputs,
    _payload_item_map,
    _stream_item_count,
)
from .runners_parallel import memo_parallel_run, memo_parallel_run_streaming

MergeFn = Callable[[list[Any]], Any]


@dataclasses.dataclass
class _RunDeps:
    chunk_hash: ChunkHashFn
    resolve_cache_path: ResolveCachePathFn
    load_payload: LoadPayloadFn
    write_chunk_payload: WriteChunkPayloadFn
    update_chunk_index: UpdateChunkIndexFn
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn
    extract_items_from_map: ExtractItemsFromMapFn | None = None
    collect_chunk_data: CollectChunkDataFn | None = None
    context: RunnerContext | None = None
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None


def _resolve_run_deps(
    *,
    cache: CacheProtocol | None,
    context: RunnerContext | None,
    chunk_hash: ChunkHashFn | None,
    resolve_cache_path: ResolveCachePathFn | None,
    load_payload: LoadPayloadFn | None,
    write_chunk_payload: WriteChunkPayloadFn | None,
    update_chunk_index: UpdateChunkIndexFn | None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None,
    extract_items_from_map: ExtractItemsFromMapFn | None,
    collect_chunk_data: CollectChunkDataFn | None,
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None,
    require_item_extractors: bool,
) -> _RunDeps:
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

    chunk_hash_resolved = resolve("chunk_hash", chunk_hash)
    resolve_cache_path_resolved = resolve("resolve_cache_path", resolve_cache_path)
    load_payload_resolved = resolve("load_payload", load_payload)
    write_chunk_payload_resolved = resolve("write_chunk_payload", write_chunk_payload)
    update_chunk_index_resolved = resolve("update_chunk_index", update_chunk_index)
    build_item_maps_from_chunk_output_resolved = resolve(
        "build_item_maps_from_chunk_output", build_item_maps_from_chunk_output
    )

    extract_items_from_map_resolved = None
    collect_chunk_data_resolved = None
    if require_item_extractors:
        extract_items_from_map_resolved = resolve(
            "extract_items_from_map", extract_items_from_map
        )
        collect_chunk_data_resolved = resolve("collect_chunk_data", collect_chunk_data)
    else:
        if extract_items_from_map is not None:
            extract_items_from_map_resolved = extract_items_from_map
        elif cache is not None and hasattr(cache, "extract_items_from_map"):
            extract_items_from_map_resolved = cache.extract_items_from_map
        if collect_chunk_data is not None:
            collect_chunk_data_resolved = collect_chunk_data
        elif cache is not None and hasattr(cache, "collect_chunk_data"):
            collect_chunk_data_resolved = cache.collect_chunk_data

    requested_items_resolved = requested_items_by_chunk
    if (
        requested_items_resolved is None
        and cache is not None
        and hasattr(cache, "requested_items_by_chunk")
    ):
        requested_items_resolved = cache.requested_items_by_chunk()

    if missing:
        missing = list(dict.fromkeys(missing))
        raise ValueError(
            "Missing runner dependencies: "
            + ", ".join(missing)
            + ". Provide them explicitly or pass cache=..."
        )

    return _RunDeps(
        chunk_hash=cast(ChunkHashFn, chunk_hash_resolved),
        resolve_cache_path=cast(ResolveCachePathFn, resolve_cache_path_resolved),
        load_payload=cast(LoadPayloadFn, load_payload_resolved),
        write_chunk_payload=cast(WriteChunkPayloadFn, write_chunk_payload_resolved),
        update_chunk_index=cast(UpdateChunkIndexFn, update_chunk_index_resolved),
        build_item_maps_from_chunk_output=cast(
            BuildItemMapsFromChunkOutputFn, build_item_maps_from_chunk_output_resolved
        ),
        extract_items_from_map=cast(
            ExtractItemsFromMapFn | None, extract_items_from_map_resolved
        ),
        collect_chunk_data=cast(CollectChunkDataFn | None, collect_chunk_data_resolved),
        context=cast(RunnerContext, context_resolved),
        requested_items_by_chunk=requested_items_resolved,
    )


def _load_chunk_payload(
    chunk_hash: ChunkHashFn,
    resolve_cache_path: ResolveCachePathFn,
    load_payload: LoadPayloadFn,
    chunk_key: ChunkKey,
) -> tuple[str, dict[str, Any] | None]:
    chunk_hash_value = chunk_hash(chunk_key)
    path = resolve_cache_path(chunk_key, chunk_hash_value)
    payload = load_payload(path)
    return chunk_hash_value, payload


def prepare_chunk_run(
    context: RunnerContext, chunk_keys: Sequence[ChunkKey]
) -> tuple[Diagnostics, Callable[[int, bool], None], Callable[[int], None], int]:
    """Prepare progress tracking for a chunk run."""
    diagnostics = Diagnostics(total_chunks=len(chunk_keys))
    total_chunks = len(chunk_keys)
    total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
    report_progress, update_processed = prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=context.verbose,
        label="planning",
    )
    return diagnostics, report_progress, update_processed, total_chunks


def run(
    cache: Any,
    exec_fn: Callable[..., Any],
    *,
    axis_indices: Mapping[str, Any] | None = None,
    **axes: Any,
) -> Tuple[Any, Diagnostics]:
    """Run memoized execution with output via the cache runner."""
    if axis_indices is not None or axes:
        sliced = cache.slice(cache.params, axis_indices=axis_indices, **axes)
        return run(sliced, exec_fn)
    exec_fn_bound = cache.bind_exec_fn(exec_fn)
    cache.write_metadata()
    chunk_keys = cache.resolved_chunk_keys()
    return run_chunks(
        chunk_keys,
        exec_fn_bound,
        cache=cache,
    )


def run_streaming(
    cache: Any,
    exec_fn: Callable[..., Any],
    *,
    axis_indices: Mapping[str, Any] | None = None,
    **axes: Any,
) -> Diagnostics:
    """Run memoized execution without returning outputs."""
    if axis_indices is not None or axes:
        sliced = cache.slice(cache.params, axis_indices=axis_indices, **axes)
        return run_streaming(sliced, exec_fn)
    exec_fn_bound = cache.bind_exec_fn(exec_fn)
    cache.write_metadata()
    chunk_keys = cache.resolved_chunk_keys()
    return run_chunks_streaming(
        chunk_keys,
        exec_fn_bound,
        cache=cache,
    )


def execute_and_save_chunk(
    chunk_key: ChunkKey,
    exec_fn: Callable[..., Any],
    chunk_hash: str,
    diagnostics: Diagnostics,
    *,
    resolve_cache_path: ResolveCachePathFn,
    write_chunk_payload: WriteChunkPayloadFn,
    update_chunk_index: UpdateChunkIndexFn,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn,
    existing_payload: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any] | None]:
    """Execute a chunk, persist payload, and update index."""
    diagnostics.executed_chunks += 1
    chunk_axes = {axis: list(values) for axis, values in chunk_key}
    chunk_output = exec_fn(**chunk_axes)
    diagnostics.max_stream_items = max(
        diagnostics.max_stream_items,
        _stream_item_count(chunk_output),
    )
    payload: dict[str, Any] = {}
    item_map, item_axis_vals = build_item_maps_from_chunk_output(
        chunk_key,
        chunk_output=chunk_output,
    )
    if item_map is not None:
        payload["items"] = item_map
        if item_axis_vals is not None:
            payload["axis_vals"] = item_axis_vals
    else:
        payload["output"] = chunk_output
    path = resolve_cache_path(chunk_key, chunk_hash)
    write_chunk_payload(
        path,
        payload,
        existing=existing_payload,
    )
    update_chunk_index(chunk_hash, chunk_key)
    return chunk_output, item_map


def run_chunks(
    chunk_keys: Sequence[ChunkKey],
    exec_fn: Callable[..., Any],
    *,
    cache: CacheProtocol | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    context: RunnerContext | None = None,
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None,
) -> Tuple[Any, Diagnostics]:
    """Run a list of chunk keys and return merged output."""
    deps = _resolve_run_deps(
        cache=cache,
        context=context,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        extract_items_from_map=extract_items_from_map,
        collect_chunk_data=collect_chunk_data,
        requested_items_by_chunk=requested_items_by_chunk,
        require_item_extractors=True,
    )
    context = cast(RunnerContext, deps.context)
    collect_chunk_data = cast(CollectChunkDataFn, deps.collect_chunk_data)
    extract_items_from_map = cast(ExtractItemsFromMapFn, deps.extract_items_from_map)
    requested_items_by_chunk = deps.requested_items_by_chunk
    collate_fn: MergeFn = (
        context.merge_fn if context.merge_fn is not None else lambda chunk: chunk
    )
    diagnostics, report_progress, update_processed, total_chunks = prepare_chunk_run(
        context, chunk_keys
    )

    def process_chunk(chunk_key: ChunkKey) -> tuple[Any, bool, bool]:
        chunk_hash_value, payload = _load_chunk_payload(
            deps.chunk_hash, deps.resolve_cache_path, deps.load_payload, chunk_key
        )
        existing_payload = payload
        requested_items = (
            requested_items_by_chunk.get(chunk_key)
            if requested_items_by_chunk is not None
            else None
        )
        if payload is not None:
            cached_output = collect_chunk_data(
                payload,
                chunk_key,
                requested_items,
                collate_fn,
            )
            if cached_output is not None:
                _log_chunk(
                    context,
                    "load",
                    chunk_key,
                    None if requested_items is None else len(requested_items),
                )
                return cached_output, True, False

        chunk_output, item_map = execute_and_save_chunk(
            chunk_key,
            exec_fn,
            chunk_hash_value,
            diagnostics,
            resolve_cache_path=deps.resolve_cache_path,
            write_chunk_payload=deps.write_chunk_payload,
            update_chunk_index=deps.update_chunk_index,
            build_item_maps_from_chunk_output=deps.build_item_maps_from_chunk_output,
            existing_payload=existing_payload,
        )

        if requested_items is None:
            _log_chunk(context, "run", chunk_key, None)
            return chunk_output, False, False
        _log_chunk(context, "run", chunk_key, len(requested_items))
        extracted = extract_items_from_map(
            item_map,
            chunk_key,
            requested_items,
        )
        result = collate_fn([extracted]) if extracted is not None else chunk_output
        return result, False, False

    outputs: list[Any] = []
    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        output, cached, _ = process_chunk(chunk_key)
        if cached:
            diagnostics.cached_chunks += 1
        outputs.append(output)
        report_progress(processed, processed == total_chunks)

    merged = _merge_outputs(context, outputs, diagnostics)
    print_chunk_summary(diagnostics, context.verbose)
    return merged, diagnostics


def run_chunks_streaming(
    chunk_keys: Sequence[ChunkKey],
    exec_fn: Callable[..., Any],
    *,
    cache: CacheProtocol | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    context: RunnerContext | None = None,
    requested_items_by_chunk: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None,
) -> Diagnostics:
    """Run chunks and flush payloads to disk only."""
    deps = _resolve_run_deps(
        cache=cache,
        context=context,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        extract_items_from_map=None,
        collect_chunk_data=None,
        requested_items_by_chunk=requested_items_by_chunk,
        require_item_extractors=False,
    )
    context = cast(RunnerContext, deps.context)
    requested_items_by_chunk = deps.requested_items_by_chunk
    build_item_maps_from_chunk_output = deps.build_item_maps_from_chunk_output
    diagnostics, report_progress, update_processed, total_chunks = prepare_chunk_run(
        context, chunk_keys
    )

    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        chunk_hash_value, payload = _load_chunk_payload(
            deps.chunk_hash, deps.resolve_cache_path, deps.load_payload, chunk_key
        )
        if payload is not None:
            if requested_items_by_chunk is None:
                diagnostics.cached_chunks += 1
                _log_chunk(context, "load", chunk_key, None)
                report_progress(processed, processed == total_chunks)
                continue
            item_map = _payload_item_map(
                build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
                resolve_cache_path=deps.resolve_cache_path,
                write_chunk_payload=deps.write_chunk_payload,
                chunk_key=chunk_key,
                payload=payload,
                chunk_hash=chunk_hash_value,
                write_back=True,
            )
            if item_map is not None:
                diagnostics.cached_chunks += 1
                requested_items = requested_items_by_chunk.get(chunk_key)
                _log_chunk(
                    context,
                    "load",
                    chunk_key,
                    None if requested_items is None else len(requested_items),
                )
                report_progress(processed, processed == total_chunks)
                continue

        execute_and_save_chunk(
            chunk_key,
            exec_fn,
            chunk_hash_value,
            diagnostics,
            resolve_cache_path=deps.resolve_cache_path,
            write_chunk_payload=deps.write_chunk_payload,
            update_chunk_index=deps.update_chunk_index,
            build_item_maps_from_chunk_output=deps.build_item_maps_from_chunk_output,
            existing_payload=None,
        )

        requested_items = (
            requested_items_by_chunk.get(chunk_key)
            if requested_items_by_chunk is not None
            else None
        )
        _log_chunk(
            context,
            "run",
            chunk_key,
            None if requested_items is None else len(requested_items),
        )

        report_progress(processed, processed == total_chunks)

    print_chunk_summary(diagnostics, context.verbose)
    return diagnostics
