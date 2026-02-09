from __future__ import annotations

from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from ._format import chunk_key_size, print_chunk_summary
from .runner_protocol import (
    BuildItemMapsFromChunkOutputFn,
    CacheProtocol,
    ChunkHashFn,
    ResolveCachePathFn,
    RunnerContext,
    UpdateChunkIndexFn,
    WriteMetadataFn,
    WriteChunkPayloadFn,
    CollectChunkDataFn,
    ExtractItemsFromMapFn,
    LoadPayloadFn,
)
from .runners_parallel import run_parallel, run_parallel_streaming
from .runners_common import (
    ChunkKey,
    Diagnostics,
    _log_chunk,
    _merge_outputs,
    _payload_item_map,
    _stream_item_count,
    prepare_progress_callbacks,
    resolve_chunk_path,
    resolve_runner_deps,
)

MergeFn = Callable[[list[Any]], Any]


def run(
    cache: Any,
    exec_fn: Callable[..., Any],
    *,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    # Manual cache methods (optional overrides)
    write_metadata: WriteMetadataFn | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    context: RunnerContext | None = None,
) -> Tuple[Any, Diagnostics]:
    """Run memoized execution with output via the cache runner.

    The cache must already represent the desired axis subset.
    collate_fn overrides cache.collate_fn for this run.
    """
    exec_fn_bound = cache.bind_exec_fn(exec_fn)
    write_metadata_fn = (
        write_metadata if write_metadata is not None else cache.write_metadata
    )
    write_metadata_fn()
    chunk_keys = cache.resolved_chunk_keys()
    return run_chunks(
        chunk_keys,
        exec_fn_bound,
        cache=cache,
        collate_fn=collate_fn,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        collect_chunk_data=collect_chunk_data,
        extract_items_from_map=extract_items_from_map,
        context=context,
    )


def run_streaming(
    cache: Any,
    exec_fn: Callable[..., Any],
    *,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    # Manual cache methods (optional overrides)
    write_metadata: WriteMetadataFn | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    context: RunnerContext | None = None,
) -> Diagnostics:
    """Run memoized execution without returning outputs.

    The cache must already represent the desired axis subset.
    collate_fn is accepted for API parity but has no effect without outputs.
    """
    exec_fn_bound = cache.bind_exec_fn(exec_fn)
    write_metadata_fn = (
        write_metadata if write_metadata is not None else cache.write_metadata
    )
    write_metadata_fn()
    chunk_keys = cache.resolved_chunk_keys()
    return run_chunks_streaming(
        chunk_keys,
        exec_fn_bound,
        cache=cache,
        collate_fn=collate_fn,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        collect_chunk_data=collect_chunk_data,
        extract_items_from_map=extract_items_from_map,
        context=context,
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
    cache: CacheProtocol,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    # Manual cache methods (optional overrides)
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    context: RunnerContext | None = None,
) -> Tuple[Any, Diagnostics]:
    """Run a list of chunk keys and return merged output.

    collate_fn overrides cache.collate_fn for this run.
    """
    deps = resolve_runner_deps(
        cache=cache,
        context=context,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        collect_chunk_data=collect_chunk_data,
        extract_items_from_map=extract_items_from_map,
        require=[
            "context",
            "chunk_hash",
            "resolve_cache_path",
            "load_payload",
            "write_chunk_payload",
            "update_chunk_index",
            "build_item_maps_from_chunk_output",
            "collect_chunk_data",
            "extract_items_from_map",
        ],
    )
    context = cast(RunnerContext, deps.context)
    collect_chunk_data = cast(CollectChunkDataFn, deps.collect_chunk_data)
    extract_items_from_map = cast(ExtractItemsFromMapFn, deps.extract_items_from_map)
    requested_items_map = cache.requested_items_by_chunk()
    collate_fn_resolved: MergeFn
    if collate_fn is not None:
        collate_fn_resolved = collate_fn
    elif context.collate_fn is not None:
        collate_fn_resolved = context.collate_fn
    else:
        collate_fn_resolved = lambda chunk: chunk
    diagnostics = Diagnostics(total_chunks=len(chunk_keys))
    total_chunks = len(chunk_keys)
    total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
    report_progress, update_processed = prepare_progress_callbacks(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=context.verbose,
        label="planning",
    )

    def process_chunk(chunk_key: ChunkKey) -> tuple[Any, bool, bool]:
        chunk_hash_value, path = resolve_chunk_path(
            cast(ChunkHashFn, deps.chunk_hash),
            cast(ResolveCachePathFn, deps.resolve_cache_path),
            chunk_key,
        )
        payload = cast(LoadPayloadFn, deps.load_payload)(path)
        existing_payload = payload
        requested_items = (
            requested_items_map.get(chunk_key)
            if requested_items_map is not None
            else None
        )
        if payload is not None:
            cached_output = collect_chunk_data(
                payload,
                chunk_key,
                requested_items,
                collate_fn_resolved,
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
            resolve_cache_path=cast(ResolveCachePathFn, deps.resolve_cache_path),
            write_chunk_payload=cast(WriteChunkPayloadFn, deps.write_chunk_payload),
            update_chunk_index=cast(UpdateChunkIndexFn, deps.update_chunk_index),
            build_item_maps_from_chunk_output=cast(
                BuildItemMapsFromChunkOutputFn, deps.build_item_maps_from_chunk_output
            ),
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
        result = (
            collate_fn_resolved([extracted]) if extracted is not None else chunk_output
        )
        return result, False, False

    outputs: list[Any] = []
    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        output, cached, _ = process_chunk(chunk_key)
        if cached:
            diagnostics.cached_chunks += 1
        outputs.append(output)
        report_progress(processed, processed == total_chunks)

    merged = _merge_outputs(
        context,
        outputs,
        diagnostics,
        collate_fn=collate_fn_resolved,
    )
    print_chunk_summary(diagnostics, context.verbose)
    return merged, diagnostics


def run_chunks_streaming(
    chunk_keys: Sequence[ChunkKey],
    exec_fn: Callable[..., Any],
    *,
    cache: CacheProtocol,
    collate_fn: Callable[[list[Any]], Any] | None = None,
    # Manual cache methods (optional overrides)
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    context: RunnerContext | None = None,
) -> Diagnostics:
    """Run chunks and flush payloads to disk only.

    collate_fn is accepted for API parity but has no effect without outputs.
    """
    deps = resolve_runner_deps(
        cache=cache,
        context=context,
        chunk_hash=chunk_hash,
        resolve_cache_path=resolve_cache_path,
        load_payload=load_payload,
        write_chunk_payload=write_chunk_payload,
        update_chunk_index=update_chunk_index,
        build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
        collect_chunk_data=collect_chunk_data,
        extract_items_from_map=extract_items_from_map,
        require=[
            "context",
            "chunk_hash",
            "resolve_cache_path",
            "load_payload",
            "write_chunk_payload",
            "update_chunk_index",
            "build_item_maps_from_chunk_output",
            "collect_chunk_data",
            "extract_items_from_map",
        ],
    )
    context = cast(RunnerContext, deps.context)
    requested_items_map = cache.requested_items_by_chunk()
    build_item_maps_from_chunk_output = cast(
        BuildItemMapsFromChunkOutputFn, deps.build_item_maps_from_chunk_output
    )
    diagnostics = Diagnostics(total_chunks=len(chunk_keys))
    total_chunks = len(chunk_keys)
    total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
    report_progress, update_processed = prepare_progress_callbacks(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=context.verbose,
        label="planning",
    )

    for processed, chunk_key in enumerate(chunk_keys, start=1):
        update_processed(chunk_key_size(chunk_key))
        chunk_hash_value, path = resolve_chunk_path(
            cast(ChunkHashFn, deps.chunk_hash),
            cast(ResolveCachePathFn, deps.resolve_cache_path),
            chunk_key,
        )
        payload = cast(LoadPayloadFn, deps.load_payload)(path)
        if payload is not None:
            if requested_items_map is None:
                diagnostics.cached_chunks += 1
                _log_chunk(context, "load", chunk_key, None)
                report_progress(processed, processed == total_chunks)
                continue
            item_map = _payload_item_map(
                build_item_maps_from_chunk_output=build_item_maps_from_chunk_output,
                resolve_cache_path=cast(ResolveCachePathFn, deps.resolve_cache_path),
                write_chunk_payload=cast(WriteChunkPayloadFn, deps.write_chunk_payload),
                chunk_key=chunk_key,
                payload=payload,
                chunk_hash=chunk_hash_value,
                write_back=True,
            )
            if item_map is not None:
                diagnostics.cached_chunks += 1
                requested_items = requested_items_map.get(chunk_key)
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
            resolve_cache_path=cast(ResolveCachePathFn, deps.resolve_cache_path),
            write_chunk_payload=cast(WriteChunkPayloadFn, deps.write_chunk_payload),
            update_chunk_index=cast(UpdateChunkIndexFn, deps.update_chunk_index),
            build_item_maps_from_chunk_output=cast(
                BuildItemMapsFromChunkOutputFn, deps.build_item_maps_from_chunk_output
            ),
            existing_payload=None,
        )

        requested_items = (
            requested_items_map.get(chunk_key)
            if requested_items_map is not None
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
