from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from ._format import prepare_progress, print_detail
from .runner_protocol import (
    BuildItemMapsFromAxisValuesFn,
    BuildItemMapsFromChunkOutputFn,
    CacheProtocol,
    CacheStatus,
    ChunkHashFn,
    CollectChunkDataFn,
    ExtractItemsFromMapFn,
    ItemHashFn,
    LoadChunkIndexFn,
    LoadPayloadFn,
    ReconstructOutputFromItemsFn,
    ReconstructPartialOutputFromItemsFn,
    ResolveCachePathFn,
    RunnerContext,
    UpdateChunkIndexFn,
    WriteChunkPayloadFn,
    WriteMetadataFn,
)

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


@dataclasses.dataclass
class Diagnostics:
    """Execution diagnostics for run and parallel runners."""

    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    partial_chunks: int = 0
    merges: int = 0
    max_stream_items: int = 0
    stream_flushes: int = 0
    max_parallel_items: int = 0


@dataclasses.dataclass
class RunnerDeps:
    write_metadata: WriteMetadataFn | None = None
    chunk_hash: ChunkHashFn | None = None
    resolve_cache_path: ResolveCachePathFn | None = None
    load_payload: LoadPayloadFn | None = None
    write_chunk_payload: WriteChunkPayloadFn | None = None
    update_chunk_index: UpdateChunkIndexFn | None = None
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn | None = None
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None
    reconstruct_output_from_items: ReconstructOutputFromItemsFn | None = None
    reconstruct_partial_output_from_items: (
        ReconstructPartialOutputFromItemsFn | None
    ) = None
    collect_chunk_data: CollectChunkDataFn | None = None
    item_hash: ItemHashFn | None = None
    load_chunk_index: LoadChunkIndexFn | None = None
    extract_items_from_map: ExtractItemsFromMapFn | None = None
    context: RunnerContext | None = None


def resolve_runner_deps(
    *,
    cache: CacheProtocol | None,
    context: RunnerContext | None = None,
    write_metadata: WriteMetadataFn | None = None,
    chunk_hash: ChunkHashFn | None = None,
    resolve_cache_path: ResolveCachePathFn | None = None,
    load_payload: LoadPayloadFn | None = None,
    write_chunk_payload: WriteChunkPayloadFn | None = None,
    update_chunk_index: UpdateChunkIndexFn | None = None,
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn | None = None,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn | None = None,
    reconstruct_output_from_items: ReconstructOutputFromItemsFn | None = None,
    reconstruct_partial_output_from_items: (
        ReconstructPartialOutputFromItemsFn | None
    ) = None,
    collect_chunk_data: CollectChunkDataFn | None = None,
    item_hash: ItemHashFn | None = None,
    load_chunk_index: LoadChunkIndexFn | None = None,
    extract_items_from_map: ExtractItemsFromMapFn | None = None,
    require: Sequence[str] | None = None,
) -> RunnerDeps:
    missing: list[str] = []
    require_set = set(require or ())

    def resolve_dep(name: str, value: Any, *, required: bool) -> Any:
        if value is not None:
            return value
        if cache is not None and hasattr(cache, name):
            return getattr(cache, name)
        if required:
            missing.append(name)
        return None

    context_resolved = context if context is not None else cache
    if context_resolved is None and "context" in require_set:
        missing.append("context")

    deps = RunnerDeps(
        write_metadata=resolve_dep(
            "write_metadata", write_metadata, required="write_metadata" in require_set
        ),
        chunk_hash=resolve_dep(
            "chunk_hash", chunk_hash, required="chunk_hash" in require_set
        ),
        resolve_cache_path=resolve_dep(
            "resolve_cache_path",
            resolve_cache_path,
            required="resolve_cache_path" in require_set,
        ),
        load_payload=resolve_dep(
            "load_payload", load_payload, required="load_payload" in require_set
        ),
        write_chunk_payload=resolve_dep(
            "write_chunk_payload",
            write_chunk_payload,
            required="write_chunk_payload" in require_set,
        ),
        update_chunk_index=resolve_dep(
            "update_chunk_index",
            update_chunk_index,
            required="update_chunk_index" in require_set,
        ),
        build_item_maps_from_axis_values=resolve_dep(
            "build_item_maps_from_axis_values",
            build_item_maps_from_axis_values,
            required="build_item_maps_from_axis_values" in require_set,
        ),
        build_item_maps_from_chunk_output=resolve_dep(
            "build_item_maps_from_chunk_output",
            build_item_maps_from_chunk_output,
            required="build_item_maps_from_chunk_output" in require_set,
        ),
        reconstruct_output_from_items=resolve_dep(
            "reconstruct_output_from_items",
            reconstruct_output_from_items,
            required="reconstruct_output_from_items" in require_set,
        ),
        reconstruct_partial_output_from_items=resolve_dep(
            "reconstruct_partial_output_from_items",
            reconstruct_partial_output_from_items,
            required="reconstruct_partial_output_from_items" in require_set,
        ),
        collect_chunk_data=resolve_dep(
            "collect_chunk_data",
            collect_chunk_data,
            required="collect_chunk_data" in require_set,
        ),
        item_hash=resolve_dep(
            "item_hash", item_hash, required="item_hash" in require_set
        ),
        load_chunk_index=resolve_dep(
            "load_chunk_index",
            load_chunk_index,
            required="load_chunk_index" in require_set,
        ),
        extract_items_from_map=resolve_dep(
            "extract_items_from_map",
            extract_items_from_map,
            required="extract_items_from_map" in require_set,
        ),
        context=context_resolved,
    )

    if missing:
        missing = list(dict.fromkeys(missing))
        raise ValueError(
            "Missing runner dependencies: "
            + ", ".join(missing)
            + ". Provide them explicitly or pass cache=..."
        )

    return deps


def resolve_cache_for_run(
    cache: Any,
    *,
    params: dict[str, Any] | None = None,
    axis_values_override: Mapping[str, Sequence[Any]] | None = None,
    extend_cache: bool = False,
    allow_superset: bool = False,
) -> CacheProtocol:
    if not hasattr(cache, "cache_for_params"):
        return cast(CacheProtocol, cache)
    if params is None:
        raise ValueError("params is required when using ChunkMemo with runners")
    axis_values = dict(axis_values_override) if axis_values_override else None
    axis_values_for_load = None
    allow_superset_for_load = allow_superset
    if extend_cache:
        axis_values_for_load = cache.axis_values
        allow_superset_for_load = True
    elif axis_values is not None and allow_superset:
        axis_values_for_load = axis_values
    if axis_values_for_load is not None:
        cache = cache.auto_load(
            root=cache.root,
            params=params,
            axis_values=axis_values_for_load,
            chunk_spec=cache.chunk_spec,
            metadata=cache.metadata,
            collate_fn=cache.collate_fn,
            chunk_enumerator=cache.chunk_enumerator,
            chunk_hash_fn=cache.chunk_hash_fn,
            path_fn=cache.path_fn,
            version=cache.version,
            axis_order=cache.axis_order,
            verbose=cache.verbose,
            profile=cache.profile,
            exclusive=cache.exclusive,
            warn_on_overlap=cache.warn_on_overlap,
            precompute_chunk_keys=cache.precompute_chunk_keys,
            allow_superset=allow_superset_for_load,
            extend_cache=extend_cache,
        )
    cache = cache.cache_for_params(params)
    if extend_cache and axis_values is not None:
        cache.cache_status(axis_values_override=axis_values, extend_cache=True)
    return cast(CacheProtocol, cache)


def _stream_item_count(output: Any) -> int:
    if isinstance(output, (list, tuple, dict)):
        return len(output)
    return 1


def _format_item_count(count: int | None) -> str:
    return "all" if count is None else str(count)


def _log_chunk(
    context: RunnerContext, action: str, chunk_key: ChunkKey, item_count: int | None
) -> None:
    if context.verbose >= 2:
        print_detail(
            f"[ChunkMemo] {action} chunk={chunk_key} items={_format_item_count(item_count)}"
        )


def _payload_item_map(
    *,
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn,
    resolve_cache_path: ResolveCachePathFn,
    write_chunk_payload: WriteChunkPayloadFn,
    chunk_key: ChunkKey,
    payload: dict[str, Any],
    chunk_hash: str | None = None,
    write_back: bool = False,
) -> dict[str, Any] | None:
    item_map = payload.get("items")
    if item_map is None:
        item_map, item_axis_vals = build_item_maps_from_chunk_output(
            chunk_key,
            chunk_output=payload.get("output"),
        )
        if item_map is not None:
            payload["items"] = item_map
            if item_axis_vals is not None:
                payload["axis_vals"] = item_axis_vals
            if write_back:
                if chunk_hash is None:
                    raise ValueError("chunk_hash required for write_back")
                path = resolve_cache_path(chunk_key, chunk_hash)
                write_chunk_payload(path, payload, existing=payload)
    return item_map


def resolve_chunk_path(
    chunk_hash: ChunkHashFn,
    resolve_cache_path: ResolveCachePathFn,
    chunk_key: ChunkKey,
) -> tuple[str, Path]:
    chunk_hash_value = chunk_hash(chunk_key)
    path = resolve_cache_path(chunk_key, chunk_hash_value)
    return chunk_hash_value, path


def _save_chunk_payload(
    *,
    resolve_cache_path: ResolveCachePathFn,
    write_chunk_payload: WriteChunkPayloadFn,
    update_chunk_index: UpdateChunkIndexFn,
    chunk_key: ChunkKey,
    chunk_output: Any,
    item_map: dict[str, Any] | None,
    cached_payloads: dict[ChunkKey, Mapping[str, Any]],
    chunk_hash: str,
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
    if spec_fn is not None:
        payload["axis_vals"] = spec_fn()
    path = resolve_cache_path(chunk_key, chunk_hash)
    write_chunk_payload(
        path,
        payload,
        existing=cached_payloads.get(chunk_key),
    )
    update_chunk_index(chunk_hash, chunk_key)
    return payload


def _require_params(cache_status: CacheStatus) -> dict[str, Any]:
    metadata = cache_status.get("metadata")
    if isinstance(metadata, Mapping):
        params = metadata.get("params")
        if isinstance(params, Mapping):
            return dict(params)
    return {}


def _require_axis_info(
    cache_status: CacheStatus,
) -> tuple[Tuple[str, ...], dict[str, dict[Any, int]]]:
    axis_order = cache_status.get("axis_order")
    if not isinstance(axis_order, (tuple, list)):
        raise ValueError("cache_status must include axis_order")
    axis_chunk_maps = cache_status.get("axis_chunk_maps")
    if not isinstance(axis_chunk_maps, Mapping):
        raise ValueError("cache_status must include axis_chunk_maps")
    return tuple(axis_order), cast(dict[str, dict[Any, int]], axis_chunk_maps)


def _merge_outputs(
    context: RunnerContext,
    outputs: list[Any],
    diagnostics: Diagnostics,
    collate_fn: Callable[[list[Any]], Any] | None = None,
) -> Any:
    diagnostics.merges += 1
    if collate_fn is not None:
        return collate_fn(outputs)
    if context.collate_fn is not None:
        return context.collate_fn(outputs)
    return outputs


def prepare_planning_progress(
    *,
    total_chunks: int,
    total_items: int,
    verbose: int,
    label: str = "planning",
) -> tuple[Callable[[int, bool], None], Callable[[int], None], Callable[[bool], None]]:
    """Prepare planning progress callbacks with gated final output."""
    report_base, update_processed = prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=verbose,
        label=label,
    )
    allow_final = False
    final_emitted = False

    def set_allow_final(value: bool) -> None:
        nonlocal allow_final
        allow_final = value

    def report_progress(processed: int, final: bool = False) -> None:
        nonlocal final_emitted
        if processed >= total_chunks:
            if not allow_final or final_emitted:
                return
            final_emitted = True
            report_base(total_chunks, True)
            return
        if final:
            return
        report_base(processed, False)

    return report_progress, update_processed, set_allow_final
