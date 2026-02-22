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

    def resolve(name: str, value: Any) -> Any:
        if value is not None:
            return value
        if cache is None:
            return None
        if not hasattr(cache, name):
            return None
        return getattr(cache, name)

    context_resolved = context if context is not None else cache
    if context_resolved is None and "context" in require_set:
        missing.append("context")

    def resolve_required(name: str, value: Any) -> Any:
        resolved = resolve(name, value)
        if resolved is None and name in require_set:
            missing.append(name)
        return resolved

    deps = RunnerDeps(
        write_metadata=resolve_required("write_metadata", write_metadata),
        chunk_hash=resolve_required("chunk_hash", chunk_hash),
        resolve_cache_path=resolve_required("resolve_cache_path", resolve_cache_path),
        load_payload=resolve_required("load_payload", load_payload),
        write_chunk_payload=resolve_required(
            "write_chunk_payload", write_chunk_payload
        ),
        update_chunk_index=resolve_required("update_chunk_index", update_chunk_index),
        build_item_maps_from_axis_values=resolve_required(
            "build_item_maps_from_axis_values", build_item_maps_from_axis_values
        ),
        build_item_maps_from_chunk_output=resolve_required(
            "build_item_maps_from_chunk_output", build_item_maps_from_chunk_output
        ),
        reconstruct_output_from_items=resolve_required(
            "reconstruct_output_from_items", reconstruct_output_from_items
        ),
        reconstruct_partial_output_from_items=resolve_required(
            "reconstruct_partial_output_from_items",
            reconstruct_partial_output_from_items,
        ),
        collect_chunk_data=resolve_required("collect_chunk_data", collect_chunk_data),
        item_hash=resolve_required("item_hash", item_hash),
        load_chunk_index=resolve_required("load_chunk_index", load_chunk_index),
        extract_items_from_map=resolve_required(
            "extract_items_from_map", extract_items_from_map
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


def prepare_progress_callbacks(
    *,
    total_chunks: int,
    total_items: int,
    verbose: int,
    label: str,
) -> tuple[Callable[[int, bool], None], Callable[[int], None]]:
    return prepare_progress(
        total_chunks=total_chunks,
        total_items=total_items,
        verbose=verbose,
        label=label,
    )


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
