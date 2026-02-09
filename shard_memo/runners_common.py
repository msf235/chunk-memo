from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, Callable, Mapping, Tuple, cast

from ._format import prepare_progress, print_detail
from .runner_protocol import (
    BuildItemMapsFromChunkOutputFn,
    CacheStatus,
    ChunkHashFn,
    ResolveCachePathFn,
    RunnerContext,
    UpdateChunkIndexFn,
    WriteChunkPayloadFn,
)

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


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
            f"[ShardMemo] {action} chunk={chunk_key} items={_format_item_count(item_count)}"
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
    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    return dict(params)


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
    context: RunnerContext, outputs: list[Any], diagnostics: Diagnostics
) -> Any:
    diagnostics.merges += 1
    if context.merge_fn is not None:
        return context.merge_fn(outputs)
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
