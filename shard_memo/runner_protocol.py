"""Runner protocols and cache status shape."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence, Tuple, TypedDict

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
AxisChunkMaps = dict[str, dict[Any, int]]


class CacheStatus(TypedDict):
    """Cache status payload consumed by runners."""

    params: dict[str, Any]
    axis_values: Mapping[str, Any]
    axis_order: Tuple[str, ...]
    axis_chunk_maps: AxisChunkMaps
    cached_chunks: list[ChunkKey]
    missing_chunks: list[ChunkKey]

    total_chunks: int
    cached_chunk_indices: list[dict[str, Any]]
    missing_chunk_indices: list[dict[str, Any]]


class RunnerContext(Protocol):
    """Runtime settings consumed by runner helpers."""

    verbose: int
    profile: bool
    merge_fn: Callable[[list[Any]], Any] | None


WriteMetadataFn = Callable[[], Path]
ChunkHashFn = Callable[[ChunkKey], str]
ResolveCachePathFn = Callable[[ChunkKey, str], Path]
LoadPayloadFn = Callable[[Path], dict[str, Any] | None]
WriteChunkPayloadFn = Callable[..., Path]
UpdateChunkIndexFn = Callable[[str, ChunkKey], None]
LoadChunkIndexFn = Callable[[], dict[str, Any] | None]
BuildItemMapsFromAxisValuesFn = Callable[
    ...,
    tuple[dict[str, Any], dict[str, dict[str, Any]]],
]
BuildItemMapsFromChunkOutputFn = Callable[
    ...,
    tuple[dict[str, Any] | None, dict[str, dict[str, Any]] | None],
]
ExtractItemsFromMapFn = Callable[..., list[Any] | None]
ReconstructOutputFromItemsFn = Callable[[ChunkKey, Mapping[str, Any]], list[Any] | None]
CollectChunkDataFn = Callable[..., Any | None]
ItemHashFn = Callable[[ChunkKey, Tuple[Any, ...]], str]


class CacheProtocol(RunnerContext, Protocol):
    """Protocol for cache objects used by runners."""

    write_metadata: WriteMetadataFn
    chunk_hash: ChunkHashFn
    resolve_cache_path: ResolveCachePathFn
    load_payload: LoadPayloadFn
    write_chunk_payload: WriteChunkPayloadFn
    update_chunk_index: UpdateChunkIndexFn
    build_item_maps_from_axis_values: BuildItemMapsFromAxisValuesFn
    build_item_maps_from_chunk_output: BuildItemMapsFromChunkOutputFn
    reconstruct_output_from_items: ReconstructOutputFromItemsFn
    collect_chunk_data: CollectChunkDataFn
    extract_items_from_map: ExtractItemsFromMapFn
    item_hash: ItemHashFn
    load_chunk_index: LoadChunkIndexFn
    requested_items_by_chunk: Callable[[], Mapping[ChunkKey, list[Tuple[Any, ...]]]]
