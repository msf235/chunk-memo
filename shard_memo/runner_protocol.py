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


CacheStatusFn = Callable[..., CacheStatus]
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
