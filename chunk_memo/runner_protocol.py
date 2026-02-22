"""Runner protocols and cache status shape."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence, Tuple, TypedDict

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
AxisChunkMaps = dict[str, dict[Any, int]]


class CacheStatus(TypedDict):
    """Cache status payload consumed by runners."""

    cache_id: str
    metadata: dict[str, Any]
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
    collate_fn: Callable[[list[Any]], Any] | None


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
ReconstructPartialOutputFromItemsFn = Callable[[ChunkKey, Mapping[str, Any]], list[Any]]
CollectChunkDataFn = Callable[..., tuple[Any | None, bool]]
ItemHashFn = Callable[[ChunkKey, Tuple[Any, ...]], str]


class CacheProtocol(RunnerContext, Protocol):
    """Protocol for cache objects used by runners."""

    def cache_status(
        self,
        *,
        axis_indices: Mapping[str, Any] | None = None,
        axis_values_override: Mapping[str, Sequence[Any]] | None = None,
        extend_cache: bool = False,
        **axes: Any,
    ) -> CacheStatus: ...

    def write_metadata(self) -> Path: ...

    def chunk_hash(self, chunk_key: ChunkKey) -> str: ...

    def resolve_cache_path(self, chunk_key: ChunkKey, chunk_hash: str) -> Path: ...

    def load_payload(self, path: Path) -> dict[str, Any] | None: ...

    def write_chunk_payload(self, *args: Any, **kwargs: Any) -> Path: ...

    def update_chunk_index(self, chunk_hash: str, chunk_key: ChunkKey) -> None: ...

    def build_item_maps_from_axis_values(self, *args: Any, **kwargs: Any) -> Any: ...

    def build_item_maps_from_chunk_output(self, *args: Any, **kwargs: Any) -> Any: ...

    def reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None: ...

    def reconstruct_partial_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any]: ...

    def collect_chunk_data(
        self, *args: Any, **kwargs: Any
    ) -> tuple[Any | None, bool]: ...

    def extend_axis_values(
        self, axis_values: Mapping[str, Sequence[Any]], *, write_metadata: bool = True
    ) -> None: ...

    def extract_items_from_map(self, *args: Any, **kwargs: Any) -> list[Any] | None: ...

    def item_hash(self, chunk_key: ChunkKey, axis_values: Tuple[Any, ...]) -> str: ...

    def load_chunk_index(self) -> dict[str, Any] | None: ...

    def requested_items_by_chunk(
        self,
    ) -> Mapping[ChunkKey, list[Tuple[Any, ...]]] | None: ...
