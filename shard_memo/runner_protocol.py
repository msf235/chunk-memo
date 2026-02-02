"""Public runner-facing protocol and cache status shape."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence, Tuple, TypedDict

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]


class MinimalCacheStatus(TypedDict):
    """Required cache status keys for runners."""

    params: dict[str, Any]
    axis_values: Mapping[str, Any]
    cached_chunks: list[ChunkKey]
    missing_chunks: list[ChunkKey]


class ChunkIndexEntry(TypedDict):
    axis: str
    start: int
    end: int


class CacheStatus(MinimalCacheStatus, total=False):
    """Cache status payload consumed by runners."""

    total_chunks: int
    cached_chunk_indices: list[ChunkIndexEntry]
    missing_chunk_indices: list[ChunkIndexEntry]


class MemoRunnerBackend(Protocol):
    """Minimum interface for runner-compatible cache backends."""

    verbose: int
    profile: bool
    merge_fn: Callable[[list[Any]], Any] | None

    def prepare_run(
        self,
        params: dict[str, Any],
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> tuple[
        dict[str, Any],
        list[ChunkKey],
        Mapping[ChunkKey, list[Tuple[Any, ...]]] | None,
    ]: ...

    def cache_status(
        self,
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> CacheStatus: ...

    def chunk_hash(self, params: dict[str, Any], chunk_key: ChunkKey) -> str: ...

    def resolve_cache_path(
        self, params: dict[str, Any], chunk_key: ChunkKey, chunk_hash: str
    ) -> Path: ...

    def load_payload(self, path: Path) -> dict[str, Any] | None: ...

    def load_chunk_index(self, params: dict[str, Any]) -> dict[str, Any] | None: ...

    def update_chunk_index(
        self, params: dict[str, Any], chunk_hash: str, chunk_key: ChunkKey
    ) -> None: ...

    def build_item_maps_from_axis_values(
        self,
        chunk_key: ChunkKey,
        axis_values: Sequence[Tuple[Any, ...]],
        outputs: Sequence[Any],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]: ...

    def iter_chunk_axis_values(
        self, chunk_key: ChunkKey
    ) -> Sequence[Tuple[Any, ...]]: ...

    def extract_items_from_map(
        self,
        item_map: Mapping[str, Any] | None,
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]],
    ) -> list[Any] | None: ...

    def reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None: ...

    def load_cached_output(
        self,
        payload: Mapping[str, Any],
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]] | None,
        collate_fn: Callable[[list[Any]], Any],
    ) -> Any | None: ...

    def item_hash(self, chunk_key: ChunkKey, axis_values: Tuple[Any, ...]) -> str: ...

    def expand_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> tuple[Mapping[str, Any], Tuple[str, ...], dict[str, dict[Any, int]]]: ...

    def write_metadata(self, params: dict[str, Any]) -> Path: ...
