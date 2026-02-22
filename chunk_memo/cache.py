import hashlib
import functools
import inspect
import itertools
import json
import pickle
import time
import warnings
from collections.abc import Sequence as ABCSequence
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from .cache_index import load_chunk_index, update_chunk_index
from .data_write_utils import (
    _apply_payload_timestamps,
    _atomic_write_json,
    _atomic_write_pickle,
)
from .runner_protocol import CacheStatus

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[ChunkKey]]
CollateFn = Callable[[list[Any]], Any]
AxisIndexMap = dict[str, dict[Any, int]]
CachePathFn = Callable[[dict[str, Any], ChunkKey, str, str], Path | str]


def _stable_serialize(value: Any) -> str:
    if isinstance(value, dict):
        items = ((k, _stable_serialize(value[k])) for k in sorted(value))
        return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        inner = ",".join(_stable_serialize(v) for v in value)
        return "[" + inner + "]"
    return repr(value)


def default_chunk_hash(
    params: dict[str, Any], chunk_key: ChunkKey, version: str
) -> str:
    payload = {
        "params": params,
        "chunk_key": chunk_key,
        "version": version,
    }
    data = _stable_serialize(payload)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _chunk_values(values: Sequence[Any], size: int) -> list[Tuple[Any, ...]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    chunks: list[Tuple[Any, ...]] = []
    for start in range(0, len(values), size):
        end = min(start + size, len(values))
        chunks.append(tuple(values[start:end]))
    return chunks


class ChunkCache:
    def __init__(
        self,
        root: str | Path | None,
        chunk_spec: dict[str, Any] | None,
        axis_values: dict[str, Any],
        params: dict[str, Any] | None = None,
        collate_fn: CollateFn | None = None,
        cache_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
        precompute_chunk_keys: bool = False,
    ) -> None:
        """Initialize a ChunkCache.

        Args:
            root: Directory for chunk cache files.
            chunk_spec: Per-axis chunk sizes (e.g., {"strat": 1, "s": 3}).
                If None, defaults to size 1 for each axis.
            collate_fn: Optional collate function for the list of chunk outputs.
            cache_chunk_enumerator: Optional chunk enumerator that defines the
                cache chunk order.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths. This hook is
                experimental and not yet thoroughly tested.
            cache_version: Cache namespace/version tag.
            axis_order: Axis iteration order (defaults to lexicographic).
            axis_values: Canonical axis values for the cache. Can be:
                - dict of iterables (lists, tuples): converted to list on init
                - dict of callables: For memory-efficient lazy loading.
                  Supports two patterns:
                  * Index-based: `lambda idx: values[idx]` - called with index
                  * List-returning: `lambda: full_list` - called once for all values
                  Use callables for large datasets to avoid loading all values into memory.
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params and axis_values as an existing cache.
            warn_on_overlap: If True, warn when caches overlap (same params but
                partially overlapping axis_values).

        Example:
            >>> cache = ChunkCache(
            ...     root="/tmp/chunk-memo",
            ...     chunk_spec={"strat": 1, "s": 3},
            ...     axis_values={"strat": ["a"], "s": [1, 2, 3]},
            ... )
            >>> params = {"alpha": 0.4}
            >>> cache.set_params(params)
            >>> for chunk_key, chunk_hash in cache.chunk_hashes_for():
            ...     path = cache.resolve_cache_path(chunk_key, chunk_hash)
            ...     payload = {"output": []}
            ...     cache.write_chunk_payload(path, payload)
        """
        root_path = Path(root) if root is not None else None
        self.params = params or {}
        if chunk_spec is None:
            chunk_spec = {
                axis: (
                    len(axis_values[axis])
                    if isinstance(axis_values[axis], (list, tuple))
                    else 1
                )
                for axis in axis_values
            }
        self.chunk_spec = chunk_spec
        self.collate_fn = collate_fn
        self.cache_chunk_enumerator = cache_chunk_enumerator
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_path_fn = cache_path_fn
        self.cache_version = cache_version
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose
        self.profile = profile
        self.exclusive = exclusive
        self.warn_on_overlap = warn_on_overlap
        self.precompute_chunk_keys = precompute_chunk_keys
        self._axis_values: dict[str, list[Any]] | None = None
        self._axis_index_map: AxisIndexMap | None = None
        self._axis_values_serializable: dict[str, Any] | None = None
        self._normalized_chunk_keys: list[ChunkKey] | None = None
        self._selected_items_by_chunk: (
            Mapping[ChunkKey, list[Tuple[Any, ...]]] | None
        ) = None
        self._checked_exclusive = False
        self._set_axis_values(axis_values)
        self._axis_values_serializable = self._make_axis_values_serializable()
        if root_path is None:
            root_path = Path.cwd() / ".chunk_memo"
        self.cache_id = self.cache_hash()
        self._memo_root_override: Path | None = None
        metadata_path = root_path / "metadata.json"
        if metadata_path.exists():
            self._memo_root_override = root_path
            self.root = root_path.parent
        else:
            self.root = root_path
        self._memo_root().mkdir(parents=True, exist_ok=True)
        if self.precompute_chunk_keys:
            self._normalized_chunk_keys = self._build_chunk_keys()

    def cache_status(
        self,
        *,
        axis_indices: Mapping[str, Any] | None = None,
        axis_values_override: Mapping[str, Sequence[Any]] | None = None,
        extend_cache: bool = False,
        **axes: Any,
    ) -> CacheStatus:
        """Return cached vs missing chunk info for a subset of axes.

        axis_indices selects axes by index (int, slice, range, or list/tuple of
        those), based on the canonical axis order. Chunk indices are
        returned as lists of ints.
        axis_values_override provides explicit axis values for axis_indices
        expansion; when extend_cache=True those values are appended in-place.
        """
        profile_start = time.monotonic() if self.profile else None
        if self._axis_values is None:
            raise ValueError("axis_values must be set before checking cache status")
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_values_override is not None and extend_cache:
            self.extend_axis_values(axis_values_override)
        axis_values = self._normalize_axes(
            axes,
            axis_indices=axis_indices,
            axis_values_override=axis_values_override,
        )
        axis_order = self._resolve_axis_order(dict(axis_values))
        axis_chunk_maps = self._axis_chunk_maps(axis_values, axis_order)
        if axis_indices is None and not axes:
            chunk_keys = self._build_chunk_keys(axis_values)
        else:
            chunk_keys, _ = self._build_chunk_plan_for_axes(axis_values)
        chunk_index = self.load_chunk_index()
        use_index = bool(chunk_index)
        if self.profile and self.verbose >= 1 and profile_start is not None:
            print(
                f"[ChunkCache] profile cache_status_build_s={time.monotonic() - profile_start:0.3f}"
            )
        cached_chunks: list[ChunkKey] = []
        cached_chunk_indices: list[dict[str, Any]] = []
        missing_chunks: list[ChunkKey] = []
        missing_chunk_indices: list[dict[str, Any]] = []
        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash(chunk_key)
            indices = self._chunk_indices_from_key(chunk_key)
            if use_index:
                exists = chunk_hash in chunk_index
            else:
                path = self.resolve_cache_path(chunk_key, chunk_hash)
                exists = path.exists()
            if exists:
                cached_chunks.append(chunk_key)
                cached_chunk_indices.append(indices)
            else:
                missing_chunks.append(chunk_key)
                missing_chunk_indices.append(indices)
        if self.profile and self.verbose >= 1 and profile_start is not None:
            print(
                f"[ChunkCache] profile cache_status_scan_s={time.monotonic() - profile_start:0.3f}"
            )
        payload = {
            "params": self.params,
            "axis_values": axis_values,
            "axis_order": axis_order,
            "axis_chunk_maps": axis_chunk_maps,
            "total_chunks": len(chunk_keys),
            "cached_chunks": cached_chunks,
            "cached_chunk_indices": cached_chunk_indices,
            "missing_chunks": missing_chunks,
            "missing_chunk_indices": missing_chunk_indices,
        }
        return cast(CacheStatus, payload)

    def slice(
        self,
        params: dict[str, Any],
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> "ChunkCache":
        """Return a sub-cache scoped to the requested axes."""
        if self._axis_values is None:
            raise ValueError("axis_values must be set before slicing")
        if not self._axis_values and (axes or axis_indices):
            raise ValueError(
                "Cannot pass axis arguments to a singleton cache (no axes)"
            )
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        selected_items: Mapping[ChunkKey, list[Tuple[Any, ...]]] | None = None
        normalized_chunk_keys: list[ChunkKey] | None = None
        if axis_indices is None and not axes:
            normalized_axis_values = self._axis_values
            if self.precompute_chunk_keys:
                normalized_chunk_keys = self._build_chunk_keys()
        else:
            normalized_axis_values = self._normalize_axes(
                axes, axis_indices=axis_indices
            )
            normalized_chunk_keys, selected_items = self._build_chunk_plan_for_axes(
                normalized_axis_values
            )
        sliced = ChunkCache(
            root=self.root,
            chunk_spec=self.chunk_spec,
            axis_values=normalized_axis_values,
            params=params,
            collate_fn=self.collate_fn,
            cache_chunk_enumerator=self.cache_chunk_enumerator,
            chunk_hash_fn=self.chunk_hash_fn,
            cache_path_fn=self.cache_path_fn,
            cache_version=self.cache_version,
            axis_order=self.axis_order,
            verbose=self.verbose,
            profile=self.profile,
            exclusive=self.exclusive,
            warn_on_overlap=self.warn_on_overlap,
            precompute_chunk_keys=self.precompute_chunk_keys,
        )
        sliced.cache_id = self.cache_id
        sliced._memo_root_override = self._memo_root()
        sliced._selected_items_by_chunk = selected_items
        if normalized_chunk_keys is not None:
            sliced._normalized_chunk_keys = normalized_chunk_keys
        if sliced.exclusive:
            sliced._check_exclusive()
        return sliced

    def bind_exec_fn(self, exec_fn: Callable[..., Any]) -> Callable[..., Any]:
        """Bind the cache params (and any fixed axes) to exec_fn."""
        return self._bind_exec_fn(exec_fn)

    def resolved_chunk_keys(self) -> list[ChunkKey]:
        """Return precomputed chunk keys if available, else build them."""
        if self._normalized_chunk_keys is not None:
            return list(self._normalized_chunk_keys)
        return self._build_chunk_keys()

    def requested_items_by_chunk(
        self,
    ) -> Mapping[ChunkKey, list[Tuple[Any, ...]]] | None:
        """Return requested items per chunk for a sliced cache, if any."""
        return self._selected_items_by_chunk

    def resolve_cache_path(self, chunk_key: ChunkKey, chunk_hash: str) -> Path:
        memo_root = self._memo_root()
        if self.cache_path_fn is None:
            return memo_root / "chunks" / f"{chunk_hash}.pkl"
        path = self.cache_path_fn(
            self.params, chunk_key, self.cache_version, chunk_hash
        )
        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj
        return memo_root / path_obj

    def _chunk_keys_for(
        self,
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> list[ChunkKey]:
        if self._axis_values is None:
            raise ValueError("axis_values must be set before building chunk keys")
        if not self._axis_values and (axes or axis_indices):
            raise ValueError(
                "Cannot pass axis arguments to a singleton cache (no axes)"
            )
        if axis_indices is None and not axes:
            return self._build_chunk_keys()
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        axis_values = self._normalize_axes(axes, axis_indices=axis_indices)
        chunk_keys, _ = self._build_chunk_plan_for_axes(axis_values)
        return chunk_keys

    def chunk_hashes_for(
        self,
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> list[tuple[ChunkKey, str]]:
        """Return chunk keys with their hashes for the requested axes."""
        chunk_keys = self._chunk_keys_for(axis_indices=axis_indices, **axes)
        return [(chunk_key, self.chunk_hash(chunk_key)) for chunk_key in chunk_keys]

    def apply_payload_timestamps(
        self,
        payload: dict[str, Any],
        *,
        existing: Mapping[str, Any] | None = None,
    ) -> None:
        _apply_payload_timestamps(payload, existing=existing)

    def write_chunk_payload(
        self,
        path: Path,
        payload: dict[str, Any],
        *,
        existing: Mapping[str, Any] | None = None,
    ) -> Path:
        self.apply_payload_timestamps(payload, existing=existing)
        _atomic_write_pickle(path, payload)
        return path

    def write_memo_json(
        self,
        path: Path,
        payload: dict[str, Any],
        *,
        existing: Mapping[str, Any] | None = None,
    ) -> Path:
        if existing is not None:
            self.apply_payload_timestamps(payload, existing=existing)
        _atomic_write_json(path, payload)
        return path

    def load_chunk_index(self) -> dict[str, Any]:
        return load_chunk_index(self._memo_root())

    def update_chunk_index(self, chunk_hash: str, chunk_key: ChunkKey) -> None:
        update_chunk_index(self._memo_root(), chunk_hash, chunk_key)

    @staticmethod
    def _normalized_hash_params_for_axis_values(
        params: dict[str, Any], axis_values: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not axis_values:
            return params
        return {key: value for key, value in params.items() if key not in axis_values}

    def _normalized_hash_params(self) -> dict[str, Any]:
        axis_values = self._axis_values or {}
        return self._normalized_hash_params_for_axis_values(self.params, axis_values)

    def chunk_hash(self, chunk_key: ChunkKey) -> str:
        normalized = self._normalized_hash_params()
        return self.chunk_hash_fn(normalized, chunk_key, self.cache_version)

    def cache_hash(self) -> str:
        specs = {
            "params": self._normalized_hash_params(),
            "axis_values": self._axis_values_serializable,
            "chunk_spec": self.chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
        }
        data = _stable_serialize(specs)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _check_exclusive(self) -> None:
        if self._checked_exclusive:
            return
        self._checked_exclusive = True
        if not self.exclusive and not self.warn_on_overlap:
            return

        if self._axis_values is None:
            return
        if self._axis_values_serializable is None:
            return
        axis_values_serializable = self._axis_values_serializable

        normalized_params = self._normalized_hash_params()
        all_caches = self.discover_caches(self.root)

        for cache in all_caches:
            metadata = cache.get("metadata")
            if metadata is None:
                continue
            meta_params = metadata.get("params", {})
            meta_axis_values = metadata.get("axis_values", {})

            if self._is_superset_compatible(
                metadata, normalized_params, axis_values_serializable
            ):
                if self.exclusive:
                    if meta_axis_values == self._axis_values_serializable:
                        raise ValueError(
                            f"Cache with same params and axis_values already exists: {cache['cache_hash']}. "
                            f"Use a different cache_version or root, or set exclusive=False."
                        )
                    else:
                        raise ValueError(
                            f"Cannot create subset cache: a superset cache already exists: {cache['cache_hash']}. "
                            f"Superset cache has axis_values={meta_axis_values}, "
                            f"requested has axis_values={self._axis_values_serializable}. "
                            f"Use the existing superset cache, or set exclusive=False."
                        )
                continue

            if self.exclusive:
                constrained_meta = {}
                for axis_name, axis_vals in meta_axis_values.items():
                    if isinstance(axis_vals, (list, tuple)) and len(axis_vals) == 1:
                        constrained_meta[axis_name] = axis_vals[0]

                merged_meta_params = {**meta_params, **constrained_meta}

                if merged_meta_params == normalized_params:
                    constrained_meta_axis_values = {
                        k: v
                        for k, v in meta_axis_values.items()
                        if k not in constrained_meta
                    }
                    if constrained_meta_axis_values.keys() <= self._axis_values.keys():
                        for axis_name in constrained_meta_axis_values:
                            meta_vals = self._axis_value_set(
                                constrained_meta_axis_values[axis_name]
                            )
                            new_vals = self._axis_value_set(
                                self._axis_values[axis_name]
                            )
                            if not meta_vals.issubset(new_vals):
                                break
                        else:
                            raise ValueError(
                                f"Cannot create superset cache: a subset cache already exists: {cache['cache_hash']}. "
                                f"Subset cache has axis_values={meta_axis_values}, "
                                f"requested has axis_values={self._axis_values_serializable}. "
                                f"Use the existing subset cache, or set exclusive=False."
                            )

            if self.warn_on_overlap and meta_axis_values and axis_values_serializable:
                overlap = self._detect_axis_overlap(
                    meta_axis_values, axis_values_serializable
                )
                if overlap:
                    warnings.warn(
                        f"Cache overlap detected: {cache['cache_hash']} has axis_values={meta_axis_values}, "
                        f"current has axis_values={axis_values_serializable}. Overlap: {overlap}. "
                        f"Consider using exclusive=True to prevent accidental conflicts.",
                        stacklevel=2,
                    )

    def _detect_axis_overlap(
        self, axis_values_a: dict[str, Any], axis_values_b: dict[str, Any]
    ) -> dict[str, list[Any]] | None:
        shared_axes = set(axis_values_a) & set(axis_values_b)
        if not shared_axes:
            return None
        overlap: dict[str, list[Any]] = {}
        for axis in shared_axes:
            values_a = self._axis_value_set(axis_values_a[axis])
            values_b = self._axis_value_set(axis_values_b[axis])
            intersection = values_a & values_b
            if intersection:
                overlap[axis] = sorted(intersection)
            else:
                return None
        return overlap

    def _memo_root(self) -> Path:
        if self._memo_root_override is not None:
            return self._memo_root_override
        return self.root / self.cache_id

    def write_metadata(self) -> Path:
        memo_root = self._memo_root()
        memo_root.mkdir(parents=True, exist_ok=True)
        path = memo_root / "metadata.json"
        existing_meta = None
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    existing_meta = json.load(handle)
            except json.JSONDecodeError:
                existing_meta = None
        payload = {
            "params": self._normalized_hash_params(),
            "axis_values": self._axis_values_serializable,
            "chunk_spec": self.chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
            "cache_hash": self.cache_hash(),
        }
        self.write_memo_json(path, payload, existing=existing_meta)
        return path

    def set_params(self, params: dict[str, Any]) -> None:
        """Update cache params and refresh cache identity."""
        self.params = params
        self.cache_id = self.cache_hash()
        self._checked_exclusive = False
        if self._memo_root_override is None:
            self._memo_root().mkdir(parents=True, exist_ok=True)
        if self.exclusive:
            self._check_exclusive()

    def _bind_exec_fn(self, exec_fn: Callable[..., Any]) -> Callable[..., Any]:
        if self._axis_values is None:
            return functools.partial(exec_fn, self.params)
        signature = inspect.signature(exec_fn)
        param_names = list(signature.parameters)
        if not param_names or param_names[0] != "params":
            return functools.partial(exec_fn, self.params)
        axis_names = param_names[1:]
        fixed_axes = {
            name: self.params[name]
            for name in axis_names
            if name in self.params and name not in self._axis_values
        }
        if not fixed_axes:
            return functools.partial(exec_fn, self.params)
        return functools.partial(exec_fn, self.params, **fixed_axes)

    def _resolve_axis_order(self, axis_values: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(axis_values))

    def _ordered_axis_values(
        self,
        axis_values: Mapping[str, Sequence[Any]],
        axis_order: Sequence[str] | None = None,
    ) -> tuple[Tuple[str, ...], list[tuple[str, Sequence[Any], int]]]:
        if axis_order is None:
            axis_order = self._resolve_axis_order(dict(axis_values))
        ordered: list[tuple[str, Sequence[Any], int]] = []
        for axis in axis_order:
            values = axis_values.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            size = self._resolve_axis_chunk_size(axis)
            ordered.append((axis, values, size))
        return tuple(axis_order), ordered

    def _ordered_axis_requests(
        self,
        axis_values: Mapping[str, Sequence[Any]],
        axis_order: Sequence[str],
    ) -> list[tuple[str, list[Any], int]]:
        ordered: list[tuple[str, list[Any], int]] = []
        for axis in axis_order:
            requested_values = list(axis_values.get(axis, []))
            if not requested_values:
                raise ValueError(f"Missing values for axis '{axis}'")
            size = self._resolve_axis_chunk_size(axis)
            ordered.append((axis, requested_values, size))
        return ordered

    def _axis_chunk_maps(
        self, axis_values: Mapping[str, Any], axis_order: Sequence[str]
    ) -> dict[str, dict[Any, int]]:
        axis_chunk_maps: dict[str, dict[Any, int]] = {}
        _, ordered = self._ordered_axis_values(axis_values, axis_order)
        for axis, values, size in ordered:
            value_to_chunk_id: dict[Any, int] = {}
            for index, value in enumerate(values):
                value_to_chunk_id[value] = index // size
            axis_chunk_maps[axis] = value_to_chunk_id
        return axis_chunk_maps

    def _set_axis_values(self, axis_values: dict[str, Any]) -> None:
        axis_order = self._resolve_axis_order(axis_values)
        for axis in axis_order:
            if axis not in axis_values:
                raise KeyError(f"Missing axis '{axis}' in axis_values")

        axis_index_map: AxisIndexMap = {}
        axis_value_lists: dict[str, list[Any]] = {}
        for axis in axis_order:
            axis_values_obj = axis_values[axis]
            values = self._materialize_axis_values(axis_values_obj)
            axis_value_lists[axis] = values
            axis_index_map[axis] = {value: index for index, value in enumerate(values)}

        self._axis_values = axis_value_lists
        self._axis_index_map = axis_index_map

    def _make_axis_values_serializable(self) -> dict[str, Any]:
        """Convert axis_values to a JSON-serializable representation."""
        if self._axis_values is None:
            raise ValueError("axis_values must be set before serializing")
        axis_order = self._resolve_axis_order(self._axis_values)
        return {axis: list(self._axis_values[axis]) for axis in axis_order}

    def _get_all_axis_values(self, axis: str) -> list[Any]:
        """Get all values for an axis."""
        if self._axis_values is None:
            raise ValueError("axis_values must be set before getting axis values")
        return list(self._axis_values[axis])

    def extend_axis_values(
        self,
        axis_values: Mapping[str, Sequence[Any]],
        *,
        write_metadata: bool = True,
    ) -> None:
        """Extend axis_values in-place with new values.

        Values already present are ignored; new values are appended in order.
        When write_metadata=True, metadata.json is updated immediately.
        """
        if self._axis_values is None or self._axis_index_map is None:
            raise ValueError("axis_values must be set before extending")
        axis_order = self._resolve_axis_order(self._axis_values)
        missing_axes = [axis for axis in axis_values if axis not in self._axis_values]
        if missing_axes:
            raise KeyError(f"Unknown axis(es) in extension: {missing_axes}")

        updated = False
        for axis in axis_order:
            if axis not in axis_values:
                continue
            current_values = self._axis_values[axis]
            current_set = set(current_values)
            new_values: list[Any] = []
            for value in axis_values[axis]:
                if value in current_set:
                    continue
                current_set.add(value)
                new_values.append(value)
            if not new_values:
                continue
            updated = True
            current_values.extend(new_values)
            self._axis_index_map[axis] = {
                value: index for index, value in enumerate(current_values)
            }

        if updated:
            self._axis_values_serializable = self._make_axis_values_serializable()
            if write_metadata:
                self.write_metadata()

    def _normalize_axes(
        self,
        axes: Mapping[str, Any] | None,
        *,
        axis_indices: Mapping[str, Any] | None = None,
        axis_values_override: Mapping[str, Sequence[Any]] | None = None,
    ) -> dict[str, list[Any]]:
        if axis_indices is not None:
            if axes:
                raise ValueError("axis_indices cannot be combined with axis values")
            return self._normalize_axis_indices(
                axis_indices,
                axis_values_override=axis_values_override,
            )
        return self._normalize_axis_values(axes or {})

    def _normalize_axis_values(self, axes: Mapping[str, Any]) -> dict[str, list[Any]]:
        """Normalize axis value selections into full axis lists."""
        if self._axis_values is None or self._axis_index_map is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_values: dict[str, list[Any]] = {}
        for axis in self._axis_values:
            if axis not in axes:
                axis_values[axis] = self._get_all_axis_values(axis)
                continue
            values = axes[axis]
            if isinstance(values, (list, tuple)):
                normalized = list(values)
            else:
                normalized = [values]
            for value in normalized:
                if value not in self._axis_index_map[axis]:
                    raise KeyError(
                        f"Value '{value}' not found in axis_values for axis '{axis}'"
                    )
            axis_values[axis] = normalized
        return axis_values

    def _normalize_axis_indices(
        self,
        axis_indices: Mapping[str, Any],
        *,
        axis_values_override: Mapping[str, Sequence[Any]] | None = None,
    ) -> dict[str, list[Any]]:
        """Normalize axis_indices into axis values using canonical axis order."""
        if self._axis_values is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_values: dict[str, list[Any]] = {}
        for axis in self._axis_values:
            if axis not in axis_indices:
                axis_values[axis] = self._get_all_axis_values(axis)
                continue
            values = axis_indices[axis]
            if axis_values_override is not None and axis in axis_values_override:
                axis_values_obj = axis_values_override[axis]
            else:
                axis_values_obj = self._axis_values[axis]
            indices = self._expand_axis_indices(values, axis, len(axis_values_obj))
            axis_values[axis] = [axis_values_obj[index] for index in indices]
        return axis_values

    def _expand_axis_indices(self, values: Any, axis: str, axis_len: int) -> list[int]:
        if isinstance(values, (list, tuple)):
            items = list(values)
        else:
            items = [values]
        resolved: list[int] = []
        for item in items:
            if isinstance(item, range):
                indices = list(item)
            elif isinstance(item, slice):
                start, stop, step = item.indices(axis_len)
                indices = list(range(start, stop, step))
            elif isinstance(item, int):
                indices = [item]
            else:
                raise TypeError(
                    f"Indices for axis '{axis}' must be int, slice, or range, got {type(item)}"
                )
            for index in indices:
                if index < 0 or index >= axis_len:
                    raise IndexError(f"Index {index} out of range for axis '{axis}'")
                resolved.append(index)
        return resolved

    def _build_chunk_keys(
        self, axis_values: Mapping[str, Sequence[Any]] | None = None
    ) -> list[ChunkKey]:
        if axis_values is None:
            if self._axis_values is None:
                raise ValueError(
                    "axis_values must be set before running memoized function"
                )
            axis_values = self._axis_values
        if self.cache_chunk_enumerator is not None:
            if self._axis_values_serializable is not None:
                return list(
                    self.cache_chunk_enumerator(dict(self._axis_values_serializable))
                )
            return list(self.cache_chunk_enumerator(dict(axis_values)))

        axis_order, ordered = self._ordered_axis_values(axis_values)
        axis_chunks = [_chunk_values(values, size) for _, values, size in ordered]

        chunk_keys: list[ChunkKey] = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def _chunk_indices_from_key(self, chunk_key: ChunkKey) -> dict[str, Any]:
        if self._axis_index_map is None:
            raise ValueError("axis_values must be set before checking cache status")
        indices: dict[str, Any] = {}
        for axis, values in chunk_key:
            axis_map = self._axis_index_map.get(axis)
            if axis_map is None:
                raise KeyError(f"Unknown axis '{axis}'")
            raw_indices = [axis_map[value] for value in values]
            indices[axis] = raw_indices
        return indices

    def _build_chunk_plan_for_axes(
        self, axis_values: Mapping[str, Sequence[Any]]
    ) -> Tuple[list[ChunkKey], dict[ChunkKey, list[Tuple[Any, ...]]]]:
        if self._axis_values is None or self._axis_index_map is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_order = self._resolve_axis_order(self._axis_values)
        per_axis_chunks: list[list[dict[str, Any]]] = []
        for axis, requested_values, size in self._ordered_axis_requests(
            axis_values, axis_order
        ):
            full_values = self._get_all_axis_values(axis)
            chunk_map: dict[int, dict[str, Any]] = {}
            for value in requested_values:
                index = self._axis_index_map[axis].get(value)
                if index is None:
                    raise KeyError(
                        f"Value '{value}' not found in axis_values for axis '{axis}'"
                    )
                chunk_id = index // size
                chunk = chunk_map.setdefault(
                    chunk_id, {"values": None, "requested": []}
                )
                chunk["requested"].append(value)
            for chunk_id, chunk in chunk_map.items():
                start = chunk_id * size
                end = min(start + size, len(full_values))
                chunk["values"] = tuple(full_values[start:end])
            per_axis_chunks.append([chunk_map[idx] for idx in sorted(chunk_map.keys())])

        chunk_keys: list[ChunkKey] = []
        requested_items: dict[ChunkKey, list[Tuple[Any, ...]]] = {}
        for combo in itertools.product(*per_axis_chunks):
            chunk_key = tuple(
                (axis, tuple(desc["values"])) for axis, desc in zip(axis_order, combo)
            )
            chunk_keys.append(chunk_key)
            requested_lists = [desc["requested"] for desc in combo]
            item_values = [
                tuple(values) for values in itertools.product(*requested_lists)
            ]
            requested_items[chunk_key] = item_values
        return chunk_keys, requested_items

    def build_item_maps_from_axis_values(
        self,
        chunk_key: ChunkKey,
        axis_values: Sequence[Tuple[Any, ...]],
        outputs: Sequence[Any],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        axis_names = [axis for axis, _ in chunk_key]
        item_map: dict[str, Any] = {}
        item_axis_vals: dict[str, dict[str, Any]] = {}
        for values, output in zip(axis_values, outputs):
            item_key = self.item_hash(chunk_key, values)
            item_map[item_key] = output
            item_axis_vals[item_key] = dict(zip(axis_names, values))
        return item_map, item_axis_vals

    def build_item_maps_from_chunk_output(
        self,
        chunk_key: ChunkKey,
        chunk_output: Any,
    ) -> tuple[dict[str, Any] | None, dict[str, dict[str, Any]] | None]:
        if not isinstance(chunk_output, (list, tuple)):
            return None, None
        axis_values = list(self.iter_chunk_axis_values(chunk_key))
        if len(axis_values) != len(chunk_output):
            return None, None
        return self.build_item_maps_from_axis_values(
            chunk_key,
            axis_values,
            chunk_output,
        )

    def reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None:
        axis_values = list(self.iter_chunk_axis_values(chunk_key))
        if not axis_values:
            return None
        outputs: list[Any] = []
        for values in axis_values:
            item_key = self.item_hash(chunk_key, values)
            if item_key not in items:
                return None
            outputs.append(items[item_key])
        return outputs

    def reconstruct_partial_output_from_items(
        self,
        chunk_key: ChunkKey,
        items: Mapping[str, Any],
    ) -> list[Any]:
        axis_values = list(self.iter_chunk_axis_values(chunk_key))
        outputs: list[Any] = []
        for values in axis_values:
            item_key = self.item_hash(chunk_key, values)
            if item_key in items:
                outputs.append(items[item_key])
        return outputs

    def extract_items_from_map(
        self,
        item_map: Mapping[str, Any] | None,
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]],
    ) -> list[Any] | None:
        if item_map is None:
            return None
        outputs: list[Any] = []
        for values in requested_items:
            item_key = self.item_hash(chunk_key, values)
            if item_key not in item_map:
                return None
            outputs.append(item_map[item_key])
        return outputs

    def item_hash(self, chunk_key: ChunkKey, axis_values: Tuple[Any, ...]) -> str:
        payload = {
            "axis_values": tuple(
                (axis, value) for (axis, _), value in zip(chunk_key, axis_values)
            ),
            "version": self.cache_version,
        }
        data = _stable_serialize(payload)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def iter_chunk_axis_values(self, chunk_key: ChunkKey) -> Sequence[Tuple[Any, ...]]:
        axis_values = [values for _, values in chunk_key]
        return list(itertools.product(*axis_values))

    def load_payload(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        return cast(dict[str, Any], payload)

    def collect_chunk_data(
        self,
        payload: Mapping[str, Any],
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]] | None,
        collate_fn: Callable[[list[Any]], Any],
    ) -> tuple[Any | None, bool]:
        if requested_items is None:
            chunk_output = payload.get("output")
            if chunk_output is None:
                items = payload.get("items")
                if items is not None:
                    chunk_output = self.reconstruct_output_from_items(chunk_key, items)
                    if chunk_output is None:
                        return (
                            self.reconstruct_partial_output_from_items(
                                chunk_key, items
                            ),
                            True,
                        )
            if chunk_output is None:
                return None, False
            return chunk_output, False
        item_map = payload.get("items")
        if item_map is None:
            return None, False
        cached_outputs: list[Any] = []
        missing = False
        for values in requested_items:
            item_key = self.item_hash(chunk_key, values)
            if item_key not in item_map:
                missing = True
                continue
            cached_outputs.append(item_map[item_key])
        if not cached_outputs:
            return None, False
        return collate_fn([cached_outputs]), missing

    def _materialize_axis_values(self, axis_values_obj: Any) -> list[Any]:
        if isinstance(axis_values_obj, ABCSequence) and not isinstance(
            axis_values_obj, (str, bytes, bytearray)
        ):
            return list(axis_values_obj)
        values = list(axis_values_obj)
        return sorted(values, key=_stable_serialize)

    def _resolve_axis_chunk_size(self, axis: str) -> int:
        spec = self.chunk_spec.get(axis)
        if spec is None:
            raise KeyError(f"Missing chunk_spec for axis '{axis}'")
        if isinstance(spec, dict):
            size = spec.get("size")
            if size is None:
                raise KeyError(f"Missing size for axis '{axis}'")
            return int(size)
        return int(spec)

    @classmethod
    def discover_caches(cls, root: str | Path) -> list[dict[str, Any]]:
        """Discover all existing caches in root.

        Returns a list of cache metadata dictionaries, each containing:
        - cache_hash: The unique hash of the cache
        - path: Path to the cache directory
        - metadata: Full metadata dict if available, None otherwise

        Args:
            root: Directory to scan for caches.
        """
        root = Path(root)
        if not root.exists():
            return []
        caches: list[dict[str, Any]] = []
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            metadata_path = entry / "metadata.json"
            metadata = cls._read_metadata_file(metadata_path, required=False)
            if metadata is None:
                continue
            caches.append(
                {
                    "cache_hash": entry.name,
                    "path": entry,
                    "metadata": metadata,
                }
            )
        return caches

    @classmethod
    def _is_superset_compatible(
        cls,
        cache_metadata: dict[str, Any] | None,
        req_params: dict[str, Any] | None,
        req_axis_values: dict[str, Any] | None,
    ) -> bool:
        """Check if an existing cache is a superset of the requested configuration.

        A cache is a superset if:
        - All requested axes are present in the cache's axes
        - All requested params are either in cache's params OR
          represented as axes in the cache with matching values

        Returns:
            True if the cache is a superset and compatible, False otherwise.
        """
        if cache_metadata is None or req_params is None or req_axis_values is None:
            return False
        meta_params = cache_metadata.get("params", {})
        meta_axis_values = cache_metadata.get("axis_values", {})
        req_axis_set = set(req_axis_values.keys())
        meta_axis_set = set(meta_axis_values.keys())

        if not req_axis_set.issubset(meta_axis_set):
            return False

        for param, req_value in req_params.items():
            if param in meta_params:
                if _stable_serialize(meta_params[param]) != _stable_serialize(
                    req_value
                ):
                    return False
            elif param in meta_axis_values:
                meta_vals = cls._axis_value_set(meta_axis_values[param])
                req_vals = cls._axis_value_set(req_value)
                if not req_vals.issubset(meta_vals):
                    return False
            else:
                return False

        return True

    @classmethod
    def cache_is_compatible(
        cls,
        cache,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        chunk_spec: dict[str, Any] | None = None,
        cache_version: str | None = None,
        axis_order: Sequence[str] | None = None,
        allow_superset: bool = False,
    ) -> bool:
        """Find caches compatible with the given criteria.

        Matching rules:
        - If a criterion is provided, it must match exactly.
        - If a criterion is None (omitted), it is ignored (wildcard).
        - If allow_superset is True with params and axis_values provided,
          also finds caches that are supersets (contain all requested data).

        Args:
            cache: Cache.
            params: Optional params dict (axis values excluded) to match.
            axis_values: Optional axis_values dict to match.
            chunk_spec: Optional chunk_spec dict to match.
            cache_version: Optional cache_version string to match.
            axis_order: Optional axis_order sequence to match.
            allow_superset: If True, find superset caches when params and
                axis_values are both provided.
        """
        metadata = cache.get("metadata")

        if allow_superset:
            return cls._is_superset_compatible(metadata, params, axis_values)

        if metadata is None:
            return False
        if params is not None:
            meta_params = metadata.get("params", {})
            if _stable_serialize(meta_params) != _stable_serialize(params):
                return False
        if axis_values is not None:
            meta_axis_values = metadata.get("axis_values", {})
            if _stable_serialize(meta_axis_values) != _stable_serialize(axis_values):
                return False
        if chunk_spec is not None:
            meta_spec = metadata.get("chunk_spec", {})
            if _stable_serialize(meta_spec) != _stable_serialize(chunk_spec):
                return False
        if cache_version is not None:
            if metadata.get("cache_version") != cache_version:
                return False
        if axis_order is not None:
            meta_axis_order = metadata.get("axis_order")
            meta_tuple = tuple(meta_axis_order) if meta_axis_order is not None else None
            if meta_tuple != tuple(axis_order):
                return False
        return True

    @classmethod
    def find_compatible_caches(
        cls,
        root: str | Path,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        chunk_spec: dict[str, Any] | None = None,
        cache_version: str | None = None,
        axis_order: Sequence[str] | None = None,
        allow_superset: bool = False,
    ) -> list[dict[str, Any]]:
        """Find caches compatible with the given criteria.

        Matching rules:
        - If a criterion is provided, it must match exactly.
        - If a criterion is None (omitted), it is ignored (wildcard).
        - If allow_superset is True with params and axis_values provided,
          also finds caches that are supersets (contain all requested data).

        Returns a list of compatible cache entries, each containing:
        - cache_hash: The unique hash of the cache
        - path: Path to the cache directory
        - metadata: Full metadata dict

        Args:
            root: Directory to scan for caches.
            params: Optional params dict (axis values excluded) to match.
            axis_values: Optional axis_values dict to match.
            chunk_spec: Optional chunk_spec dict to match.
            cache_version: Optional cache_version string to match.
            axis_order: Optional axis_order sequence to match.
            allow_superset: If True, find superset caches when params and
                axis_values are both provided.
        """
        all_caches = cls.discover_caches(root)
        compatible: list[dict[str, Any]] = []

        for cache in all_caches:
            if cls.cache_is_compatible(
                cache,
                params,
                axis_values,
                chunk_spec,
                cache_version,
                axis_order,
                allow_superset,
            ):
                compatible.append(cache)

        return compatible

    @classmethod
    def auto_load(
        cls,
        root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any] | None = None,
        chunk_spec: dict[str, Any] | None = None,
        collate_fn: CollateFn | None = None,
        cache_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
        allow_superset: bool = False,
    ) -> "ChunkCache":
        """Smart load that finds an existing cache or creates a new one.

        Finds caches matching the given params. If axis_values is provided:
        - When allow_superset=False: requires exact match
        - When allow_superset=True: finds exact matches or superset caches
        If axis_values is None, requires exactly one cache matching params
        (otherwise raises ambiguity error).

        When creating a new cache with axis_values, if chunk_spec is
        not provided, uses chunk size 1 for all axes as default.

        Args:
            root: Directory for caches.
            params: Params dict (axis values excluded).
            axis_values: Optional axis_values dict. If provided:
                - With allow_superset=False: requires exact match
                - With allow_superset=True: finds exact or superset matches
                - If None: finds caches with matching params (must be exactly 1)
            chunk_spec: Optional chunk spec. Required when creating
                a new cache with axis_values. Defaults to size 1 for
                all axes.
            collate_fn: Optional collate function.
            cache_chunk_enumerator: Optional chunk enumerator.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths.
            cache_version: Cache namespace/version tag.
            axis_order: Axis iteration order.
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params and axis_values as another cache, or when creating
                a subset/superset of an existing cache.
            warn_on_overlap: If True, warn when caches overlap.
            allow_superset: If True with axis_values, find superset caches
                that contain all requested data.

        Returns:
            A ChunkCache instance ready for execution.

        Raises:
            ValueError: If multiple caches match (ambiguous), if axis_values
                provided without matching cache and axis_names missing from
                chunk_spec, or if multiple superset caches match.
        """
        root = Path(root)
        axis_values_map = axis_values or {}

        def summarize_axis_values(value: Any) -> str:
            if not isinstance(value, Mapping):
                return repr(value)
            summarized: dict[str, Any] = {}
            for key, axis_vals in value.items():
                if isinstance(axis_vals, (list, tuple)):
                    if len(axis_vals) <= 6:
                        summarized[key] = list(axis_vals)
                    else:
                        summarized[key] = (
                            list(axis_vals[:3]) + ["..."] + list(axis_vals[-2:])
                        )
                else:
                    summarized[key] = axis_vals
            rendered = repr(summarized)
            if len(rendered) > 200:
                return rendered[:197] + "..."
            return rendered

        # resolve memo hash / cache selection
        normalized_params = cls._normalized_hash_params_for_axis_values(params, {})
        compatible_caches = cls.find_compatible_caches(
            root,
            params=normalized_params,
            axis_values=axis_values,
            allow_superset=allow_superset,
        )

        cache_hash: str | None
        if len(compatible_caches) == 1:
            cache_hash = compatible_caches[0]["cache_hash"]
        elif len(compatible_caches) > 1:
            matches = []
            for cache in compatible_caches:
                axis_values_summary = summarize_axis_values(
                    cache.get("metadata", {}).get("axis_values")
                )
                matches.append(
                    f"{cache['cache_hash']} (axis_values={axis_values_summary})"
                )
            raise ValueError(
                f"Ambiguous: {len(compatible_caches)} caches match the given params. "
                f"Use axis_values parameter to disambiguate, or use one of "
                f"ChunkCache.load_from_cache() to pick a specific cache.\n"
                f"Matches:\n  " + "\n  ".join(matches)
            )
        else:
            cache_hash = None

        # load existing cache or create new
        if cache_hash is not None:
            return cls.load_from_cache(
                root=root,
                cache_hash=cache_hash,
                collate_fn=collate_fn,
                cache_chunk_enumerator=cache_chunk_enumerator,
                chunk_hash_fn=chunk_hash_fn,
                cache_path_fn=cache_path_fn,
                verbose=verbose,
                profile=profile,
                exclusive=exclusive,
                warn_on_overlap=warn_on_overlap,
            )
        return cls(
            root=root,
            chunk_spec=chunk_spec,
            axis_values=axis_values_map,
            collate_fn=collate_fn,
            cache_chunk_enumerator=cache_chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            cache_path_fn=cache_path_fn,
            cache_version=cache_version,
            axis_order=axis_order,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
        )

    @classmethod
    def load_from_cache(
        cls,
        root: str | Path,
        cache_hash: str,
        collate_fn: CollateFn | None = None,
        cache_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> "ChunkCache":
        """Load a ChunkCache instance from an existing cache by cache_hash.

        Raises:
            FileNotFoundError: If the cache directory or metadata.json does not exist.
            ValueError: If metadata is invalid.

        Args:
            root: Directory containing caches.
            cache_hash: The hash identifying the specific cache.
            collate_fn: Optional collate function for the list of chunk outputs.
            cache_chunk_enumerator: Optional chunk enumerator.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths.
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params and axis_values as another cache.
            warn_on_overlap: If True, warn when caches overlap.
        """
        root = Path(root)
        cache_path = root / cache_hash
        metadata_path = cache_path / "metadata.json"
        if not cache_path.exists():
            raise FileNotFoundError(f"Cache directory not found: {cache_path}")
        metadata = cls._read_metadata_file(metadata_path, required=True)
        if metadata is None:
            raise ValueError(f"Invalid metadata in {metadata_path}")
        axis_values = metadata.get("axis_values")
        if axis_values is None:
            raise ValueError("Metadata missing 'axis_values'")
        chunk_spec = metadata.get("chunk_spec")
        if chunk_spec is None:
            raise ValueError("Metadata missing 'chunk_spec'")
        instance = cls(
            root=cache_path,
            chunk_spec=chunk_spec,
            axis_values=axis_values,
            params=metadata.get("params"),
            collate_fn=collate_fn,
            cache_chunk_enumerator=cache_chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            cache_path_fn=cache_path_fn,
            cache_version=metadata.get("cache_version", "v1"),
            axis_order=metadata.get("axis_order"),
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
        )
        return instance

    @staticmethod
    def _axis_value_set(values: Any) -> set[Any]:
        if isinstance(values, (list, tuple)):
            return set(values)
        return {values}

    @staticmethod
    def _read_metadata_file(
        metadata_path: Path, *, required: bool
    ) -> dict[str, Any] | None:
        if not metadata_path.exists():
            if required:
                raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
            return None
        try:
            with open(metadata_path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, IOError) as exc:
            if required:
                raise ValueError(f"Invalid metadata in {metadata_path}") from exc
            return None
