import functools
import hashlib
import inspect
import itertools
import json
import pickle
import time
import warnings
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from .cache_index import load_chunk_index, update_chunk_index
from .data_write_utils import (
    _apply_payload_timestamps,
    _atomic_write_json,
    _atomic_write_pickle,
)
from .runner_protocol import CacheStatus, MemoRunnerBackend
from .runners import Diagnostics, run, run_streaming

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[ChunkKey]]
MergeFn = Callable[[list[Any]], Any]
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
        cache_root: str | Path,
        cache_chunk_spec: dict[str, Any] | None,
        axis_values: dict[str, Any],
        merge_fn: MergeFn | None = None,
        cache_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> None:
        """Initialize a ChunkCache.

        Args:
            cache_root: Directory for chunk cache files.
            cache_chunk_spec: Per-axis chunk sizes (e.g., {"strat": 1, "s": 3}).
                If None, defaults to size 1 for each axis.
            merge_fn: Optional merge function for the list of chunk outputs.
            cache_chunk_enumerator: Optional chunk enumerator that defines the
                cache chunk order.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths. This hook is
                experimental and not yet thoroughly tested.
            cache_version: Cache namespace/version tag.
            axis_order: Axis iteration order (defaults to lexicographic).
            axis_values: Canonical split spec for the cache. Can be:
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
            ...     cache_root="/tmp/shard-memo",
            ...     cache_chunk_spec={"strat": 1, "s": 3},
            ...     axis_values={"strat": ["a"], "s": [1, 2, 3]},
            ... )
            >>> params = {"alpha": 0.4}
            >>> paths = cache.paths_for(params)
            >>> for chunk_key, chunk_hash, path in paths["chunk_paths"]:
            ...     payload = {"output": []}
            ...     cache.write_chunk_payload(path, payload)
        """
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        if cache_chunk_spec is None:
            cache_chunk_spec = {
                axis: (
                    len(axis_values[axis])
                    if isinstance(axis_values[axis], (list, tuple))
                    else 1
                )
                for axis in axis_values
            }
        self.cache_chunk_spec = cache_chunk_spec
        self.merge_fn = merge_fn
        self.cache_chunk_enumerator = cache_chunk_enumerator
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_path_fn = cache_path_fn
        self.cache_version = cache_version
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose
        self.profile = profile
        self.exclusive = exclusive
        self.warn_on_overlap = warn_on_overlap
        self._axis_values: dict[str, list[Any]] | None = None
        self._axis_index_map: AxisIndexMap | None = None
        self._axis_values_serializable: dict[str, Any] | None = None
        self._checked_exclusive = False
        self._set_axis_values(axis_values)
        self._axis_values_serializable = self._make_axis_values_serializable()

    def run_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output.

        Supports axis selection by value or by index via axis_indices.
        Uses runners.run under the hood.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=False)

    def streaming_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Diagnostics]]:
        """Decorator for streaming memoized execution to disk only.

        Supports axis selection by value or by index via axis_indices.
        Uses runners.run_streaming under the hood.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=True)

    def run(
        self,
        params: dict[str, Any],
        exec_fn: Callable[..., Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> Tuple[Any, Diagnostics]:
        return run(
            self,
            params,
            exec_fn,
            axis_indices=axis_indices,
            **axes,
        )

    def run_streaming(
        self,
        params: dict[str, Any],
        exec_fn: Callable[..., Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> Diagnostics:
        return run_streaming(
            self,
            params,
            exec_fn,
            axis_indices=axis_indices,
            **axes,
        )

    def _prepare_params_and_extras(
        self,
        params: dict[str, Any],
        bound_args: Mapping[str, Any],
        params_arg: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        axis_names = set(self._axis_values or {})
        extras = {k: v for k, v in bound_args.items() if k != params_arg}
        axis_inputs = {k: v for k, v in extras.items() if k in axis_names}
        exec_extras = {k: v for k, v in extras.items() if k not in axis_names}
        merged_params = dict(params)
        merged_params.update(exec_extras)
        return merged_params, exec_extras, axis_inputs

    def _build_wrapper(
        self, *, params_arg: str, streaming: bool
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            signature = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if params_arg in kwargs and args:
                    raise ValueError(
                        f"'{params_arg}' passed as both positional and keyword"
                    )

                axis_indices = kwargs.pop("axis_indices", None)
                bound = signature.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                if params_arg not in bound.arguments:
                    raise ValueError(f"Missing required argument '{params_arg}'")

                params = bound.arguments[params_arg]
                if not isinstance(params, dict):
                    raise ValueError(f"'{params_arg}' must be a dict")

                merged_params, exec_extras, axis_inputs = (
                    self._prepare_params_and_extras(params, bound.arguments, params_arg)
                )

                exec_fn = functools.partial(func, **exec_extras)
                if streaming:
                    return run_streaming(
                        cast(MemoRunnerBackend, self),
                        merged_params,
                        exec_fn=exec_fn,
                        axis_indices=axis_indices,
                        **axis_inputs,
                    )
                return run(
                    cast(MemoRunnerBackend, self),
                    merged_params,
                    exec_fn=exec_fn,
                    axis_indices=axis_indices,
                    **axis_inputs,
                )

            def cache_status(
                params: dict[str, Any],
                *,
                axis_indices: Mapping[str, Any] | None = None,
                **axes: Any,
            ):
                merged_params, _, axis_inputs = self._prepare_params_and_extras(
                    params, axes, params_arg
                )
                return self.cache_status(
                    merged_params, axis_indices=axis_indices, **axis_inputs
                )

            setattr(wrapper, "cache_status", cache_status)
            return wrapper

        return decorator

    def cache_status(
        self,
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> CacheStatus:
        """Return cached vs missing chunk info for a subset of axes.

        axis_indices selects axes by index (int, slice, range, or list/tuple of
        those), based on the canonical split spec order. Chunk indices are
        returned as lists of ints.
        """
        profile_start = time.monotonic() if self.profile else None
        if self._axis_values is None:
            raise ValueError("axis_values must be set before checking cache status")
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        axis_values = self._normalize_axes(axes, axis_indices=axis_indices)
        chunk_keys = self._build_chunk_keys(axis_values)
        chunk_index = self.load_chunk_index(params)
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
            chunk_hash = self.chunk_hash(params, chunk_key)
            indices = self._chunk_indices_from_key(chunk_key)
            if use_index:
                exists = chunk_hash in chunk_index
            else:
                path = self.resolve_cache_path(params, chunk_key, chunk_hash)
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
            "params": params,
            "axis_values": axis_values,
            "total_chunks": len(chunk_keys),
            "cached_chunks": cached_chunks,
            "cached_chunk_indices": cached_chunk_indices,
            "missing_chunks": missing_chunks,
            "missing_chunk_indices": missing_chunk_indices,
        }
        return cast(CacheStatus, payload)

    def prepare_run(
        self,
        params: dict[str, Any],
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> tuple[
        dict[str, Any], list[ChunkKey], Mapping[ChunkKey, list[Tuple[Any, ...]]] | None
    ]:
        if self._axis_values is None:
            raise ValueError("axis_values must be set before running memoized function")
        if not self._axis_values and (axes or axis_indices):
            raise ValueError(
                "Cannot pass axis arguments to a singleton cache (no axes)"
            )
        self._check_exclusive(params)
        self.write_metadata(params)
        if axis_indices is None and not axes:
            chunk_keys = self._build_chunk_keys()
            return self._axis_values, chunk_keys, None
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        axis_values = self._normalize_axes(axes, axis_indices=axis_indices)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(axis_values)
        return axis_values, chunk_keys, requested_items

    def resolve_cache_path(
        self, params: dict[str, Any], chunk_key: ChunkKey, chunk_hash: str
    ) -> Path:
        memo_root = self._memo_root(params)
        if self.cache_path_fn is None:
            return memo_root / "chunks" / f"{chunk_hash}.pkl"
        path = self.cache_path_fn(params, chunk_key, self.cache_version, chunk_hash)
        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj
        return memo_root / path_obj

    def _chunk_keys_for(
        self,
        params: dict[str, Any],
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
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> list[tuple[ChunkKey, str]]:
        """Return chunk keys with their hashes for the requested axes."""
        chunk_keys = self._chunk_keys_for(
            params,
            axis_indices=axis_indices,
            **axes,
        )
        return [
            (chunk_key, self.chunk_hash(params, chunk_key)) for chunk_key in chunk_keys
        ]

    def paths_for(
        self,
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> dict[str, Any]:
        """Return cache paths for the requested axes."""
        memo_root = self._memo_root(params)
        chunk_keys = self._chunk_keys_for(
            params,
            axis_indices=axis_indices,
            **axes,
        )
        chunk_paths: list[tuple[ChunkKey, str, Path]] = []
        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash(params, chunk_key)
            path = self.resolve_cache_path(params, chunk_key, chunk_hash)
            chunk_paths.append((chunk_key, chunk_hash, path))
        return {
            "memo_root": memo_root,
            "metadata_path": memo_root / "metadata.json",
            "chunk_paths": chunk_paths,
        }

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

    def load_chunk_index(self, params: dict[str, Any]) -> dict[str, Any]:
        return load_chunk_index(self._memo_root(params))

    def update_chunk_index(
        self,
        params: dict[str, Any],
        chunk_hash: str,
        chunk_key: ChunkKey,
    ) -> None:
        update_chunk_index(self._memo_root(params), chunk_hash, chunk_key)

    @staticmethod
    def _normalized_hash_params_for_axis_values(
        params: dict[str, Any], axis_values: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not axis_values:
            return params
        return {key: value for key, value in params.items() if key not in axis_values}

    def _normalized_hash_params(self, params: dict[str, Any]) -> dict[str, Any]:
        axis_values = self._axis_values or {}
        return self._normalized_hash_params_for_axis_values(params, axis_values)

    def chunk_hash(self, params: dict[str, Any], chunk_key: ChunkKey) -> str:
        normalized = self._normalized_hash_params(params)
        return self.chunk_hash_fn(normalized, chunk_key, self.cache_version)

    def cache_hash(self, params: dict[str, Any]) -> str:
        specs = {
            "params": self._normalized_hash_params(params),
            "axis_values": self._axis_values_serializable,
            "cache_chunk_spec": self.cache_chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
        }
        data = _stable_serialize(specs)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _check_exclusive(self, params: dict[str, Any]) -> None:
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

        normalized_params = self._normalized_hash_params(params)
        all_caches = self.discover_caches(self.cache_root)

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
                            f"Use a different cache_version or cache_root, or set exclusive=False."
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

    def _memo_root(self, params: dict[str, Any]) -> Path:
        return self.cache_root / self.cache_hash(params)

    def write_metadata(self, params: dict[str, Any]) -> Path:
        memo_root = self._memo_root(params)
        path = memo_root / "metadata.json"
        existing_meta = None
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    existing_meta = json.load(handle)
            except json.JSONDecodeError:
                existing_meta = None
        payload = {
            "params": self._normalized_hash_params(params),
            "axis_values": self._axis_values_serializable,
            "cache_chunk_spec": self.cache_chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
            "cache_hash": self.cache_hash(params),
        }
        self.write_memo_json(path, payload, existing=existing_meta)
        return path

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

    def expand_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> tuple[Mapping[str, Any], Tuple[str, ...], dict[str, dict[Any, int]]]:
        axis_values = cache_status.get("axis_values")
        if not isinstance(axis_values, Mapping):
            raise ValueError("cache_status must include axis_values for memo axes")
        axis_order = self._resolve_axis_order(dict(axis_values))
        axis_chunk_maps = self._axis_chunk_maps(axis_values, axis_order)
        return axis_values, axis_order, axis_chunk_maps

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

    def _normalize_axes(
        self,
        axes: Mapping[str, Any] | None,
        *,
        axis_indices: Mapping[str, Any] | None = None,
    ) -> dict[str, list[Any]]:
        if axis_indices is not None:
            if axes:
                raise ValueError("axis_indices cannot be combined with axis values")
            return self._normalize_axis_indices(axis_indices)
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
        self, axis_indices: Mapping[str, Any]
    ) -> dict[str, list[Any]]:
        """Normalize axis_indices into axis values using canonical split order."""
        if self._axis_values is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_values: dict[str, list[Any]] = {}
        for axis in self._axis_values:
            if axis not in axis_indices:
                axis_values[axis] = self._get_all_axis_values(axis)
                continue
            values = axis_indices[axis]
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
    ) -> Any | None:
        if requested_items is None:
            chunk_output = payload.get("output")
            if chunk_output is None:
                items = payload.get("items")
                if items is not None:
                    chunk_output = self.reconstruct_output_from_items(chunk_key, items)
            if chunk_output is None:
                raise ValueError("Cache payload missing required data")
            return chunk_output
        item_map = payload.get("items")
        cached_outputs = self.extract_items_from_map(
            item_map,
            chunk_key,
            requested_items,
        )
        if cached_outputs is None:
            return None
        return collate_fn([cached_outputs])

    def _materialize_axis_values(self, axis_values_obj: Any) -> list[Any]:
        values = list(axis_values_obj)
        return sorted(values, key=_stable_serialize)

    def _resolve_axis_chunk_size(self, axis: str) -> int:
        spec = self.cache_chunk_spec.get(axis)
        if spec is None:
            raise KeyError(f"Missing cache_chunk_spec for axis '{axis}'")
        if isinstance(spec, dict):
            size = spec.get("size")
            if size is None:
                raise KeyError(f"Missing size for axis '{axis}'")
            return int(size)
        return int(spec)

    @classmethod
    def discover_caches(cls, cache_root: str | Path) -> list[dict[str, Any]]:
        """Discover all existing caches in cache_root.

        Returns a list of cache metadata dictionaries, each containing:
        - cache_hash: The unique hash of the cache
        - path: Path to the cache directory
        - metadata: Full metadata dict if available, None otherwise

        Args:
            cache_root: Directory to scan for caches.
        """
        cache_root = Path(cache_root)
        if not cache_root.exists():
            return []
        caches: list[dict[str, Any]] = []
        for entry in cache_root.iterdir():
            if not entry.is_dir():
                continue
            metadata_path = entry / "metadata.json"
            metadata = cls._read_metadata_file(metadata_path, required=False)
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
        cache_chunk_spec: dict[str, Any] | None = None,
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
            cache_chunk_spec: Optional cache_chunk_spec dict to match.
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
        if cache_chunk_spec is not None:
            meta_spec = metadata.get("cache_chunk_spec", {})
            if _stable_serialize(meta_spec) != _stable_serialize(cache_chunk_spec):
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
        cache_root: str | Path,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        cache_chunk_spec: dict[str, Any] | None = None,
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
            cache_root: Directory to scan for caches.
            params: Optional params dict (axis values excluded) to match.
            axis_values: Optional axis_values dict to match.
            cache_chunk_spec: Optional cache_chunk_spec dict to match.
            cache_version: Optional cache_version string to match.
            axis_order: Optional axis_order sequence to match.
            allow_superset: If True, find superset caches when params and
                axis_values are both provided.
        """
        all_caches = cls.discover_caches(cache_root)
        compatible: list[dict[str, Any]] = []

        for cache in all_caches:
            if cls.cache_is_compatible(
                cache,
                params,
                axis_values,
                cache_chunk_spec,
                cache_version,
                axis_order,
                allow_superset,
            ):
                compatible.append(cache)

        return compatible

    @classmethod
    def auto_load(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any] | None = None,
        cache_chunk_spec: dict[str, Any] | None = None,
        merge_fn: MergeFn | None = None,
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

        When creating a new cache with axis_values, if cache_chunk_spec is
        not provided, uses chunk size 1 for all axes as default.

        Args:
            cache_root: Directory for caches.
            params: Params dict (axis values excluded).
            axis_values: Optional axis_values dict. If provided:
                - With allow_superset=False: requires exact match
                - With allow_superset=True: finds exact or superset matches
                - If None: finds caches with matching params (must be exactly 1)
            cache_chunk_spec: Optional chunk spec. Required when creating
                a new cache with axis_values. Defaults to size 1 for
                all axes.
            merge_fn: Optional merge function.
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
                cache_chunk_spec, or if multiple superset caches match.
        """
        cache_root = Path(cache_root)
        axis_values_map = axis_values or {}

        # resolve memo hash / cache selection
        normalized_params = cls._normalized_hash_params_for_axis_values(params, {})
        compatible_caches = cls.find_compatible_caches(
            cache_root,
            params=normalized_params,
            axis_values=axis_values,
            allow_superset=allow_superset,
        )

        cache_hash: str | None
        if len(compatible_caches) == 1:
            cache_hash = compatible_caches[0]["cache_hash"]
        elif len(compatible_caches) > 1:
            matches = [
                f"{c['cache_hash']} (axis_values={c.get('metadata', {}).get('axis_values')})"
                for c in compatible_caches
            ]
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
                cache_root=cache_root,
                cache_hash=cache_hash,
                merge_fn=merge_fn,
                cache_chunk_enumerator=cache_chunk_enumerator,
                chunk_hash_fn=chunk_hash_fn,
                cache_path_fn=cache_path_fn,
                verbose=verbose,
                profile=profile,
                exclusive=exclusive,
                warn_on_overlap=warn_on_overlap,
            )
        return cls(
            cache_root=cache_root,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values_map,
            merge_fn=merge_fn,
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
        cache_root: str | Path,
        cache_hash: str,
        merge_fn: MergeFn | None = None,
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
            cache_root: Directory containing caches.
            cache_hash: The hash identifying the specific cache.
            merge_fn: Optional merge function for the list of chunk outputs.
            cache_chunk_enumerator: Optional chunk enumerator.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths.
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params and axis_values as another cache.
            warn_on_overlap: If True, warn when caches overlap.
        """
        cache_root = Path(cache_root)
        cache_path = cache_root / cache_hash
        metadata_path = cache_path / "metadata.json"
        if not cache_path.exists():
            raise FileNotFoundError(f"Cache directory not found: {cache_path}")
        metadata = cls._read_metadata_file(metadata_path, required=True)
        if metadata is None:
            raise ValueError(f"Invalid metadata in {metadata_path}")
        axis_values = metadata.get("axis_values")
        if axis_values is None:
            raise ValueError("Metadata missing 'axis_values'")
        cache_chunk_spec = metadata.get("cache_chunk_spec")
        if cache_chunk_spec is None:
            raise ValueError("Metadata missing 'cache_chunk_spec'")
        instance = cls(
            cache_root=cache_root,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values,
            merge_fn=merge_fn,
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
