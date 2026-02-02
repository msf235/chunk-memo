import dataclasses
import functools
import hashlib
import inspect
import itertools
import json
import math
import pickle
import time
import warnings
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple, cast

from .cache_index import load_chunk_index, update_chunk_index
from .cache_layout import resolve_cache_path
from .cache_utils import (
    _atomic_write_json,
    _atomic_write_pickle,
    _now_iso,
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
        memo_chunk_spec: dict[str, Any],
        axis_values: dict[str, Any],
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> None:
        """Initialize a ChunkMemo cache.

        Args:
            cache_root: Directory for chunk cache files.
            memo_chunk_spec: Per-axis chunk sizes (e.g., {"strat": 1, "s": 3}).
            merge_fn: Optional merge function for the list of chunk outputs.
            memo_chunk_enumerator: Optional chunk enumerator that defines the
                memo chunk order.
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
        """
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.memo_chunk_spec = memo_chunk_spec
        self.merge_fn = merge_fn
        self.memo_chunk_enumerator = memo_chunk_enumerator
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_path_fn = cache_path_fn
        self.cache_version = cache_version
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose
        self.profile = profile
        self.exclusive = exclusive
        self.warn_on_overlap = warn_on_overlap
        self._axis_values: dict[str, Any] | None = None
        self._axis_index_map: AxisIndexMap | None = None
        self._axis_values_serializable: dict[str, Any] | None = None
        self._checked_exclusive = False
        self._set_axis_values(axis_values)
        self._axis_values_serializable = self._make_axis_values_serializable(
            axis_values
        )

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
        those), based on the canonical split spec order.
        """
        profile_start = time.monotonic() if self.profile else None
        if self._axis_values is None:
            raise ValueError("axis_values must be set before checking cache status")
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            axis_values = self._normalize_axis_indices(axis_indices)
        else:
            axis_values = self._normalize_axes(axes)
        index_format = self._infer_index_format(axis_indices)
        chunk_keys = self._build_chunk_keys_for_axes(axis_values)
        chunk_index = self.load_chunk_index(params)
        use_index = bool(chunk_index)
        if self.profile and self.verbose >= 1 and profile_start is not None:
            print(
                f"[ChunkMemo] profile cache_status_build_s={time.monotonic() - profile_start:0.3f}"
            )
        cached_chunks: list[ChunkKey] = []
        cached_chunk_indices: list[dict[str, Any]] = []
        missing_chunks: list[ChunkKey] = []
        missing_chunk_indices: list[dict[str, Any]] = []
        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash(params, chunk_key)
            indices = self._chunk_indices_from_key(chunk_key, index_format)
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
                f"[ChunkMemo] profile cache_status_scan_s={time.monotonic() - profile_start:0.3f}"
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
        if axis_indices is not None:
            axis_values = self._normalize_axis_indices(axis_indices)
        else:
            axis_values = self._normalize_axes(axes)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(axis_values)
        return axis_values, chunk_keys, requested_items

    def resolve_cache_path(
        self, params: dict[str, Any], chunk_key: ChunkKey, chunk_hash: str
    ) -> Path:
        return resolve_cache_path(
            memo_root_path=self._memo_root(params),
            cache_path_fn=self.cache_path_fn,
            params=params,
            chunk_key=chunk_key,
            cache_version=self.cache_version,
            chunk_hash=chunk_hash,
        )

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

    def memo_hash(self, params: dict[str, Any]) -> str:
        payload = {
            "params": self._normalized_hash_params(params),
            "axis_values": self._axis_values_serializable,
            "memo_chunk_spec": self.memo_chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
        }
        data = _stable_serialize(payload)
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
                            f"Cache with same params and axis_values already exists: {cache['memo_hash']}. "
                            f"Use a different cache_version or cache_root, or set exclusive=False."
                        )
                    else:
                        raise ValueError(
                            f"Cannot create subset cache: a superset cache already exists: {cache['memo_hash']}. "
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
                            meta_vals = (
                                set(constrained_meta_axis_values[axis_name])
                                if isinstance(
                                    constrained_meta_axis_values[axis_name],
                                    (list, tuple),
                                )
                                else {constrained_meta_axis_values[axis_name]}
                            )
                            new_vals = (
                                set(self._axis_values[axis_name])
                                if isinstance(
                                    self._axis_values[axis_name], (list, tuple)
                                )
                                else {self._axis_values[axis_name]}
                            )
                            if not meta_vals.issubset(new_vals):
                                break
                        else:
                            raise ValueError(
                                f"Cannot create superset cache: a subset cache already exists: {cache['memo_hash']}. "
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
                        f"Cache overlap detected: {cache['memo_hash']} has axis_values={meta_axis_values}, "
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
        return self.cache_root / self.memo_hash(params)

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
        created_at = None if existing_meta is None else existing_meta.get("created_at")
        if created_at is None:
            created_at = _now_iso()
        payload = {
            "created_at": created_at,
            "updated_at": _now_iso(),
            "params": self._normalized_hash_params(params),
            "axis_values": self._axis_values_serializable,
            "memo_chunk_spec": self.memo_chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
            "memo_hash": self.memo_hash(params),
        }
        _atomic_write_json(path, payload)
        return path

    def _resolve_axis_order(self, axis_values: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(axis_values))

    def _axis_values_from_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        axis_values = cache_status.get("axis_values")
        if not isinstance(axis_values, Mapping):
            raise ValueError("cache_status must include axis_values for memo axes")
        return axis_values

    def _axis_order_for_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> Tuple[str, ...]:
        axis_values = self._axis_values_from_cache_status(cache_status)
        return self._resolve_axis_order(dict(axis_values))

    def expand_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> tuple[Mapping[str, Any], Tuple[str, ...], dict[str, dict[Any, int]]]:
        axis_values = self._axis_values_from_cache_status(cache_status)
        axis_order = self._axis_order_for_cache_status(cache_status)
        axis_chunk_maps = self._axis_chunk_maps(axis_values, axis_order)
        return axis_values, axis_order, axis_chunk_maps

    def _axis_chunk_maps(
        self, axis_values: Mapping[str, Any], axis_order: Sequence[str]
    ) -> dict[str, dict[Any, int]]:
        axis_chunk_maps: dict[str, dict[Any, int]] = {}
        for axis in axis_order:
            values = axis_values.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            size = self._resolve_axis_chunk_size(axis)
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
        axis_value_getters: dict[str, Callable[[int], Any]] = {}
        for axis in axis_order:
            axis_values_obj = axis_values[axis]
            values, getter = self._materialize_axis_values(axis_values_obj)
            axis_value_getters[axis] = getter
            axis_index_map[axis] = {value: index for index, value in enumerate(values)}

        self._axis_values = axis_value_getters
        self._axis_index_map = axis_index_map

    def _make_axis_values_serializable(
        self, axis_values: dict[str, Any]
    ) -> dict[str, Any]:
        """Convert axis_values to a JSON-serializable representation."""
        axis_order = self._resolve_axis_order(axis_values)
        result = {}
        for axis in axis_order:
            axis_values_obj = axis_values[axis]
            if callable(axis_values_obj):
                result[axis] = {"type": "callable", "signature": str(axis_values_obj)}
            else:
                result[axis] = list(axis_values_obj)
        return result

    def _get_all_axis_values(self, axis: str) -> list[Any]:
        """Get all values for an axis, handling both callables and lists."""
        if self._axis_index_map is None:
            raise ValueError("axis_values must be set before getting axis values")
        axis_map = self._axis_index_map[axis]
        values: list[Any] = [None] * len(axis_map)
        for value, index in axis_map.items():
            values[index] = value
        return values

    def _normalize_axes(self, axes: Mapping[str, Any]) -> dict[str, list[Any]]:
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
            if callable(axis_values_obj):
                full_values = self._get_all_axis_values(axis)
                indices = self._expand_axis_indices(values, axis, len(full_values))
                axis_values[axis] = [axis_values_obj(index) for index in indices]
            else:
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

    def _build_chunk_keys(self) -> list[ChunkKey]:
        if self._axis_values is None:
            raise ValueError("axis_values must be set before running memoized function")
        return self._build_chunk_keys_for_axes(self._axis_values)

    def _chunk_indices_from_key(
        self, chunk_key: ChunkKey, index_format: Mapping[str, str] | None
    ) -> dict[str, Any]:
        if self._axis_index_map is None:
            raise ValueError("axis_values must be set before checking cache status")
        indices: dict[str, Any] = {}
        for axis, values in chunk_key:
            axis_map = self._axis_index_map.get(axis)
            if axis_map is None:
                raise KeyError(f"Unknown axis '{axis}'")
            raw_indices = [axis_map[value] for value in values]
            format_kind = None if index_format is None else index_format.get(axis)
            indices[axis] = self._format_indices(raw_indices, format_kind)
        return indices

    def _infer_index_format(
        self, axis_indices: Mapping[str, Any] | None
    ) -> dict[str, str] | None:
        if axis_indices is None:
            return None
        formats: dict[str, str] = {}
        for axis, values in axis_indices.items():
            formats[axis] = self._detect_index_format(values)
        return formats

    def _detect_index_format(self, values: Any) -> str:
        if isinstance(values, range):
            return "range"
        if isinstance(values, slice):
            return "slice"
        if isinstance(values, (list, tuple)):
            kinds = {self._detect_index_format(value) for value in values}
            if len(kinds) == 1:
                return kinds.pop()
            return "list"
        if isinstance(values, int):
            return "int"
        return "list"

    def _indices_step(self, indices: list[int]) -> int | None:
        if len(indices) < 2:
            return 1
        step = indices[1] - indices[0]
        if step == 0:
            return None
        for prev, current in zip(indices, indices[1:]):
            if current - prev != step:
                return None
        return step

    def _format_indices(self, indices: list[int], format_kind: str | None) -> Any:
        if format_kind == "int":
            return indices[:]
        if format_kind == "range":
            return self._indices_to_span(indices, kind="range")
        if format_kind == "slice":
            return self._indices_to_span(indices, kind="slice") or indices[:]
        return indices[:]

    def _indices_to_span(
        self, indices: list[int], *, kind: str
    ) -> range | slice | None:
        if not indices:
            return range(0, 0) if kind == "range" else slice(0, 0, None)
        if len(indices) == 1:
            start = indices[0]
            stop = indices[0] + 1
            return range(start, stop) if kind == "range" else slice(start, stop, None)
        step = self._indices_step(indices)
        if step is None:
            return None
        start = indices[0]
        stop = indices[-1] + step
        return range(start, stop, step) if kind == "range" else slice(start, stop, step)

    def _build_chunk_keys_for_axes(
        self, axis_values: Mapping[str, Sequence[Any]]
    ) -> list[ChunkKey]:
        if self.memo_chunk_enumerator is not None:
            if self._axis_values_serializable is not None:
                return list(
                    self.memo_chunk_enumerator(dict(self._axis_values_serializable))
                )
            return list(self.memo_chunk_enumerator(dict(axis_values)))

        axis_order = self._resolve_axis_order(dict(axis_values))
        axis_chunks: list[list[Tuple[Any, ...]]] = []
        for axis in axis_order:
            values = axis_values.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            size = self._resolve_axis_chunk_size(axis)
            if callable(values):
                values = self._get_all_axis_values(axis)
            axis_chunks.append(_chunk_values(values, size))

        chunk_keys: list[ChunkKey] = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def _build_chunk_plan_for_axes(
        self, axis_values: Mapping[str, Sequence[Any]]
    ) -> Tuple[list[ChunkKey], dict[ChunkKey, list[Tuple[Any, ...]]]]:
        if self._axis_values is None or self._axis_index_map is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_order = self._resolve_axis_order(self._axis_values)
        per_axis_chunks: list[list[dict[str, Any]]] = []
        for axis in axis_order:
            requested_values = list(axis_values.get(axis, []))
            if not requested_values:
                raise ValueError(f"Missing values for axis '{axis}'")
            size = self._resolve_axis_chunk_size(axis)
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

    def _build_item_map(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> dict[str, Any] | None:
        axis_values = self._item_axis_values_for_output(chunk_key, chunk_output)
        if axis_values is None:
            return None
        item_map, _ = self.build_item_maps_from_axis_values(
            chunk_key,
            axis_values,
            chunk_output,
        )
        return item_map

    def _build_item_axis_vals_map(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> dict[str, dict[str, Any]] | None:
        axis_values = self._item_axis_values_for_output(chunk_key, chunk_output)
        if axis_values is None:
            return None
        _, item_axis_vals = self.build_item_maps_from_axis_values(
            chunk_key,
            axis_values,
            chunk_output,
        )
        return item_axis_vals

    def _item_axis_values_for_output(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> list[Tuple[Any, ...]] | None:
        if not isinstance(chunk_output, (list, tuple)):
            return None
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if len(axis_values) != len(chunk_output):
            return None
        return axis_values

    def reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None:
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if not axis_values:
            return None
        outputs: list[Any] = []
        for values in axis_values:
            item_key = self.item_hash(chunk_key, values)
            if item_key not in items:
                return None
            outputs.append(items[item_key])
        return outputs

    def _extract_cached_items(
        self,
        payload: Mapping[str, Any],
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]],
    ) -> list[Any] | None:
        item_map = payload.get("items")
        return self.extract_items_from_map(item_map, chunk_key, requested_items)

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

    def _iter_chunk_axis_values(self, chunk_key: ChunkKey) -> Sequence[Tuple[Any, ...]]:
        axis_values = [values for _, values in chunk_key]
        return list(itertools.product(*axis_values))

    def load_payload(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        return cast(dict[str, Any], payload)

    def load_cached_output(
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
        cached_outputs = self._extract_cached_items(
            payload,
            chunk_key,
            requested_items,
        )
        if cached_outputs is None:
            return None
        return collate_fn([cached_outputs])

    def _collect_indexed_callable_values(
        self, axis_values_obj: Callable[[int], Any]
    ) -> list[Any]:
        values: list[Any] = []
        index = 0
        while True:
            try:
                value = axis_values_obj(index)
                values.append(value)
                index += 1
            except (IndexError, KeyError):
                break
            except Exception:
                values.append(axis_values_obj(index))
                index += 1
                if index > 10000:
                    break
        return values

    def _materialize_axis_values(
        self, axis_values_obj: Any
    ) -> tuple[list[Any], Callable[[int], Any]]:
        if callable(axis_values_obj):
            try:
                values_result = axis_values_obj()
                if isinstance(values_result, (list, tuple)):
                    values = list(values_result)
                    getter = lambda idx, vals=values: vals[idx]
                else:
                    values = [values_result]
                    getter = axis_values_obj
            except TypeError:
                values = self._collect_indexed_callable_values(axis_values_obj)
                getter = axis_values_obj
            return values, getter
        values = list(axis_values_obj)
        return values, lambda idx, values=values: values[idx]

    def _resolve_axis_chunk_size(self, axis: str) -> int:
        spec = self.memo_chunk_spec.get(axis)
        if spec is None:
            raise KeyError(f"Missing memo_chunk_spec for axis '{axis}'")
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
        - memo_hash: The unique hash of the cache
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
                    "memo_hash": entry.name,
                    "path": entry,
                    "metadata": metadata,
                }
            )
        return caches

    @classmethod
    def _is_superset_compatible(
        cls,
        cache_metadata: dict[str, Any],
        req_params: dict[str, Any],
        req_axis_values: dict[str, Any],
    ) -> bool:
        """Check if an existing cache is a superset of the requested configuration.

        A cache is a superset if:
        - All requested axes are present in the cache's axes
        - All requested params are either in cache's params OR
          represented as axes in the cache with matching values

        Returns:
            True if the cache is a superset and compatible, False otherwise.
        """
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
    def find_compatible_caches(
        cls,
        cache_root: str | Path,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        memo_chunk_spec: dict[str, Any] | None = None,
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
        - memo_hash: The unique hash of the cache
        - path: Path to the cache directory
        - metadata: Full metadata dict

        Args:
            cache_root: Directory to scan for caches.
            params: Optional params dict (axis values excluded) to match.
            axis_values: Optional axis_values dict to match.
            memo_chunk_spec: Optional memo_chunk_spec dict to match.
            cache_version: Optional cache_version string to match.
            axis_order: Optional axis_order sequence to match.
            allow_superset: If True, find superset caches when params and
                axis_values are both provided.
        """
        all_caches = cls.discover_caches(cache_root)
        compatible: list[dict[str, Any]] = []

        if allow_superset and params is not None and axis_values is not None:
            for cache in all_caches:
                metadata = cache.get("metadata")
                if metadata is None:
                    continue
                if cls._is_superset_compatible(metadata, params, axis_values):
                    compatible.append(cache)
            return compatible

        for cache in all_caches:
            metadata = cache.get("metadata")
            if metadata is None:
                continue
            if params is not None:
                meta_params = metadata.get("params", {})
                if _stable_serialize(meta_params) != _stable_serialize(params):
                    continue
            if axis_values is not None:
                meta_axis_values = metadata.get("axis_values", {})
                if _stable_serialize(meta_axis_values) != _stable_serialize(
                    axis_values
                ):
                    continue
            if memo_chunk_spec is not None:
                meta_spec = metadata.get("memo_chunk_spec", {})
                if _stable_serialize(meta_spec) != _stable_serialize(memo_chunk_spec):
                    continue
            if cache_version is not None:
                if metadata.get("cache_version") != cache_version:
                    continue
            if axis_order is not None:
                meta_axis_order = metadata.get("axis_order")
                meta_tuple = (
                    tuple(meta_axis_order) if meta_axis_order is not None else None
                )
                if meta_tuple != tuple(axis_order):
                    continue
            compatible.append(cache)
        return compatible

    @classmethod
    def find_overlapping_caches(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Find caches that overlap with the given params and axis_values.

        Overlap means caches have the same params and at least one
        intersecting axis with shared values.

        Returns a list of overlapping cache entries, each containing:
        - memo_hash: The unique hash of the cache
        - path: Path to the cache directory
        - metadata: Full metadata dict
        - overlap: Dict of axis names to overlapping values

        Args:
            cache_root: Directory to scan for caches.
            params: Params dict (axis values excluded) to match.
            axis_values: axis_values dict to check for overlap.
        """
        all_caches = cls.discover_caches(cache_root)
        overlapping: list[dict[str, Any]] = []
        for cache in all_caches:
            metadata = cache.get("metadata")
            if metadata is None:
                continue
            meta_params = metadata.get("params", {})
            if _stable_serialize(meta_params) != _stable_serialize(params):
                continue
            meta_axis_values = metadata.get("axis_values", {})
            if not meta_axis_values or not axis_values:
                continue
            overlap_dict = {}
            has_all_intersections = True
            for axis in set(meta_axis_values) & set(axis_values):
                meta_vals = cls._axis_value_set(meta_axis_values[axis])
                vals = cls._axis_value_set(axis_values[axis])
                intersection = meta_vals & vals
                if intersection:
                    overlap_dict[axis] = sorted(intersection)
                else:
                    has_all_intersections = False
                    break
            if has_all_intersections and overlap_dict:
                overlapping.append({**cache, "overlap": overlap_dict})
        return overlapping

    @classmethod
    def auto_load(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any] | None = None,
        memo_chunk_spec: dict[str, Any] | None = None,
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
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

        When creating a new cache with axis_values, if memo_chunk_spec is
        not provided, uses chunk size 1 for all axes as default.

        Args:
            cache_root: Directory for caches.
            params: Params dict (axis values excluded).
            axis_values: Optional axis_values dict. If provided:
                - With allow_superset=False: requires exact match
                - With allow_superset=True: finds exact or superset matches
                - If None: finds caches with matching params (must be exactly 1)
            memo_chunk_spec: Optional chunk spec. Required when creating
                a new cache with axis_values. Defaults to size 1 for
                all axes.
            merge_fn: Optional merge function.
            memo_chunk_enumerator: Optional chunk enumerator.
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
            A ChunkMemo instance ready for execution.

        Raises:
            ValueError: If multiple caches match (ambiguous), if axis_values
                provided without matching cache and axis_names missing from
                memo_chunk_spec, or if multiple superset caches match.
        """
        cache_root = Path(cache_root)
        normalized_params = cls._normalized_hash_params_for_axis_values(params, {})

        if axis_values is not None:
            if allow_superset:
                compatible_caches = cls.find_compatible_caches(
                    cache_root,
                    params=normalized_params,
                    axis_values=axis_values,
                    allow_superset=True,
                )
            else:
                compatible_caches = cls.find_compatible_caches(
                    cache_root,
                    params=normalized_params,
                    axis_values=axis_values,
                )

            if len(compatible_caches) == 1:
                cache = compatible_caches[0]
                return cls.load_from_cache(
                    cache_root=cache_root,
                    memo_hash=cache["memo_hash"],
                    merge_fn=merge_fn,
                    memo_chunk_enumerator=memo_chunk_enumerator,
                    chunk_hash_fn=chunk_hash_fn,
                    cache_path_fn=cache_path_fn,
                    verbose=verbose,
                    profile=profile,
                    exclusive=exclusive,
                    warn_on_overlap=warn_on_overlap,
                )
            if len(compatible_caches) > 1:
                matches = [
                    f"{c['memo_hash']} (axis_values={c.get('metadata', {}).get('axis_values')})"
                    for c in compatible_caches
                ]
                raise ValueError(
                    f"Ambiguous: {len(compatible_caches)} caches match the given criteria. "
                    f"Matching hashes: {matches}. "
                    f"Use one of ChunkMemo.load_from_cache() to pick a specific cache."
                )
            if memo_chunk_spec is None:
                memo_chunk_spec = {
                    axis: (
                        len(axis_values[axis])
                        if isinstance(axis_values[axis], (list, tuple))
                        else 1
                    )
                    for axis in axis_values
                }
            return cls(
                cache_root=cache_root,
                memo_chunk_spec=memo_chunk_spec,
                axis_values=axis_values,
                merge_fn=merge_fn,
                memo_chunk_enumerator=memo_chunk_enumerator,
                chunk_hash_fn=chunk_hash_fn,
                cache_path_fn=cache_path_fn,
                cache_version=cache_version,
                axis_order=axis_order,
                verbose=verbose,
                profile=profile,
                exclusive=exclusive,
                warn_on_overlap=warn_on_overlap,
            )

        matching_caches = cls.find_compatible_caches(
            cache_root,
            params=normalized_params,
        )
        if len(matching_caches) == 1:
            cache = matching_caches[0]
            return cls.load_from_cache(
                cache_root=cache_root,
                memo_hash=cache["memo_hash"],
                merge_fn=merge_fn,
                memo_chunk_enumerator=memo_chunk_enumerator,
                chunk_hash_fn=chunk_hash_fn,
                cache_path_fn=cache_path_fn,
                verbose=verbose,
                profile=profile,
                exclusive=exclusive,
                warn_on_overlap=warn_on_overlap,
            )
        if len(matching_caches) > 1:
            matches = [
                f"{c['memo_hash']} (axis_values={c.get('metadata', {}).get('axis_values')})"
                for c in matching_caches
            ]
            raise ValueError(
                f"Ambiguous: {len(matching_caches)} caches match the given params. "
                f"Use axis_values parameter to disambiguate, or use one of "
                f"ChunkMemo.load_from_cache() to pick a specific cache.\n"
                f"Matches:\n  " + "\n  ".join(matches)
            )
        return cls(
            cache_root=cache_root,
            memo_chunk_spec={},
            axis_values={},
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunk_enumerator,
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
        memo_hash: str,
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> "ChunkCache":
        """Load a ChunkMemo instance from an existing cache by memo_hash.

        Raises:
            FileNotFoundError: If the cache directory or metadata.json does not exist.
            ValueError: If metadata is invalid.

        Args:
            cache_root: Directory containing caches.
            memo_hash: The hash identifying the specific cache.
            merge_fn: Optional merge function for the list of chunk outputs.
            memo_chunk_enumerator: Optional chunk enumerator.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths.
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params and axis_values as another cache.
            warn_on_overlap: If True, warn when caches overlap.
        """
        cache_root = Path(cache_root)
        cache_path = cache_root / memo_hash
        metadata_path = cache_path / "metadata.json"
        if not cache_path.exists():
            raise FileNotFoundError(f"Cache directory not found: {cache_path}")
        metadata = cls._read_metadata_file(metadata_path, required=True)
        if metadata is None:
            raise ValueError(f"Invalid metadata in {metadata_path}")
        axis_values = metadata.get("axis_values")
        if axis_values is None:
            raise ValueError("Metadata missing 'axis_values'")
        memo_chunk_spec = metadata.get("memo_chunk_spec")
        if memo_chunk_spec is None:
            raise ValueError("Metadata missing 'memo_chunk_spec'")
        instance = cls(
            cache_root=cache_root,
            memo_chunk_spec=memo_chunk_spec,
            axis_values=axis_values,
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunk_enumerator,
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


class ChunkMemo:
    """Facade that pairs a cache with runner helpers."""

    def __init__(
        self,
        cache_root: str | Path,
        memo_chunk_spec: dict[str, Any],
        axis_values: dict[str, Any],
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> None:
        self.cache = ChunkCache(
            cache_root=cache_root,
            memo_chunk_spec=memo_chunk_spec,
            axis_values=axis_values,
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            cache_path_fn=cache_path_fn,
            cache_version=cache_version,
            axis_order=axis_order,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
        )

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.cache, name)

    @property
    def verbose(self) -> int:
        return self.cache.verbose

    @property
    def profile(self) -> bool:
        return self.cache.profile

    @property
    def merge_fn(self) -> MergeFn | None:
        return self.cache.merge_fn

    def prepare_run(
        self,
        params: dict[str, Any],
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> tuple[
        dict[str, Any],
        list[ChunkKey],
        Mapping[ChunkKey, list[Tuple[Any, ...]]] | None,
    ]:
        return self.cache.prepare_run(params, axis_indices, **axes)

    def cache_status(
        self,
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> CacheStatus:
        return self.cache.cache_status(params, axis_indices=axis_indices, **axes)

    def chunk_hash(self, params: dict[str, Any], chunk_key: ChunkKey) -> str:
        return self.cache.chunk_hash(params, chunk_key)

    def resolve_cache_path(
        self, params: dict[str, Any], chunk_key: ChunkKey, chunk_hash: str
    ) -> Path:
        return self.cache.resolve_cache_path(params, chunk_key, chunk_hash)

    def load_payload(self, path: Path) -> dict[str, Any] | None:
        return self.cache.load_payload(path)

    def load_chunk_index(self, params: dict[str, Any]) -> dict[str, Any] | None:
        return self.cache.load_chunk_index(params)

    def update_chunk_index(
        self, params: dict[str, Any], chunk_hash: str, chunk_key: ChunkKey
    ) -> None:
        self.cache.update_chunk_index(params, chunk_hash, chunk_key)

    def build_item_maps_from_axis_values(
        self,
        chunk_key: ChunkKey,
        axis_values: Sequence[Tuple[Any, ...]],
        outputs: Sequence[Any],
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        return self.cache.build_item_maps_from_axis_values(
            chunk_key,
            axis_values,
            outputs,
        )

    def extract_items_from_map(
        self,
        item_map: Mapping[str, Any] | None,
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]],
    ) -> list[Any] | None:
        return self.cache.extract_items_from_map(item_map, chunk_key, requested_items)

    def reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None:
        return self.cache.reconstruct_output_from_items(chunk_key, items)

    def load_cached_output(
        self,
        payload: Mapping[str, Any],
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]] | None,
        collate_fn: Callable[[list[Any]], Any],
    ) -> Any | None:
        return self.cache.load_cached_output(
            payload,
            chunk_key,
            requested_items,
            collate_fn,
        )

    def item_hash(self, chunk_key: ChunkKey, axis_values: Tuple[Any, ...]) -> str:
        return self.cache.item_hash(chunk_key, axis_values)

    def expand_cache_status(
        self, cache_status: Mapping[str, Any]
    ) -> tuple[Mapping[str, Any], Tuple[str, ...], dict[str, dict[Any, int]]]:
        return self.cache.expand_cache_status(cache_status)

    def write_metadata(self, params: dict[str, Any]) -> Path:
        return self.cache.write_metadata(params)

    @classmethod
    def from_cache(cls, cache: ChunkCache) -> "ChunkMemo":
        instance = cls.__new__(cls)
        instance.cache = cache
        return instance

    @classmethod
    def auto_load(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any] | None = None,
        memo_chunk_spec: dict[str, Any] | None = None,
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
        allow_superset: bool = False,
    ) -> "ChunkMemo":
        cache = ChunkCache.auto_load(
            cache_root=cache_root,
            params=params,
            axis_values=axis_values,
            memo_chunk_spec=memo_chunk_spec,
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            cache_path_fn=cache_path_fn,
            cache_version=cache_version,
            axis_order=axis_order,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
            allow_superset=allow_superset,
        )
        return cls.from_cache(cache)

    @classmethod
    def load_from_cache(
        cls,
        cache_root: str | Path,
        memo_hash: str,
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
    ) -> "ChunkMemo":
        cache = ChunkCache.load_from_cache(
            cache_root=cache_root,
            memo_hash=memo_hash,
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            cache_path_fn=cache_path_fn,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
        )
        return cls.from_cache(cache)

    @classmethod
    def discover_caches(cls, cache_root: str | Path) -> list[dict[str, Any]]:
        return ChunkCache.discover_caches(cache_root)

    @classmethod
    def find_compatible_caches(
        cls,
        cache_root: str | Path,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        memo_chunk_spec: dict[str, Any] | None = None,
        cache_version: str | None = None,
        axis_order: Sequence[str] | None = None,
        allow_superset: bool = False,
    ) -> list[dict[str, Any]]:
        return ChunkCache.find_compatible_caches(
            cache_root=cache_root,
            params=params,
            axis_values=axis_values,
            memo_chunk_spec=memo_chunk_spec,
            cache_version=cache_version,
            axis_order=axis_order,
            allow_superset=allow_superset,
        )

    @classmethod
    def find_overlapping_caches(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any],
    ) -> list[dict[str, Any]]:
        return ChunkCache.find_overlapping_caches(
            cache_root=cache_root,
            params=params,
            axis_values=axis_values,
        )
