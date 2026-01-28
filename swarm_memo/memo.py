import dataclasses
import functools
import hashlib
import inspect
import itertools
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[ChunkKey]]
MergeFn = Callable[[List[Any]], Any]
AxisIndexMap = Dict[str, Dict[Any, int]]
CachePathFn = Callable[[dict[str, Any], ChunkKey, str, str], Path | str]


@dataclasses.dataclass
class Diagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    merges: int = 0
    max_stream_items: int = 0


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


def _chunk_values(values: Sequence[Any], size: int) -> List[Tuple[Any, ...]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    chunks: List[Tuple[Any, ...]] = []
    for start in range(0, len(values), size):
        end = min(start + size, len(values))
        chunks.append(tuple(values[start:end]))
    return chunks


def _stream_item_count(output: Any) -> int:
    if isinstance(output, (list, tuple, dict)):
        return len(output)
    return 1


def _atomic_write_pickle(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
        ) as handle:
            tmp_path = Path(handle.name)
            pickle.dump(payload, handle, protocol=5)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


class ChunkMemo:
    def __init__(
        self,
        cache_root: str | Path,
        memo_chunk_spec: dict[str, Any],
        split_spec: dict[str, Any],
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_path_fn: CachePathFn | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
    ) -> None:
        """Initialize a ChunkMemo runner.

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
            split_spec: Canonical split spec for the cache.
            verbose: Verbosity flag.
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
        self._split_spec: dict[str, Any] | None = None
        self._axis_index_map: AxisIndexMap | None = None
        self._set_split_spec(split_spec)

    def run_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output."""
        return self._build_wrapper(params_arg=params_arg, streaming=False)

    def streaming_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Diagnostics]]:
        """Decorator for streaming memoized execution to disk only."""
        return self._build_wrapper(params_arg=params_arg, streaming=True)

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
                extras = {k: v for k, v in bound.arguments.items() if k != params_arg}

                axis_names = set(self._split_spec or {})
                axis_inputs = {k: v for k, v in extras.items() if k in axis_names}
                exec_extras = {k: v for k, v in extras.items() if k not in axis_names}
                merged_params = dict(params)
                merged_params.update(exec_extras)

                exec_fn = functools.partial(func, **exec_extras)
                if streaming:
                    return self.run_streaming(
                        merged_params,
                        exec_fn=exec_fn,
                        axis_indices=axis_indices,
                        **axis_inputs,
                    )
                return self.run(
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
                axis_names = set(self._split_spec or {})
                exec_extras = {k: v for k, v in axes.items() if k not in axis_names}
                merged_params = dict(params)
                merged_params.update(exec_extras)
                axis_inputs = {k: v for k, v in axes.items() if k in axis_names}
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
    ) -> dict[str, Any]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before checking cache status")
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            split_spec = self._normalize_axis_indices(axis_indices)
        else:
            split_spec = self._normalize_axes(axes)
        index_format = self._infer_index_format(axis_indices)
        chunk_keys = self._build_chunk_keys_for_axes(split_spec)
        cached_chunks: List[ChunkKey] = []
        cached_chunk_indices: List[Dict[str, Any]] = []
        missing_chunks: List[ChunkKey] = []
        missing_chunk_indices: List[Dict[str, Any]] = []
        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            indices = self._chunk_indices_from_key(chunk_key, index_format)
            if path.exists():
                cached_chunks.append(chunk_key)
                cached_chunk_indices.append(indices)
            else:
                missing_chunks.append(chunk_key)
                missing_chunk_indices.append(indices)
        return {
            "params": params,
            "axis_values": split_spec,
            "total_chunks": len(chunk_keys),
            "cached_chunks": cached_chunks,
            "cached_chunk_indices": cached_chunk_indices,
            "missing_chunks": missing_chunks,
            "missing_chunk_indices": missing_chunk_indices,
        }

    def run(
        self,
        params: dict[str, Any],
        exec_fn: Callable[..., Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> Tuple[Any, Diagnostics]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        if axis_indices is None and not axes:
            chunk_keys = self._build_chunk_keys()
            return self._run_chunks(params, chunk_keys, exec_fn)
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            split_spec = self._normalize_axis_indices(axis_indices)
        else:
            split_spec = self._normalize_axes(axes)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(split_spec)
        return self._run_chunks(
            params,
            chunk_keys,
            exec_fn,
            requested_items_by_chunk=requested_items,
        )

    def run_streaming(
        self,
        params: dict[str, Any],
        exec_fn: Callable[..., Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> Diagnostics:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        if axis_indices is None and not axes:
            chunk_keys = self._build_chunk_keys()
            return self._run_chunks_streaming(params, chunk_keys, exec_fn)
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            split_spec = self._normalize_axis_indices(axis_indices)
        else:
            split_spec = self._normalize_axes(axes)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(split_spec)
        return self._run_chunks_streaming(
            params,
            chunk_keys,
            exec_fn,
            requested_items_by_chunk=requested_items,
        )

    def _run_chunks(
        self,
        params: dict[str, Any],
        chunk_keys: Sequence[ChunkKey],
        exec_fn: Callable[..., Any],
        *,
        requested_items_by_chunk: (
            Mapping[ChunkKey, List[Tuple[Any, ...]]] | None
        ) = None,
    ) -> Tuple[Any, Diagnostics]:
        outputs: List[Any] = []
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))

        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            if path.exists():
                with open(path, "rb") as handle:
                    payload = pickle.load(handle)
                requested_items = None
                if requested_items_by_chunk is not None:
                    requested_items = requested_items_by_chunk.get(chunk_key)
                if requested_items is None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 1:
                        print(f"[ChunkMemo] load chunk={chunk_key} items=all")
                    outputs.append(payload["output"])
                    continue
                cached_outputs = self._extract_cached_items(
                    payload,
                    chunk_key,
                    requested_items,
                )
                if cached_outputs is not None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 1:
                        print(
                            f"[ChunkMemo] load chunk={chunk_key} items={len(requested_items)}"
                        )
                    outputs.append(cached_outputs)
                    continue

            diagnostics.executed_chunks += 1
            chunk_axes = {axis: list(values) for axis, values in chunk_key}
            chunk_output = exec_fn(params, **chunk_axes)
            diagnostics.max_stream_items = max(
                diagnostics.max_stream_items,
                _stream_item_count(chunk_output),
            )
            payload = {"output": chunk_output}
            item_map = self._build_item_map(chunk_key, chunk_output)
            if item_map is not None:
                payload["items"] = item_map
            _atomic_write_pickle(path, payload)

            if requested_items_by_chunk is None:
                if self.verbose >= 1:
                    print(f"[ChunkMemo] run chunk={chunk_key} items=all")
                outputs.append(chunk_output)
            else:
                requested_items = requested_items_by_chunk.get(chunk_key)
                if requested_items is None:
                    if self.verbose >= 1:
                        print(f"[ChunkMemo] run chunk={chunk_key} items=all")
                    outputs.append(chunk_output)
                else:
                    if self.verbose >= 1:
                        print(
                            f"[ChunkMemo] run chunk={chunk_key} items={len(requested_items)}"
                        )
                    extracted = self._extract_items_from_map(
                        item_map,
                        chunk_key,
                        requested_items,
                    )
                    outputs.append(extracted if extracted is not None else chunk_output)

        diagnostics.merges += 1
        if self.merge_fn is not None:
            merged = self.merge_fn(outputs)
        else:
            merged = outputs
        if self.verbose >= 1:
            print(
                "[ChunkMemo] summary "
                f"cached={diagnostics.cached_chunks} "
                f"executed={diagnostics.executed_chunks} "
                f"total={diagnostics.total_chunks}"
            )
        return merged, diagnostics

    def _run_chunks_streaming(
        self,
        params: dict[str, Any],
        chunk_keys: Sequence[ChunkKey],
        exec_fn: Callable[..., Any],
        *,
        requested_items_by_chunk: Mapping[ChunkKey, List[Tuple[Any, ...]]]
        | None = None,
    ) -> Diagnostics:
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))

        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            if path.exists():
                if requested_items_by_chunk is None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 1:
                        print(f"[ChunkMemo] load chunk={chunk_key} items=all")
                    continue
                with open(path, "rb") as handle:
                    payload = pickle.load(handle)
                item_map = payload.get("items")
                if item_map is None:
                    item_map = self._build_item_map(chunk_key, payload.get("output"))
                    if item_map is not None:
                        payload["items"] = item_map
                        _atomic_write_pickle(path, payload)
                if item_map is not None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 1:
                        requested_items = requested_items_by_chunk.get(chunk_key)
                        item_count = (
                            "all" if requested_items is None else len(requested_items)
                        )
                        print(f"[ChunkMemo] load chunk={chunk_key} items={item_count}")
                    continue

            diagnostics.executed_chunks += 1
            chunk_axes = {axis: list(values) for axis, values in chunk_key}
            chunk_output = exec_fn(params, **chunk_axes)
            diagnostics.max_stream_items = max(
                diagnostics.max_stream_items,
                _stream_item_count(chunk_output),
            )
            payload = {"output": chunk_output}
            item_map = self._build_item_map(chunk_key, chunk_output)
            if item_map is not None:
                payload["items"] = item_map
            _atomic_write_pickle(path, payload)
            if self.verbose >= 1:
                if requested_items_by_chunk is None:
                    print(f"[ChunkMemo] run chunk={chunk_key} items=all")
                else:
                    requested_items = requested_items_by_chunk.get(chunk_key)
                    item_count = (
                        "all" if requested_items is None else len(requested_items)
                    )
                    print(f"[ChunkMemo] run chunk={chunk_key} items={item_count}")

        if self.verbose >= 1:
            print(
                "[ChunkMemo] summary "
                f"cached={diagnostics.cached_chunks} "
                f"executed={diagnostics.executed_chunks} "
                f"total={diagnostics.total_chunks}"
            )
        return diagnostics

    def _resolve_cache_path(
        self, params: dict[str, Any], chunk_key: ChunkKey, chunk_hash: str
    ) -> Path:
        if self.cache_path_fn is None:
            return self.cache_root / f"{chunk_hash}.pkl"
        path = self.cache_path_fn(params, chunk_key, self.cache_version, chunk_hash)
        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj
        return self.cache_root / path_obj

    def _resolve_axis_order(self, split_spec: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(split_spec))

    def _set_split_spec(self, split_spec: dict[str, Any]) -> None:
        axis_order = self._resolve_axis_order(split_spec)
        for axis in axis_order:
            if axis not in split_spec:
                raise KeyError(f"Missing axis '{axis}' in split_spec")
        axis_index_map: AxisIndexMap = {}
        for axis in axis_order:
            axis_index_map[axis] = {
                value: index for index, value in enumerate(split_spec[axis])
            }
        self._split_spec = {axis: list(split_spec[axis]) for axis in axis_order}
        self._axis_index_map = axis_index_map

    def _normalize_axes(self, axes: Mapping[str, Any]) -> dict[str, List[Any]]:
        if self._split_spec is None or self._axis_index_map is None:
            raise ValueError("split_spec must be set before running memoized function")
        split_spec: dict[str, List[Any]] = {}
        for axis in self._split_spec:
            if axis not in axes:
                split_spec[axis] = list(self._split_spec[axis])
                continue
            values = axes[axis]
            if isinstance(values, (list, tuple)):
                normalized = list(values)
            else:
                normalized = [values]
            for value in normalized:
                if value not in self._axis_index_map[axis]:
                    raise KeyError(
                        f"Value '{value}' not found in split_spec for axis '{axis}'"
                    )
            split_spec[axis] = normalized
        return split_spec

    def _normalize_axis_indices(
        self, axis_indices: Mapping[str, Any]
    ) -> dict[str, List[Any]]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        split_spec: dict[str, List[Any]] = {}
        for axis in self._split_spec:
            if axis not in axis_indices:
                split_spec[axis] = list(self._split_spec[axis])
                continue
            values = axis_indices[axis]
            indices = self._expand_axis_indices(
                values, axis, len(self._split_spec[axis])
            )
            split_spec[axis] = [self._split_spec[axis][index] for index in indices]
        return split_spec

    def _expand_axis_indices(self, values: Any, axis: str, axis_len: int) -> List[int]:
        if isinstance(values, (list, tuple)):
            items = list(values)
        else:
            items = [values]
        resolved: List[int] = []
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

    def _build_chunk_keys(self) -> List[ChunkKey]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        return self._build_chunk_keys_for_axes(self._split_spec)

    def _chunk_indices_from_key(
        self, chunk_key: ChunkKey, index_format: Mapping[str, str] | None
    ) -> Dict[str, Any]:
        if self._axis_index_map is None:
            raise ValueError("split_spec must be set before checking cache status")
        indices: Dict[str, Any] = {}
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
    ) -> Dict[str, str] | None:
        if axis_indices is None:
            return None
        formats: Dict[str, str] = {}
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

    def _format_indices(self, indices: List[int], format_kind: str | None) -> Any:
        if format_kind == "int":
            return indices[:]
        if format_kind == "range":
            return self._indices_to_range(indices)
        if format_kind == "slice":
            return self._indices_to_slice(indices) or indices[:]
        return indices[:]

    def _indices_to_range(self, indices: List[int]) -> range | None:
        if not indices:
            return range(0, 0)
        if len(indices) == 1:
            return range(indices[0], indices[0] + 1)
        step = indices[1] - indices[0]
        if step == 0:
            return None
        for prev, current in zip(indices, indices[1:]):
            if current - prev != step:
                return None
        return range(indices[0], indices[-1] + step, step)

    def _indices_to_slice(self, indices: List[int]) -> slice | None:
        if not indices:
            return slice(0, 0, None)
        if len(indices) == 1:
            return slice(indices[0], indices[0] + 1, None)
        step = indices[1] - indices[0]
        if step == 0:
            return None
        for prev, current in zip(indices, indices[1:]):
            if current - prev != step:
                return None
        return slice(indices[0], indices[-1] + step, step)

    def _build_chunk_keys_for_axes(
        self, split_spec: Mapping[str, Sequence[Any]]
    ) -> List[ChunkKey]:
        if self.memo_chunk_enumerator is not None:
            return list(self.memo_chunk_enumerator(dict(split_spec)))

        axis_order = self._resolve_axis_order(dict(split_spec))
        axis_chunks: List[List[Tuple[Any, ...]]] = []
        for axis in axis_order:
            values = split_spec.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in split_spec")
            size = self._resolve_axis_chunk_size(axis)
            axis_chunks.append(_chunk_values(values, size))

        chunk_keys: List[ChunkKey] = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def _build_chunk_plan_for_axes(
        self, split_spec: Mapping[str, Sequence[Any]]
    ) -> Tuple[List[ChunkKey], Dict[ChunkKey, List[Tuple[Any, ...]]]]:
        if self._split_spec is None or self._axis_index_map is None:
            raise ValueError("split_spec must be set before running memoized function")
        axis_order = self._resolve_axis_order(self._split_spec)
        per_axis_chunks: List[List[dict[str, Any]]] = []
        for axis in axis_order:
            requested_values = list(split_spec.get(axis, []))
            if not requested_values:
                raise ValueError(f"Missing values for axis '{axis}'")
            size = self._resolve_axis_chunk_size(axis)
            full_values = list(self._split_spec[axis])
            chunk_map: Dict[int, dict[str, Any]] = {}
            for value in requested_values:
                index = self._axis_index_map[axis].get(value)
                if index is None:
                    raise KeyError(
                        f"Value '{value}' not found in split_spec for axis '{axis}'"
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

        chunk_keys: List[ChunkKey] = []
        requested_items: Dict[ChunkKey, List[Tuple[Any, ...]]] = {}
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

    def _build_item_map(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> Dict[str, Any] | None:
        if not isinstance(chunk_output, (list, tuple)):
            return None
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if len(axis_values) != len(chunk_output):
            return None
        return {
            self._item_hash(chunk_key, values): output
            for values, output in zip(axis_values, chunk_output)
        }

    def _extract_cached_items(
        self,
        payload: Mapping[str, Any],
        chunk_key: ChunkKey,
        requested_items: List[Tuple[Any, ...]],
    ) -> List[Any] | None:
        item_map = payload.get("items")
        return self._extract_items_from_map(item_map, chunk_key, requested_items)

    def _extract_items_from_map(
        self,
        item_map: Mapping[str, Any] | None,
        chunk_key: ChunkKey,
        requested_items: List[Tuple[Any, ...]],
    ) -> List[Any] | None:
        if item_map is None:
            return None
        outputs: List[Any] = []
        for values in requested_items:
            item_key = self._item_hash(chunk_key, values)
            if item_key not in item_map:
                return None
            outputs.append(item_map[item_key])
        return outputs

    def _item_hash(self, chunk_key: ChunkKey, axis_values: Tuple[Any, ...]) -> str:
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
