import dataclasses
import functools
import hashlib
import inspect
import itertools
import pickle
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[ChunkKey]]
MergeFn = Callable[[List[Any]], Any]
AxisIndexMap = Dict[str, Dict[Any, int]]


@dataclasses.dataclass
class Diagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    merges: int = 0


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


class ChunkMemo:
    def __init__(
        self,
        cache_root: str | Path,
        memo_chunk_spec: dict[str, Any],
        exec_fn: Callable[..., Any],
        merge_fn: MergeFn | None = None,
        memo_chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[[dict[str, Any], ChunkKey, str], str] | None = None,
        cache_version: str = "v1",
        axis_order: Sequence[str] | None = None,
        split_spec: dict[str, Any] | None = None,
        verbose: int = 1,
    ) -> None:
        """Initialize a ChunkMemo runner.

        Args:
            cache_root: Directory for chunk cache files.
            memo_chunk_spec: Per-axis chunk sizes (e.g., {"strat": 1, "s": 3}).
            exec_fn: Executes one memo chunk with params and axis vectors.
            merge_fn: Optional merge function for the list of chunk outputs.
            memo_chunk_enumerator: Optional chunk enumerator that defines the
                memo chunk order.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_version: Cache namespace/version tag.
            axis_order: Axis iteration order (defaults to lexicographic).
            split_spec: Optional default split spec for wrapper calls.
            verbose: Verbosity flag.
        """
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.memo_chunk_spec = memo_chunk_spec
        self.exec_fn = exec_fn
        self.merge_fn = merge_fn
        self.memo_chunk_enumerator = memo_chunk_enumerator
        self.chunk_hash_fn = chunk_hash_fn or default_chunk_hash
        self.cache_version = cache_version
        self.axis_order = tuple(axis_order) if axis_order is not None else None
        self.verbose = verbose
        self._split_spec: dict[str, Any] | None = None
        self._axis_index_map: AxisIndexMap | None = None
        if split_spec is not None:
            self._set_split_spec(split_spec)

    def run_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output."""
        return self._build_wrapper(params_arg=params_arg)

    def _build_wrapper(
        self, *, params_arg: str
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
                runner = self._clone_with_exec_fn(exec_fn)
                return runner.run_with_axes(
                    merged_params, axis_indices=axis_indices, **axis_inputs
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
                runner = self._clone_with_exec_fn(func)
                return runner.cache_status(
                    merged_params, axis_indices=axis_indices, **axis_inputs
                )

            setattr(wrapper, "cache_status", cache_status)
            return wrapper

        return decorator

    def _clone_with_exec_fn(self, exec_fn: Callable[..., Any]) -> "ChunkMemo":
        return ChunkMemo(
            cache_root=self.cache_root,
            memo_chunk_spec=self.memo_chunk_spec,
            exec_fn=exec_fn,
            merge_fn=self.merge_fn,
            memo_chunk_enumerator=self.memo_chunk_enumerator,
            chunk_hash_fn=self.chunk_hash_fn,
            cache_version=self.cache_version,
            axis_order=self.axis_order,
            split_spec=self._split_spec,
            verbose=self.verbose,
        )

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
            path = self.cache_root / f"{chunk_hash}.pkl"
            indices = self._chunk_indices_from_key(chunk_key, index_format)
            if path.exists():
                cached_chunks.append(chunk_key)
                cached_chunk_indices.append(indices)
            else:
                missing_chunks.append(chunk_key)
                missing_chunk_indices.append(indices)
        return {
            "axis_values": split_spec,
            "total_chunks": len(chunk_keys),
            "cached_chunks": cached_chunks,
            "cached_chunk_indices": cached_chunk_indices,
            "missing_chunks": missing_chunks,
            "missing_chunk_indices": missing_chunk_indices,
        }

    def run(
        self, params: dict[str, Any], split_spec: dict[str, Any]
    ) -> Tuple[Any, Diagnostics]:
        self._set_split_spec(split_spec)
        return self.run_with_params(params)

    def run_with_axes(
        self,
        params: dict[str, Any],
        *,
        axis_indices: Mapping[str, Any] | None = None,
        **axes: Any,
    ) -> Tuple[Any, Diagnostics]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            split_spec = self._normalize_axis_indices(axis_indices)
        else:
            split_spec = self._normalize_axes(axes)
        chunk_keys = self._build_chunk_keys_for_axes(split_spec)
        return self._run_chunks(params, chunk_keys)

    def run_with_params(self, params: dict[str, Any]) -> Tuple[Any, Diagnostics]:
        if self._split_spec is None:
            raise ValueError("split_spec must be set before running memoized function")
        chunk_keys = self._build_chunk_keys()
        return self._run_chunks(params, chunk_keys)

    def _run_chunks(
        self, params: dict[str, Any], chunk_keys: Sequence[ChunkKey]
    ) -> Tuple[Any, Diagnostics]:
        outputs: List[Any] = []
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))

        for chunk_key in chunk_keys:
            chunk_hash = self.chunk_hash_fn(params, chunk_key, self.cache_version)
            path = self.cache_root / f"{chunk_hash}.pkl"
            if path.exists():
                diagnostics.cached_chunks += 1
                with open(path, "rb") as handle:
                    outputs.append(pickle.load(handle)["output"])
                continue

            diagnostics.executed_chunks += 1
            chunk_axes = {axis: list(values) for axis, values in chunk_key}
            chunk_output = self.exec_fn(params, **chunk_axes)
            with open(path, "wb") as handle:
                pickle.dump({"output": chunk_output}, handle, protocol=5)
            outputs.append(chunk_output)

        diagnostics.merges += 1
        if self.merge_fn is not None:
            merged = self.merge_fn(outputs)
        else:
            merged = outputs
        return merged, diagnostics

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
