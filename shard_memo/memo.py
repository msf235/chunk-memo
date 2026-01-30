import dataclasses
import functools
import hashlib
import inspect
import itertools
import json
import math
import os
import pickle
import tempfile
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple

from ._format import (
    chunk_key_size,
    format_params,
    format_rate_eta,
    format_spec,
    print_detail,
    print_progress,
)

PROGRESS_REPORT_DIVISOR = 50

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]
MemoChunkEnumerator = Callable[[dict[str, Any]], Sequence[ChunkKey]]
MergeFn = Callable[[list[Any]], Any]
AxisIndexMap = dict[str, dict[Any, int]]
CachePathFn = Callable[[dict[str, Any], ChunkKey, str, str], Path | str]


@dataclasses.dataclass
class Diagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    merges: int = 0
    max_stream_items: int = 0
    stream_flushes: int = 0
    max_parallel_items: int = 0


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


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            dir=path.parent,
            prefix=f".{path.name}.",
            encoding="utf-8",
        ) as handle:
            tmp_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True)
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _apply_payload_timestamps(
    payload: dict[str, Any],
    existing: Mapping[str, Any] | None = None,
) -> None:
    now = _now_iso()
    created_at = None if existing is None else existing.get("created_at")
    if created_at is None:
        created_at = now
    payload["created_at"] = created_at
    payload["updated_at"] = now


def _build_chunk_index_entry(
    existing_entry: Mapping[str, Any] | None,
    chunk_key: ChunkKey,
) -> dict[str, Any]:
    created_at = None if existing_entry is None else existing_entry.get("created_at")
    if created_at is None:
        created_at = _now_iso()
    return {
        "created_at": created_at,
        "updated_at": _now_iso(),
        "chunk_key": chunk_key,
    }


def _format_progress(
    processed: int, total: int, cached: int, executed: int, width: int = 30
) -> str:
    if total <= 0:
        total = 1
    filled = int(width * processed / total)
    bar = "=" * filled + "-" * (width - filled)
    return f"[ShardMemo] progress {processed}/{total} [{bar}] cached={cached}"


class ShardMemo:
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
        """Initialize a ShardMemo runner.

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
            axis_values: Canonical split spec for the cache.
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
        self._checked_exclusive = False
        self._set_axis_values(axis_values)

    def run_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output.

        Supports axis selection by value or by index via axis_indices.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=False)

    def streaming_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Diagnostics]]:
        """Decorator for streaming memoized execution to disk only.

        Supports axis selection by value or by index via axis_indices.
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
    ) -> dict[str, Any]:
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
        chunk_index = self._load_chunk_index(params)
        use_index = bool(chunk_index)
        if self.profile and self.verbose >= 1 and profile_start is not None:
            print(
                f"[ShardMemo] profile cache_status_build_s={time.monotonic() - profile_start:0.3f}"
            )
        cached_chunks: list[ChunkKey] = []
        cached_chunk_indices: list[dict[str, Any]] = []
        missing_chunks: list[ChunkKey] = []
        missing_chunk_indices: list[dict[str, Any]] = []
        for chunk_key in chunk_keys:
            chunk_hash = self._chunk_hash(params, chunk_key)
            indices = self._chunk_indices_from_key(chunk_key, index_format)
            if use_index:
                exists = chunk_hash in chunk_index
            else:
                path = self._resolve_cache_path(params, chunk_key, chunk_hash)
                exists = path.exists()
            if exists:
                cached_chunks.append(chunk_key)
                cached_chunk_indices.append(indices)
            else:
                missing_chunks.append(chunk_key)
                missing_chunk_indices.append(indices)
        if self.profile and self.verbose >= 1 and profile_start is not None:
            print(
                f"[ShardMemo] profile cache_status_scan_s={time.monotonic() - profile_start:0.3f}"
            )
        return {
            "params": params,
            "axis_values": axis_values,
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
        """Run memoized execution over the canonical grid or a subset.

        axis_indices selects axes by index (int, slice, range, or list/tuple of
        those), based on the canonical split spec order.
        """
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
            if self.verbose == 1:
                self._print_run_header(params, self._axis_values, chunk_keys)
            return self._run_chunks(params, chunk_keys, exec_fn)
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            axis_values = self._normalize_axis_indices(axis_indices)
        else:
            axis_values = self._normalize_axes(axes)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(axis_values)
        if self.verbose == 1:
            self._print_run_header(params, axis_values, chunk_keys)
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
        """Run streaming memoized execution without returning outputs.

        axis_indices selects axes by index (int, slice, range, or list/tuple of
        those), based on the canonical split spec order.
        """
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
            if self.verbose == 1:
                self._print_run_header(params, self._axis_values, chunk_keys)
            return self._run_chunks_streaming(params, chunk_keys, exec_fn)
        if axis_indices is not None and axes:
            raise ValueError("axis_indices cannot be combined with axis values")
        if axis_indices is not None:
            axis_values = self._normalize_axis_indices(axis_indices)
        else:
            axis_values = self._normalize_axes(axes)
        chunk_keys, requested_items = self._build_chunk_plan_for_axes(axis_values)
        if self.verbose == 1:
            self._print_run_header(params, axis_values, chunk_keys)
        return self._run_chunks_streaming(
            params,
            chunk_keys,
            exec_fn,
            requested_items_by_chunk=requested_items,
        )

    def _execute_and_save_chunk(
        self,
        params: dict[str, Any],
        chunk_key: ChunkKey,
        exec_fn: Callable[..., Any],
        path: Path,
        chunk_hash: str,
        diagnostics: Diagnostics,
        existing_payload: Mapping[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any] | None]:
        diagnostics.executed_chunks += 1
        chunk_axes = {axis: list(values) for axis, values in chunk_key}
        chunk_output = exec_fn(params, **chunk_axes)
        diagnostics.max_stream_items = max(
            diagnostics.max_stream_items,
            _stream_item_count(chunk_output),
        )
        payload: dict[str, Any] = {}
        item_map = self._build_item_map(chunk_key, chunk_output)
        if item_map is not None:
            payload["items"] = item_map
            item_spec = self._build_item_spec_map(chunk_key, chunk_output)
            if item_spec is not None:
                payload["spec"] = item_spec
        else:
            payload["output"] = chunk_output
        _apply_payload_timestamps(payload, existing=existing_payload)
        _atomic_write_pickle(path, payload)
        self._update_chunk_index(params, chunk_hash, chunk_key)
        return chunk_output, item_map

    def _run_chunks(
        self,
        params: dict[str, Any],
        chunk_keys: Sequence[ChunkKey],
        exec_fn: Callable[..., Any],
        *,
        requested_items_by_chunk: (
            Mapping[ChunkKey, list[Tuple[Any, ...]]] | None
        ) = None,
    ) -> Tuple[Any, Diagnostics]:
        outputs: list[Any] = []
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))
        total_chunks = len(chunk_keys)
        progress_step = max(1, total_chunks // PROGRESS_REPORT_DIVISOR)
        start_time = time.monotonic()
        total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
        processed_items = 0

        collate_fn = self.merge_fn if self.merge_fn is not None else lambda chunk: chunk

        def report_progress(processed: int, final: bool = False) -> None:
            if self.verbose != 1:
                return
            if (
                not final
                and processed % progress_step != 0
                and processed != total_chunks
            ):
                return
            message = format_rate_eta(
                "planning",
                processed_items,
                total_items,
                start_time,
            )
            print_progress(message, final=final)

        for processed, chunk_key in enumerate(chunk_keys, start=1):
            chunk_hash = self._chunk_hash(params, chunk_key)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            processed_items += chunk_key_size(chunk_key)
            existing_payload: Mapping[str, Any] | None = None
            if path.exists():
                with open(path, "rb") as handle:
                    payload = pickle.load(handle)
                existing_payload = payload
                requested_items = None
                if requested_items_by_chunk is not None:
                    requested_items = requested_items_by_chunk.get(chunk_key)
                if requested_items is None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 2:
                        print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
                    chunk_output = payload.get("output")
                    if chunk_output is None:
                        items = payload.get("items")
                        if items is not None:
                            chunk_output = self._reconstruct_output_from_items(
                                chunk_key, items
                            )
                    if chunk_output is None:
                        raise ValueError("Cache payload missing required data")
                    outputs.append(chunk_output)
                    report_progress(processed, final=processed == total_chunks)
                    continue
                cached_outputs = self._extract_cached_items(
                    payload,
                    chunk_key,
                    requested_items,
                )
                if cached_outputs is not None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 2:
                        print_detail(
                            f"[ShardMemo] load chunk={chunk_key} items={len(requested_items)}"
                        )
                    outputs.append(collate_fn([cached_outputs]))
                    report_progress(processed, final=processed == total_chunks)
                    continue

            chunk_output, item_map = self._execute_and_save_chunk(
                params,
                chunk_key,
                exec_fn,
                path,
                chunk_hash,
                diagnostics,
                existing_payload,
            )

            if requested_items_by_chunk is None:
                if self.verbose >= 2:
                    print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
                outputs.append(chunk_output)
            else:
                requested_items = requested_items_by_chunk.get(chunk_key)
                if requested_items is None:
                    if self.verbose >= 2:
                        print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
                    outputs.append(chunk_output)
                else:
                    if self.verbose >= 2:
                        print_detail(
                            f"[ShardMemo] run chunk={chunk_key} items={len(requested_items)}"
                        )
                    extracted = self._extract_items_from_map(
                        item_map,
                        chunk_key,
                        requested_items,
                    )
                    outputs.append(
                        collate_fn([extracted])
                        if extracted is not None
                        else chunk_output
                    )

            report_progress(processed, final=processed == total_chunks)

        diagnostics.merges += 1
        if self.merge_fn is not None:
            merged = self.merge_fn(outputs)
        else:
            merged = outputs
        if self.verbose >= 2:
            print_detail(
                "[ShardMemo] summary "
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
        requested_items_by_chunk: (
            Mapping[ChunkKey, list[Tuple[Any, ...]]] | None
        ) = None,
    ) -> Diagnostics:
        diagnostics = Diagnostics(total_chunks=len(chunk_keys))
        total_chunks = len(chunk_keys)
        progress_step = max(1, total_chunks // PROGRESS_REPORT_DIVISOR)
        start_time = time.monotonic()
        total_items = sum(chunk_key_size(chunk_key) for chunk_key in chunk_keys)
        processed_items = 0

        def report_progress(processed: int, final: bool = False) -> None:
            if self.verbose != 1:
                return
            if (
                not final
                and processed % progress_step != 0
                and processed != total_chunks
            ):
                return
            message = format_rate_eta(
                "planning",
                processed_items,
                total_items,
                start_time,
            )
            print_progress(message, final=final)

        for processed, chunk_key in enumerate(chunk_keys, start=1):
            chunk_hash = self._chunk_hash(params, chunk_key)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            processed_items += chunk_key_size(chunk_key)
            existing_payload: Mapping[str, Any] | None = None
            if path.exists():
                if requested_items_by_chunk is None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 2:
                        print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
                    report_progress(processed, final=processed == total_chunks)
                    continue
                with open(path, "rb") as handle:
                    payload = pickle.load(handle)
                existing_payload = payload
                item_map = payload.get("items")
                if item_map is None:
                    item_map = self._build_item_map(chunk_key, payload.get("output"))
                    if item_map is not None:
                        payload["items"] = item_map
                        _atomic_write_pickle(path, payload)
                if item_map is not None:
                    diagnostics.cached_chunks += 1
                    if self.verbose >= 2:
                        requested_items = requested_items_by_chunk.get(chunk_key)
                        item_count = (
                            "all" if requested_items is None else len(requested_items)
                        )
                        print_detail(
                            f"[ShardMemo] load chunk={chunk_key} items={item_count}"
                        )
                    report_progress(processed, final=processed == total_chunks)
                    continue

            chunk_output, item_map = self._execute_and_save_chunk(
                params, chunk_key, exec_fn, path, chunk_hash, diagnostics, None
            )

            if self.verbose >= 2:
                if requested_items_by_chunk is None:
                    print_detail(f"[ShardMemo] run chunk={chunk_key} items=all")
                else:
                    requested_items = requested_items_by_chunk.get(chunk_key)
                    item_count = (
                        "all" if requested_items is None else len(requested_items)
                    )
                    print_detail(
                        f"[ShardMemo] run chunk={chunk_key} items={item_count}"
                    )

            report_progress(processed, final=processed == total_chunks)

        if self.verbose >= 2:
            print_detail(
                "[ShardMemo] summary "
                f"cached={diagnostics.cached_chunks} "
                f"executed={diagnostics.executed_chunks} "
                f"total={diagnostics.total_chunks}"
            )
        return diagnostics

    def _resolve_cache_path(
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

    def _chunk_index_path(self, params: dict[str, Any]) -> Path:
        return self._memo_root(params) / "chunks_index.json"

    def _load_chunk_index(self, params: dict[str, Any]) -> dict[str, Any]:
        path = self._chunk_index_path(params)
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError:
            return {}
        if isinstance(data, dict):
            return data
        return {}

    def _update_chunk_index(
        self,
        params: dict[str, Any],
        chunk_hash: str,
        chunk_key: ChunkKey,
    ) -> None:
        index = self._load_chunk_index(params)
        entry = _build_chunk_index_entry(index.get(chunk_hash), chunk_key)
        index[chunk_hash] = entry
        _atomic_write_json(self._chunk_index_path(params), index)

    def _normalized_hash_params(self, params: dict[str, Any]) -> dict[str, Any]:
        if not self._axis_values:
            return params
        return {
            key: value for key, value in params.items() if key not in self._axis_values
        }

    def _chunk_hash(self, params: dict[str, Any], chunk_key: ChunkKey) -> str:
        normalized = self._normalized_hash_params(params)
        return self.chunk_hash_fn(normalized, chunk_key, self.cache_version)

    def _memo_hash(self, params: dict[str, Any]) -> str:
        payload = {
            "params": self._normalized_hash_params(params),
            "axis_values": self._axis_values,
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

        normalized_params = self._normalized_hash_params(params)
        all_caches = self.discover_caches(self.cache_root)

        for cache in all_caches:
            metadata = cache.get("metadata")
            if metadata is None:
                continue
            meta_params = metadata.get("params", {})
            if _stable_serialize(meta_params) != _stable_serialize(normalized_params):
                continue
            meta_axis_values = metadata.get("axis_values", {})

            if _stable_serialize(meta_axis_values) == _stable_serialize(
                self._axis_values
            ):
                if self.exclusive:
                    raise ValueError(
                        f"Cache with same params and axis_values already exists: {cache['memo_hash']}. "
                        f"Use a different cache_version or cache_root, or set exclusive=False."
                    )
                continue

            if self.warn_on_overlap and meta_axis_values and self._axis_values:
                overlap = self._detect_axis_overlap(meta_axis_values, self._axis_values)
                if overlap:
                    warnings.warn(
                        f"Cache overlap detected: {cache['memo_hash']} has axis_values={meta_axis_values}, "
                        f"current has axis_values={self._axis_values}. Overlap: {overlap}. "
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
            values_a = (
                set(axis_values_a[axis])
                if isinstance(axis_values_a[axis], (list, tuple))
                else {axis_values_a[axis]}
            )
            values_b = (
                set(axis_values_b[axis])
                if isinstance(axis_values_b[axis], (list, tuple))
                else {axis_values_b[axis]}
            )
            intersection = values_a & values_b
            if intersection:
                overlap[axis] = sorted(intersection)
            else:
                return None
        return overlap

    def _memo_root(self, params: dict[str, Any]) -> Path:
        return self.cache_root / self._memo_hash(params)

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
            "axis_values": self._axis_values,
            "memo_chunk_spec": self.memo_chunk_spec,
            "cache_version": self.cache_version,
            "axis_order": self.axis_order,
            "memo_hash": self._memo_hash(params),
        }
        _atomic_write_json(path, payload)
        return path

    def _print_run_header(
        self,
        params: dict[str, Any],
        axis_values: Mapping[str, Any],
        chunk_keys: Sequence[ChunkKey],
    ) -> None:
        axis_order = self._resolve_axis_order(dict(axis_values))
        chunk_index = self._load_chunk_index(params)
        use_index = bool(chunk_index)
        cached_count = 0
        for chunk_key in chunk_keys:
            chunk_hash = self._chunk_hash(params, chunk_key)
            path = self._resolve_cache_path(params, chunk_key, chunk_hash)
            if (use_index and chunk_hash in chunk_index) or (
                not use_index and path.exists()
            ):
                cached_count += 1
        execute_count = len(chunk_keys) - cached_count
        lines = []
        lines.extend(format_params(params))
        lines.extend(format_spec(axis_values, axis_order))
        lines.append(f"[ShardMemo] cache_root: {self.cache_root}")
        lines.append(f"[ShardMemo] memo_chunk_spec: {self.memo_chunk_spec}")
        lines.append(f"[ShardMemo] cache_version: {self.cache_version}")
        lines.append(f"[ShardMemo] axis_order: {self.axis_order}")
        lines.append(f"[ShardMemo] plan: cached={cached_count} execute={execute_count}")

    def _resolve_axis_order(self, axis_values: dict[str, Any]) -> Tuple[str, ...]:
        if self.axis_order is not None:
            return self.axis_order
        return tuple(sorted(axis_values))

    def _set_axis_values(self, axis_values: dict[str, Any]) -> None:
        axis_order = self._resolve_axis_order(axis_values)
        for axis in axis_order:
            if axis not in axis_values:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
        axis_index_map: AxisIndexMap = {}
        for axis in axis_order:
            axis_index_map[axis] = {
                value: index for index, value in enumerate(axis_values[axis])
            }
        self._axis_values = {axis: list(axis_values[axis]) for axis in axis_order}
        self._axis_index_map = axis_index_map

    def _normalize_axes(self, axes: Mapping[str, Any]) -> dict[str, list[Any]]:
        if self._axis_values is None or self._axis_index_map is None:
            raise ValueError("axis_values must be set before running memoized function")
        axis_values: dict[str, list[Any]] = {}
        for axis in self._axis_values:
            if axis not in axes:
                axis_values[axis] = list(self._axis_values[axis])
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
                axis_values[axis] = list(self._axis_values[axis])
                continue
            values = axis_indices[axis]
            indices = self._expand_axis_indices(
                values, axis, len(self._axis_values[axis])
            )
            axis_values[axis] = [self._axis_values[axis][index] for index in indices]
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

    def _format_indices(self, indices: list[int], format_kind: str | None) -> Any:
        if format_kind == "int":
            return indices[:]
        if format_kind == "range":
            return self._indices_to_range(indices)
        if format_kind == "slice":
            return self._indices_to_slice(indices) or indices[:]
        return indices[:]

    def _indices_to_range(self, indices: list[int]) -> range | None:
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

    def _indices_to_slice(self, indices: list[int]) -> slice | None:
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
        self, axis_values: Mapping[str, Sequence[Any]]
    ) -> list[ChunkKey]:
        if self.memo_chunk_enumerator is not None:
            return list(self.memo_chunk_enumerator(dict(axis_values)))

        axis_order = self._resolve_axis_order(dict(axis_values))
        axis_chunks: list[list[Tuple[Any, ...]]] = []
        for axis in axis_order:
            values = axis_values.get(axis)
            if values is None:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            size = self._resolve_axis_chunk_size(axis)
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
            full_values = list(self._axis_values[axis])
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

    def _build_item_map(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> dict[str, Any] | None:
        if not isinstance(chunk_output, (list, tuple)):
            return None
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if len(axis_values) != len(chunk_output):
            return None
        return {
            self._item_hash(chunk_key, values): output
            for values, output in zip(axis_values, chunk_output)
        }

    def _build_item_spec_map(
        self, chunk_key: ChunkKey, chunk_output: Any
    ) -> dict[str, dict[str, Any]] | None:
        if not isinstance(chunk_output, (list, tuple)):
            return None
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if len(axis_values) != len(chunk_output):
            return None
        axis_names = [axis for axis, _ in chunk_key]
        item_spec: dict[str, dict[str, Any]] = {}
        for values in axis_values:
            item_key = self._item_hash(chunk_key, values)
            item_spec[item_key] = dict(zip(axis_names, values))
        return item_spec

    def _reconstruct_output_from_items(
        self, chunk_key: ChunkKey, items: Mapping[str, Any]
    ) -> list[Any] | None:
        axis_values = list(self._iter_chunk_axis_values(chunk_key))
        if not axis_values:
            return None
        outputs: list[Any] = []
        for values in axis_values:
            item_key = self._item_hash(chunk_key, values)
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
        return self._extract_items_from_map(item_map, chunk_key, requested_items)

    def _extract_items_from_map(
        self,
        item_map: Mapping[str, Any] | None,
        chunk_key: ChunkKey,
        requested_items: list[Tuple[Any, ...]],
    ) -> list[Any] | None:
        if item_map is None:
            return None
        outputs: list[Any] = []
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
            metadata = None
            if metadata_path.exists():
                try:
                    with open(metadata_path, "r", encoding="utf-8") as handle:
                        metadata = json.load(handle)
                except (json.JSONDecodeError, IOError):
                    pass
            caches.append(
                {
                    "memo_hash": entry.name,
                    "path": entry,
                    "metadata": metadata,
                }
            )
        return caches

    @classmethod
    def find_compatible_caches(
        cls,
        cache_root: str | Path,
        params: dict[str, Any] | None = None,
        axis_values: dict[str, Any] | None = None,
        memo_chunk_spec: dict[str, Any] | None = None,
        cache_version: str | None = None,
        axis_order: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find caches compatible with the given criteria.

        Matching rules:
        - If a criterion is provided, it must match exactly.
        - If a criterion is None (omitted), it is ignored (wildcard).

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
        """
        all_caches = cls.discover_caches(cache_root)
        compatible: list[dict[str, Any]] = []
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
                meta_vals = (
                    set(meta_axis_values[axis])
                    if isinstance(meta_axis_values[axis], (list, tuple))
                    else {meta_axis_values[axis]}
                )
                vals = (
                    set(axis_values[axis])
                    if isinstance(axis_values[axis], (list, tuple))
                    else {axis_values[axis]}
                )
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
    ) -> "ShardMemo":
        """Load a ShardMemo instance from an existing cache by memo_hash.

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
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
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

    @classmethod
    def create_singleton(
        cls,
        cache_root: str | Path,
        params: dict[str, Any],
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
    ) -> "ShardMemo":
        """Create a singleton cache with no axes (empty axis_values).

        Useful for memoizing functions that depend only on params, not on axes.
        The cache will contain a single chunk covering all data.

        Args:
            cache_root: Directory for chunk cache files.
            params: Parameters dict (no axis values).
            merge_fn: Optional merge function for the list of chunk outputs.
            memo_chunk_enumerator: Optional chunk enumerator.
            chunk_hash_fn: Optional override for chunk hashing.
            cache_path_fn: Optional override for cache file paths.
            cache_version: Cache namespace/version tag.
            axis_order: Axis iteration order (not used for singleton, kept for API consistency).
            verbose: Verbosity flag.
            profile: Enable profiling output.
            exclusive: If True, error when creating a cache with same
                params as an existing singleton cache.
            warn_on_overlap: If True, warn when caches overlap (not applicable for singleton).
        """
        memo_chunk_spec: dict[str, Any] = {}
        axis_values: dict[str, Any] = {}
        instance = cls(
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
        return instance
