"""Memo wrapper helpers and re-exports."""

from __future__ import annotations

import functools
import inspect
import itertools
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence, Tuple, cast

from .cache import CachePathFn, ChunkCache, CollateFn, MemoChunkEnumerator
from .cache_index import chunk_index_path
from .data_write_utils import _atomic_write_json
from .identity import params_to_cache_id, stable_serialize
from .runners import Diagnostics, run, run_parallel, run_streaming
from .runners_common import resolve_cache_for_run


class ChunkMemo:
    """Chunk cache manager with decorator helpers."""

    def __init__(
        self,
        root: str | Path | None,
        chunk_spec: dict[str, Any] | None,
        axis_values: dict[str, Any],
        *,
        metadata: dict[str, Any] | None = None,
        collate_fn: CollateFn | None = None,
        chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[
            [str, Tuple[Tuple[str, Tuple[Any, ...]], ...], str], str
        ]
        | None = None,
        path_fn: CachePathFn | None = None,
        version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
        precompute_chunk_keys: bool = False,
    ) -> None:
        self.root = root
        self.chunk_spec = chunk_spec
        self.axis_values = axis_values
        self.metadata = metadata or {}
        self.collate_fn = collate_fn
        self.chunk_enumerator = chunk_enumerator
        self.chunk_hash_fn = chunk_hash_fn
        self.path_fn = path_fn
        self.version = version
        self.axis_order = axis_order
        self.verbose = verbose
        self.profile = profile
        self.exclusive = exclusive
        self.warn_on_overlap = warn_on_overlap
        self.precompute_chunk_keys = precompute_chunk_keys
        self._caches: dict[str, ChunkCache] = {}

    @classmethod
    def auto_load(
        cls,
        root: str | Path,
        params: dict[str, Any],
        axis_values: dict[str, Any] | None = None,
        chunk_spec: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        collate_fn: CollateFn | None = None,
        chunk_enumerator: MemoChunkEnumerator | None = None,
        chunk_hash_fn: Callable[
            [str, Tuple[Tuple[str, Tuple[Any, ...]], ...], str], str
        ]
        | None = None,
        path_fn: CachePathFn | None = None,
        version: str = "v1",
        axis_order: Sequence[str] | None = None,
        verbose: int = 1,
        profile: bool = False,
        exclusive: bool = False,
        warn_on_overlap: bool = False,
        precompute_chunk_keys: bool = False,
        allow_superset: bool = False,
        extend_cache: bool = False,
    ) -> "ChunkMemo":
        if not isinstance(params, dict):
            raise ValueError("'params' must be a dict")
        cache_id = params_to_cache_id(params)
        root_path = Path(root)
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

        cache_path = root_path / cache_id
        base_metadata = dict(metadata or {})
        base_metadata.pop("params", None)

        def params_are_superset(
            existing_params: Mapping[str, Any] | None,
            requested_params: Mapping[str, Any],
        ) -> bool:
            if not isinstance(existing_params, Mapping):
                return False
            for key, value in existing_params.items():
                if key not in requested_params:
                    return False
                if stable_serialize(requested_params[key]) != stable_serialize(value):
                    return False
            return True

        def find_cache_with_subset_params() -> dict[str, Any] | None:
            caches = ChunkCache.discover_caches(root_path)
            matches: list[dict[str, Any]] = []
            for cache in caches:
                cache_meta = cache.get("metadata", {})
                existing_params = None
                if isinstance(cache_meta, Mapping):
                    meta_payload = cache_meta.get("metadata", {})
                    if isinstance(meta_payload, Mapping):
                        existing_params = meta_payload.get("params")
                if params_are_superset(existing_params, params):
                    matches.append(cache)
            if not matches:
                return None
            if len(matches) > 1:
                raise ValueError(
                    "Multiple caches found with params that are subsets of the requested params. "
                    "Please disambiguate by passing exact params or cleanup existing caches."
                )
            return matches[0]

        def resolve_cache_path_with_metadata(
            memo_root: Path,
            chunk_key: Tuple[Tuple[str, Tuple[Any, ...]], ...],
            chunk_hash: str,
            metadata_payload: Mapping[str, Any],
            cache_path_fn: CachePathFn | None,
        ) -> Path:
            if cache_path_fn is None:
                return memo_root / "chunks" / f"{chunk_hash}.pkl"
            payload = dict(metadata_payload)
            path = cache_path_fn(payload, chunk_key, version, chunk_hash)
            path_obj = Path(path)
            if path_obj.is_absolute():
                return path_obj
            return memo_root / path_obj

        def migrate_cache_id(
            cache: ChunkCache,
            *,
            new_cache_id: str,
            new_metadata: dict[str, Any],
            old_axis_names: set[str],
            new_axis_names: set[str],
        ) -> ChunkCache:
            old_memo_root = cache._memo_root()
            new_memo_root = root_path / new_cache_id
            if new_memo_root.exists() and new_memo_root != old_memo_root:
                raise ValueError(
                    f"Target cache directory already exists: {new_memo_root}"
                )
            old_index = cache.load_chunk_index()
            old_metadata = dict(cache.metadata)
            old_memo_root.rename(new_memo_root)
            cache._memo_root_override = new_memo_root
            cache.set_identity(new_cache_id, metadata=new_metadata)
            if old_axis_names != new_axis_names:
                _atomic_write_json(chunk_index_path(new_memo_root), {})
                return cache
            if not old_index:
                return cache
            new_index: dict[str, Any] = {}
            for old_hash, entry in old_index.items():
                chunk_key = entry.get("chunk_key")
                if chunk_key is None:
                    continue
                new_hash = cache.chunk_hash(chunk_key)
                old_path = resolve_cache_path_with_metadata(
                    new_memo_root,
                    chunk_key,
                    old_hash,
                    old_metadata,
                    cache.path_fn,
                )
                new_path = resolve_cache_path_with_metadata(
                    new_memo_root,
                    chunk_key,
                    new_hash,
                    new_metadata,
                    cache.path_fn,
                )
                if not old_path.exists():
                    continue
                if new_hash != old_hash:
                    new_path.parent.mkdir(parents=True, exist_ok=True)
                    if new_path.exists():
                        raise ValueError(
                            f"Chunk path collision during cache migration: {new_path}"
                        )
                    old_path.rename(new_path)
                new_index[new_hash] = {"chunk_key": chunk_key}
            _atomic_write_json(chunk_index_path(new_memo_root), new_index)
            return cache

        cache_from_subset: dict[str, Any] | None = None
        if not cache_path.exists() and extend_cache:
            cache_from_subset = find_cache_with_subset_params()
            if cache_from_subset is not None:
                cache_path = Path(cache_from_subset.get("path", cache_path))

        if cache_path.exists():
            cache = ChunkCache.load_from_cache(
                root=root_path,
                cache_id=cache_path.name,
                collate_fn=collate_fn,
                chunk_enumerator=chunk_enumerator,
                chunk_hash_fn=chunk_hash_fn,
                path_fn=path_fn,
                verbose=verbose,
                profile=profile,
                exclusive=exclusive,
                warn_on_overlap=warn_on_overlap,
            )
            if not base_metadata:
                base_metadata = dict(cache.metadata)
                base_metadata.pop("params", None)
            old_axis_names = set(cache._axis_values_serializable or {})
            if axis_values is not None:
                meta_axis_values = cache._axis_values_serializable
                if meta_axis_values is None:
                    raise ValueError("Cache missing axis_values")
                if extend_cache:
                    cache.extend_axis_values(
                        axis_values,
                        allow_new_axes=True,
                        chunk_spec_override=chunk_spec,
                    )
                elif allow_superset:
                    if not ChunkCache._is_superset_compatible(
                        {"axis_values": meta_axis_values}, axis_values
                    ):
                        raise ValueError(
                            "Requested axis_values are not a subset of existing cache. "
                            f"Existing axis_values={summarize_axis_values(meta_axis_values)}, "
                            f"requested={summarize_axis_values(axis_values)}."
                        )
                    cache = cache.slice(**axis_values)
                elif stable_serialize(meta_axis_values) != stable_serialize(
                    axis_values
                ):
                    raise ValueError(
                        "Cache with same cache_id exists but axis_values differ. "
                        f"Existing axis_values={summarize_axis_values(meta_axis_values)}, "
                        f"requested={summarize_axis_values(axis_values)}."
                    )
            if cache_path.name != cache_id and extend_cache:
                merged_metadata = dict(base_metadata)
                merged_metadata["params"] = params
                new_axis_names = set(cache._axis_values_serializable or {})
                cache = migrate_cache_id(
                    cache,
                    new_cache_id=cache_id,
                    new_metadata=merged_metadata,
                    old_axis_names=old_axis_names,
                    new_axis_names=new_axis_names,
                )
            merged_metadata = dict(base_metadata)
            merged_metadata["params"] = params
            if cache.metadata != merged_metadata:
                cache.set_identity(cache.cache_id, metadata=merged_metadata)
            memo = cls(
                root=root_path,
                chunk_spec=cache.chunk_spec,
                axis_values=(
                    cache._axis_values_serializable
                    if cache._axis_values_serializable is not None
                    else (axis_values if axis_values is not None else {})
                ),
                metadata=base_metadata,
                collate_fn=cache.collate_fn,
                chunk_enumerator=cache.chunk_enumerator,
                chunk_hash_fn=cache.chunk_hash_fn,
                path_fn=cache.path_fn,
                version=cache.version,
                axis_order=cache.axis_order,
                verbose=cache.verbose,
                profile=cache.profile,
                exclusive=cache.exclusive,
                warn_on_overlap=cache.warn_on_overlap,
                precompute_chunk_keys=cache.precompute_chunk_keys,
            )
            memo._caches[cache_id] = cache
            return memo

        if axis_values is None:
            raise ValueError("axis_values is required when creating a new cache")
        merged_metadata = dict(base_metadata)
        merged_metadata["params"] = params
        cache = ChunkCache(
            root=root_path,
            cache_id=cache_id,
            metadata=merged_metadata,
            chunk_spec=chunk_spec,
            axis_values=axis_values_map,
            collate_fn=collate_fn,
            chunk_enumerator=chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            path_fn=path_fn,
            version=version,
            axis_order=axis_order,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
            precompute_chunk_keys=precompute_chunk_keys,
        )
        memo = cls(
            root=root_path,
            chunk_spec=cache.chunk_spec,
            axis_values=axis_values_map,
            metadata=base_metadata,
            collate_fn=collate_fn,
            chunk_enumerator=chunk_enumerator,
            chunk_hash_fn=chunk_hash_fn,
            path_fn=path_fn,
            version=version,
            axis_order=axis_order,
            verbose=verbose,
            profile=profile,
            exclusive=exclusive,
            warn_on_overlap=warn_on_overlap,
            precompute_chunk_keys=precompute_chunk_keys,
        )
        memo._caches[cache_id] = cache
        return memo

    def cache_for_params(self, params: dict[str, Any]) -> ChunkCache:
        if not isinstance(params, dict):
            raise ValueError("'params' must be a dict")
        cache_id = params_to_cache_id(params)
        cache = self._caches.get(cache_id)
        merged_metadata = dict(self.metadata)
        merged_metadata["params"] = params
        if cache is None:
            cache = ChunkCache(
                root=self.root,
                cache_id=cache_id,
                metadata=merged_metadata,
                chunk_spec=self.chunk_spec,
                axis_values=self.axis_values,
                collate_fn=self.collate_fn,
                chunk_enumerator=self.chunk_enumerator,
                chunk_hash_fn=self.chunk_hash_fn,
                path_fn=self.path_fn,
                version=self.version,
                axis_order=self.axis_order,
                verbose=self.verbose,
                profile=self.profile,
                exclusive=self.exclusive,
                warn_on_overlap=self.warn_on_overlap,
                precompute_chunk_keys=self.precompute_chunk_keys,
            )
            self._caches[cache_id] = cache
        elif cache.metadata != merged_metadata:
            cache.set_identity(cache_id, metadata=merged_metadata)
        return cache

    def cache(
        self,
        *,
        params_arg: str = "params",
        map_fn: Callable[..., Iterable[Any]] | None = None,
        map_fn_kwargs: Mapping[str, Any] | None = None,
        max_workers: int = 1,
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output.

        Infers memoization params from non-axis arguments unless an explicit
        params dict is provided. Supports axis selection by value or by index
        via axis_indices. Uses runners.run under the hood.
        """
        return self._build_wrapper(
            params_arg=params_arg,
            streaming=False,
            map_fn=map_fn,
            map_fn_kwargs=map_fn_kwargs,
            max_workers=max_workers,
        )

    def stream_cache(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Diagnostics]]:
        """Decorator for streaming memoized execution to disk only.

        Infers memoization params from non-axis arguments unless an explicit
        params dict is provided. Supports axis selection by value or by index
        via axis_indices. Uses runners.run_streaming under the hood.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=True)

    def _split_bound_args(
        self,
        bound_args: Mapping[str, Any],
        params_arg: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        axis_names = set(self.axis_values or {})
        extras = {k: v for k, v in bound_args.items() if k != params_arg}
        axis_inputs = {k: v for k, v in extras.items() if k in axis_names}
        exec_extras = {k: v for k, v in extras.items() if k not in axis_names}
        return exec_extras, axis_inputs

    def _merge_params(
        self,
        base_params: dict[str, Any] | None,
        exec_extras: dict[str, Any],
        *,
        params_provided: bool,
    ) -> dict[str, Any]:
        merged_params = dict(base_params or {})
        if params_provided:
            merged_params = dict(exec_extras)
            merged_params.update(base_params or {})
        else:
            merged_params.update(exec_extras)
        return merged_params

    def _build_wrapper(
        self,
        *,
        params_arg: str,
        streaming: bool,
        map_fn: Callable[..., Iterable[Any]] | None = None,
        map_fn_kwargs: Mapping[str, Any] | None = None,
        max_workers: int = 1,
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
                extend_cache = kwargs.pop("extend_cache", False)
                bound = signature.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                params_provided = params_arg in bound.arguments
                params = bound.arguments.get(params_arg)
                if params_provided and not isinstance(params, dict):
                    raise ValueError(f"'{params_arg}' must be a dict")

                exec_extras, axis_inputs = self._split_bound_args(
                    bound.arguments, params_arg
                )
                base_params: dict[str, Any] | None = (
                    params if params_provided else self.metadata.get("params", {})
                )
                merged_params = self._merge_params(
                    base_params,
                    exec_extras,
                    params_provided=params_provided,
                )
                cache = resolve_cache_for_run(
                    self,
                    params=merged_params,
                    axis_values_override=axis_inputs if extend_cache else None,
                    extend_cache=extend_cache,
                    allow_superset=extend_cache,
                )

                exec_kwargs = dict(exec_extras)
                if params_arg in signature.parameters:
                    exec_kwargs[params_arg] = merged_params
                exec_fn = functools.partial(func, **exec_kwargs)
                sliced = cast(ChunkCache, cache).slice(
                    axis_indices=axis_indices, **axis_inputs
                )
                if streaming:
                    return run_streaming(sliced, exec_fn)
                if max_workers > 1 or map_fn is not None:
                    if map_fn is None:
                        _require_top_level_function(func)
                    return run_parallel(
                        _build_items_for_parallel(sliced),
                        exec_fn=exec_fn,
                        cache=sliced,
                        map_fn=map_fn or _map_process_pool,
                        map_fn_kwargs=_resolve_map_kwargs(map_fn_kwargs, max_workers),
                    )
                return run(sliced, exec_fn)

            def cache_status(
                params: dict[str, Any] | None = None,
                *,
                axis_indices: Mapping[str, Any] | None = None,
                **axes: Any,
            ):
                if params is not None and not isinstance(params, dict):
                    raise ValueError(f"'{params_arg}' must be a dict")
                exec_extras, axis_inputs = self._split_bound_args(axes, params_arg)
                base_params = (
                    params if params is not None else self.metadata.get("params", {})
                )
                merged_params = self._merge_params(
                    base_params,
                    exec_extras,
                    params_provided=params is not None,
                )
                cache = self.cache_for_params(merged_params)
                return cache.cache_status(axis_indices=axis_indices, **axis_inputs)

            setattr(wrapper, "cache_status", cache_status)
            return wrapper

        return decorator


def _map_process_pool(
    func: Callable[..., Any], items: Iterable[Any], **kwargs: Any
) -> Iterable[Any]:
    max_workers = kwargs.get("max_workers")
    chunksize = kwargs.get("chunksize")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if chunksize is None:
            return list(executor.map(func, items))
        return list(executor.map(func, items, chunksize=chunksize))


def _resolve_map_kwargs(
    map_fn_kwargs: Mapping[str, Any] | None, max_workers: int
) -> dict[str, Any]:
    resolved = dict(map_fn_kwargs or {})
    if max_workers > 1 and "max_workers" not in resolved:
        resolved["max_workers"] = max_workers
    return resolved


def _build_items_for_parallel(cache: ChunkCache) -> list[dict[str, Any]]:
    status = cache.cache_status()
    axis_values = status.get("axis_values", {})
    axis_order = status.get("axis_order", tuple(axis_values))
    axis_lists: list[list[Any]] = []
    for axis in axis_order:
        values = axis_values.get(axis, [])
        if isinstance(values, (list, tuple)):
            axis_lists.append(list(values))
        elif isinstance(values, Sequence):
            axis_lists.append(list(values))
        else:
            axis_lists.append([values])
    return [dict(zip(axis_order, values)) for values in itertools.product(*axis_lists)]


def _require_top_level_function(func: Callable[..., Any]) -> None:
    qualname = getattr(func, "__qualname__", "")
    if "<locals>" in qualname:
        raise ValueError(
            "Parallel execution requires a top-level function (module scope) so it can be "
            "pickled for ProcessPoolExecutor. Move the function to module scope or pass a "
            "custom map_fn that does not require pickling."
        )
