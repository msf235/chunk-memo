"""Memo wrapper helpers and re-exports."""

from __future__ import annotations

import functools
import inspect
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, Tuple

from .cache import CachePathFn, ChunkCache, CollateFn, MemoChunkEnumerator
from .identity import params_to_cache_id
from .runners import Diagnostics, run, run_streaming


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
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output.

        Infers memoization params from non-axis arguments unless an explicit
        params dict is provided. Supports axis selection by value or by index
        via axis_indices. Uses runners.run under the hood.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=False)

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

                cache = self.cache_for_params(merged_params)

                exec_kwargs = dict(exec_extras)
                if params_arg in signature.parameters:
                    exec_kwargs[params_arg] = merged_params
                exec_fn = functools.partial(func, **exec_kwargs)
                sliced = cache.slice(axis_indices=axis_indices, **axis_inputs)
                if streaming:
                    return run_streaming(sliced, exec_fn)
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
