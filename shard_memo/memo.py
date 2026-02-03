"""Memo wrapper helpers and re-exports."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Mapping, Tuple, cast

from .cache import ChunkCache
from .runner_protocol import RunnerContext
from .runners import Diagnostics, run, run_streaming


class ChunkMemo:
    def __init__(self, cache: ChunkCache) -> None:
        self.cache = cache

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
        axis_names = set(self.cache._axis_values or {})
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
                        merged_params,
                        exec_fn,
                        prepare_run=self.cache.prepare_run,
                        chunk_hash=self.cache.chunk_hash,
                        resolve_cache_path=self.cache.resolve_cache_path,
                        load_payload=self.cache.load_payload,
                        write_chunk_payload=self.cache.write_chunk_payload,
                        update_chunk_index=self.cache.update_chunk_index,
                        build_item_maps_from_chunk_output=self.cache.build_item_maps_from_chunk_output,
                        context=cast(RunnerContext, self.cache),
                        axis_indices=axis_indices,
                        **axis_inputs,
                    )
                return run(
                    merged_params,
                    exec_fn,
                    prepare_run=self.cache.prepare_run,
                    chunk_hash=self.cache.chunk_hash,
                    resolve_cache_path=self.cache.resolve_cache_path,
                    load_payload=self.cache.load_payload,
                    write_chunk_payload=self.cache.write_chunk_payload,
                    update_chunk_index=self.cache.update_chunk_index,
                    build_item_maps_from_chunk_output=self.cache.build_item_maps_from_chunk_output,
                    extract_items_from_map=self.cache.extract_items_from_map,
                    collect_chunk_data=self.cache.collect_chunk_data,
                    context=cast(RunnerContext, self.cache),
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
                return self.cache.cache_status(
                    merged_params, axis_indices=axis_indices, **axis_inputs
                )

            setattr(wrapper, "cache_status", cache_status)
            return wrapper

        return decorator
