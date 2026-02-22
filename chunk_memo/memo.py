"""Memo wrapper helpers and re-exports."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Mapping, Tuple, cast

from .cache import ChunkCache
from .runners import Diagnostics, run, run_streaming


class ChunkMemo:
    def __init__(self, cache: ChunkCache) -> None:
        self.cache = cache

    def run_wrap(
        self, *, params_arg: str = "params"
    ) -> Callable[[Callable[..., Any]], Callable[..., Tuple[Any, Diagnostics]]]:
        """Decorator for running memoized execution with output.

        Infers memoization params from non-axis arguments unless an explicit
        params dict is provided. Supports axis selection by value or by index
        via axis_indices. Uses runners.run under the hood.
        """
        return self._build_wrapper(params_arg=params_arg, streaming=False)

    def streaming_wrap(
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
        axis_names = set(self.cache._axis_values or {})
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
                base_params: dict[str, Any] | None = params if params_provided else {}
                merged_params = self._merge_params(
                    base_params,
                    exec_extras,
                    params_provided=params_provided,
                )

                exec_fn = functools.partial(func, **exec_extras)
                sliced = self.cache.slice(
                    merged_params,
                    axis_indices=axis_indices,
                    **axis_inputs,
                )
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
                base_params = params if params is not None else self.cache.params
                merged_params = self._merge_params(
                    base_params,
                    exec_extras,
                    params_provided=params is not None,
                )
                self.cache.params = merged_params
                return self.cache.cache_status(axis_indices=axis_indices, **axis_inputs)

            setattr(wrapper, "cache_status", cache_status)
            return wrapper

        return decorator
