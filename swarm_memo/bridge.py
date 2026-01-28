import dataclasses
import functools
import inspect
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Sequence, Tuple

from .memo import ChunkKey, ChunkMemo, Diagnostics


@dataclasses.dataclass
class BridgeDiagnostics:
    total_chunks: int = 0
    cached_chunks: int = 0
    executed_chunks: int = 0
    executed_points: int = 0


def _exec_with_item(
    exec_fn: Callable[..., Any],
    params: dict[str, Any],
    item: Any,
) -> Any:
    if isinstance(item, Mapping):
        return exec_fn(params, **item)
    return exec_fn(params, item)


def _default_map_fn(
    func: Callable[..., Any],
    items: Iterable[Any],
    **kwargs: Any,
) -> List[Any]:
    max_workers = kwargs.get("max_workers")
    chunksize = kwargs.get("chunksize")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if chunksize is None:
            return list(executor.map(func, items))
        return list(executor.map(func, items, chunksize=chunksize))


def _ensure_iterable(items: Iterable[Any]) -> List[Any]:
    if isinstance(items, list):
        return items
    return list(items)


def _resolve_axis_order(
    memo: ChunkMemo, axis_values: Mapping[str, Any]
) -> Tuple[str, ...]:
    if memo.axis_order is not None:
        return tuple(memo.axis_order)
    return tuple(sorted(axis_values))


def _build_item_axis_extractor(
    memo: ChunkMemo,
    cache_status: Mapping[str, Any],
    items: Sequence[Any],
) -> Callable[[Any], Tuple[Any, ...]]:
    axis_values = cache_status.get("axis_values")
    if not isinstance(axis_values, Mapping):
        raise ValueError("cache_status must include axis_values for memo axes")
    axis_order = _resolve_axis_order(memo, axis_values)

    if items:
        sample = items[0]
        if isinstance(sample, Mapping):
            axis_names = list(axis_order)

            def extract(item: Any) -> Tuple[Any, ...]:
                if not isinstance(item, Mapping):
                    raise ValueError("Mixed item types: expected mapping items")
                return tuple(item[name] for name in axis_names)

            return extract
        if isinstance(sample, (tuple, list)):
            if len(sample) == len(axis_order):
                axis_count = len(axis_order)

                def extract(item: Any) -> Tuple[Any, ...]:
                    if isinstance(item, Mapping):
                        raise ValueError("Mixed item types: expected positional items")
                    if isinstance(item, tuple):
                        values = item
                    elif isinstance(item, list):
                        values = tuple(item)
                    else:
                        values = (item,)
                    if len(values) != axis_count:
                        raise ValueError(
                            "Item does not match axis order length for memo axes"
                        )
                    return tuple(values)

                return extract
        if len(axis_order) == 1:

            def extract(item: Any) -> Tuple[Any, ...]:
                if isinstance(item, Mapping):
                    raise ValueError("Mixed item types: expected positional items")
                return (item,)

            return extract

    signature = inspect.signature(memo.exec_fn)
    param_names = list(signature.parameters)
    if not param_names or param_names[0] != "params":
        raise ValueError("exec_fn must accept 'params' as the first argument")
    axis_names = [name for name in param_names[1:] if name in axis_order]
    if not axis_names:
        raise ValueError(
            "Positional items require exec_fn to list axis arguments in its signature"
        )
    axis_positions = {name: index for index, name in enumerate(axis_names)}

    def extract(item: Any) -> Tuple[Any, ...]:
        if isinstance(item, Mapping):
            raise ValueError("Mixed item types: expected positional items")
        if isinstance(item, tuple):
            values = item
        elif isinstance(item, list):
            values = tuple(item)
        else:
            values = (item,)
        if len(values) < len(axis_names):
            raise ValueError("Item does not provide enough positional values")
        values_by_axis = {
            axis_name: values[index] for axis_name, index in axis_positions.items()
        }
        missing = [name for name in axis_order if name not in values_by_axis]
        if missing:
            raise ValueError(
                f"Item does not provide positional values for axes: {missing}"
            )
        return tuple(values_by_axis[name] for name in axis_order)

    return extract


def _chunk_key_matches_axes(axis_values: Tuple[Any, ...], chunk_key: ChunkKey) -> bool:
    for (_, values), value in zip(chunk_key, axis_values):
        if value not in values:
            return False
    return True


def _expand_items_to_chunks(
    items: Sequence[Any],
    chunk_keys: Sequence[ChunkKey],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
) -> List[List[Any]]:
    chunked: List[List[Any]] = [[] for _ in chunk_keys]
    for item in items:
        axis_values = axis_extractor(item)
        for index, chunk_key in enumerate(chunk_keys):
            if _chunk_key_matches_axes(axis_values, chunk_key):
                chunked[index].append(item)
                break
    return chunked


def memo_parallel_run(
    memo: ChunkMemo,
    items: Iterable[Any],
    *,
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> tuple[Any, Diagnostics]:
    if map_fn is None:
        map_fn = _default_map_fn

    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    params_dict: dict[str, Any] = dict(params)

    cached_chunks: List[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: List[ChunkKey] = list(cache_status.get("missing_chunks", []))

    outputs: List[Any] = []
    exec_outputs: List[Any] = []
    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))
    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda chunk: chunk
    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    for chunk_key in cached_chunks:
        chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
        path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
        if not path.exists():
            continue
        diagnostics.cached_chunks += 1
        with open(path, "rb") as handle:
            outputs.append(pickle.load(handle)["output"])

    item_list = _ensure_iterable(items)
    if not item_list:
        return [], diagnostics
    axis_extractor = _build_item_axis_extractor(memo, cache_status, item_list)
    missing_items = [
        item
        for item in item_list
        if any(
            _chunk_key_matches_axes(axis_extractor(item), key) for key in missing_chunks
        )
    ]
    if not missing_items and missing_chunks:
        missing_items = item_list

    if missing_items:
        diagnostics.executed_chunks = len(missing_chunks)
        exec_fn = functools.partial(_exec_with_item, memo.exec_fn, params_dict)
        exec_outputs = list(
            map_fn(
                exec_fn,
                missing_items,
                **map_fn_kwargs,
            )
        )

        chunked_items = _expand_items_to_chunks(
            missing_items,
            missing_chunks,
            axis_extractor,
        )
        cursor = 0
        for chunk_key, chunk_items in zip(missing_chunks, chunked_items):
            chunk_size = len(chunk_items)
            chunk_output = collate_fn(exec_outputs[cursor : cursor + chunk_size])
            chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
            path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
            with open(path, "wb") as handle:
                pickle.dump({"output": chunk_output}, handle, protocol=5)
            outputs.append(chunk_output)
            cursor += chunk_size

    diagnostics.merges += 1
    if memo.merge_fn is not None:
        merged = memo.merge_fn(outputs)
    else:
        merged = outputs
    if not merged and item_list:
        merged = exec_outputs if missing_items else []
    return merged, diagnostics
