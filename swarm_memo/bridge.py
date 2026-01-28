import dataclasses
import functools
import inspect
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Sequence, Tuple

from .memo import ChunkKey, ChunkMemo, Diagnostics, _atomic_write_pickle


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
    exec_fn: Callable[..., Any],
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

    signature = inspect.signature(exec_fn)
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


def _build_item_map_for_chunk(
    memo: ChunkMemo,
    chunk_key: ChunkKey,
    chunk_items: Sequence[Any],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
    outputs: Sequence[Any],
) -> dict[str, Any]:
    item_map: dict[str, Any] = {}
    for item, output in zip(chunk_items, outputs):
        axis_values = axis_extractor(item)
        item_key = memo._item_hash(chunk_key, axis_values)
        item_map[item_key] = output
    return item_map


def memo_parallel_run(
    memo: ChunkMemo,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
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

    item_list = _ensure_iterable(items)
    if not item_list:
        return [], diagnostics
    axis_extractor = _build_item_axis_extractor(
        memo,
        cache_status,
        item_list,
        exec_fn,
    )

    cached_chunk_items = _expand_items_to_chunks(
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks(
        item_list,
        missing_chunks,
        axis_extractor,
    )

    missing_items: List[Any] = []
    missing_items_by_chunk: dict[ChunkKey, List[Any]] = {}
    missing_chunk_order: List[ChunkKey] = []
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}

    def _register_missing(chunk_key: ChunkKey, items_to_add: List[Any]) -> None:
        if not items_to_add:
            return
        if chunk_key not in missing_items_by_chunk:
            missing_items_by_chunk[chunk_key] = []
            missing_chunk_order.append(chunk_key)
        missing_items_by_chunk[chunk_key].extend(items_to_add)
        missing_items.extend(items_to_add)

    for chunk_key, chunk_items in zip(cached_chunks, cached_chunk_items):
        if not chunk_items:
            continue
        chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
        path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
        if not path.exists():
            _register_missing(chunk_key, chunk_items)
            continue
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        item_map = payload.get("items")
        if item_map is None:
            item_map = memo._build_item_map(chunk_key, payload.get("output"))
        if item_map is None:
            cached_payloads[chunk_key] = payload
            _register_missing(chunk_key, chunk_items)
            continue
        item_outputs: List[Any] = []
        for item in chunk_items:
            axis_values = axis_extractor(item)
            item_key = memo._item_hash(chunk_key, axis_values)
            if item_key not in item_map:
                cached_payloads[chunk_key] = payload
                _register_missing(chunk_key, chunk_items)
                item_outputs = []
                break
            item_outputs.append(item_map[item_key])
        if item_outputs:
            diagnostics.cached_chunks += 1
            outputs.append(collate_fn(item_outputs))

    for chunk_key, chunk_items in zip(missing_chunks, missing_chunk_items):
        if chunk_items:
            _register_missing(chunk_key, chunk_items)

    if not missing_items and missing_chunks:
        missing_items = item_list

    if missing_items:
        diagnostics.executed_chunks = len(missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_outputs = list(
            map_fn(
                exec_fn,
                missing_items,
                **map_fn_kwargs,
            )
        )

        cursor = 0
        for chunk_key in missing_chunk_order:
            chunk_items = missing_items_by_chunk.get(chunk_key, [])
            chunk_size = len(chunk_items)
            if chunk_size == 0:
                continue
            chunk_outputs = exec_outputs[cursor : cursor + chunk_size]
            chunk_output = collate_fn(chunk_outputs)
            item_map = _build_item_map_for_chunk(
                memo,
                chunk_key,
                chunk_items,
                axis_extractor,
                chunk_outputs,
            )
            chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
            path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
            if chunk_key in missing_chunks:
                payload: dict[str, Any] = {"output": chunk_output, "items": item_map}
            else:
                payload = dict(cached_payloads.get(chunk_key, {}))
                payload["items"] = {**payload.get("items", {}), **item_map}
                if "output" not in payload and chunk_output is not None:
                    payload["output"] = chunk_output
            _atomic_write_pickle(path, payload)
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


def memo_parallel_run_streaming(
    memo: ChunkMemo,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> Diagnostics:
    if map_fn is None:
        map_fn = _default_map_fn

    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    params_dict: dict[str, Any] = dict(params)

    cached_chunks: List[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: List[ChunkKey] = list(cache_status.get("missing_chunks", []))

    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))
    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda chunk: chunk
    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    item_list = _ensure_iterable(items)
    if not item_list:
        return diagnostics
    axis_extractor = _build_item_axis_extractor(
        memo,
        cache_status,
        item_list,
        exec_fn,
    )

    cached_chunk_items = _expand_items_to_chunks(
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks(
        item_list,
        missing_chunks,
        axis_extractor,
    )

    missing_items: List[Any] = []
    missing_items_by_chunk: dict[ChunkKey, List[Any]] = {}
    missing_chunk_order: List[ChunkKey] = []
    cached_payloads: dict[ChunkKey, Mapping[str, Any]] = {}

    def _register_missing(chunk_key: ChunkKey, items_to_add: List[Any]) -> None:
        if not items_to_add:
            return
        if chunk_key not in missing_items_by_chunk:
            missing_items_by_chunk[chunk_key] = []
            missing_chunk_order.append(chunk_key)
        missing_items_by_chunk[chunk_key].extend(items_to_add)
        missing_items.extend(items_to_add)

    for chunk_key, chunk_items in zip(cached_chunks, cached_chunk_items):
        if not chunk_items:
            continue
        chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
        path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
        if not path.exists():
            _register_missing(chunk_key, chunk_items)
            continue
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        item_map = payload.get("items")
        if item_map is None:
            item_map = memo._build_item_map(chunk_key, payload.get("output"))
        if item_map is None:
            cached_payloads[chunk_key] = payload
            _register_missing(chunk_key, chunk_items)
            continue
        missing = False
        for item in chunk_items:
            axis_values = axis_extractor(item)
            item_key = memo._item_hash(chunk_key, axis_values)
            if item_key not in item_map:
                cached_payloads[chunk_key] = payload
                _register_missing(chunk_key, chunk_items)
                missing = True
                break
        if not missing:
            diagnostics.cached_chunks += 1

    for chunk_key, chunk_items in zip(missing_chunks, missing_chunk_items):
        if chunk_items:
            _register_missing(chunk_key, chunk_items)

    if not missing_items and missing_chunks:
        missing_items = item_list

    if missing_items:
        diagnostics.executed_chunks = len(missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_outputs = list(
            map_fn(
                exec_fn,
                missing_items,
                **map_fn_kwargs,
            )
        )

        cursor = 0
        for chunk_key in missing_chunk_order:
            chunk_items = missing_items_by_chunk.get(chunk_key, [])
            chunk_size = len(chunk_items)
            if chunk_size == 0:
                continue
            chunk_outputs = exec_outputs[cursor : cursor + chunk_size]
            chunk_output = collate_fn(chunk_outputs)
            item_map = _build_item_map_for_chunk(
                memo,
                chunk_key,
                chunk_items,
                axis_extractor,
                chunk_outputs,
            )
            chunk_hash = memo.chunk_hash_fn(params_dict, chunk_key, memo.cache_version)
            path = Path(memo.cache_root) / f"{chunk_hash}.pkl"
            if chunk_key in missing_chunks:
                payload: dict[str, Any] = {"output": chunk_output, "items": item_map}
            else:
                payload = dict(cached_payloads.get(chunk_key, {}))
                payload["items"] = {**payload.get("items", {}), **item_map}
                if "output" not in payload and chunk_output is not None:
                    payload["output"] = chunk_output
            _atomic_write_pickle(path, payload)
            cursor += chunk_size

    return diagnostics
