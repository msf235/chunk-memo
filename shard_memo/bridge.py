import dataclasses
import functools
import inspect
import math
import pickle
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Sequence, Tuple

from .memo import ChunkKey, ShardMemo, Diagnostics, _atomic_write_pickle


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


def _progress_map_fn(
    func: Callable[..., Any],
    items: Iterable[Any],
    **kwargs: Any,
) -> Iterable[Any]:
    max_workers = kwargs.get("max_workers")
    chunksize = kwargs.get("chunksize")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        if chunksize is None:
            yield from executor.map(func, items)
            return
        yield from executor.map(func, items, chunksize=chunksize)


def _ensure_iterable(items: Iterable[Any]) -> List[Any]:
    if isinstance(items, list):
        return items
    return list(items)


def _resolve_axis_order(
    memo: ShardMemo, axis_values: Mapping[str, Any]
) -> Tuple[str, ...]:
    if memo.axis_order is not None:
        return tuple(memo.axis_order)
    return tuple(sorted(axis_values))


def _build_item_axis_extractor(
    memo: ShardMemo,
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


def _chunk_key_size(chunk_key: ChunkKey) -> int:
    if not chunk_key:
        return 0
    return math.prod(len(values) for _, values in chunk_key)


def _format_eta(seconds: float) -> str:
    if seconds < 0 or not math.isfinite(seconds):
        return "--:--:--"
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _format_rate_eta(
    label: str,
    processed: int,
    total: int,
    start_time: float,
    *,
    rate_processed: int | None = None,
    rate_total: int | None = None,
) -> str:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    if rate_processed is None:
        rate_processed = processed
    if rate_total is None:
        rate_total = total
    rate = rate_processed / elapsed
    percent = 100.0 if total <= 0 else (processed / total * 100.0)
    remaining = 0 if rate_total <= 0 else max(rate_total - rate_processed, 0)
    eta = remaining / rate if rate > 0 else float("inf")
    return (
        f"[ShardMemo] {label} {processed}/{total} "
        f"({percent:0.1f}%) rate={rate:0.1f}/s ETA={_format_eta(eta)}"
    )


_progress_state = {"last_len": 0, "last_msg": ""}


def _print_progress(message: str, *, final: bool) -> None:
    if final:
        print(message, end="\n", flush=True)
        _progress_state["last_len"] = 0
        _progress_state["last_msg"] = ""
        return
    pad = max(_progress_state["last_len"] - len(message), 0)
    print(message + (" " * pad), end="\r", flush=True)
    _progress_state["last_len"] = len(message)
    _progress_state["last_msg"] = message


def _print_detail(message: str) -> None:
    last_len = _progress_state.get("last_len", 0)
    last_msg = _progress_state.get("last_msg", "")
    if last_len:
        print()
    print(message)
    if last_msg:
        print(last_msg, end="\r", flush=True)
        _progress_state["last_len"] = len(last_msg)


def _expand_items_to_chunks_slow(
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


def _build_axis_chunk_maps(
    memo: ShardMemo,
    axis_values: Mapping[str, Any],
    axis_order: Sequence[str],
) -> dict[str, dict[Any, int]]:
    axis_chunk_maps: dict[str, dict[Any, int]] = {}
    for axis in axis_order:
        values = axis_values.get(axis)
        if values is None:
            raise KeyError(f"Missing axis '{axis}' in axis_values")
        size = memo._resolve_axis_chunk_size(axis)
        value_to_chunk_id: dict[Any, int] = {}
        for index, value in enumerate(values):
            value_to_chunk_id[value] = index // size
        axis_chunk_maps[axis] = value_to_chunk_id
    return axis_chunk_maps


def _expand_items_to_chunks_fast(
    memo: ShardMemo,
    cache_status: Mapping[str, Any],
    items: Sequence[Any],
    chunk_keys: Sequence[ChunkKey],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
) -> List[List[Any]]:
    if not chunk_keys:
        return [[] for _ in chunk_keys]
    axis_values = cache_status.get("axis_values")
    if not isinstance(axis_values, Mapping):
        return _expand_items_to_chunks_slow(items, chunk_keys, axis_extractor)
    axis_order = _resolve_axis_order(memo, axis_values)
    axis_chunk_maps = _build_axis_chunk_maps(memo, axis_values, axis_order)
    chunk_index_map: dict[Tuple[int, ...], int] = {}
    for index, chunk_key in enumerate(chunk_keys):
        chunk_ids: List[int] = []
        for axis, values in chunk_key:
            chunk_ids.append(axis_chunk_maps[axis][values[0]])
        chunk_index_map[tuple(chunk_ids)] = index

    chunked: List[List[Any]] = [[] for _ in chunk_keys]
    for item in items:
        axis_vals = axis_extractor(item)
        if len(axis_vals) != len(axis_order):
            raise ValueError("Item axis values do not match axis order")
        chunk_ids = []
        for axis, value in zip(axis_order, axis_vals):
            if value not in axis_chunk_maps[axis]:
                raise KeyError(
                    f"Value '{value}' not found in axis_values for axis '{axis}'"
                )
            chunk_ids.append(axis_chunk_maps[axis][value])
        chunk_index = chunk_index_map.get(tuple(chunk_ids))
        if chunk_index is None:
            continue
        chunked[chunk_index].append(item)
    return chunked


def _build_item_map_for_chunk(
    memo: ShardMemo,
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


def _build_item_spec_for_chunk(
    memo: ShardMemo,
    chunk_key: ChunkKey,
    chunk_items: Sequence[Any],
    axis_extractor: Callable[[Any], Tuple[Any, ...]],
) -> dict[str, dict[str, Any]]:
    axis_names = [axis for axis, _ in chunk_key]
    item_spec: dict[str, dict[str, Any]] = {}
    for item in chunk_items:
        axis_values = axis_extractor(item)
        item_key = memo._item_hash(chunk_key, axis_values)
        item_spec[item_key] = dict(zip(axis_names, axis_values))
    return item_spec


def _format_axis_values(values: Any) -> str:
    if isinstance(values, (list, tuple)):
        if len(values) <= 4:
            inner = ", ".join(repr(value) for value in values)
            return f"[{inner}]"
        head = ", ".join(repr(value) for value in values[:2])
        tail = ", ".join(repr(value) for value in values[-2:])
        return f"[{head}, ..., {tail}]"
    return repr(values)


def _format_params(params: Mapping[str, Any]) -> List[str]:
    lines = ["[ShardMemo] params:"]
    if not params:
        lines.append("  (none)")
        return lines
    for key, value in params.items():
        lines.append(f"  {key}={value!r}")
    return lines


def _format_spec(
    axis_values: Mapping[str, Any], axis_order: Sequence[str]
) -> List[str]:
    lines = ["[ShardMemo] spec:"]
    for axis in axis_order:
        values = axis_values.get(axis)
        lines.append(f"  {axis}={_format_axis_values(values)}")
    return lines


def memo_parallel_run(
    memo: ShardMemo,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> tuple[Any, Diagnostics]:
    if map_fn is None:
        map_fn = _progress_map_fn if memo.verbose == 1 else _default_map_fn

    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    params_dict: dict[str, Any] = dict(params)
    memo.write_metadata(params_dict)

    cached_chunks: List[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: List[ChunkKey] = list(cache_status.get("missing_chunks", []))
    chunk_index = memo._load_chunk_index(params_dict)
    use_index = bool(chunk_index)

    outputs: List[Any] = []
    exec_outputs: List[Any] = []
    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))
    total_chunks = diagnostics.total_chunks
    progress_step = max(1, total_chunks // 50)

    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda chunk: chunk
    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    if memo.verbose == 1:
        axis_values = cache_status.get("axis_values")
        if isinstance(axis_values, Mapping):
            axis_order = _resolve_axis_order(memo, axis_values)
            lines = []
            lines.extend(_format_params(params_dict))
            lines.extend(_format_spec(axis_values, axis_order))
            lines.append(
                f"[ShardMemo] plan: cached={len(cached_chunks)} execute={len(missing_chunks)}"
            )
            print("\n".join(lines))

    item_list = _ensure_iterable(items)
    if not item_list:
        return [], diagnostics
    total_items = len(item_list)
    planning_start = time.monotonic()
    processed_items = 0

    def report_progress_main(processed: int, final: bool = False) -> None:
        if memo.verbose != 1:
            return
        if not final and processed % progress_step != 0 and processed != total_chunks:
            return
        message = _format_rate_eta(
            "planning",
            processed_items,
            total_items,
            planning_start,
        )
        _print_progress(message, final=final)

    axis_extractor = _build_item_axis_extractor(
        memo,
        cache_status,
        item_list,
        exec_fn,
    )

    cached_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        missing_chunks,
        axis_extractor,
    )
    missing_items: List[Any] = []
    missing_item_keys: List[ChunkKey] = []
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
        missing_item_keys.extend([chunk_key] * len(items_to_add))

    for processed, (chunk_key, chunk_items) in enumerate(
        zip(cached_chunks, cached_chunk_items), start=1
    ):
        if not chunk_items:
            report_progress_main(processed, final=processed == total_chunks)
            continue
        processed_items += len(chunk_items)
        full_chunk = len(chunk_items) == _chunk_key_size(chunk_key)
        chunk_hash = memo._chunk_hash(params_dict, chunk_key)
        path = memo._resolve_cache_path(params_dict, chunk_key, chunk_hash)
        if not path.exists():
            _register_missing(chunk_key, chunk_items)
            continue
        with open(path, "rb") as handle:
            payload = pickle.load(handle)
        if full_chunk:
            diagnostics.cached_chunks += 1
            if memo.verbose >= 2:
                _print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
            outputs.append(payload["output"])
            report_progress_main(processed, final=processed == total_chunks)
            continue
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
            if memo.verbose >= 2:
                _print_detail(
                    f"[ShardMemo] load chunk={chunk_key} items={len(item_outputs)}"
                )
            outputs.append(collate_fn(item_outputs))
        report_progress_main(processed, final=processed == total_chunks)

    base_index = len(cached_chunks)
    for offset, (chunk_key, chunk_items) in enumerate(
        zip(missing_chunks, missing_chunk_items), start=1
    ):
        if chunk_items:
            _register_missing(chunk_key, chunk_items)
        processed_items += len(chunk_items)
        report_progress_main(
            base_index + offset,
            final=(base_index + offset) == total_chunks,
        )

    if not missing_items and missing_chunks:
        missing_items = item_list

    if missing_items:
        diagnostics.executed_chunks = len(missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_iter = map_fn(
            exec_fn,
            missing_items,
            **map_fn_kwargs,
        )
        exec_outputs = []
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        exec_start = time.monotonic()
        last_report = time.monotonic()
        report_interval = 2.0
        for index, result in enumerate(exec_iter, start=1):
            exec_outputs.append(result)
            now = time.monotonic()
            if memo.verbose == 1 and (now - last_report) >= report_interval:
                processed_items = cached_items_total + index
                message = _format_rate_eta(
                    "exec_items",
                    processed_items,
                    total_items_all,
                    exec_start,
                    rate_processed=index,
                    rate_total=len(missing_items),
                )
                _print_progress(message, final=False)
                last_report = now
        if memo.verbose == 1:
            processed_items = cached_items_total + len(missing_items)
            message = _format_rate_eta(
                "exec_items",
                processed_items,
                total_items_all,
                exec_start,
                rate_processed=len(missing_items),
                rate_total=len(missing_items),
            )
            _print_progress(message, final=True)

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
            chunk_hash = memo._chunk_hash(params_dict, chunk_key)
            path = memo._resolve_cache_path(params_dict, chunk_key, chunk_hash)
            if chunk_key in missing_chunks:
                payload: dict[str, Any] = {"output": chunk_output, "items": item_map}
            else:
                payload = dict(cached_payloads.get(chunk_key, {}))
                payload["items"] = {**payload.get("items", {}), **item_map}
                if "output" not in payload and chunk_output is not None:
                    payload["output"] = chunk_output
            payload["spec"] = _build_item_spec_for_chunk(
                memo,
                chunk_key,
                chunk_items,
                axis_extractor,
            )
            _atomic_write_pickle(path, payload)
            memo._update_chunk_index(params_dict, chunk_hash, chunk_key)
            if memo.verbose >= 2:
                _print_detail(f"[ShardMemo] run chunk={chunk_key} items={chunk_size}")
            outputs.append(chunk_output)
            cursor += chunk_size
            report_progress_main(
                base_index + len(missing_chunks),
                final=(base_index + len(missing_chunks)) == total_chunks,
            )
    diagnostics.merges += 1
    if memo.merge_fn is not None:
        merged = memo.merge_fn(outputs)
    else:
        merged = outputs
    if not merged and item_list:
        merged = exec_outputs if missing_items else []
    if memo.verbose >= 2:
        _print_detail(
            "[ShardMemo] summary "
            f"cached={diagnostics.cached_chunks} "
            f"executed={diagnostics.executed_chunks} "
            f"total={diagnostics.total_chunks}"
        )
    report_progress_main(total_chunks, final=True)
    return merged, diagnostics


def memo_parallel_run_streaming(
    memo: ShardMemo,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> Diagnostics:
    profile_start = time.monotonic() if memo.profile else None
    if map_fn is None:
        map_fn = _progress_map_fn if memo.verbose == 1 else _default_map_fn

    params = cache_status.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("cache_status must include params")
    params_dict: dict[str, Any] = dict(params)
    memo.write_metadata(params_dict)
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(f"[ShardMemo] profile metadata_s={time.monotonic() - profile_start:0.3f}")

    cached_chunks: List[ChunkKey] = list(cache_status.get("cached_chunks", []))
    missing_chunks: List[ChunkKey] = list(cache_status.get("missing_chunks", []))
    chunk_index = memo._load_chunk_index(params_dict)
    use_index = bool(chunk_index)

    diagnostics = Diagnostics(total_chunks=len(cached_chunks) + len(missing_chunks))
    total_chunks = diagnostics.total_chunks
    progress_step = max(1, total_chunks // 50)
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile cache_status_s={time.monotonic() - profile_start:0.3f}"
        )

    def report_progress_streaming(processed: int, final: bool = False) -> None:
        if memo.verbose != 1:
            return
        if not final and processed % progress_step != 0 and processed != total_chunks:
            return
        message = _format_rate_eta(
            "planning",
            processed_items,
            total_items,
            planning_start,
        )
        _print_progress(message, final=final)

    if collate_fn is None:
        collate_fn = memo.merge_fn if memo.merge_fn is not None else lambda chunk: chunk
    if map_fn_kwargs is None:
        map_fn_kwargs = {}

    # reuse profile_start for subsequent timings

    if memo.verbose == 1:
        axis_values = cache_status.get("axis_values")
        if isinstance(axis_values, Mapping):
            axis_order = _resolve_axis_order(memo, axis_values)
            lines = []
            lines.extend(_format_params(params_dict))
            lines.extend(_format_spec(axis_values, axis_order))
            lines.append(
                f"[ShardMemo] plan: cached={len(cached_chunks)} execute={len(missing_chunks)}"
            )
            print("\n".join(lines))

    item_list = _ensure_iterable(items)
    if not item_list:
        return diagnostics
    total_items = len(item_list)
    planning_start = time.monotonic()
    processed_items = 0
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile items_list_s={time.monotonic() - profile_start:0.3f}"
        )
    axis_extractor = _build_item_axis_extractor(
        memo,
        cache_status,
        item_list,
        exec_fn,
    )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile axis_extractor_s={time.monotonic() - profile_start:0.3f}"
        )

    cached_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        cached_chunks,
        axis_extractor,
    )
    missing_chunk_items = _expand_items_to_chunks_fast(
        memo,
        cache_status,
        item_list,
        missing_chunks,
        axis_extractor,
    )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile expand_chunks_s={time.monotonic() - profile_start:0.3f}"
        )

    missing_items: List[Any] = []
    missing_item_keys: List[ChunkKey] = []
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
        missing_item_keys.extend([chunk_key] * len(items_to_add))

    for processed, (chunk_key, chunk_items) in enumerate(
        zip(cached_chunks, cached_chunk_items), start=1
    ):
        if not chunk_items:
            report_progress_streaming(processed, final=processed == total_chunks)
            continue
        processed_items += len(chunk_items)
        full_chunk = len(chunk_items) == _chunk_key_size(chunk_key)
        chunk_hash = memo._chunk_hash(params_dict, chunk_key)
        if use_index:
            exists = chunk_hash in chunk_index
        else:
            path = memo._resolve_cache_path(params_dict, chunk_key, chunk_hash)
            exists = path.exists()
        if not exists:
            _register_missing(chunk_key, chunk_items)
            continue
        if full_chunk:
            diagnostics.cached_chunks += 1
            if memo.verbose >= 2:
                _print_detail(f"[ShardMemo] load chunk={chunk_key} items=all")
            report_progress_streaming(processed, final=processed == total_chunks)
            continue
        path = memo._resolve_cache_path(params_dict, chunk_key, chunk_hash)
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
            if memo.verbose >= 2:
                _print_detail(
                    f"[ShardMemo] load chunk={chunk_key} items={len(chunk_items)}"
                )
        report_progress_streaming(processed, final=processed == total_chunks)
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile cached_scan_s={time.monotonic() - profile_start:0.3f}"
        )

    base_index = len(cached_chunks)
    for offset, (chunk_key, chunk_items) in enumerate(
        zip(missing_chunks, missing_chunk_items), start=1
    ):
        if chunk_items:
            _register_missing(chunk_key, chunk_items)
        processed_items += len(chunk_items)
        report_progress_streaming(
            base_index + offset,
            final=(base_index + offset) == total_chunks,
        )
    if memo.profile and memo.verbose >= 1 and profile_start is not None:
        print(
            f"[ShardMemo] profile missing_map_s={time.monotonic() - profile_start:0.3f}"
        )

    if not missing_items and missing_chunks:
        missing_items = item_list

    if missing_items:
        diagnostics.executed_chunks = len(missing_chunk_order)
        exec_fn = functools.partial(_exec_with_item, exec_fn, params_dict)
        exec_iter = map_fn(
            exec_fn,
            missing_items,
            **map_fn_kwargs,
        )
        total_items_all = len(item_list)
        cached_items_total = total_items_all - len(missing_items)
        exec_start = time.monotonic()
        expected_counts = {
            chunk_key: len(items) for chunk_key, items in missing_items_by_chunk.items()
        }
        buffers: dict[ChunkKey, dict[str, List[Any]]] = {}
        current_buffer_items = 0
        last_report = time.monotonic()
        report_interval = 2.0
        for index, result in enumerate(exec_iter, start=1):
            now = time.monotonic()
            if memo.verbose == 1 and (now - last_report) >= report_interval:
                cached_total = diagnostics.cached_chunks + diagnostics.stream_flushes
                processed_items_exec = cached_items_total + index
                message = _format_rate_eta(
                    "exec_items",
                    processed_items_exec,
                    total_items_all,
                    exec_start,
                    rate_processed=index,
                    rate_total=len(missing_items),
                )
                _print_progress(message, final=False)
                last_report = now

            chunk_key = missing_item_keys[index - 1]
            buffer = buffers.setdefault(chunk_key, {"items": [], "outputs": []})
            buffer["items"].append(missing_items[index - 1])
            buffer["outputs"].append(result)
            current_buffer_items += 1
            diagnostics.max_parallel_items = max(
                diagnostics.max_parallel_items, current_buffer_items
            )
            if len(buffer["outputs"]) < expected_counts[chunk_key]:
                continue

            chunk_outputs = buffer["outputs"]
            chunk_items = buffer["items"]
            chunk_output = collate_fn(chunk_outputs)
            item_map = _build_item_map_for_chunk(
                memo,
                chunk_key,
                chunk_items,
                axis_extractor,
                chunk_outputs,
            )
            chunk_hash = memo._chunk_hash(params_dict, chunk_key)
            path = memo._resolve_cache_path(params_dict, chunk_key, chunk_hash)
            if chunk_key in missing_chunks:
                payload: dict[str, Any] = {"output": chunk_output, "items": item_map}
            else:
                payload = dict(cached_payloads.get(chunk_key, {}))
                payload["items"] = {**payload.get("items", {}), **item_map}
                if "output" not in payload and chunk_output is not None:
                    payload["output"] = chunk_output
            payload["spec"] = _build_item_spec_for_chunk(
                memo,
                chunk_key,
                chunk_items,
                axis_extractor,
            )
            _atomic_write_pickle(path, payload)
            memo._update_chunk_index(params_dict, chunk_hash, chunk_key)
            diagnostics.stream_flushes += 1
            if memo.verbose >= 2:
                _print_detail(
                    f"[ShardMemo] run chunk={chunk_key} items={len(chunk_items)}"
                )
            current_buffer_items -= len(chunk_items)
            buffers.pop(chunk_key, None)

        if memo.verbose == 1:
            cached_total = diagnostics.cached_chunks + diagnostics.stream_flushes
            processed_items_exec = cached_items_total + len(missing_items)
            message = _format_rate_eta(
                "exec_items",
                processed_items_exec,
                total_items_all,
                exec_start,
                rate_processed=len(missing_items),
                rate_total=len(missing_items),
            )
            _print_progress(message, final=True)

    if memo.verbose >= 1:
        _print_detail(
            f"[ShardMemo] stream_mem_max items={diagnostics.max_parallel_items}"
        )
    if memo.verbose >= 2:
        _print_detail(
            "[ShardMemo] summary "
            f"cached={diagnostics.cached_chunks} "
            f"executed={diagnostics.executed_chunks} "
            f"total={diagnostics.total_chunks}"
        )
    report_progress_streaming(total_chunks, final=True)

    return diagnostics
