import itertools
import functools
import tempfile

from shard_memo import ChunkCache, memo_parallel_run
from shard_memo.runners import run as _memo_run

from .utils import exec_fn_grid, flatten_outputs


def collate_fn(outputs):
    return outputs


def _run_kwargs(memo):
    return {
        "prepare_run": memo.prepare_run,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "extract_items_from_map": memo.extract_items_from_map,
        "collect_chunk_data": memo.collect_chunk_data,
        "context": memo,
    }


def _parallel_kwargs(memo):
    return {
        "cache_status_fn": memo.cache_status,
        "write_metadata": memo.write_metadata,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_axis_values": memo.build_item_maps_from_axis_values,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "reconstruct_output_from_items": memo.reconstruct_output_from_items,
        "collect_chunk_data": memo.collect_chunk_data,
        "item_hash": memo.item_hash,
        "context": memo,
    }


def _set_params(memo, params):
    memo.set_params(params)


def memo_run(memo, params, exec_fn, **kwargs):
    _set_params(memo, params)
    exec_fn_bound = functools.partial(exec_fn, params)
    return _memo_run(exec_fn_bound, **_run_kwargs(memo), **kwargs)


def test_memo_parallel_run_caches_missing_points():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )
        params = {"alpha": 0.4}
        _set_params(memo, params)
        items = [
            {"strat": "a", "s": 1},
            {"strat": "b", "s": 4},
            {"strat": "a", "s": 2},
        ]
        cache_status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])

        output, diag = memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
            cache_status=cache_status,
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 2
        assert output


def test_memo_parallel_run_reuses_partial_chunks():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )
        params = {"alpha": 0.4}
        _set_params(memo, params)
        memo_run(memo, params, exec_fn_grid)

        items = [
            {"strat": "a", "s": 1},
            {"strat": "a", "s": 4},
            {"strat": "b", "s": 2},
        ]
        cache_status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])

        output, diag = memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
            cache_status=cache_status,
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 0
        assert diag.cached_chunks == 3
        assert len(output) == 3
        observed = {(item["strat"], item["s"]) for item in flatten_outputs(output)}
        assert observed == {("a", 1), ("a", 4), ("b", 2)}
