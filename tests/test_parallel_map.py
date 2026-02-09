import tempfile

import functools

from shard_memo import ChunkCache, memo_parallel_run
from shard_memo.runners import run as _memo_run

from .utils import exec_fn_grid, flatten_outputs, item_from_index


def _parallel_kwargs(memo):
    return {
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
    memo.write_metadata()


def memo_run(memo, params, exec_fn, **kwargs):
    _set_params(memo, params)
    return _memo_run(memo, exec_fn, **kwargs)


def test_memo_parallel_run_returns_requested_points():
    params = {"alpha": 0.4}
    axis_values = {"strat": ["a", "b"], "s": [1, 2, 3]}
    items = [(0, 0), (1, 2), (0, 1), (1, 0)]

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)
        memo_run(memo, params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3])

        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            [item_from_index(item, axis_values) for item in items],
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

    assert diag.executed_chunks == len(status["missing_chunks"])
    expected = {(axis_values["strat"][i], axis_values["s"][j]) for i, j in items}
    observed = {(item["strat"], item["s"]) for item in flatten_outputs(outputs)}
    assert observed == expected
