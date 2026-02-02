import tempfile

from shard_memo import ChunkCache, memo_parallel_run
from shard_memo.runners import run as memo_run

from .utils import exec_fn_grid, flatten_outputs, item_from_index


def test_memo_parallel_run_returns_requested_points():
    params = {"alpha": 0.4}
    axis_values = {"strat": ["a", "b"], "s": [1, 2, 3]}
    items = [(0, 0), (1, 2), (0, 1), (1, 0)]

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkCache(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        memo_run(memo, params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3])

        status = memo.cache_status(
            params, strat=axis_values["strat"], s=axis_values["s"]
        )
        outputs, diag = memo_parallel_run(
            memo,
            [item_from_index(item, axis_values) for item in items],
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

    assert diag.executed_chunks == len(status["missing_chunks"])
    expected = {(axis_values["strat"][i], axis_values["s"][j]) for i, j in items}
    observed = {(item["strat"], item["s"]) for item in flatten_outputs(outputs)}
    assert observed == expected
