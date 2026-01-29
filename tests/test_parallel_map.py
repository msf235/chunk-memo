import tempfile

from shard_memo import ShardMemo, memo_parallel_run

from .utils import exec_fn_grid, flatten_outputs, item_from_index


def test_memo_parallel_run_returns_requested_points():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3]}
    items = [(0, 0), (1, 2), (0, 1), (1, 0)]

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )
        memo.run(params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3])

        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        outputs, diag = memo_parallel_run(
            memo,
            [item_from_index(item, split_spec) for item in items],
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

    assert diag.executed_chunks == len(status["missing_chunks"])
    expected = {(split_spec["strat"][i], split_spec["s"][j]) for i, j in items}
    observed = {(item["strat"], item["s"]) for item in flatten_outputs(outputs)}
    assert observed == expected
