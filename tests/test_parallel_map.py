import itertools
import tempfile

from swarm_memo import ChunkMemo, memo_parallel_run


def exec_fn(params, strat, s):
    if isinstance(strat, (list, tuple)) and isinstance(s, (list, tuple)):
        outputs = []
        for strat_value, s_value in itertools.product(strat, s):
            outputs.append(
                {"alpha": params["alpha"], "strat": strat_value, "s": s_value}
            )
        return outputs
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def _item_from_index(item, split_spec):
    return {
        "strat": split_spec["strat"][item[0]],
        "s": split_spec["s"][item[1]],
    }


def test_memo_parallel_run_returns_requested_points():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3]}
    items = [(0, 0), (1, 2), (0, 1), (1, 0)]

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )
        memo.run(params, exec_fn=exec_fn, strat=["a"], s=[1, 2, 3])

        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        outputs, diag = memo_parallel_run(
            memo,
            [_item_from_index(item, split_spec) for item in items],
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

    assert diag.executed_chunks == len(status["missing_chunks"])
    expected = {(split_spec["strat"][i], split_spec["s"][j]) for i, j in items}
    flattened = []
    for chunk in outputs:
        if isinstance(chunk, list):
            flattened.extend(chunk)
        else:
            flattened.append(chunk)
    observed = {(item["strat"], item["s"]) for item in flattened}
    assert observed == expected
