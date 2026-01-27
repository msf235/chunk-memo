import itertools
import tempfile

from swarm_memo import ChunkMemo, memoized_map


def exec_fn(params, strat, s):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        outputs.append({"alpha": params["alpha"], "strat": strat_value, "s": s_value})
    return outputs


def index_fn(item):
    return {"strat": item[0], "s": item[1]}


def item_to_axes(item, split_spec):
    return {
        "strat": [split_spec["strat"][item[0]]],
        "s": [split_spec["s"][item[1]]],
    }


def test_memoized_map_preserves_order():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3]}
    items = [(0, 0), (1, 2), (0, 1), (1, 0)]

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn,
            split_spec=split_spec,
        )
        memo.run(params, {"strat": ["a"], "s": [1, 2, 3]})
        memo._set_split_spec(split_spec)
        memoized_exec = memo.run_wrap()(exec_fn)

        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 2), "s": slice(0, 3)}
        )
        outputs = memoized_map(
            status,
            memoized_exec,
            items,
            params=params,
            index_fn=index_fn,
            item_to_axes=lambda item: item_to_axes(item, split_spec),
            use_processes=False,
        )

    expected = [
        exec_fn(params, strat=[split_spec["strat"][i]], s=[split_spec["s"][j]])
        for i, j in items
    ]
    assert outputs == expected
