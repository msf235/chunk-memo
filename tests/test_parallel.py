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


def _item_dicts(split_spec):
    return [
        {"strat": strat, "s": s}
        for strat, s in itertools.product(split_spec["strat"], split_spec["s"])
    ]


def _observed_items(outputs):
    flattened = []
    for chunk in outputs:
        if isinstance(chunk, list):
            flattened.extend(chunk)
        else:
            flattened.append(chunk)
    return {(item["strat"], item["s"]) for item in flattened}


def test_memo_parallel_run_missing_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )

        items = _item_dicts(split_spec)
        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.cached_chunks == 0
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert _observed_items(outputs) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }


def test_memo_parallel_run_with_memoized_cache_status():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )
        memo.run(params, exec_fn=exec_fn, strat=["a"], s=[1, 2, 3, 4])

        items = _item_dicts(split_spec)
        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert status["cached_chunks"]
        assert status["missing_chunks"]
        assert diag.cached_chunks == len(status["cached_chunks"])
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert _observed_items(outputs) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }


def test_memo_parallel_run_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )

        items = _item_dicts(split_spec)
        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 0
        assert diag.cached_chunks == len(status["cached_chunks"])
        assert _observed_items(outputs) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }


def test_parallel_run_populates_memo_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            split_spec=split_spec,
        )

        items = _item_dicts(split_spec)
        status = memo.cache_status(params, strat=split_spec["strat"], s=split_spec["s"])
        memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        output, diag = memo.run(params, exec_fn)
        assert diag.executed_chunks == 0
        assert diag.cached_chunks == diag.total_chunks
        assert _observed_items(output) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }
