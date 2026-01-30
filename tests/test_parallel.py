import tempfile
from concurrent.futures import ProcessPoolExecutor

from shard_memo import ShardMemo, memo_parallel_run, memo_parallel_run_streaming

from .utils import exec_fn_grid, item_dicts, observed_items


def test_memo_parallel_run_missing_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )

        items = item_dicts(axis_values)
        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.cached_chunks == 0
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert observed_items(outputs) == {
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
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        memo.run(params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3, 4])

        items = item_dicts(axis_values)
        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert status["cached_chunks"]
        assert status["missing_chunks"]
        assert diag.cached_chunks == len(status["cached_chunks"])
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert observed_items(outputs) == {
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
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )

        items = item_dicts(axis_values)
        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 0
        assert diag.cached_chunks == len(status["cached_chunks"])
        assert observed_items(outputs) == {
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
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )

        items = item_dicts(axis_values)
        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        output, diag = memo.run(params, exec_fn_grid)
        assert diag.executed_chunks == 0
        assert diag.cached_chunks == diag.total_chunks
        assert observed_items(output) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }


def test_parallel_run_streaming_populates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )

        items = item_dicts(axis_values)
        status = memo.cache_status(params, strat=axis_values["strat"], s=axis_values["s"])
        with ProcessPoolExecutor(max_workers=2) as executor:
            diag = memo_parallel_run_streaming(
                memo,
                items,
                exec_fn=exec_fn_grid,
                cache_status=status,
                map_fn=executor.map,
                map_fn_kwargs={"chunksize": 1},
            )

        assert diag.executed_chunks == len(status["missing_chunks"])
        output, diag2 = memo.run(params, exec_fn_grid)
        assert diag2.executed_chunks == 0
        assert diag2.cached_chunks == diag2.total_chunks
        assert observed_items(output) == {
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("b", 1),
            ("b", 2),
            ("b", 3),
            ("b", 4),
        }
