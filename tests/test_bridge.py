import itertools
import tempfile

from shard_memo import ChunkCache, memo_parallel_run
from shard_memo.runners import run as memo_run

from .utils import exec_fn_grid, flatten_outputs


def collate_fn(outputs):
    return outputs


def test_memo_parallel_run_caches_missing_points():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )

        params = {"alpha": 0.4}
        items = [
            {"strat": "a", "s": 1},
            {"strat": "b", "s": 4},
            {"strat": "a", "s": 2},
        ]
        cache_status = memo.cache_status(
            params, strat=axis_values["strat"], s=axis_values["s"]
        )

        output, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
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
            memo_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )

        params = {"alpha": 0.4}
        memo_run(memo, params, exec_fn_grid)

        items = [
            {"strat": "a", "s": 1},
            {"strat": "a", "s": 4},
            {"strat": "b", "s": 2},
        ]
        cache_status = memo.cache_status(
            params, strat=axis_values["strat"], s=axis_values["s"]
        )

        output, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn_grid,
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
