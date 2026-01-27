import itertools
import tempfile

from swarm_memo import ChunkMemo, memo_parallel_run


def exec_fn(params, point):
    strat, s = point
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def collate_fn(outputs):
    return outputs


def test_memo_parallel_run_caches_missing_points():
    with tempfile.TemporaryDirectory() as temp_dir:
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn,
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            split_spec=split_spec,
        )

        params = {"alpha": 0.4}
        points = [("a", 1), ("b", 4), ("a", 2)]
        cache_status = memo.cache_status(
            params, strat=split_spec["strat"], s=split_spec["s"]
        )

        output, diag = memo_parallel_run(
            memo,
            params,
            points,
            cache_status=cache_status,
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(point) for point in items],
        )

        assert diag.executed_chunks == len(cache_status["missing_chunks"])
        assert output
