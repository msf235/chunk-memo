import itertools
import tempfile

from swarm_memo import ChunkMemo, memo_parallel_run


def exec_fn(params, strat, s):
    if isinstance(strat, (list, tuple)) and isinstance(s, (list, tuple)):
        outputs = []
        for strat_value in strat:
            for s_value in s:
                outputs.append(
                    {"alpha": params["alpha"], "strat": strat_value, "s": s_value}
                )
        return outputs
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def collate_fn(outputs):
    return outputs


def _flatten_outputs(outputs):
    flattened = []
    for chunk in outputs:
        if isinstance(chunk, list):
            flattened.extend(chunk)
        else:
            flattened.append(chunk)
    return flattened


def test_memo_parallel_run_caches_missing_points():
    with tempfile.TemporaryDirectory() as temp_dir:
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            split_spec=split_spec,
        )

        params = {"alpha": 0.4}
        items = [
            {"strat": "a", "s": 1},
            {"strat": "b", "s": 4},
            {"strat": "a", "s": 2},
        ]
        cache_status = memo.cache_status(
            params, strat=split_spec["strat"], s=split_spec["s"]
        )

        output, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=cache_status,
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 2
        assert output


def test_memo_parallel_run_reuses_partial_chunks():
    with tempfile.TemporaryDirectory() as temp_dir:
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            merge_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            split_spec=split_spec,
        )

        params = {"alpha": 0.4}
        memo.run(params, exec_fn)

        items = [
            {"strat": "a", "s": 1},
            {"strat": "a", "s": 4},
            {"strat": "b", "s": 2},
        ]
        cache_status = memo.cache_status(
            params, strat=split_spec["strat"], s=split_spec["s"]
        )

        output, diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=cache_status,
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 0
        assert diag.cached_chunks == 3
        assert len(output) == 3
        observed = {(item["strat"], item["s"]) for item in _flatten_outputs(output)}
        assert observed == {("a", 1), ("a", 4), ("b", 2)}
