import itertools
import functools
import tempfile

from chunk_memo import ChunkCache, run_parallel
from chunk_memo.runners import run as _memo_run

from .utils import exec_fn_grid, flatten_outputs


def collate_fn(outputs):
    return outputs


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


def slice_and_run(memo, params, exec_fn, **kwargs):
    axis_indices = kwargs.pop("axis_indices", None)
    sliced = memo.slice(params, axis_indices=axis_indices, **kwargs)
    return _memo_run(sliced, exec_fn)


def test_run_parallel_caches_missing_points():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            collate_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )
        params = {"alpha": 0.4}
        _set_params(memo, params)
        items = [
            {"strat": "a", "s": 1},
            {"strat": "b", "s": 4},
            {"strat": "a", "s": 2},
        ]
        output, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 2
        assert output


def test_run_parallel_reuses_partial_chunks():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            collate_fn=lambda chunks: list(itertools.chain.from_iterable(chunks)),
            axis_values=axis_values,
        )
        params = {"alpha": 0.4}
        _set_params(memo, params)
        slice_and_run(memo, params, exec_fn_grid)

        items = [
            {"strat": "a", "s": 1},
            {"strat": "a", "s": 4},
            {"strat": "b", "s": 2},
        ]
        output, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            collate_fn=collate_fn,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.executed_chunks == 0
        assert diag.cached_chunks == 3
        assert len(output) == 3
        observed = {(item["strat"], item["s"]) for item in flatten_outputs(output)}
        assert observed == {("a", 1), ("a", 4), ("b", 2)}
