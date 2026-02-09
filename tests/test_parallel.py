import tempfile
import functools
from concurrent.futures import ProcessPoolExecutor

from shard_memo import ChunkCache, memo_parallel_run, memo_parallel_run_streaming
from shard_memo.runners import run as _memo_run

from .utils import exec_fn_grid, item_dicts, observed_items


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


def _parallel_streaming_kwargs(memo):
    return {
        "write_metadata": memo.write_metadata,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "load_chunk_index": memo.load_chunk_index,
        "build_item_maps_from_axis_values": memo.build_item_maps_from_axis_values,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "reconstruct_output_from_items": memo.reconstruct_output_from_items,
        "item_hash": memo.item_hash,
        "context": memo,
    }


def _set_params(memo, params):
    memo.set_params(params)
    memo.write_metadata()


def memo_run(memo, params, exec_fn, **kwargs):
    _set_params(memo, params)
    return _memo_run(memo, exec_fn, **kwargs)


def test_memo_parallel_run_missing_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
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
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)
        memo_run(memo, params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3, 4])

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
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
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
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
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            **_parallel_kwargs(memo),
            cache_status=status,
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        output, diag = memo_run(memo, params, exec_fn_grid)
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
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        with ProcessPoolExecutor(max_workers=2) as executor:
            diag = memo_parallel_run_streaming(
                items,
                exec_fn=functools.partial(exec_fn_grid, params),
                **_parallel_streaming_kwargs(memo),
                cache_status=status,
                map_fn=executor.map,
                map_fn_kwargs={"chunksize": 1},
            )

        assert diag.executed_chunks == len(status["missing_chunks"])
        output, diag2 = memo_run(memo, params, exec_fn_grid)
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
