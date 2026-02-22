import tempfile
import functools
import itertools

import pytest  # type: ignore[import-not-found]
from concurrent.futures import ProcessPoolExecutor

from chunk_memo import ChunkCache, run_parallel
from chunk_memo.runners import run as _memo_run
from chunk_memo.runners import run_streaming as _memo_run_streaming

from .utils import exec_fn_grid, item_dicts, observed_items


def _parallel_kwargs(memo):
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


def slice_and_run_streaming(memo, params, exec_fn, **kwargs):
    axis_indices = kwargs.pop("axis_indices", None)
    sliced = memo.slice(params, axis_indices=axis_indices, **kwargs)
    return _memo_run_streaming(sliced, exec_fn)


def test_run_parallel_missing_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
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


def test_run_parallel_with_memoized_cache_status():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)
        slice_and_run(memo, params, exec_fn=exec_fn_grid, strat=["a"], s=[1, 2, 3, 4])

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
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


def test_run_parallel_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        outputs, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
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


def test_run_parallel_populates_memo_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        output, diag = slice_and_run(memo, params, exec_fn_grid)
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


def test_run_parallel_flush_on_chunk_populates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        status = memo.cache_status(strat=axis_values["strat"], s=axis_values["s"])
        with ProcessPoolExecutor(max_workers=2) as executor:
            _, diag = run_parallel(
                items,
                exec_fn=functools.partial(exec_fn_grid, params),
                cache=memo,
                **_parallel_kwargs(memo),
                map_fn=executor.map,
                map_fn_kwargs={"chunksize": 1},
                flush_on_chunk=True,
                return_output=False,
            )

        assert diag.executed_chunks == len(status["missing_chunks"])
        output, diag2 = slice_and_run(memo, params, exec_fn_grid)
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


def test_run_parallel_populates_cache_for_run_and_streaming():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        def exec_fail(*_args, **_kwargs):
            raise AssertionError("exec_fn should not run when cache is full")

        output, diag = slice_and_run(memo, params, exec_fail)
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

        diag_stream = slice_and_run_streaming(memo, params, exec_fail)
        assert diag_stream.executed_chunks == 0
        assert diag_stream.cached_chunks == diag_stream.total_chunks


def test_run_parallel_flush_on_chunk_populates_cache_for_run_and_streaming():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            flush_on_chunk=True,
            return_output=False,
        )

        def exec_fail(*_args, **_kwargs):
            raise AssertionError("exec_fn should not run when cache is full")

        output, diag = slice_and_run(memo, params, exec_fail)
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

        diag_stream = slice_and_run_streaming(memo, params, exec_fail)
        assert diag_stream.executed_chunks == 0
        assert diag_stream.cached_chunks == diag_stream.total_chunks


def test_run_parallel_resume_after_interrupt():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        update_calls = 0

        def update_chunk_index(chunk_hash, chunk_key):
            nonlocal update_calls
            memo.update_chunk_index(chunk_hash, chunk_key)
            update_calls += 1
            if update_calls == 1:
                raise RuntimeError("interrupted")

        with pytest.raises(RuntimeError, match="interrupted"):
            run_parallel(
                items,
                exec_fn=functools.partial(exec_fn_grid, params),
                cache=memo,
                **{**_parallel_kwargs(memo), "update_chunk_index": update_chunk_index},
                map_fn_kwargs={"chunksize": 1},
                map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            )

        output, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        )

        assert diag.cached_chunks == 1
        assert diag.executed_chunks == 1
        assert observed_items(output) == {("a", 1), ("a", 2), ("a", 3), ("a", 4)}


def test_run_parallel_flush_on_chunk_resume_after_interrupt():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
        )
        _set_params(memo, params)

        items = item_dicts(axis_values)
        update_calls = 0

        def update_chunk_index(chunk_hash, chunk_key):
            nonlocal update_calls
            memo.update_chunk_index(chunk_hash, chunk_key)
            update_calls += 1
            if update_calls == 1:
                raise RuntimeError("interrupted")

        with pytest.raises(RuntimeError, match="interrupted"):
            run_parallel(
                items,
                exec_fn=functools.partial(exec_fn_grid, params),
                cache=memo,
                **{**_parallel_kwargs(memo), "update_chunk_index": update_chunk_index},
                map_fn_kwargs={"chunksize": 1},
                map_fn=lambda func, items, **kwargs: [func(item) for item in items],
                flush_on_chunk=True,
                return_output=False,
            )

        _, diag = run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            flush_on_chunk=True,
            return_output=False,
        )

        assert diag.cached_chunks == 1
        assert diag.executed_chunks == 1


def test_run_parallel_grid_items_no_missing_seeds():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        p3_list = [0.7, 1.0]
        mem_list = [4, 5, 6]
        run_strats = ["a", "b"]
        seed_values = list(range(20))
        axis_values = {
            "p3": p3_list,
            "mem_capacity": mem_list,
            "strat": run_strats,
            "seed": seed_values,
        }
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={
                "p3": 1,
                "mem_capacity": 1,
                "strat": 1,
                "seed": max(1, min(200, len(seed_values))),
            },
            axis_values=axis_values,
            params=params,
            axis_order=("p3", "mem_capacity", "strat", "seed"),
        )
        memo.set_params(params)

        combos = list(itertools.product(p3_list, mem_list))
        items = [
            {
                "p3": p3,
                "mem_capacity": mem_capacity,
                "strat": strat,
                "seed": seed,
            }
            for p3, mem_capacity in combos
            for strat in run_strats
            for seed in seed_values
        ]

        def exec_seed_point(p3, mem_capacity, strat, seed):
            return p3, mem_capacity, strat, seed

        outputs, _diag = run_parallel(
            items,
            exec_fn=exec_seed_point,
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            map_fn_kwargs={"chunksize": 100},
            cache=memo,
            collate_fn=None,
        )

        if outputs and isinstance(outputs[0], list):
            outputs = [item for chunk in outputs for item in chunk]

        per_combo: dict[tuple[float, int], dict[str, dict[int, object]]] = {}
        for p3, mem_capacity, strat, seed in outputs:
            combo_key = (p3, mem_capacity)
            per_combo.setdefault(combo_key, {}).setdefault(strat, {})[seed] = True

        for p3, mem_capacity in combos:
            combo_key = (p3, mem_capacity)
            per_strat = per_combo.get(combo_key, {})
            for strat in run_strats:
                seed_map = per_strat.get(strat, {})
                missing = len(seed_values) - len(seed_map)
                assert missing == 0


def test_run_parallel_reuses_superset_cache_for_subset_axes():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values_full = {
            "p3": [0.7, 1.0],
            "mem_capacity": [4, 5, 6],
            "strat": ["a", "b"],
            "seed": list(range(200)),
        }
        memo_full = ChunkCache(
            root=temp_dir,
            chunk_spec={
                "p3": 1,
                "mem_capacity": 1,
                "strat": 1,
                "seed": 200,
            },
            axis_values=axis_values_full,
            params=params,
            axis_order=("p3", "mem_capacity", "strat", "seed"),
        )
        memo_full.set_params(params)

        items_full = [
            {
                "p3": p3,
                "mem_capacity": mem_capacity,
                "strat": strat,
                "seed": seed,
            }
            for p3 in axis_values_full["p3"]
            for mem_capacity in axis_values_full["mem_capacity"]
            for strat in axis_values_full["strat"]
            for seed in axis_values_full["seed"]
        ]

        def exec_seed_point(p3, mem_capacity, strat, seed):
            return p3, mem_capacity, strat, seed

        run_parallel(
            items_full,
            exec_fn=exec_seed_point,
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            map_fn_kwargs={"chunksize": 100},
            cache=memo_full,
            collate_fn=None,
        )

        axis_values_subset = {
            "p3": [0.7],
            "mem_capacity": [5],
            "strat": ["a", "b"],
            "seed": list(range(20)),
        }
        memo_subset = ChunkCache.auto_load(
            root=temp_dir,
            params=params,
            axis_values=axis_values_subset,
            allow_superset=True,
        )

        items_subset = [
            {
                "p3": p3,
                "mem_capacity": mem_capacity,
                "strat": strat,
                "seed": seed,
            }
            for p3 in axis_values_subset["p3"]
            for mem_capacity in axis_values_subset["mem_capacity"]
            for strat in axis_values_subset["strat"]
            for seed in axis_values_subset["seed"]
        ]

        def exec_fail(*_args, **_kwargs):
            raise AssertionError("exec_fn should not run for superset cache")

        outputs, diag = run_parallel(
            items_subset,
            exec_fn=exec_fail,
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            map_fn_kwargs={"chunksize": 100},
            cache=memo_subset,
            collate_fn=None,
        )

        assert diag.executed_chunks == 0
        expected_cached_chunks = (
            len(axis_values_subset["p3"])
            * len(axis_values_subset["mem_capacity"])
            * len(axis_values_subset["strat"])
        )
        assert diag.cached_chunks == expected_cached_chunks
        if outputs and isinstance(outputs[0], list):
            outputs = [item for chunk in outputs for item in chunk]
        observed = {(p3, mem, strat, seed) for p3, mem, strat, seed in outputs}
        expected = {
            (p3, mem_capacity, strat, seed)
            for p3 in axis_values_subset["p3"]
            for mem_capacity in axis_values_subset["mem_capacity"]
            for strat in axis_values_subset["strat"]
            for seed in axis_values_subset["seed"]
        }
        assert observed == expected


def test_run_parallel_extend_cache_for_new_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {
            "strat": ["a"],
            "s": [1, 2],
        }
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
            params=params,
        )
        memo.set_params(params)

        items = [
            {"strat": "a", "s": 1},
            {"strat": "a", "s": 2},
            {"strat": "a", "s": 3},
        ]

        def exec_seed_point(strat, s):
            return strat, s

        outputs, diag = run_parallel(
            items,
            exec_fn=exec_seed_point,
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            map_fn_kwargs={"chunksize": 1},
            cache=memo,
            collate_fn=None,
            extend_cache=True,
        )

        assert diag.executed_chunks == 2
        axis_values_after = memo.cache_status().get("axis_values", {})
        assert axis_values_after.get("s") == [1, 2, 3]
        if outputs and isinstance(outputs[0], list):
            outputs = [item for chunk in outputs for item in chunk]
        assert set(outputs) == {("a", 1), ("a", 2), ("a", 3)}


def test_run_parallel_flush_on_chunk_partial_chunks_load_as_partial():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
            collate_fn=lambda chunks: [item for chunk in chunks for item in chunk],
        )
        _set_params(memo, params)

        items = [{"strat": "a", "s": 1}, {"strat": "a", "s": 3}]
        run_parallel(
            items,
            exec_fn=functools.partial(exec_fn_grid, params),
            cache=memo,
            **_parallel_kwargs(memo),
            map_fn_kwargs={"chunksize": 1},
            map_fn=lambda func, items, **kwargs: [func(item) for item in items],
            flush_on_chunk=True,
            return_output=False,
        )

        def exec_fail(*_args, **_kwargs):
            raise AssertionError("exec_fn should not run for partial cache hits")

        output, diag = slice_and_run(memo, params, exec_fail)
        assert diag.executed_chunks == 0
        assert diag.cached_chunks == diag.total_chunks
        assert diag.partial_chunks == diag.total_chunks
        assert observed_items(output) == {("a", 1), ("a", 3)}
