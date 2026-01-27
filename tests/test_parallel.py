import itertools
import tempfile
import time

from swarm_memo import ChunkMemo, process_pool_parallel


def exec_fn(params, strat, s):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        outputs.append({"alpha": params["alpha"], "strat": strat_value, "s": s_value})
    return outputs


def exec_fn_sleep(params, strat, s):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        time.sleep(0.02)
        outputs.append({"alpha": params["alpha"], "strat": strat_value, "s": s_value})
    return outputs


def _indices_to_values(index_spec, axis_values):
    if isinstance(index_spec, slice):
        indices = list(range(*index_spec.indices(len(axis_values))))
    elif isinstance(index_spec, range):
        indices = list(index_spec)
    elif isinstance(index_spec, (list, tuple)):
        indices = list(index_spec)
    else:
        indices = [index_spec]
    return [axis_values[index] for index in indices]


def _chunk_indices_to_axes(chunk_indices, split_spec):
    return {
        axis: _indices_to_values(indices, split_spec[axis])
        for axis, indices in chunk_indices.items()
    }


def _chunk_indices_to_axes_for_split_spec(split_spec):
    return {
        "split_spec": split_spec,
        "callable": _chunk_indices_to_axes,
    }


def _chunk_indices_to_axes_wrapper(chunk_indices, helper):
    return helper["callable"](chunk_indices, helper["split_spec"])


def _filter_split_spec(split_spec, axes):
    return {axis: split_spec[axis] for axis in axes}


def _expected_outputs(params, cache_status, split_spec):
    outputs = []
    for chunk_key in cache_status["cached_chunks"]:
        axes = {axis: list(values) for axis, values in chunk_key}
        outputs.extend(exec_fn(params, **axes))
    for chunk_indices in cache_status["missing_chunk_indices"]:
        axes = _chunk_indices_to_axes(chunk_indices, split_spec)
        outputs.extend(exec_fn(params, **axes))
    return outputs


def test_process_pool_parallel_missing_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn,
            split_spec=split_spec,
        )
        memoized_exec = memo.run_wrap()(exec_fn)

        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 1), "s": slice(2, 4)}
        )
        helper = _chunk_indices_to_axes_for_split_spec(split_spec)
        outputs, diag = process_pool_parallel(
            status,
            exec_fn,
            params=params,
            chunk_indices_to_axes=lambda chunk_indices: _chunk_indices_to_axes_wrapper(
                chunk_indices, helper
            ),
            use_processes=False,
        )

        assert diag.cached_chunks == 0
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert outputs == _expected_outputs(params, status, split_spec)


def test_process_pool_parallel_with_memoized_cache_status():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn,
            split_spec=split_spec,
        )
        memo.run(params, {"strat": ["a"], "s": [1, 2, 3, 4]})
        memo._set_split_spec(split_spec)
        memoized_exec = memo.run_wrap()(exec_fn)

        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 2), "s": slice(0, 4)}
        )
        helper = _chunk_indices_to_axes_for_split_spec(split_spec)
        outputs, diag = process_pool_parallel(
            status,
            exec_fn,
            params=params,
            chunk_indices_to_axes=lambda chunk_indices: _chunk_indices_to_axes_wrapper(
                chunk_indices, helper
            ),
            use_processes=False,
        )

        assert status["cached_chunks"]
        assert status["missing_chunks"]
        assert diag.cached_chunks == len(status["cached_chunks"])
        assert diag.executed_chunks == len(status["missing_chunks"])
        assert outputs == _expected_outputs(params, status, split_spec)


def test_parallel_timing_cache_speedup():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn_sleep,
            split_spec=split_spec,
        )
        memoized_exec = memo.run_wrap()(exec_fn_sleep)
        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 2), "s": slice(0, 4)}
        )
        helper = _chunk_indices_to_axes_for_split_spec(split_spec)
        start = time.perf_counter()
        process_pool_parallel(
            status,
            memoized_exec,
            params=params,
            chunk_indices_to_axes=lambda chunk_indices: _chunk_indices_to_axes_wrapper(
                chunk_indices, helper
            ),
            use_processes=False,
        )
        cold_time = time.perf_counter() - start
        print(f"parallel_cold_s: {cold_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn_sleep,
            split_spec=split_spec,
        )
        memo.run(params, {"strat": ["a"], "s": [1, 2, 3, 4]})
        memo._set_split_spec(split_spec)
        memoized_exec = memo.run_wrap()(exec_fn_sleep)
        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 2), "s": slice(0, 4)}
        )
        helper = _chunk_indices_to_axes_for_split_spec(split_spec)
        start = time.perf_counter()
        process_pool_parallel(
            status,
            memoized_exec,
            params=params,
            chunk_indices_to_axes=lambda chunk_indices: _chunk_indices_to_axes_wrapper(
                chunk_indices, helper
            ),
            use_processes=False,
        )
        half_time = time.perf_counter() - start
        print(f"parallel_half_s: {half_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_fn=exec_fn_sleep,
            split_spec=split_spec,
        )
        memo.run(params, split_spec)
        memoized_exec = memo.run_wrap()(exec_fn_sleep)
        status = memoized_exec.cache_status(
            params, axis_indices={"strat": range(0, 2), "s": slice(0, 4)}
        )
        helper = _chunk_indices_to_axes_for_split_spec(split_spec)
        start = time.perf_counter()
        process_pool_parallel(
            status,
            memoized_exec,
            params=params,
            chunk_indices_to_axes=lambda chunk_indices: _chunk_indices_to_axes_wrapper(
                chunk_indices, helper
            ),
            use_processes=False,
        )
        warm_time = time.perf_counter() - start
        print(f"parallel_warm_s: {warm_time:0.4f}")

    assert min(cold_time, half_time, warm_time) >= 0
    assert half_time <= cold_time * 0.95
    assert warm_time <= half_time * 0.95
