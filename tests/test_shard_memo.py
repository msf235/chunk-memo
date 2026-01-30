import itertools
import tempfile
import time

import pytest

from shard_memo import ShardMemo

from .utils import exec_fn_grid


def exec_fn_sleep(params, strat, s):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        time.sleep(0.05)
        outputs.append({"alpha": params["alpha"], "strat": strat_value, "s": s_value})
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def exec_point_extra_default(params, strat, s, extra=1):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        outputs.append(
            {
                "alpha": params["alpha"],
                "extra": extra,
                "strat": strat_value,
                "s": s_value,
            }
        )
    return outputs


def exec_point_extra_param(params, strat, s, extra=2):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        outputs.append(
            {
                "alpha": params["alpha"],
                "extra": extra,
                "strat": strat_value,
                "s": s_value,
            }
        )
    return outputs


def run_memo(cache_root, *, merge=True, chunk_spec=None, axis_values):
    return ShardMemo(
        cache_root=cache_root,
        memo_chunk_spec=chunk_spec or {"strat": 1, "s": 3},
        axis_values=axis_values,
        merge_fn=merge_fn if merge else None,
    )


def test_basic_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        output, diag = memo.run(params, exec_fn_sleep)
        assert output
        assert diag.executed_chunks == 1

        output2, diag2 = memo.run(params, exec_fn_sleep)
        assert output2 == output
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_incremental_split_extension():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo.run(params, exec_fn_grid)
        assert diag.total_chunks == 1

        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag2 = memo.run(params, exec_fn_grid)
        assert diag2.total_chunks == 2
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 2


def test_param_change_invalidates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo.run({"alpha": 0.4}, exec_fn_grid)
        assert diag.executed_chunks == 1

        _, diag2 = memo.run({"alpha": 0.5}, exec_fn_grid)
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 1


def test_chunk_count_grid():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b", "c"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo.run(params, exec_fn_grid)
        assert diag.total_chunks == 6
        assert diag.executed_chunks == 6


def test_merge_fn_optional_returns_nested():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, merge=False, axis_values=axis_values)
        output, diag = memo.run(params, exec_fn_grid)
        assert diag.total_chunks == 2
        assert isinstance(output, list)
        assert isinstance(output[0], list)


def test_run_wrap_positional_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)

        exec_point = memo.run_wrap()(exec_point_extra_default)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 1


def test_run_wrap_executes_with_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)

        exec_point = memo.run_wrap()(exec_fn_grid)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output


def test_run_wrap_param_merge():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)

        exec_point = memo.run_wrap()(exec_point_extra_param)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3], extra=3)
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 3

        output2, diag2 = exec_point(params, strat=["a"], s=[1, 2, 3], extra=3)
        assert output2 == output
        assert diag2.executed_chunks == 0

        indexed_output, indexed_diag = exec_point(
            params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}, extra=3
        )
        assert indexed_output == output
        assert indexed_diag.executed_chunks == 0

        status = exec_point.cache_status(
            params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}, extra=3
        )
        assert status["total_chunks"] == 1
        assert status["cached_chunks"]
        assert isinstance(status["cached_chunk_indices"][0]["s"], slice)


def test_run_wrap_duplicate_params_arg():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)

        exec_point = memo.run_wrap()(exec_fn_grid)
        params = {"alpha": 0.4}
        with pytest.raises(ValueError, match="both positional and keyword"):
            exec_point(params, params=params, strat=["a"], s=[1, 2, 3])

        with pytest.raises(ValueError, match="axis_indices cannot be combined"):
            exec_point(
                params,
                strat=["a"],
                s=[1, 2, 3],
                axis_indices={"strat": [0], "s": [0, 1, 2]},
            )


def test_run_axis_indices_range_slice():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}

        output, diag = memo.run(
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag.executed_chunks == 1
        observed = {(item["strat"], item["s"]) for item in output}
        assert observed == {("a", 2), ("a", 3)}

        diag2 = memo.run_streaming(
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag2.executed_chunks == 0


def test_memo_chunk_enumerator_order():
    memo_chunk_spec = {"strat": 1, "s": 2}

    def build_chunk_keys(axis_values):
        axis_order = sorted(axis_values)
        axis_chunks = []
        for axis in axis_order:
            values = axis_values[axis]
            size = memo_chunk_spec[axis]
            axis_chunks.append(
                [tuple(values[i : i + size]) for i in range(0, len(values), size)]
            )
        chunk_keys = []
        for product in itertools.product(*axis_chunks):
            chunk_key = tuple(
                (axis, tuple(values)) for axis, values in zip(axis_order, product)
            )
            chunk_keys.append(chunk_key)
        return chunk_keys

    def memo_chunk_enumerator(axis_values):
        return list(reversed(build_chunk_keys(axis_values)))

    def exec_fn_marker(params, strat, s):
        return [{"marker": f"{strat[0]}-{s[0]}"}]

    def merge_markers(chunks):
        return [chunk[0]["marker"] for chunk in chunks]

    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ShardMemo(
            cache_root=temp_dir,
            memo_chunk_spec=memo_chunk_spec,
            axis_values=axis_values,
            merge_fn=merge_markers,
            memo_chunk_enumerator=memo_chunk_enumerator,
        )
        params = {"alpha": 0.4}
        output, diag = memo.run(params, exec_fn_marker)
        assert diag.executed_chunks == 4
        expected = []
        for chunk_key in memo_chunk_enumerator(axis_values):
            chunk_axes = {axis: list(values) for axis, values in chunk_key}
            expected.append(f"{chunk_axes['strat'][0]}-{chunk_axes['s'][0]}")
        assert output == expected


@pytest.mark.flaky(reruns=2)
def test_timing_cache_speedup():
    params = {"alpha": 0.4}
    axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        start = time.perf_counter()
        memo.run(params, exec_fn_sleep)
        cold_time = time.perf_counter() - start
        print(f"cold_cache_s: {cold_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        memo.run(params, exec_fn=exec_fn_sleep, strat=["a"], s=[1, 2, 3, 4])
        start = time.perf_counter()
        memo.run(params, exec_fn_sleep)
        half_time = time.perf_counter() - start
        print(f"half_cache_s: {half_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        memo.run(params, exec_fn_sleep)
        start = time.perf_counter()
        memo.run(params, exec_fn_sleep)
        warm_time = time.perf_counter() - start
        print(f"warm_cache_s: {warm_time:0.4f}")

    assert warm_time < half_time < cold_time


def test_streaming_diagnostics_bound_memory():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4, 5]}
        memo = run_memo(
            temp_dir,
            axis_values=axis_values,
            chunk_spec={"strat": 1, "s": 2},
        )

        params = {"alpha": 0.4}
        diag = memo.run_streaming(params, exec_fn_grid)

        assert diag.executed_chunks == 3
        assert diag.max_stream_items == 2
