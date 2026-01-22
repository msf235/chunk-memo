import time
import tempfile
import time

import pytest

from swarm_memo import SwarmMemo


def enumerate_points(params, split_spec):
    points = []
    for strat in split_spec["strat"]:
        for s in split_spec["s"]:
            points.append((strat, s))
    return points


def exec_fn(params, point):
    strat, s = point
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def exec_fn_sleep(params, point):
    strat, s = point
    time.sleep(0.05)
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def collate_fn(outputs):
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def exec_point_extra_default(params, point, extra=1):
    return {"alpha": params["alpha"], "extra": extra, "point": point}


def exec_point_extra_param(params, point, extra=2):
    return {"alpha": params["alpha"], "extra": extra, "point": point}


def exec_point_basic(params, point):
    return {"alpha": params["alpha"], "point": point}


def run_memo(cache_root, exec_chunk_size=2):
    return SwarmMemo(
        cache_root=cache_root,
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=exec_chunk_size,
        enumerate_points=enumerate_points,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        max_workers=2,
    )


def run_memo_sleep(cache_root, exec_chunk_size=2):
    return SwarmMemo(
        cache_root=cache_root,
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=exec_chunk_size,
        enumerate_points=enumerate_points,
        exec_fn=exec_fn_sleep,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        max_workers=2,
    )


def test_basic_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo_sleep(temp_dir)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        output, diag = memo.run(params, split_spec)
        assert output
        assert diag.executed_chunks == 1

        output2, diag2 = memo.run(params, split_spec)
        assert output2 == output
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_incremental_split_extension():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        _, diag = memo.run(params, split_spec)
        assert diag.total_chunks == 1

        split_spec = {"strat": ["a"], "s": [1, 2, 3, 4]}
        _, diag2 = memo.run(params, split_spec)
        assert diag2.total_chunks == 2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 1


def test_param_change_invalidates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        _, diag = memo.run({"alpha": 0.4}, split_spec)
        assert diag.executed_chunks == 1

        _, diag2 = memo.run({"alpha": 0.5}, split_spec)
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 1


def test_chunk_count_grid():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b", "c"], "s": [1, 2, 3, 4]}
        _, diag = memo.run(params, split_spec)
        assert diag.total_chunks == 6
        assert diag.executed_chunks == 6


def test_exec_chunk_size_output_stability():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, exec_chunk_size=1)
        output_small, _ = memo.run(params, split_spec)
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, exec_chunk_size=4)
        output_large, _ = memo.run(params, split_spec)
    assert output_small == output_large


def test_streaming_then_run_uses_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        diag_stream = memo.run_streaming(params, split_spec)
        assert diag_stream.stream_flushes == diag_stream.executed_chunks

        _, diag = memo.run(params, split_spec)
        assert diag.executed_chunks == 0
        assert diag.cached_chunks == diag.total_chunks


def test_streaming_order_preserved():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, exec_chunk_size=3)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo.run_streaming(params, split_spec)
        output, _ = memo.run(params, split_spec)
        expected = [
            {"alpha": 0.4, "strat": "a", "s": 1},
            {"alpha": 0.4, "strat": "a", "s": 2},
            {"alpha": 0.4, "strat": "a", "s": 3},
            {"alpha": 0.4, "strat": "a", "s": 4},
        ]
        assert output == expected


def test_run_wrap_positional_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)

        exec_point = memo.run_wrap()(exec_point_extra_default)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        output, diag = exec_point(params, split_spec=split_spec)
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 1


def test_run_wrap_missing_split_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)

        exec_point = memo.run_wrap()(exec_point_basic)
        params = {"alpha": 0.4}
        with pytest.raises(ValueError, match="split_spec"):
            exec_point(params)


def test_run_wrap_param_merge():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)

        exec_point = memo.run_wrap()(exec_point_extra_param)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        output, diag = exec_point(params, split_spec=split_spec, extra=3)
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 3

        output2, diag2 = exec_point(params, split_spec=split_spec, extra=3)
        assert output2 == output
        assert diag2.executed_chunks == 0


def test_run_wrap_duplicate_params_arg():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)

        exec_point = memo.run_wrap()(exec_point_basic)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        with pytest.raises(ValueError, match="both positional and keyword"):
            exec_point(params, params=params, split_spec=split_spec)


def test_streaming_wrap_returns_diagnostics():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir)

        exec_point = memo.streaming_wrap()(exec_point_basic)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        diag = exec_point(params, split_spec=split_spec)
        assert diag.executed_chunks == 1
        assert diag.stream_flushes == 1


@pytest.mark.flaky(reruns=2)
def test_timing_cache_speedup():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo_sleep(temp_dir)
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}

        start = time.perf_counter()
        memo.run(params, split_spec)
        first = time.perf_counter() - start

        start = time.perf_counter()
        memo.run(params, split_spec)
        second = time.perf_counter() - start

        assert second < first * 0.3
