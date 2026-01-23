import itertools
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
    time.sleep(0.05)
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def exec_fn_sleep(params, point):
    strat, s = point
    print("calling exec_fn_sleep", params, point)
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


def exec_fn_chunk(params, point):
    chunk_id, value = point
    time.sleep(0.05)
    return {"chunk": chunk_id, "value": value}


def exec_fn_chunked(params, point):
    strat, s = point
    time.sleep(0.05)
    chunk_id = ((s - 1) // 4) + 1
    return {"chunk": chunk_id, "strat": strat, "s": s}


def run_memo(cache_root, exec_chunk_size=2):
    return SwarmMemo(
        cache_root=cache_root,
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=exec_chunk_size,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
        max_workers=2,
    )


def run_memo_sleep(cache_root, exec_chunk_size=2):
    return SwarmMemo(
        cache_root=cache_root,
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=exec_chunk_size,
        exec_fn=exec_fn_sleep,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
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


def test_default_enumerator_output_matches():
    with tempfile.TemporaryDirectory() as temp_dir:
        default_root = f"{temp_dir}/default"
        explicit_root = f"{temp_dir}/explicit"
        memo_default = SwarmMemo(
            cache_root=default_root,
            memo_chunk_spec={"strat": 1, "s": 3},
            exec_chunk_size=2,
            exec_fn=exec_fn,
            collate_fn=collate_fn,
            merge_fn=merge_fn,
            point_enumerator=None,
            memo_chunk_enumerator=None,
            max_workers=2,
        )

        def enumerate_points_lex(params, split_spec):
            axis_order = sorted(split_spec)
            points = []
            for combo in itertools.product(*(split_spec[axis] for axis in axis_order)):
                points.append(combo)
            return points

        memo_explicit = SwarmMemo(
            cache_root=explicit_root,
            memo_chunk_spec={"strat": 1, "s": 3},
            exec_chunk_size=2,
            exec_fn=exec_fn,
            collate_fn=collate_fn,
            merge_fn=merge_fn,
            point_enumerator=enumerate_points_lex,
            max_workers=2,
        )
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": [1, 2, 3]}
        output_default, diag_default = memo_default.run(params, split_spec)
        output_explicit, diag_explicit = memo_explicit.run(params, split_spec)
        assert output_default == output_explicit
        assert diag_default.total_chunks == diag_explicit.total_chunks
        assert diag_default.executed_chunks == diag_explicit.executed_chunks


def test_reverse_lexicographic_enumerator():
    def enumerate_points_reverse(params, split_spec):
        axis_order = sorted(split_spec, reverse=True)
        points = []
        for combo in itertools.product(*(split_spec[axis] for axis in axis_order)):
            points.append(combo)
        return points

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = SwarmMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 2},
            exec_chunk_size=2,
            exec_fn=exec_fn,
            collate_fn=collate_fn,
            merge_fn=merge_fn,
            point_enumerator=enumerate_points_reverse,
            max_workers=2,
        )
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a", "b"], "s": [1, 2]}
        output, diag = memo.run(params, split_spec)
        assert diag.executed_chunks == 2
        assert output[0]["strat"] in {"a", "b"}


def test_streaming_buffer_bound():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = SwarmMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 5},
            exec_chunk_size=3,
            exec_fn=exec_fn,
            collate_fn=collate_fn,
            merge_fn=merge_fn,
            point_enumerator=enumerate_points,
            max_workers=2,
        )
        params = {"alpha": 0.4}
        split_spec = {
            "strat": ["a"],
            "s": list(range(1, 31)),
        }
        diag = memo.run_streaming(params, split_spec)
        assert diag.max_stream_buffer <= 7
        print("max_stream_buffer", diag.max_stream_buffer)
        print("max_out_of_order", diag.max_out_of_order)


def test_streaming_flush_order_odd_even():
    flush_order = []

    def memo_chunks_odd_even(split_spec):
        strat_values = split_spec["strat"]
        s_values = split_spec["s"]
        points = [(strat_values[0], s) for s in s_values]
        chunks = [points[i : i + 4] for i in range(0, len(points), 4)]
        odd = [chunk for idx, chunk in enumerate(chunks, start=1) if idx % 2 == 1]
        even = [chunk for idx, chunk in enumerate(chunks, start=1) if idx % 2 == 0]
        return odd + even

    def collate_with_flush(outputs):
        if outputs:
            flush_order.append(outputs[0]["chunk"])
        return outputs

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = SwarmMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"strat": 1, "s": 4},
            exec_chunk_size=3,
            exec_fn=exec_fn_chunked,
            collate_fn=collate_with_flush,
            merge_fn=merge_fn,
            memo_chunk_enumerator=memo_chunks_odd_even,
            point_enumerator=None,
            max_workers=2,
        )
        params = {"alpha": 0.4}
        split_spec = {"strat": ["a"], "s": list(range(1, 31))}
        ordered_points = []
        for chunk in memo_chunks_odd_even(split_spec):
            ordered_points.extend(chunk)
        print(
            "Ordering points by memo_chunk_enumerator: "
            f"{ordered_points}.\n"
            "Execution chunks will be fed to the parallel executor "
            "with respect to this order."
        )
        diag = memo.run_streaming(params, split_spec)
        expected_order = [1, 3, 5, 7, 2, 4, 6, 8]
        assert diag.stream_flushes == len(expected_order)
        assert flush_order == expected_order
        print("max_stream_buffer_odd_even", diag.max_stream_buffer)
        print("max_out_of_order_odd_even", diag.max_out_of_order)


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
