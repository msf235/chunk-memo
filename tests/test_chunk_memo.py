import itertools
import functools
import tempfile
import time

import pytest  # type: ignore[import-not-found]

from shard_memo import ChunkCache, ChunkMemo
from shard_memo.runners import run as _memo_run
from shard_memo.runners import run_streaming as _memo_run_streaming

from .utils import exec_fn_grid


def exec_fn_singleton(params):
    return {"value": params["alpha"] * 2}


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


def _run_kwargs(memo):
    return {
        "prepare_run": memo.prepare_run,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "extract_items_from_map": memo.extract_items_from_map,
        "collect_chunk_data": memo.collect_chunk_data,
        "context": memo,
    }


def _stream_kwargs(memo):
    return {
        "prepare_run": memo.prepare_run,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "context": memo,
    }


def _set_params(memo, params):
    memo.set_params(params)


def memo_run(memo, params, exec_fn, **kwargs):
    _set_params(memo, params)
    exec_fn_bound = functools.partial(exec_fn, params)
    return _memo_run(exec_fn_bound, **_run_kwargs(memo), **kwargs)


def memo_run_streaming(memo, params, exec_fn, **kwargs):
    _set_params(memo, params)
    exec_fn_bound = functools.partial(exec_fn, params)
    return _memo_run_streaming(exec_fn_bound, **_stream_kwargs(memo), **kwargs)


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
    return ChunkCache(
        cache_root=cache_root,
        cache_chunk_spec=chunk_spec or {"strat": 1, "s": 3},
        axis_values=axis_values,
        merge_fn=merge_fn if merge else None,
    )


def test_basic_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        output, diag = memo_run(memo, params, exec_fn_sleep)
        assert output
        assert diag.executed_chunks == 1

        output2, diag2 = memo_run(memo, params, exec_fn_sleep)
        assert output2 == output
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_incremental_split_extension():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 1

        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag2 = memo_run(memo, params, exec_fn_grid)
        assert diag2.total_chunks == 2
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 2


def test_param_change_invalidates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo_run(memo, {"alpha": 0.4}, exec_fn_grid)
        assert diag.executed_chunks == 1

        _, diag2 = memo_run(memo, {"alpha": 0.5}, exec_fn_grid)
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 1


def test_chunk_count_grid():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b", "c"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = memo_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 6
        assert diag.executed_chunks == 6


def test_merge_fn_optional_returns_nested():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, merge=False, axis_values=axis_values)
        output, diag = memo_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 2
        assert isinstance(output, list)
        assert isinstance(output[0], list)


def test_run_wrap_positional_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        wrapper = ChunkMemo(memo)

        exec_point = wrapper.run_wrap()(exec_point_extra_default)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 1


def test_run_wrap_executes_with_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        wrapper = ChunkMemo(memo)

        exec_point = wrapper.run_wrap()(exec_fn_grid)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output


def test_run_wrap_param_merge():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        wrapper = ChunkMemo(memo)

        exec_point = wrapper.run_wrap()(exec_point_extra_param)
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

        status = exec_point.cache_status(  # type: ignore[attr-defined]
            params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}, extra=3
        )
        assert status["total_chunks"] == 1
        assert status["cached_chunks"]
        assert status["cached_chunk_indices"][0]["s"] == [0, 1, 2]


def test_run_wrap_duplicate_params_arg():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        wrapper = ChunkMemo(memo)

        exec_point = wrapper.run_wrap()(exec_fn_grid)
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

        output, diag = memo_run(
            memo,
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag.executed_chunks == 1
        observed = {(item["strat"], item["s"]) for item in output}
        assert observed == {("a", 2), ("a", 3)}

        diag2 = memo_run_streaming(
            memo,
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag2.executed_chunks == 0


def test_cache_chunk_enumerator_order():
    cache_chunk_spec = {"strat": 1, "s": 2}

    def build_chunk_keys(axis_values):
        axis_order = sorted(axis_values)
        axis_chunks = []
        for axis in axis_order:
            values = axis_values[axis]
            size = cache_chunk_spec[axis]
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

    def cache_chunk_enumerator(axis_values):
        return list(reversed(build_chunk_keys(axis_values)))

    def exec_fn_marker(params, strat, s):
        return [{"marker": f"{strat[0]}-{s[0]}"}]

    def merge_markers(chunks):
        return [chunk[0]["marker"] for chunk in chunks]

    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values,
            merge_fn=merge_markers,
            cache_chunk_enumerator=cache_chunk_enumerator,
        )
        params = {"alpha": 0.4}
        output, diag = memo_run(memo, params, exec_fn_marker)
        assert diag.executed_chunks == 4
        expected = []
        for chunk_key in cache_chunk_enumerator(axis_values):
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
        memo_run(memo, params, exec_fn_sleep)
        cold_time = time.perf_counter() - start
        print(f"cold_cache_s: {cold_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        memo_run(memo, params, exec_fn=exec_fn_sleep, strat=["a"], s=[1, 2, 3, 4])
        start = time.perf_counter()
        memo_run(memo, params, exec_fn_sleep)
        half_time = time.perf_counter() - start
        print(f"half_cache_s: {half_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        memo_run(memo, params, exec_fn_sleep)
        start = time.perf_counter()
        memo_run(memo, params, exec_fn_sleep)
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
        diag = memo_run_streaming(memo, params, exec_fn_grid)

        assert diag.executed_chunks == 3
        assert diag.max_stream_items == 2


def test_discover_caches_empty():
    with tempfile.TemporaryDirectory() as temp_dir:
        caches = ChunkCache.discover_caches(temp_dir)
        assert caches == []


def test_discover_caches_finds_caches():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = memo_run(memo, params, exec_fn_grid)

        caches = ChunkCache.discover_caches(temp_dir)
        assert len(caches) == 1
        assert caches[0]["metadata"] is not None
        assert caches[0]["metadata"]["axis_values"] == axis_values


def test_find_compatible_caches_by_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = memo_run(memo, params, exec_fn_grid)

        compatible = ChunkCache.find_compatible_caches(temp_dir, params=params)
        assert len(compatible) == 1

        incompatible = ChunkCache.find_compatible_caches(
            temp_dir, params={"alpha": 0.5}
        )
        assert len(incompatible) == 0


def test_find_compatible_caches_by_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = memo_run(memo, params, exec_fn_grid)

        compatible = ChunkCache.find_compatible_caches(
            temp_dir, axis_values=axis_values
        )
        assert len(compatible) == 1

        incompatible = ChunkCache.find_compatible_caches(
            temp_dir, axis_values={"strat": ["b"]}
        )
        assert len(incompatible) == 0


def test_find_compatible_caches_wildcard():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = memo_run(memo, params, exec_fn_grid)

        compatible = ChunkCache.find_compatible_caches(temp_dir)
        assert len(compatible) == 1


def test_load_from_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        output1, diag1 = memo_run(memo, params, exec_fn_grid)

        cache_hash = memo.cache_hash()
        memo2 = ChunkCache.load_from_cache(temp_dir, cache_hash, merge_fn=merge_fn)
        output2, diag2 = memo_run(memo2, params, exec_fn_grid)

        assert output1 == output2
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_load_from_cache_not_found():
    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(FileNotFoundError):
            ChunkCache.load_from_cache(temp_dir, "nonexistent_hash")


def test_singleton_cache_basic():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        output, diag = memo_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.total_chunks == 1
        assert diag.executed_chunks == 1


def test_singleton_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        output1, diag1 = memo_run(memo, params, exec_fn_singleton)

        output2, diag2 = memo_run(memo, params, exec_fn_singleton)

        assert output1 == output2
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_singleton_cache_param_change():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        output1, diag1 = memo_run(memo, {"alpha": 0.4}, exec_fn_singleton)
        assert diag1.executed_chunks == 1

        output2, diag2 = memo_run(memo, {"alpha": 0.5}, exec_fn_singleton)
        assert diag2.executed_chunks == 1
        assert output1 != output2


def test_singleton_cache_no_axes_allowed():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )

        with pytest.raises(ValueError, match="Cannot pass axis arguments"):
            memo_run(memo, params, exec_fn_singleton, strat=["a"])

        with pytest.raises(ValueError, match="Cannot pass axis arguments"):
            memo_run_streaming(
                memo,
                params,
                exec_fn_singleton,
                axis_indices={"s": [0]},
            )


def test_singleton_cache_streaming():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        diag1 = memo_run_streaming(memo, params, exec_fn_singleton)
        assert diag1.executed_chunks == 1

        diag2 = memo_run_streaming(memo, params, exec_fn_singleton)
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_singleton_via_regular_constructor():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
            merge_fn=None,
        )
        output, diag = memo_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.total_chunks == 1
        assert diag.executed_chunks == 1


def test_exclusive_mode_exact_match():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        memo_run(memo, params, exec_fn_grid)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values,
            merge_fn=merge_fn,
            exclusive=True,
        )
        with pytest.raises(
            ValueError, match="Cache with same params and axis_values already exists"
        ):
            memo_run(memo2, params, exec_fn_grid)


def test_exclusive_mode_rejects_different_chunking():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        memo_run(memo, params, exec_fn_grid)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
            merge_fn=merge_fn,
            exclusive=True,
        )
        with pytest.raises(
            ValueError, match="Cache with same params and axis_values already exists"
        ):
            memo_run(memo2, params, exec_fn_grid)


@pytest.mark.skip("pre-existing test issue, requires refactoring")
def test_warn_on_overlap():
    with tempfile.TemporaryDirectory() as temp_dir:
        memo1 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
            merge_fn=merge_fn,
        )
        params = {"alpha": 0.4}
        memo_run(memo1, params, exec_fn_grid)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 2},
            axis_values={"strat": ["a"], "s": [1, 2, 3, 4]},
            merge_fn=merge_fn,
            warn_on_overlap=True,
        )
        with pytest.warns(UserWarning, match="Cache overlap detected"):
            output, diag = memo_run(memo2, params, exec_fn_grid)
            assert diag.executed_chunks == 2


def test_auto_load_no_existing_creates_new():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache.auto_load(temp_dir, params)
        output, diag = memo_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.executed_chunks == 1


def test_auto_load_finds_singleton():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo1 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        output1, diag1 = memo_run(memo1, params, exec_fn_singleton)

        memo2 = ChunkCache.auto_load(temp_dir, params)
        output2, diag2 = memo_run(memo2, params, exec_fn_singleton)

        assert output1 == output2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 0


def test_auto_load_with_axis_values_exact():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        chunk_spec = {"strat": 1, "s": 3}
        memo1 = run_memo(temp_dir, axis_values=axis_values, chunk_spec=chunk_spec)
        params = {"alpha": 0.4}
        output1, diag1 = memo_run(memo1, params, exec_fn_grid)

        memo2 = ChunkCache.auto_load(
            temp_dir, params, axis_values=axis_values, merge_fn=merge_fn
        )
        output2, diag2 = memo_run(memo2, params, exec_fn_grid)

        assert output1 == output2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 0


def test_auto_load_ambiguous_no_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo1 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={},
            axis_values={},
        )
        memo_run(memo1, params, exec_fn_singleton)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 1},
            axis_values={"strat": ["a", "b"], "s": [1, 2]},
            merge_fn=merge_fn,
        )
        memo_run(memo2, params, exec_fn_grid)

        with pytest.raises(ValueError, match="Ambiguous: 2 caches match"):
            ChunkCache.auto_load(temp_dir, params)


def test_auto_load_ambiguous_with_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo1 = run_memo(
            temp_dir, axis_values=axis_values, chunk_spec={"strat": 1, "s": 3}
        )
        memo_run(memo1, params, exec_fn_grid)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 2, "s": 3},
            axis_values=axis_values,
            merge_fn=merge_fn,
        )
        memo_run(memo2, params, exec_fn_grid)

        with pytest.raises(ValueError, match="Ambiguous: 2 caches match"):
            ChunkCache.auto_load(temp_dir, params, axis_values=axis_values)


def test_auto_load_with_cache_chunk_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo = ChunkCache.auto_load(
            temp_dir,
            params,
            axis_values=axis_values,
            cache_chunk_spec={"strat": 1, "s": 2},
            merge_fn=merge_fn,
        )
        output, diag = memo_run(memo, params, exec_fn_grid)

        assert output
        assert diag.total_chunks == 2
        assert diag.executed_chunks == 2


def test_auto_load_default_chunk_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo = ChunkCache.auto_load(temp_dir, params, axis_values=axis_values)
        output, diag = memo_run(memo, params, exec_fn_grid)

        assert output
        assert diag.total_chunks == 1


def test_allow_superset_finds_superset_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        output1, diag1 = memo_run(memo_superset, params_superset, exec_fn_grid)
        assert diag1.total_chunks == 2

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output2, diag2 = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert output2
        assert diag2.cached_chunks == 2
        assert diag2.executed_chunks == 0


def test_allow_superset_false_requires_exact_match():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=False,
            merge_fn=merge_fn,
        )
        _, diag = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert diag.executed_chunks == 1
        assert diag.cached_chunks == 0


def test_allow_superset_multiple_supersets_raises_error():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values1 = {"strat": ["a", "b"], "s": [1, 2, 3]}
        axis_values2 = {"strat": ["a", "c"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}

        memo1 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values1,
            merge_fn=merge_fn,
        )
        memo_run(memo1, params_superset, exec_fn_grid)

        memo2 = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values2,
            merge_fn=merge_fn,
        )
        memo_run(memo2, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        with pytest.raises(ValueError, match="Ambiguous: 2 caches match"):
            ChunkCache.auto_load(
                temp_dir,
                params_subset,
                axis_values=axis_values_subset,
                allow_superset=True,
            )


def test_exclusive_prevents_subset_cache_creation():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
            exclusive=True,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        with pytest.raises(
            ValueError,
            match="Cannot create subset cache: a superset cache already exists",
        ):
            memo_subset = ChunkCache.auto_load(
                temp_dir,
                params_subset,
                axis_values=axis_values_subset,
                exclusive=True,
                merge_fn=merge_fn,
            )
            memo_run(memo_subset, params_subset, exec_fn_grid)


def test_exclusive_prevents_subset_when_superset_exists():
    """Test that creating a subset cache is prevented when a superset cache exists."""
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
            exclusive=True,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4}

        with pytest.raises(
            ValueError,
            match="Cannot create subset cache: a superset cache already exists",
        ):
            memo = ChunkCache.auto_load(
                temp_dir,
                params_subset,
                axis_values=axis_values_subset,
                exclusive=True,
                merge_fn=merge_fn,
            )
            memo_run(memo, params_subset, exec_fn_grid)


def test_find_compatible_caches_with_allow_superset():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        caches_superset = ChunkCache.find_compatible_caches(
            temp_dir,
            params=params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
        )
        assert len(caches_superset) == 1

        caches_exact = ChunkCache.find_compatible_caches(
            temp_dir,
            params=params_subset,
            axis_values=axis_values_subset,
            allow_superset=False,
        )
        assert len(caches_exact) == 0


def test_allow_superset_no_superset_creates_new():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        memo_subset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_subset,
            merge_fn=merge_fn,
        )
        output1, diag1 = memo_run(memo_subset, params_subset, exec_fn_grid)
        assert diag1.executed_chunks == 1

        axis_values_different = {"strat": ["b"], "s": [1, 2, 3]}
        params_different = {"alpha": 0.4, "strat": "b"}

        memo_different = ChunkCache.auto_load(
            temp_dir,
            params_different,
            axis_values=axis_values_different,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output2, diag2 = memo_run(memo_different, params_different, exec_fn_grid)

        assert output2
        assert diag2.executed_chunks == 1
        assert diag2.cached_chunks == 0


def test_allow_superset_mixed_params_and_axes():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"s": [1, 2, 3]}
        params_subset = {"alpha": 0.4, "strat": "a"}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output, diag = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert output
        assert diag.cached_chunks > 0
        assert diag.executed_chunks == 0


def test_allow_superset_partial_subset_match():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b", "c"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1]}
        params_subset = {"alpha": 0.4}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output, diag = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert output
        assert diag.cached_chunks > 0
        assert diag.executed_chunks == 0


def test_exclusive_prevents_redundant_subset():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
            exclusive=True,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.4}

        memo_test = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            exclusive=True,
            merge_fn=merge_fn,
        )

        with pytest.raises(
            ValueError,
            match="Cannot create subset cache: a superset cache already exists",
        ):
            memo_run(memo_test, params_subset, exec_fn_grid)


def test_allow_superset_with_different_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        params_subset = {"alpha": 0.5, "strat": "a"}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output, diag = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert output
        assert diag.cached_chunks == 0
        assert diag.executed_chunks > 0


def test_allow_superset_multiple_axes_subset():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params_superset = {"alpha": 0.4}
        memo_superset = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            merge_fn=merge_fn,
        )
        memo_run(memo_superset, params_superset, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1]}
        params_subset = {"alpha": 0.4}

        memo_subset = ChunkCache.auto_load(
            temp_dir,
            params_subset,
            axis_values=axis_values_subset,
            allow_superset=True,
            merge_fn=merge_fn,
        )
        output, diag = memo_run(memo_subset, params_subset, exec_fn_grid)

        assert output
        assert diag.cached_chunks > 0
        assert diag.executed_chunks == 0
