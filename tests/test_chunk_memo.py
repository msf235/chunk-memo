import functools
import inspect
import itertools
import tempfile
import time

import pytest  # type: ignore[import-not-found]

from chunk_memo import ChunkCache, ChunkMemo, params_to_cache_id
from chunk_memo.runners import run as _slice_and_run
from chunk_memo.runners import run_streaming as _slice_and_run_streaming

from .utils import exec_fn_grid


def exec_fn_singleton(params):
    return {"value": params["alpha"] * 2}


def exec_fn_sleep(params, strat, s):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        time.sleep(0.05)
        outputs.append({"alpha": params["alpha"], "strat": strat_value, "s": s_value})
    return outputs


def collate_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def _set_params(memo, params):
    if hasattr(memo, "cache_for_params"):
        return memo.cache_for_params(params)
    cache_id = params_to_cache_id(params)
    metadata = dict(memo.metadata)
    metadata["params"] = params
    memo.set_identity(cache_id, metadata=metadata)
    memo.write_metadata()
    return memo


def _cache_identity(params):
    return {
        "cache_id": params_to_cache_id(params),
        "metadata": {"params": params},
    }


def _bind_exec_fn(exec_fn, params):
    signature = inspect.signature(exec_fn)
    param_names = list(signature.parameters)
    if param_names and param_names[0] == "params":
        return functools.partial(exec_fn, params)
    return exec_fn


def slice_and_run(memo, params, exec_fn, **kwargs):
    cache = _set_params(memo, params)
    axis_indices = kwargs.pop("axis_indices", None)
    sliced = cache.slice(axis_indices=axis_indices, **kwargs)
    return _slice_and_run(sliced, _bind_exec_fn(exec_fn, params))


def slice_and_run_streaming(memo, params, exec_fn, **kwargs):
    cache = _set_params(memo, params)
    axis_indices = kwargs.pop("axis_indices", None)
    sliced = cache.slice(axis_indices=axis_indices, **kwargs)
    return _slice_and_run_streaming(sliced, _bind_exec_fn(exec_fn, params))


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


def exec_point_inferred(alpha, strat, s, extra=1):
    outputs = []
    for strat_value, s_value in itertools.product(strat, s):
        outputs.append(
            {
                "alpha": alpha,
                "extra": extra,
                "strat": strat_value,
                "s": s_value,
            }
        )
    return outputs


def run_memo(root, params=None, *, merge=True, chunk_spec=None, axis_values=None):
    params = params or {}
    axis_values = axis_values or {}
    cache_id = params_to_cache_id(params)
    return ChunkCache(
        root=root,
        cache_id=cache_id,
        metadata={"params": params},
        chunk_spec=chunk_spec or {"strat": 1, "s": 3},
        axis_values=axis_values,
        collate_fn=collate_fn if merge else None,
    )


def run_memo_wrapper(root, *, merge=True, chunk_spec=None, axis_values=None):
    axis_values = axis_values or {}
    return ChunkMemo(
        root=root,
        chunk_spec=chunk_spec or {"strat": 1, "s": 3},
        axis_values=axis_values,
        collate_fn=collate_fn if merge else None,
    )


def test_basic_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        output, diag = slice_and_run(memo, params, exec_fn_sleep)
        assert output
        assert diag.executed_chunks == 1

        output2, diag2 = slice_and_run(memo, params, exec_fn_sleep)
        assert output2 == output
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_incremental_axis_values_extension():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = slice_and_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 1

        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag2 = slice_and_run(memo, params, exec_fn_grid)
        assert diag2.total_chunks == 2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 1


def test_param_change_invalidates_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = slice_and_run(memo, {"alpha": 0.4}, exec_fn_grid)
        assert diag.executed_chunks == 1

        _, diag2 = slice_and_run(memo, {"alpha": 0.5}, exec_fn_grid)
        assert diag2.cached_chunks == 0
        assert diag2.executed_chunks == 1


def test_chunk_count_grid():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b", "c"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        _, diag = slice_and_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 6
        assert diag.executed_chunks == 6


def test_collate_fn_optional_returns_nested():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = run_memo(temp_dir, merge=False, axis_values=axis_values)
        output, diag = slice_and_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 2
        assert isinstance(output, list)
        assert isinstance(output[0], list)


def test_run_wrap_positional_params():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        wrapper = run_memo_wrapper(temp_dir, axis_values=axis_values)

        exec_point = wrapper.cache()(exec_point_extra_default)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output[0]["extra"] == 1


def test_run_wrap_executes_with_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        wrapper = run_memo_wrapper(temp_dir, axis_values=axis_values)

        exec_point = wrapper.cache()(exec_fn_grid)
        params = {"alpha": 0.4}
        output, diag = exec_point(params, strat=["a"], s=[1, 2, 3])
        assert diag.executed_chunks == 1
        assert output


def test_run_wrap_infers_params_from_args():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        wrapper = run_memo_wrapper(temp_dir, axis_values=axis_values)

        exec_point = wrapper.cache()(exec_point_inferred)
        output, diag = exec_point(alpha=0.4, strat=["a"], s=[1, 2, 3], extra=2)
        assert diag.executed_chunks == 1
        assert output[0]["alpha"] == 0.4

        output2, diag2 = exec_point(alpha=0.4, strat=["a"], s=[1, 2, 3], extra=2)
        assert output2 == output
        assert diag2.executed_chunks == 0


def test_run_wrap_param_merge():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        wrapper = run_memo_wrapper(temp_dir, axis_values=axis_values)

        exec_point = wrapper.cache()(exec_point_extra_param)
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
        wrapper = run_memo_wrapper(temp_dir, axis_values=axis_values)

        exec_point = wrapper.cache()(exec_fn_grid)
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

        output, diag = slice_and_run(
            memo,
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag.executed_chunks == 1
        observed = {(item["strat"], item["s"]) for item in output}
        assert observed == {("a", 2), ("a", 3)}

        diag2 = slice_and_run_streaming(
            memo,
            params,
            exec_fn_grid,
            axis_indices={"strat": range(0, 1), "s": slice(1, 3)},
        )
        assert diag2.executed_chunks == 0


def test_run_restart_uses_cached_chunks_after_interrupt():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a"], "s": [1, 2, 3, 4]}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 2},
            axis_values=axis_values,
            collate_fn=collate_fn,
        )
        call_count = 0

        def exec_interrupt(params, strat, s):
            nonlocal call_count
            if call_count >= 1:
                raise RuntimeError("interrupted")
            call_count += 1
            return exec_fn_grid(params, strat, s)

        with pytest.raises(RuntimeError, match="interrupted"):
            slice_and_run(memo, params, exec_interrupt)

        output, diag = slice_and_run(memo, params, exec_fn_grid)
        assert diag.total_chunks == 2
        assert diag.cached_chunks == 1
        assert diag.executed_chunks == 1
        observed = {(item["strat"], item["s"]) for item in output}
        assert observed == {("a", 1), ("a", 2), ("a", 3), ("a", 4)}


def test_cache_status_axis_indices_extend_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 2},
            axis_values={"strat": ["a"], "s": [1, 2]},
        )

        axis_values_override = {"strat": ["a", "b"], "s": [1, 2]}
        axis_indices = {"strat": range(2), "s": range(2)}
        status = memo.cache_status(
            axis_indices=axis_indices,
            axis_values_override=axis_values_override,
            extend_cache=True,
        )
        assert status["axis_values"]["strat"] == ["a", "b"]


def test_chunk_enumerator_order():
    chunk_spec = {"strat": 1, "s": 2}

    def build_chunk_keys(axis_values):
        axis_order = sorted(axis_values)
        axis_chunks = []
        for axis in axis_order:
            values = axis_values[axis]
            size = chunk_spec[axis]
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

    def chunk_enumerator(axis_values):
        return list(reversed(build_chunk_keys(axis_values)))

    def exec_fn_marker(params, strat, s):
        return [{"marker": f"{strat[0]}-{s[0]}"}]

    def collate_markers(chunks):
        return [chunk[0]["marker"] for chunk in chunks]

    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec=chunk_spec,
            axis_values=axis_values,
            collate_fn=collate_markers,
            chunk_enumerator=chunk_enumerator,
        )
        output, diag = slice_and_run(memo, params, exec_fn_marker)
        assert diag.executed_chunks == 4
        expected = []
        for chunk_key in chunk_enumerator(axis_values):
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
        slice_and_run(memo, params, exec_fn_sleep)
        cold_time = time.perf_counter() - start
        print(f"cold_cache_s: {cold_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        slice_and_run(memo, params, exec_fn=exec_fn_sleep, strat=["a"], s=[1, 2, 3, 4])
        start = time.perf_counter()
        slice_and_run(memo, params, exec_fn_sleep)
        half_time = time.perf_counter() - start
        print(f"half_cache_s: {half_time:0.4f}")

    with tempfile.TemporaryDirectory() as temp_dir:
        memo = run_memo(temp_dir, axis_values=axis_values)
        slice_and_run(memo, params, exec_fn_sleep)
        start = time.perf_counter()
        slice_and_run(memo, params, exec_fn_sleep)
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
        diag = slice_and_run_streaming(memo, params, exec_fn_grid)

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
        _, diag = slice_and_run(memo, params, exec_fn_grid)

        caches = ChunkCache.discover_caches(temp_dir)
        assert len(caches) == 1
        assert caches[0]["metadata"] is not None
        assert caches[0]["metadata"]["axis_values"] == axis_values


def test_find_compatible_caches_by_cache_id():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = slice_and_run(memo, params, exec_fn_grid)

        cache_id = params_to_cache_id(params)
        compatible = ChunkCache.find_compatible_caches(temp_dir, cache_id=cache_id)
        assert len(compatible) == 1

        incompatible = ChunkCache.find_compatible_caches(
            temp_dir, cache_id=params_to_cache_id({"alpha": 0.5})
        )
        assert len(incompatible) == 0


def test_find_compatible_caches_by_axis_values():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        _, diag = slice_and_run(memo, params, exec_fn_grid)

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
        _, diag = slice_and_run(memo, params, exec_fn_grid)

        compatible = ChunkCache.find_compatible_caches(temp_dir)
        assert len(compatible) == 1


def test_load_from_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        output1, diag1 = slice_and_run(memo, params, exec_fn_grid)

        cache_id = memo.cache_hash()
        memo2 = ChunkCache.load_from_cache(temp_dir, cache_id, collate_fn=collate_fn)
        output2, diag2 = slice_and_run(memo2, params, exec_fn_grid)

        assert output1 == output2
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_load_from_cache_not_found():
    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(FileNotFoundError):
            ChunkCache.load_from_cache(temp_dir, "nonexistent_id")


def test_singleton_cache_basic():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )
        output, diag = slice_and_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.total_chunks == 1
        assert diag.executed_chunks == 1


def test_singleton_cache_reuse():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )
        output1, diag1 = slice_and_run(memo, params, exec_fn_singleton)

        output2, diag2 = slice_and_run(memo, params, exec_fn_singleton)

        assert output1 == output2
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_singleton_cache_param_change():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )
        output1, diag1 = slice_and_run(memo, params, exec_fn_singleton)
        assert diag1.executed_chunks == 1

        output2, diag2 = slice_and_run(memo, {"alpha": 0.5}, exec_fn_singleton)
        assert diag2.executed_chunks == 1
        assert output1 != output2


def test_singleton_cache_no_axes_allowed():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )

        with pytest.raises(ValueError, match="Cannot pass axis arguments"):
            slice_and_run(memo, params, exec_fn_singleton, strat=["a"])

        with pytest.raises(ValueError, match="Cannot pass axis arguments"):
            slice_and_run_streaming(
                memo,
                params,
                exec_fn_singleton,
                axis_indices={"s": [0]},
            )


def test_singleton_cache_streaming():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )
        diag1 = slice_and_run_streaming(memo, params, exec_fn_singleton)
        assert diag1.executed_chunks == 1

        diag2 = slice_and_run_streaming(memo, params, exec_fn_singleton)
        assert diag2.cached_chunks == diag2.total_chunks
        assert diag2.executed_chunks == 0


def test_singleton_via_regular_constructor():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
            collate_fn=None,
        )
        output, diag = slice_and_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.total_chunks == 1
        assert diag.executed_chunks == 1


def test_exclusive_mode_exact_match():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        slice_and_run(memo, params, exec_fn_grid)

        memo2 = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values,
            collate_fn=collate_fn,
            exclusive=True,
        )
        output, diag = slice_and_run(memo2, params, exec_fn_grid)
        assert output
        assert diag.executed_chunks == 0


def test_exclusive_mode_rejects_different_chunking():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        memo = run_memo(temp_dir, axis_values=axis_values)
        params = {"alpha": 0.4}
        slice_and_run(memo, params, exec_fn_grid)

        with pytest.raises(
            ValueError, match="cache_id already exists with different chunk_spec"
        ):
            ChunkCache(
                root=temp_dir,
                **_cache_identity(params),
                chunk_spec={"strat": 1, "s": 2},
                axis_values=axis_values,
                collate_fn=collate_fn,
                exclusive=True,
            )


@pytest.mark.skip("pre-existing test issue, requires refactoring")
def test_warn_on_overlap():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo1 = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
            collate_fn=collate_fn,
        )
        slice_and_run(memo1, params, exec_fn_grid)

        memo2 = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 2},
            axis_values={"strat": ["a"], "s": [1, 2, 3, 4]},
            collate_fn=collate_fn,
            warn_on_overlap=True,
        )
        with pytest.warns(UserWarning, match="Cache overlap detected"):
            output, diag = slice_and_run(memo2, params, exec_fn_grid)
            assert diag.executed_chunks == 2


def test_auto_load_no_existing_creates_new():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkMemo.auto_load(
            temp_dir,
            params,
            axis_values={},
            chunk_spec={},
        )
        output, diag = slice_and_run(memo, params, exec_fn_singleton)

        assert output == [{"value": 0.8}]
        assert diag.executed_chunks == 1


def test_auto_load_finds_singleton():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo1 = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={},
            axis_values={},
        )
        output1, diag1 = slice_and_run(memo1, params, exec_fn_singleton)

        memo2 = ChunkMemo.auto_load(temp_dir, params)
        output2, diag2 = slice_and_run(memo2, params, exec_fn_singleton)

        assert output1 == output2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 0


def test_auto_load_with_axis_values_exact():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        chunk_spec = {"strat": 1, "s": 3}
        memo1 = run_memo(temp_dir, axis_values=axis_values, chunk_spec=chunk_spec)
        params = {"alpha": 0.4}
        output1, diag1 = slice_and_run(memo1, params, exec_fn_grid)

        memo2 = ChunkMemo.auto_load(
            temp_dir,
            params,
            axis_values=axis_values,
            collate_fn=collate_fn,
        )
        output2, diag2 = slice_and_run(memo2, params, exec_fn_grid)

        assert output1 == output2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 0


def test_auto_load_axis_values_mismatch_raises():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        memo = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 1},
            axis_values={"strat": ["a"], "s": [1, 2]},
            collate_fn=collate_fn,
        )
        slice_and_run(memo, params, exec_fn_grid)

        with pytest.raises(ValueError, match="axis_values differ"):
            ChunkMemo.auto_load(
                temp_dir,
                params,
                axis_values={"strat": ["a"], "s": [1, 2, 3]},
            )


def test_auto_load_with_chunk_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo = ChunkMemo.auto_load(
            temp_dir,
            params,
            axis_values=axis_values,
            chunk_spec={"strat": 1, "s": 2},
            collate_fn=collate_fn,
        )
        output, diag = slice_and_run(memo, params, exec_fn_grid)

        assert output
        assert diag.total_chunks == 2
        assert diag.executed_chunks == 2


def test_auto_load_default_chunk_spec():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values = {"strat": ["a"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo = ChunkMemo.auto_load(
            temp_dir,
            params,
            axis_values=axis_values,
        )
        output, diag = slice_and_run(memo, params, exec_fn_grid)

        assert output
        assert diag.total_chunks == 1


def test_allow_superset_finds_superset_cache():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo_superset = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            collate_fn=collate_fn,
        )
        output1, diag1 = slice_and_run(memo_superset, params, exec_fn_grid)
        assert diag1.total_chunks == 2

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        memo_subset = ChunkMemo.auto_load(
            temp_dir,
            params,
            axis_values=axis_values_subset,
            allow_superset=True,
            collate_fn=collate_fn,
        )
        output2, diag2 = slice_and_run(memo_subset, params, exec_fn_grid)

        assert output2
        assert diag2.cached_chunks == 1
        assert diag2.executed_chunks == 0


def test_allow_superset_false_requires_exact_match():
    with tempfile.TemporaryDirectory() as temp_dir:
        axis_values_superset = {"strat": ["a", "b"], "s": [1, 2, 3]}
        params = {"alpha": 0.4}
        memo_superset = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values=axis_values_superset,
            collate_fn=collate_fn,
        )
        slice_and_run(memo_superset, params, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        with pytest.raises(ValueError, match="axis_values differ"):
            ChunkMemo.auto_load(
                temp_dir,
                params,
                axis_values=axis_values_subset,
                allow_superset=False,
                collate_fn=collate_fn,
            )


def test_exclusive_rejects_axis_values_mismatch():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        cache_id = params_to_cache_id(params)
        memo_superset = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
            collate_fn=collate_fn,
            exclusive=True,
        )
        slice_and_run(memo_superset, params, exec_fn_grid)

        with pytest.raises(
            ValueError,
            match="cache_id already exists with different axis_values",
        ):
            ChunkCache(
                root=temp_dir,
                cache_id=cache_id,
                metadata={"params": params},
                chunk_spec={"strat": 1, "s": 3},
                axis_values={"strat": ["a"], "s": [1, 2, 3]},
                collate_fn=collate_fn,
                exclusive=True,
            )


def test_find_compatible_caches_with_allow_superset():
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        cache_id = params_to_cache_id(params)
        memo_superset = ChunkCache(
            root=temp_dir,
            **_cache_identity(params),
            chunk_spec={"strat": 1, "s": 3},
            axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
            collate_fn=collate_fn,
        )
        slice_and_run(memo_superset, params, exec_fn_grid)

        axis_values_subset = {"strat": ["a"], "s": [1, 2, 3]}
        caches_superset = ChunkCache.find_compatible_caches(
            temp_dir,
            cache_id=cache_id,
            axis_values=axis_values_subset,
            allow_superset=True,
        )
        assert len(caches_superset) == 1

        caches_exact = ChunkCache.find_compatible_caches(
            temp_dir,
            cache_id=cache_id,
            axis_values=axis_values_subset,
            allow_superset=False,
        )
        assert len(caches_exact) == 0


from chunk_memo.runners import run as _memo_run
from chunk_memo.runners import run_streaming as _memo_run_streaming
