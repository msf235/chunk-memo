#!/usr/bin/env python3
"""
Example demonstrating callable axis_values for memory efficiency.

This shows how to use index-based functions for axis_values instead of
loading full lists into memory at initialization.
"""

import tempfile

from shard_memo import ChunkCache, run


def make_lazy_axis_values():
    """
    Returns axis_values dict with callable index functions instead of full lists.

    Each callable takes an index and returns the value at that index.
    This allows lazy loading - values are only computed when needed.
    """

    def s_values(idx):
        """Lazy access to 's' axis values."""
        values = [1, 2, 3, 4]
        return values[idx]

    def strat_values(idx):
        """Lazy access to 'strat' axis values."""
        values = ["a", "b"]
        return values[idx]

    return {
        "s": s_values,
        "strat": strat_values,
    }


def example_with_lists():
    """Traditional approach - full lists in memory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = {"strat": ["a", "b"], "s": [1, 2, 3, 4]}
        cache_chunk_spec = {"strat": 1, "s": 2}

        def exec_fn(params, strat, s):
            return [{"result": f"{strat[0]}-{s[0]}"}]

        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values,
            merge_fn=lambda chunks: [c[0]["result"] for c in chunks],
        )

        memo.set_params(params)
        output, diag = run(memo, exec_fn)
        print(f"Lists approach: {output}")
        print(f"  Executed: {diag.executed_chunks}, Cached: {diag.cached_chunks}")


def example_with_callables():
    """Memory-efficient approach - lazy loading with callables."""
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        axis_values = make_lazy_axis_values()
        cache_chunk_spec = {"strat": 1, "s": 2}

        def exec_fn(params, strat, s):
            return [{"result": f"{strat[0]}-{s[0]}"}]

        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values,
            merge_fn=lambda chunks: [c[0]["result"] for c in chunks],
        )

        memo.set_params(params)
        output, diag = run(memo, exec_fn)
        print(f"Callables approach: {output}")
        print(f"  Executed: {diag.executed_chunks}, Cached: {diag.cached_chunks}")


def example_mixed():
    """Mixed approach - some axes use lists, others use callables."""
    with tempfile.TemporaryDirectory() as temp_dir:
        params = {"alpha": 0.4}
        # 'strat' uses a list (small, in memory)
        # 's' uses a callable (potentially large, lazy loaded)
        axis_values = {
            "strat": ["a", "b"],
            "s": lambda idx: [1, 2, 3, 4][idx],
        }
        cache_chunk_spec = {"strat": 1, "s": 2}

        def exec_fn(params, strat, s):
            return [{"result": f"{strat[0]}-{s[0]}"}]

        memo = ChunkCache(
            cache_root=temp_dir,
            cache_chunk_spec=cache_chunk_spec,
            axis_values=axis_values,
            merge_fn=lambda chunks: [c[0]["result"] for c in chunks],
        )

        memo.set_params(params)
        output, diag = run(memo, exec_fn)
        print(f"Mixed approach: {output}")
        print(f"  Executed: {diag.executed_chunks}, Cached: {diag.cached_chunks}")


if __name__ == "__main__":
    print("=== ChunkCache Callable Axis Values Example ===\n")

    print("1. Traditional approach with lists:")
    example_with_lists()
    print()

    print("2. Memory-efficient approach with callables:")
    example_with_callables()
    print()

    print("3. Mixed approach (lists + callables):")
    example_mixed()
