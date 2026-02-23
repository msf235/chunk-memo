# chunk-memo

chunk-memo provides chunked memoization for grid-style parameter sweeps. You define a
parameter grid (axis values) and a partitioning of this grid into chunks for disk read/write efficiency.
Cached chunk outputs are reused on subsequent runs (memoization).
Parallel runners are provided that allow you to pass your own map function (such as
ProcessPoolExecutor.map).

## What problem does it solve?

Suppose you have:

- Fixed parameters (values that do not vary across the grid).
- Axis values (lists of values you want to sweep over).

You want to:

1. Evaluate a function across the grid.
2. Cache results so subsequent runs only compute what is new.
3. Chunk outputs into reasonable file sizes, without losing the ability to load
   arbitrary subsets of parameter values.

chunk-memo breaks the grid into memo chunks and stores each chunk on disk. When
axis values change, only the new chunks are computed.

## Concepts

- Point: one combination of axis values (e.g., `(strat="a", s=2)`).
- Axis values: a dictionary of lists defining the grid, e.g.
  `{ "strat": ["a", "b"], "s": [1, 2, 3] }`.
- Memo chunk: a block of points created by chunking each axis list and taking
  the cartesian product of those bins. Each memo chunk is written to a single
  file that can serve partial reads for subsets of points.

## Installation

pip install memo-chunk

## Quick start

```python
from chunk_memo import ChunkMemo

axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}

memo = ChunkMemo(
    root="./memo_cache",
    chunk_spec={"strat": 1, "s": 3},
    axis_values=axis_values,
)

@memo.cache()
def foo(alpha, strat, s):
    outputs = []
    for strat_value in strat:
        for s_value in s:
            outputs.append(
                {"alpha": alpha, "strat": strat_value, "s": s_value, "value": len(strat_value) + alpha*s_value}
            )
    return outputs

output, diag = foo(alpha=0.5, strat=["aaa", "bb"], s=[1, 2])
print(output)
print(diag)
```

Pass a value for `max_workers` that is larger than 1 to `memo.cache` to get parallel execution.
Note that when using parallelization the wrapped function must be defined at the highest scope level (module scope).

## Module layout

- `chunk_memo/cache.py`: cache logic (`ChunkCache`).
- `chunk_memo/runners.py`: serial runner.
- `chunk_memo/runners_parallel.py`: parallel runner.
- `chunk_memo/runners_common.py`: shared runner utilities and planning helpers.

## API overview

### ChunkCache

`ChunkCache` owns the cache state and exposes the cache interface.
`ChunkMemo` manages one or more `ChunkCache` instances keyed by memoization params.

```python
ChunkCache(
    root: str | Path,
    cache_id: str,
    metadata: dict[str, Any] | None = None,
    chunk_spec: dict[str, int | dict],
    axis_values: dict[str, Any],
    collate_fn: Callable[[list], Any] | None = None,
    chunk_enumerator: Callable[[dict], Sequence[tuple]] | None = None,
    chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
    path_fn: Callable[[dict, tuple, str, str], Path | str] | None = None,
    version: str = "v1",
    axis_order: Sequence[str] | None = None,
    verbose: int = 1,
    exclusive: bool = False,
    warn_on_overlap: bool = False,
)
```

### ChunkMemo

`ChunkMemo` is a cache manager that provides `cache` and `stream_cache`
decorators for memoization. A stream_cache flushes data to disk as
the function is executed.

### axis_values: lists and iterables

`axis_values` must be concrete iterables (lists, tuples, ranges, or other
iterable objects). Callables are not supported by the current implementation.

Ordering rules:

- If the value is a sequence (list/tuple), order is preserved.
- If the value is a non-sequence iterable, values are materialized and sorted by
  a stable serialization of each value. This makes order deterministic but may
  differ from the original iteration order.

Axis values must be hashable and unique within each axis. Duplicates will
collapse to a single entry in the internal index map.

### run_chunks / run_chunks_streaming (low-level)

These are low-level helpers that expect a cache object and a list of chunk keys.
The cache should already represent the desired axis subset (use `memo.slice(...)`
first if needed). You can pass `collate_fn` to override the cache-level
`collate_fn` for the duration of the call.


