# chunk-memo

chunk-memo provides chunked memoization for grid-style parameter sweeps. You define a
parameter grid (axis values) and a partitioning of this grid into chunks for disk efficiency.
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



```python
memo = ChunkMemo.auto_load(
    root,
    params=params_dict,
    axis_values=axis_values_dict,
    chunk_spec=chunk_spec_dict,
    collate_fn=collate_fn,
    chunk_enumerator=chunk_enumerator,
    chunk_hash_fn=chunk_hash_fn,
    version="v1",
    axis_order=axis_order,
    verbose=1,
    profile=False,
    exclusive=False,
    warn_on_overlap=False,
    allow_superset=False,
)
cache = memo.cache_for_params(params_dict)
```

Streamlined memoization that finds or creates a cache. Behavior:

- If `axis_values` is provided and `allow_superset=False` (default): finds an
  exact match (same derived `cache_id` + `axis_values`), or creates a new cache
  with specified (or default) `chunk_spec`.
- If `axis_values` is provided and `allow_superset=True`: finds an exact match
  or finds a superset cache that contains all requested data.
- If `axis_values` is not provided: loads the cache with matching derived
  `cache_id` if it exists.
- When creating a new cache without `chunk_spec`, defaults to chunk size 1
  for all axes.

## Parallel runners

Parallel runners are independent from memoization. They consume cache metadata
and delegate missing work to a user-supplied map function, while already-cached
chunks are handled locally. The public functions are re-exported from
`chunk_memo` for compatibility and implemented in `chunk_memo/runners.py` and
`chunk_memo/runners_parallel.py`.

## Streamlined memoization

### auto_load (module function)

```python
from chunk_memo import auto_load


memo = auto_load(
    root="cache_dir",
    params=params,
    axis_values=axis_values_dict,
    chunk_spec=chunk_spec_dict,
    collate_fn=collate_fn,
    exclusive=False,
    warn_on_overlap=False,
    allow_superset=False,
)
```

Convenience wrapper for `ChunkMemo.auto_load()`. Returns a `ChunkMemo` manager
that finds an existing cache or creates a new one based on params and
`axis_values`.

Use cases:

- Quick start: let the library find or create an appropriate cache.
- Exclusive workflow: prevent duplicate caches with `exclusive=True`. Also
  prevents creating subset/superset caches when overlapping data exists.
- Flexible chunking: `auto_load` handles different `chunk_spec` values for
  the same data.
- Subset detection: use `allow_superset=True` to find and reuse existing caches
  that are supersets of your requested data.

```python
import functools

from chunk_memo import run_parallel


parallel_cache = memo.cache_for_params(params).slice(
    axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
)

parallel_output, parallel_diag = run_parallel(
    items,
    exec_fn=functools.partial(exec_fn, params),
    cache=parallel_cache,
)
```

Parallel runner notes:

- `run_parallel` expects a cache already sliced to the desired axis subset.
- You can pass explicit kwargs to override cache methods.
- Missing items are executed via `map_fn` (defaults to a `ProcessPoolExecutor`).
- Cached chunks are loaded locally, with partial reuse when `items` is a subset.
- Use `flush_on_chunk=True` and `return_output=False` to stream payloads to disk
  without retaining outputs in memory.
- Use `extend_cache=True` to grow a cache's `axis_values` in-place when items
  include values outside the current cache.

Item formats for `run_parallel`:

- Mapping items: each item is a dict with keys for every axis name (in any
  order).
- Positional items: each item is a tuple/list ordered by `axis_order`.
- Single-axis shorthand: if there is exactly one axis, a scalar item is allowed.

### run_parallel

```python
run_parallel(
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache: CacheProtocol,
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
    flush_on_chunk: bool = False,
    return_output: bool = True,
    extend_cache: bool = False,
    # Manual cache methods (optional overrides)
    write_metadata: Callable[[], Path] | None = None,
    chunk_hash: Callable[[ChunkKey], str] | None = None,
    resolve_cache_path: Callable[[ChunkKey, str], Path] | None = None,
    load_payload: Callable[[Path], dict[str, Any] | None] | None = None,
    write_chunk_payload: Callable[..., Path] | None = None,
    update_chunk_index: Callable[[str, ChunkKey], None] | None = None,
    load_chunk_index: Callable[[], dict[str, Any] | None] | None = None,
    build_item_maps_from_axis_values: Callable[..., Any] | None = None,
    build_item_maps_from_chunk_output: Callable[..., Any] | None = None,
    reconstruct_output_from_items: Callable[..., Any] | None = None,
    collect_chunk_data: Callable[..., Any] | None = None,
    item_hash: Callable[[ChunkKey, Tuple[Any, ...]], str] | None = None,
    context: RunnerContext | None = None,
) -> tuple[Any, Diagnostics]
```

## Caching behavior

- Each chunk file is named by a hash of `(cache_id, chunk_key, version)`.
- Cache files live under a memo directory named by `cache_id`, with a
  `metadata.json` file for memo-level context.
- Cache files include a `spec` payload keyed by item hash, describing per-item
  axis values.
- Chunk-level timestamps live in `chunks_index.json` under the memo directory.
- Chunks are sharded for disk efficiency, but lookups can return subsets of a
  chunk when you request fewer axis values.
- Changing axis values creates new chunks automatically.
- Adding values to a list reuses existing chunks and computes only new ones.
- Partial chunk payloads are allowed. Runners return whatever cached outputs are
  available and increment `diagnostics.partial_chunks`.

## Defaults and ordering

- `root` defaults to `.chunk_memo` under the current working directory.
- When not provided, `axis_order` defaults to lexicographic order of axis names.
- If `chunk_spec` is omitted, each axis defaults to a chunk size of 1. For
  list/tuple axes this is overridden to the full axis length; other iterables
  still default to 1.

## Examples

### Subset detection and cache reuse

You can define caches with broad axis coverage and then find them for more
specific requests using `allow_superset=True`:

```python
import functools

from chunk_memo import ChunkCache, params_to_cache_id, run, run_streaming


# Create a broad cache with strat=["a", "b"]
broad_cache = ChunkCache(
    root="./memo_cache",
    cache_id=params_to_cache_id({"alpha": 0.4}),
    metadata={"params": {"alpha": 0.4}},
    chunk_spec={"strat": 1, "s": 3},
    axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
    collate_fn=collate_fn,
)
params = {"alpha": 0.4}
cache_id = params_to_cache_id(params)
broad_cache.set_identity(cache_id, metadata={"params": params})
output1, diag1 = run(broad_cache, functools.partial(exec_fn, params))

# Executes all 6 points: (a,1), (a,2), (a,3), (b,1), (b,2), (b,3)

# Later, request just strat="a" - will find and reuse the broad cache
from chunk_memo import auto_load


memo_a = auto_load(
    root="./memo_cache",
    params=params,
    axis_values={"strat": ["a"]},  # Subset of original
    allow_superset=True,  # Enable superset detection
    collate_fn=collate_fn,
)
cache_a = memo_a.cache_for_params(params)
output2, diag2 = run(cache_a, functools.partial(exec_fn, params))

# Only executes the 3 points for strat="a", reuses cached data
assert diag2.cached_chunks == 1
assert diag2.executed_chunks == 0
```

### Flexible axis/param mapping

`cache_id` is derived from your params, so changing params changes the cache
identity. If you want axis reductions to share a cache, keep params stable and
use `axis_values` subsets with `allow_superset=True`.

## Running examples

Run the basic example script:

```bash
python examples/basic.py
```

## Notes

- The exec function receives axis vectors for the current memo chunk.
- Memoization and parallel execution are decoupled so you can introduce new
  executors without changing `ChunkCache`.

### ChunkCache

`ChunkCache` holds cache configuration, metadata, and chunk planning. It does
not execute runs directly; use `run` / `run_streaming` from `chunk_memo`.

```python
import functools

from chunk_memo import ChunkCache, run


cache = ChunkCache(
    root="./cache",
    cache_id=params_to_cache_id(params),
    metadata={"params": params},
    chunk_spec={"strat": 1, "s": 2},
    axis_values={"strat": ["a", "b"], "s": [1, 2, 3, 4]},
)
cache.set_identity(params_to_cache_id(params), metadata={"params": params})
output, diag = run(cache, functools.partial(exec_fn, params))
```
