# shard-memo

shard-memo provides sharded memoization for grid-style experiments. You define a
parameter grid (split variables), the library partitions it into reusable memo
chunks for disk efficiency, and cached chunk outputs are reused on subsequent
runs, including partial reuse for smaller parameter selections. Memoization and
parallel execution are independent: memoization lives in `ShardMemoCache`, while
runner helpers (serial and parallel) consume cache metadata and execute missing
work.

## What problem does it solve?

Suppose you have:

- Fixed parameters (values that do not vary across the grid).
- Split variables (lists of values you want to sweep over).

You want to:

1. Evaluate a function across the grid.
2. Cache results so subsequent runs only compute what is new.
3. Shard outputs into reasonable file sizes, without losing the ability to load
   arbitrary subsets of parameter values.

shard-memo breaks the grid into memo chunks and stores each chunk on disk. When
split values change, only the new chunks are computed.

## Concepts

- Point: one combination of split values (e.g., `(strat="a", s=2)`).
- Split spec: a dictionary of lists defining the grid,
  e.g. `{ "strat": ["a", "b"], "s": [1, 2, 3] }`.
- Memo chunk: a block of points created by chunking each axis list and taking the
  cartesian product of those bins. Each memo chunk is written to a single file
  that can serve partial reads for subsets of points.

## Installation

Local install from the repo:

```bash
pip install -e .
```

## Quick start

```python
from shard_memo import ShardMemo
from shard_memo.runners import run


def exec_fn(params, strat, s):
    outputs = []
    for strat_value in strat:
        for s_value in s:
            outputs.append(
                {"strat": strat_value, "s": s_value, "value": len(strat_value) + s_value}
            )
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


params = {"alpha": 0.4}
axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
memo = ShardMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    axis_values=axis_values,
    merge_fn=merge_fn,
)
output, diag = run(memo, params, exec_fn)
print(output)
print(diag)
```

## Module layout

- `shard_memo/memo.py`: cache logic (`ShardMemoCache`) and facade (`ShardMemo`).
- `shard_memo/runners.py`: execution runners (serial and parallel).
- `shard_memo/bridge.py`: removed (parallel runners live in `shard_memo.runners`).
- `shard_memo/__init__.py`: re-exports public APIs, including `auto_load()`.

## API overview

### ShardMemo

`ShardMemo` is a facade that owns a `ShardMemoCache` and exposes the cache
interface, including `run_wrap` and `streaming_wrap`.

```python
ShardMemo(
    cache_root: str | Path,
    memo_chunk_spec: dict[str, int | dict],
    axis_values: dict[str, Any],
    merge_fn: Callable[[list], Any] | None = None,
    memo_chunk_enumerator: Callable[[dict], Sequence[tuple]] | None = None,
    chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
    cache_path_fn: Callable[[dict, tuple, str, str], Path | str] | None = None,
    cache_version: str = "v1",
    axis_order: Sequence[str] | None = None,
    verbose: int = 1,
    exclusive: bool = False,
    warn_on_overlap: bool = False,
)
```

Notes:
- `memo_chunk_spec`: per-axis chunk sizes, e.g. `{"strat": 1, "s": 3}`.
- `exec_fn(params, **axes)`: chunk-level function; each axis receives a
  vector of values for that chunk. Supply it to `run` or use
  `run_wrap`/`streaming_wrap`.
- `merge_fn` defaults to returning a list of chunk outputs.
- `axis_values` defines the canonical grid for cache chunking.
- `cache_path_fn` can be used to place cache files in nested directories. Paths
  are resolved under a memo-specific cache directory.
  This hook is experimental and not yet thoroughly tested.
- `exclusive`: if True, error when creating a cache with same `params` and
  `axis_values` as an existing cache (different `memo_chunk_spec` still conflicts).
  Also prevents creating subset/superset caches when overlapping data exists.
- `warn_on_overlap`: if True, warn when caches have same `params` but partially
  overlapping `axis_values`.

### axis_values: Memory-efficient lazy loading

The `axis_values` parameter supports both traditional lists/tuples and **callable functions** for memory-efficient lazy loading.

#### Why use callables?

When working with very large datasets (millions of values), storing full lists in memory can be expensive. Callable axis_values enable:
- **Lazy loading**: Values are computed on-demand, not upfront
- **External data sources**: Load values from databases, files, or APIs as needed
- **Memory efficiency**: Only required values are loaded into memory at runtime

#### Callable patterns

**Index-based callable** (recommended for large datasets):
```python
axis_values = {
    "large_axis": lambda idx: expensive_data_source[idx],
}
```
The callable receives an index and returns the value at that index.

**List-returning callable** (useful when full iteration is needed):
```python
axis_values = {
    "small_axis": lambda: ["a", "b", "c"],
}
```
The callable takes no arguments and returns the full list.

#### Mixed approach (small lists + large callables)

You can mix both patterns for optimal memory usage:
```python
axis_values = {
    "small_axis": ["a", "b"],  # Small list - keep in memory
    "large_axis": lambda idx: large_db.query(idx),  # Large - lazy load
}
```

#### Usage example

```python
from shard_memo import ShardMemo

# Traditional approach - all values in memory
axis_values_lists = {
    "strat": ["a", "b"],
    "s": [1, 2, 3, 4],
}

# Memory-efficient approach - lazy loading
axis_values_callables = {
    "strat": lambda idx: ["a", "b"][idx],  # Could load from disk/DB
    "s": lambda idx: [1, 2, 3, 4][idx],
}

# Both work identically
memo = ShardMemo(
    cache_root="./cache",
    memo_chunk_spec={"strat": 1, "s": 2},
    axis_values=axis_values_callables,  # or axis_values_lists
    merge_fn=merge_fn,
)

output, diag = run(memo, params, exec_fn)
```

#### Important notes

- Callable axis_values are converted to a serializable representation for metadata
- Cache lookups and chunking still work identically regardless of list vs callable
- When using callables with external sources, ensure values are deterministic and consistent
- See `examples/callable_axis_values.py` for a complete working example

### run

```python
output, diagnostics = run(memo, params, exec_fn)
# Or run a subset
# output, diagnostics = run(memo, params, exec_fn, strat=["a"], s=[1, 2, 3])
# output, diagnostics = run(memo, params, exec_fn, axis_indices={"strat": range(0, 1), "s": slice(0, 3)})
```

Runs missing chunks, caches them, and returns merged output with diagnostics.

### run_streaming

```python
diagnostics = run_streaming(memo, params, exec_fn)
# Or run a subset
# diagnostics = run_streaming(memo, params, exec_fn, strat=["a"], s=[1, 2, 3])
# diagnostics = run_streaming(memo, params, exec_fn, axis_indices={"strat": range(0, 1), "s": slice(0, 3)})
```

Executes missing chunks and flushes them to disk without returning outputs.

### run_wrap (memoized wrapper)

```python
@memo.run_wrap()
def exec_point(params, strat, s, extra=1):
    ...

output, diag = exec_point(params, strat=["a"], s=[1, 2, 3], extra=2)
# Or by index
# output, diag = exec_point(params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}, extra=2)
```

- The wrapper accepts axis values directly (singletons or lists).
- You can also pass `axis_indices` (same keys as `axis_values`) with ints, ranges,
  or slices to select by index.
- Extra keyword arguments are merged into memoization params and also passed to
  the exec function.

### streaming_wrap (memoized streaming wrapper)

```python
@memo.streaming_wrap()
def exec_point(params, strat, s):
    ...

diagnostics = exec_point(params, strat=["a"], s=[1, 2, 3])
```

- Streaming wrappers return diagnostics only and write cache outputs to disk.

### cache_status

`cache_status` returns a structured view of cached vs missing chunks for a
selection of axes. It returns both the chunk keys and the corresponding index
ranges.

```python
status = exec_point.cache_status(
    params,
    axis_indices={"strat": range(0, 1), "s": slice(0, 3)},
    extra=2,
)

status["cached_chunks"]
status["cached_chunk_indices"]
status["missing_chunks"]
status["missing_chunk_indices"]
```

### Class methods (cache introspection)

#### discover_caches

```python
caches = ShardMemo.discover_caches(cache_root)
```

Lists all existing caches in `cache_root`. Returns a list of dictionaries with:
- `memo_hash`: The unique hash of the cache
- `path`: Path to the cache directory
- `metadata`: Full metadata dict if available, `None` otherwise

#### find_compatible_caches

```python
caches = ShardMemo.find_compatible_caches(
    cache_root,
    params=params_dict,
    axis_values=axis_values_dict,
    memo_chunk_spec=chunk_spec_dict,
    cache_version="v1",
    axis_order=None,
    allow_superset=False,
)
```

Find caches compatible with the given criteria. Matching rules:
- If a criterion is provided, it must match exactly (when `allow_superset=False`).
- If a criterion is omitted (`None`), it acts as a wildcard.
- When `allow_superset=True`, finds caches that are supersets of the requested configuration:
  - All requested axes are present in the cache's axes
  - All requested params match either the cache's params or corresponding axis values

Returns a list of compatible cache entries (same structure as `discover_caches`).

#### find_overlapping_caches

```python
overlaps = ShardMemo.find_overlapping_caches(
    cache_root,
    params=params_dict,
    axis_values=axis_values_dict,
)
```

Find caches that overlap with the given `params` and `axis_values`. Overlap means:
- Same `params`
- All shared axes have intersecting values

Returns a list of overlapping cache entries with an additional `overlap` key containing a dict of axis names to overlapping values.

#### load_from_cache

```python
memo = ShardMemo.load_from_cache(
    cache_root,
    memo_hash="abc123...",
    merge_fn=merge_fn,
    verbose=1,
    exclusive=False,
    warn_on_overlap=False,
)
```

Load a `ShardMemo` instance from an existing cache by its hash. Raises `FileNotFoundError` if the cache doesn't exist, or `ValueError` if metadata is invalid.

#### singleton cache

```python
memo = ShardMemo(
    cache_root,
    memo_chunk_spec={},
    axis_values={},
)
```

Create a singleton cache with no axes (empty `axis_values`). Useful for memoizing functions that depend only on `params`, not on split variables. Uses `memo_chunk_spec={}` and `axis_values={}`.

#### auto_load (ShardMemo classmethod)

```python
memo = ShardMemo.auto_load(
    cache_root,
    params=params_dict,
    axis_values=axis_values_dict,
    memo_chunk_spec=chunk_spec_dict,
    merge_fn=merge_fn,
    memo_chunk_enumerator=memo_chunk_enumerator,
    chunk_hash_fn=chunk_hash_fn,
    cache_path_fn=cache_path_fn,
    cache_version="v1",
    axis_order=axis_order,
    verbose=1,
    profile=False,
    exclusive=False,
    warn_on_overlap=False,
    allow_superset=False,
)
```

Streamlined memoization that finds or creates a cache. Behavior:
- If `axis_values` is provided and `allow_superset=False` (default): finds an exact match (same `params` + `axis_values`), or creates a new cache with specified (or default) `memo_chunk_spec`.
- If `axis_values` is provided and `allow_superset=True`: finds an exact match OR finds a superset cache that contains all requested data. A cache is a superset if:
  - All requested axes are present in the cache's axes
  - All requested params match either the cache's params or corresponding axis values
- If `axis_values` is not provided: finds caches with matching `params`. Requires exactly 1 match, or raises `ValueError` (ambiguous).
- When creating a new cache without `memo_chunk_spec`, defaults to chunk size 1 for all axes.

## Parallel runners

Parallel runners are independent from memoization. They consume cache metadata
and delegate missing work to a user-supplied map function, while already-cached
chunks are handled locally. The public functions are re-exported from
`shard_memo` for compatibility and implemented in `shard_memo.runners`.

## Streamlined memoization

### auto_load (module function)

```python
from shard_memo import auto_load

memo = auto_load(
    cache_root="cache_dir",
    params=params_dict,
    axis_values=axis_values_dict,
    memo_chunk_spec=chunk_spec_dict,
    merge_fn=merge_fn,
    exclusive=False,
    warn_on_overlap=False,
    allow_superset=False,
)
```

Convenience wrapper for `ShardMemo.auto_load()`. Automatically finds an existing
cache or creates a new one based on your `params` and `axis_values`.

Use cases:
- Quick start: let the library find or create a appropriate cache
- Exclusive workflow: prevent duplicate caches with `exclusive=True`.
  Also prevents creating subset/superset caches when overlapping data exists.
- Flexible chunking: `auto_load` handles different `memo_chunk_spec` values
  for the same data
- Subset detection: use `allow_superset=True` to find and reuse existing
  caches that are supersets of your requested data. This allows you to:
  - Define reduced `axis_values` by moving some axes to `params`
  - Find caches with broader axis coverage automatically
  - Avoid creating redundant caches for overlapping data

```python
from shard_memo import memo_parallel_run, memo_parallel_run_streaming

status = exec_point.cache_status(
    params,
    axis_indices={"strat": range(0, 1), "s": slice(0, 3)},
    extra=2,
)

parallel_output, parallel_diag = memo_parallel_run(
    memo,
    items,
    exec_fn=exec_fn,
    cache_status=status,
)

stream_diag = memo_parallel_run_streaming(
    memo,
    items,
    exec_fn=exec_fn,
    cache_status=status,
)
```

Parallel runner notes:
- `memo_parallel_run` expects a `cache_status`-shaped dict.
- Missing items are executed via `map_fn` (defaults to a `ProcessPoolExecutor`).
- Cached chunks are loaded locally, with partial reuse when `items` is a subset.

### memo_parallel_run

```python
memo_parallel_run(
    memo: ShardMemo | ShardMemoCache,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> tuple[Any, Diagnostics]
```

### memo_parallel_run_streaming

```python
memo_parallel_run_streaming(
    memo: ShardMemo | ShardMemoCache,
    items: Iterable[Any],
    *,
    exec_fn: Callable[..., Any],
    cache_status: Mapping[str, Any],
    map_fn: Callable[..., Iterable[Any]] | None = None,
    map_fn_kwargs: Mapping[str, Any] | None = None,
    collate_fn: Callable[[List[Any]], Any] | None = None,
) -> Diagnostics
```

## Caching behavior

- Each chunk file is named by a hash of `(params, chunk_key, cache_version)`.
- Cache files live under a memo directory hashed from params, split spec, and
  chunk spec, with a `metadata.json` file for memo-level context.
- Cache files include a `spec` payload keyed by item hash, describing per-item
  axis values.
- Chunk-level timestamps live in `chunks_index.json` under the memo directory.
- Chunks are sharded for disk efficiency, but lookups can return subsets of a
  chunk when you request fewer axis values.
- Changing split values creates new chunks automatically.
- Adding values to a list reuses existing chunks and computes only new ones.

## Examples

### Subset detection and cache reuse

You can define caches with broad axis coverage and then find them for
more specific requests using `allow_superset=True`:

```python
# Create a broad cache with strat=["a", "b"]
from shard_memo import ShardMemo

broad_cache = ShardMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
    merge_fn=merge_fn,
)
params = {"alpha": 0.4}
output1, diag1 = run(broad_cache, params, exec_fn)
# Executes all 6 points: (a,1), (a,2), (a,3), (b,1), (b,2), (b,3)

# Later, request just strat="a" - will find and reuse the broad cache
from shard_memo import auto_load

memo_a = auto_load(
    cache_root="./memo_cache",
    params=params,
    axis_values={"strat": ["a"]},  # Subset of original
    allow_superset=True,  # Enable superset detection
    merge_fn=merge_fn,
)
output2, diag2 = run(memo_a, params, exec_fn)
# Only executes the 3 points for strat="a", reuses cached data
assert diag2.cached_chunks == 1  # All data reused
assert diag2.executed_chunks == 0
```

### Flexible axis/param mapping

You can choose whether to include an axis in the split spec or as a
fixed parameter value, depending on your workflow:

```python
# Option 1: Include strat as axis (full sweep over strat values)
memo_full = ShardMemo.auto_load(
    cache_root="./cache",
    params={"alpha": 0.4},
    axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
    allow_superset=True,
)

# Option 2: Fix strat as a parameter (single value)
# Useful when you only need one value or plan to iterate externally
memo_fixed = ShardMemo.auto_load(
    cache_root="./cache",
    params={"alpha": 0.4, "strat": "a"},  # strat as param
    axis_values={"s": [1, 2, 3]},  # Reduced axes
    allow_superset=True,
)
```

Both approaches work, and `allow_superset=True` will find the appropriate cache
or create a new one that covers your requested data.

## Running examples

Run the basic example script:

```bash
python examples/basic.py
```

Run the callable axis_values example (memory-efficient lazy loading):

```bash
python examples/callable_axis_values.py
```

## Notes

- The exec function receives axis vectors for the current memo chunk.
- Memoization and parallel execution are decoupled so you can introduce new
  executors without changing `ShardMemoCache`.

### ShardMemoCache

`ShardMemoCache` holds cache configuration, metadata, and chunk planning. It does
not execute runs directly; use `run` / `run_streaming` from `shard_memo.runners`.

```python
from shard_memo import ShardMemoCache
from shard_memo.runners import run

cache = ShardMemoCache(
    cache_root="./cache",
    memo_chunk_spec={"strat": 1, "s": 2},
    axis_values={"strat": ["a", "b"], "s": [1, 2, 3, 4]},
)
output, diag = run(cache, params, exec_fn)
```
