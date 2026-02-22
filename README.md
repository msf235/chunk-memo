# chunk-memo

chunk-memo provides chunked memoization for grid-style parameter sweeps. You
define a parameter grid (axis values) and a partitioning of this grid into
chunks for disk efficiency. Cached chunk outputs are reused on subsequent runs
(memoization). Parallel runners are provided that allow you to pass your own map
function (such as ProcessPoolExecutor.map).

## What problem does it solve?

Suppose you have:

- Fixed parameters (values that do not vary across the grid Axis values (lists).
- of values you want to sweep over                                            ).

You want to:

1. Evaluate a function across the grid. 2. Cache results so subsequent runs only
compute what is new. 3. Chunk outputs into reasonable file sizes, without losing
the ability to load arbitrary subsets of parameter values.

chunk-memo breaks the grid into memo chunks and stores each chunk on disk. When
axis values change, only the new chunks are computed.

## Concepts

- Point: one combination of axis values (e.g., `(strat="a", s=2)`). - Axis
values: a dictionary of lists defining the grid, e.g. `{ "strat": ["a", "b"],
"s": [1, 2, 3] }`. - Memo chunk: a block of points created by chunking each axis
list and taking the cartesian product of those bins. Each memo chunk is written
to a single file that can serve partial reads for subsets of points.

## Installation

pip install memo-chunk

## Quick start

```pytho from chunk_memo import ChunkCache, ru n


def exec_fn(params, strat, s): outputs = [] for strat_value in strat: for
s_value in s: outputs.append( {"strat": strat_value, "s": s_value, "value":
len(strat_value) + s_value} ) return outputs


params = {"alpha": 0.4 axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}

memo = ChunkCache( cache_root="./memo_cache", cache_chunk_spec={"strat": 1, "s":
3}, axis_values=axis_values, params=params, )

output, diag = run(memo, exec_fn) print(output) print(diag) ```

## Module layout

- `chunk_memo/cache.py`: cache logic (`ChunkCache`) runners.py`: serial runner .
- `chunk_memo/runners_parallel.py`: parallel runner runners_common.py`: shared .
- `chunk_memo/runner utilities and planning helpers bridge.py`: removed        .
- `chunk_memo/__init__.py`: re-exports public APIs, including `auto_load()`    .

## API overview

### ChunkCache

`ChunkCache` owns the cache state and exposes the cache interface, including
`run_wrap` and `streaming_wrap`.

```python ChunkCache( root: str | Path, chunk_spec: dict[str, int | dict],
axis_values: dict[str, Any], collate_fn: Callable[[list], Any] | None = None,
cache_chunk_enumerator: Callable[[dict], Sequence[tuple]] | None = None,
chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None, cache_path_fn:
Callable[[dict, tuple, str, str], Path | str] | None = None, cache_version: str
= "v1", axis_order: Sequence[str] | None = None, verbose: int = 1, exclusive:
bool = False, warn_on_overlap: bool = False, ) ```

Notes:

- `chunk_spec`: per-axis chunk sizes, e.g. `{"strat": 1, "s": 3}`. -
`exec_fn(params, **axes)`: chunk-level function; each axis receives a vector of
values for that chunk. Supply it to `run` or use `run_wrap`/`streaming_wrap`.
- `collate_fn` defaults to returning a list of chunk outputs. - `axis_values`
defines the canonical grid for cache chunking. - `cache_path_fn` can be
used to place cache files in nested directories. Paths are resolved under a
memo-specific cache directory. This hook is experimental and not yet thoroughly
tested. - `exclusive`: if True, error when creating a cache with same `params`
and `axis_values` as an existing cache (different `chunk_spec` still conflicts).
Also prevents creating subset/superset caches when overlapping data exists. -
`warn_on_overlap`: if True, warn when caches have same `params` but partially
overlapping `axis_values`.

### axis_values: lists and iterables

`axis_values` must be concrete iterables (lists, tuples, ranges, or other
iterable objects). Callables are not supported by the current implementation.

Ordering rules:

- If the value is a sequence (list/tuple), order is preserved. - If the value
is a non-sequence iterable, values are materialized and sorted by a stable
serialization of each value. This makes order deterministic but may differ from
the original iteration order.

Axis values must be hashable and unique within each axis. Duplicates will
collapse to a single entry in the internal index map.

### run

```python memo.set_params(params) output, diagnostics = run(memo, exec_fn)

# Or run a subset sliced = memo.slice(params, strat=["a"], s=[1, 2, 3]) output,
diagnostics = run(sliced, exec_fn)

sliced = memo.slice(params, axis_indices={"strat": range(0, 1), "s": slice(0,
3)}) output, diagnostics = run(sliced, exec_fn) ```

Selection is handled via `memo.slice(...)` before calling the runners. Pass
`collate_fn` to override the cache-level `collate_fn` for this run.

### Cache vs runners

`ChunkCache` is run-agnostic. It exposes cache semantics (axis normalization,
selection via `slice`, hashing, and I/O). Execution is handled by runner helpers
like `run` and `run_streaming`, which consume a cache plus an exec function.
This keeps cache state independent from execution state.

Runs missing chunks, caches them, and returns merged output with diagnostics   .
Pass `collate_fn` to override the cache-level `collate_fn` for this run        .

### run_streaming

```python memo.set_params(params) diagnostics = run_streaming(memo, exec_fn)

# Or run a subset via slicing sliced = memo.slice(params, strat=["a"], s=[1, 2,
3]) diagnostics = run_streaming(sliced, exec_fn)

sliced = memo.slice(params, axis_indices={"strat": range(0, 1), "s": slice(0,
3)}) diagnostics = run_streaming(sliced, exec_fn) ```

Executes missing chunks and flushes them to disk without returning      outputs.
`collate_fn` is accepted for API parity but has no effect without       outputs.

### run_chunks / run_chunks_streaming (low-level)

These are low-level helpers that expect a cache object and a list of chunk
keys. The cache should already represent the desired axis subset (use
`memo.slice(...)` first if needed). You can pass `collate_fn` to override the
cache-level `collate_fn` for the duration of the call.

```python chunk_keys = memo.resolved_chunk_keys() output, diagnostics =
run_chunks(chunk_keys, exec_fn, cache=memo)

diagnostics = run_chunks_streaming(chunk_keys, exec_fn, cache=memo) ```

### run_wrap (memoized wrapper)

```python wrapper = ChunkMemo(memo)


@wrapper.run_wrap() def exec_point(params, strat, s, extra=1): ...


output, diag = exec_point(params, strat=["a"], s=[1, 2, 3], extra=2)

# Or by index output, diag = exec_point( params, axis_indices={"strat": range(0,
1), "s": slice(0, 3)}, extra=2, ) ```

- The wrapper accepts axis values directly (singletons or lists). - You can also
pass `axis_indices` (same keys as `axis_values`) with ints, ranges, or slices to
select by index. - Extra keyword arguments are merged into memoization params
and also passed to the exec function.

### streaming_wrap (memoized streaming wrapper)

```python wrapper = ChunkMemo(memo)


@wrapper.streaming_wrap() def exec_point(params, strat, s): ...


diagnostics = exec_point(params, strat=["a"], s=[1, 2, 3]) ```

- Streaming wrappers return diagnostics only and write cache outputs to disk.

### cache_status

`cache_status` returns a structured view of cached vs missing chunks for a
selection of axes. It returns both the chunk keys and the corresponding index
ranges.

```python status = exec_point.cache_status( params, axis_indices={"strat":
range(0, 1), "s": slice(0, 3)}, extra=2, )

status["cached_chunks"] status["cached_chunk_indices"] status["missing_chunks"]
status["missing_chunk_indices"] ```

### Class methods (cache introspection)

#### discover_caches

```python caches = ChunkCache.discover_caches(root) ```

Lists all existing caches in `root`. Returns a list of dictionaries with:

- `cache_hash`: The unique hash of the cache path`: Path to the cache directory.
- `metadata`: Full metadata dict if available, `None` otherwise                .

#### find_compatible_caches

```python caches = ChunkCache.find_compatible_caches( root, params=params_dict,
axis_values=axis_values_dict, chunk_spec=chunk_spec_dict, cache_version="v1",
axis_order=None, allow_superset=False, ) ```

Find caches compatible with the given criteria. Matching rules:

- If a criterion is provided, it must match exactly (when
`allow_superset=False`). - If a criterion is omitted (`None`), it acts as a
wildcard. - When `allow_superset=True`, finds caches that are supersets of the
requested configuration: - All requested axes are present in the cache's axes.
- All requested params match either the cache's params or corresponding axis
values.

Returns a list of compatible cache entries (same structure as
`discover_caches`).

#### load_from_cache

```python memo = ChunkCache.load_from_cache( root, cache_hash="abc123...",
collate_fn=collate_fn, verbose=1, exclusive=False, warn_on_overlap=False, ) ```

Load a `ChunkCache` instance from an existing cache by its hash. Raises
`FileNotFoundError` if the cache doesn't exist, or `ValueError` if metadata is
invalid.

#### singleton cache

```python memo = ChunkCache( root, chunk_spec={}, axis_values={}, ) ```

Create a singleton cache with no axes (empty `axis_values`). Useful for
memoizing functions that depend only on `params`, not on axis values. Uses
`chunk_spec={}` and `axis_values={}`.

#### auto_load (ChunkCache classmethod)

```python memo = ChunkCache.auto_load( root, params=params_dict,
axis_values=axis_values_dict, chunk_spec=chunk_spec_dict, collate_fn=collate_fn,
cache_chunk_enumerator=cache_chunk_enumerator, chunk_hash_fn=chunk_hash_fn,
cache_path_fn=cache_path_fn, cache_version="v1", axis_order=axis_order,
verbose=1, profile=False, exclusive=False, warn_on_overlap=False,
allow_superset=False, ) ```

Streamlined memoization that finds or creates a cache. Behavior:

- If `axis_values` is provided and `allow_superset=False` (default): finds
an exact match (same `params` + `axis_values`), or creates a new cache with
specified (or default) `chunk_spec`. - If `axis_values` is provided and
`allow_superset=True`: finds an exact match or finds a superset cache that
contains all requested data. - If `axis_values` is not provided: finds caches
with matching `params`. Requires exactly 1 match, or raises `ValueError`
(ambiguous). - When creating a new cache without `chunk_spec`, defaults to chunk
size 1 for all axes.

## Parallel runners

Parallel runners are independent from memoization. They consume cache metadata
and delegate missing work to a user-supplied map function, while already-cached
chunks are handled locally. The public functions are re-exported from
`chunk_memo` for compatibility and implemented in `chunk_memo/runners.py` and
`chunk_memo/runners_parallel.py`.

## Streamlined memoization

### auto_load (module function)

```python from chunk_memo import auto_load


memo = auto_load( root="cache_dir", params=params_dict,
axis_values=axis_values_dict, chunk_spec=chunk_spec_dict, collate_fn=collate_fn,
exclusive=False, warn_on_overlap=False, allow_superset=False, ) ```

Convenience wrapper for `ChunkCache.auto_load()`. Automatically finds an
existing cache or creates a new one based on your `params` and `axis_values`.

Use cases:

- Quick start: let the library find or create an appropriate cache. - Exclusive
workflow: prevent duplicate caches with `exclusive=True`. Also prevents creating
subset/superset caches when overlapping data exists. - Flexible chunking:
`auto_load` handles different `chunk_spec` values for the same data. - Subset
detection: use `allow_superset=True` to find and reuse existing caches that
are supersets of your requested data. This allows you to: - Define reduced
`axis_values` by moving some axes to `params`. - Find caches with broader axis
coverage automatically. - Avoid creating redundant caches for overlapping data.

```python import functools

from chunk_memo import run_parallel


parallel_cache = memo.slice( params, axis_indices={"strat": range(0, 1), "s":
slice(0, 3)}, extra=2, )

parallel_output, parallel_diag = run_parallel( items,
exec_fn=functools.partial(exec_fn, params), cache=parallel_cache, ) ```

Parallel runner notes:

- `run_parallel` expects a cache already sliced to the desired axis subset.
- You can pass explicit kwargs to override cache methods. - Missing items
are executed via `map_fn` (defaults to a `ProcessPoolExecutor`). - Cached
chunks are loaded locally, with partial reuse when `items` is a subset. - Use
`flush_on_chunk=True` and `return_output=False` to stream payloads to disk
without retaining outputs in memory. - Use `extend_cache=True` to grow a cache's
`axis_values` in-place when items include values outside the current cache.

Item formats for `run_parallel`:

- Mapping items: each item is a dict with keys for every axis name (in any
order). - Positional items: each item is a tuple/list ordered by `axis_order`. -
Single-axis shorthand: if there is exactly one axis, a scalar item is allowed.

### run_parallel

```python run_parallel( items: Iterable[Any], *, exec_fn: Callable[...,
Any], cache: CacheProtocol, map_fn: Callable[..., Iterable[Any]] | None
= None, map_fn_kwargs: Mapping[str, Any] | None = None, collate_fn:
Callable[[List[Any]], Any] | None = None, flush_on_chunk: bool = False,
return_output: bool = True, extend_cache: bool = False, # Manual cache methods
(optional overrides) write_metadata: Callable[[], Path] | None = None,
chunk_hash: Callable[[ChunkKey], str] | None = None, resolve_cache_path:
Callable[[ChunkKey, str], Path] | None = None, load_payload: Callable[[Path],
dict[str, Any] | None] | None = None, write_chunk_payload: Callable[...,
Path] | None = None, update_chunk_index: Callable[[str, ChunkKey], None]
| None = None, load_chunk_index: Callable[[], dict[str, Any] | None]
| None = None, build_item_maps_from_axis_values: Callable[..., Any] |
None = None, build_item_maps_from_chunk_output: Callable[..., Any] |
None = None, reconstruct_output_from_items: Callable[..., Any] | None =
None, collect_chunk_data: Callable[..., Any] | None = None, item_hash:
Callable[[ChunkKey, Tuple[Any, ...]], str] | None = None, context: RunnerContext
| None = None, ) -> tuple[Any, Diagnostics] ```

## Caching behavior

- Each chunk file is named by a hash of `(params, chunk_key, cache_version)`.
- Cache files live under a memo directory hashed from params, axis values,
and chunk spec, with a `metadata.json` file for memo-level context. - Cache
files include a `spec` payload keyed by item hash, describing per-item axis
values. - Chunk-level timestamps live in `chunks_index.json` under the memo
directory. - Chunks are sharded for disk efficiency, but lookups can return
subsets of a chunk when you request fewer axis values. - Changing axis
values creates new chunks automatically. - Adding values to a list reuses
existing chunks and computes only new ones. - Partial chunk payloads are
allowed. Runners return whatever cached outputs are available and increment
`diagnostics.partial_chunks`.

## Defaults and ordering

- `root` defaults to `.chunk_memo` under the current working directory. - When
not provided, `axis_order` defaults to lexicographic order of axis names. - If
`chunk_spec` is omitted, each axis defaults to a chunk size of 1. For list/tuple
axes this is overridden to the full axis length; other iterables still default
to 1.

## Examples

### Subset detection and cache reuse

You can define caches with broad axis coverage and then find them for more
specific requests using `allow_superset=True`:

```python from chunk_memo import ChunkCache, run, run_streaming


# Create a broad cache with strat=["a", "b"] broad_cache = ChunkCache(
root="./memo_cache", chunk_spec={"strat": 1, "s": 3}, axis_values={"strat":
["a", "b"], "s": [1, 2, 3]}, collate_fn=collate_fn, ) params = {"alpha": 0.4}
broad_cache.set_params(params) output1, diag1 = run(broad_cache, exec_fn)

# Executes all 6 points: (a,1), (a,2), (a,3), (b,1), (b,2), (b,3)

# Later, request just strat="a" - will find and reuse the broad cache from
chunk_memo import auto_load


memo_a = auto_load( root="./memo_cache", params=params, axis_values={"strat":
["a"]}, # Subset of original allow_superset=True, # Enable superset detection
collate_fn=collate_fn, ) memo_a.set_params(params) output2, diag2 = run(memo_a,
exec_fn)

# Only executes the 3 points for strat="a", reuses cached data assert
diag2.cached_chunks == 1 assert diag2.executed_chunks == 0 ```

### Flexible axis/param mapping

You can choose whether to include an axis in `axis_values` or as a fixed
parameter value, depending on your workflow:

```python # Option 1: Include strat as axis (full sweep over strat values)
memo_full = ChunkCache.auto_load( root="./cache", params={"alpha": 0.4},
axis_values={"strat": ["a", "b"], "s": [1, 2, 3]}, allow_superset=True, )

# Option 2: Fix strat as a parameter (single value) # Useful when you only need
one value or plan to iterate externally memo_fixed = ChunkCache.auto_load(
root="./cache", params={"alpha": 0.4, "strat": "a"}, # strat as param
axis_values={"s": [1, 2, 3]}, # Reduced axes allow_superset=True, ) ```

Both approaches work, and `allow_superset=True` will find the appropriate cache
or create a new one that covers your requested data.

## Running examples

Run the basic example script:

```bash python examples/basic.py ```

## Notes

- The exec function receives axis vectors for the current memo chunk. -
Memoization and parallel execution are decoupled so you can introduce new
executors without changing `ChunkCache`.

### ChunkCache

`ChunkCache` holds cache configuration, metadata, and chunk planning. It does
not execute runs directly; use `run` / `run_streaming` from `chunk_memo`.

```pytho from chunk_memo import ChunkCache, ru n


cache = ChunkCache( root="./cache", chunk_spec={"strat": 1, "s": 2},
axis_values={"strat": ["a", "b"], "s": [1, 2, 3, 4]}, ) cache.set_params(params)
output, diag = run(cache, exec_fn) ```
