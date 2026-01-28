# swarm-memo

swarm-memo provides sharded memoization for grid-style experiments. You define a
parameter grid (split variables), the library partitions it into reusable memo
chunks for disk efficiency, and cached chunk outputs are reused on subsequent
runs, including partial reuse for smaller parameter selections. Memoization and
parallel execution are independent: memoization is in `ChunkMemo`, while a
parallel wrapper consumes cache metadata and can execute the missing work.

## What problem does it solve?

Suppose you have:

- Fixed parameters (values that do not vary across the grid).
- Split variables (lists of values you want to sweep over).

You want to:

1. Evaluate a function across the grid.
2. Cache results so subsequent runs only compute what is new.
3. Shard outputs into reasonable file sizes, without losing the ability to load
   arbitrary subsets of parameter values.

swarm-memo breaks the grid into memo chunks and stores each chunk on disk. When
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
from swarm_memo import ChunkMemo


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
split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
memo = ChunkMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    split_spec=split_spec,
    merge_fn=merge_fn,
)
output, diag = memo.run(params, exec_fn)
print(output)
print(diag)
```

## Module layout

- `swarm_memo/memo.py`: memoization (ChunkMemo) and cache introspection.
- `swarm_memo/parallel.py`: parallel wrapper utilities (currently ProcessPool).
- `swarm_memo/__init__.py`: re-exports public APIs.

## API overview

### ChunkMemo

```python
ChunkMemo(
    cache_root: str | Path,
    memo_chunk_spec: dict[str, int | dict],
    split_spec: dict[str, Any],
    merge_fn: Callable[[list], Any] | None = None,
    memo_chunk_enumerator: Callable[[dict], Sequence[tuple]] | None = None,
    chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
    cache_version: str = "v1",
    axis_order: Sequence[str] | None = None,
    verbose: int = 1,
)
```

Notes:
- `memo_chunk_spec`: per-axis chunk sizes, e.g. `{"strat": 1, "s": 3}`.
- `exec_fn(params, **axes)`: chunk-level function; each axis receives the
  vector of values for that chunk. Supply it to `run` or use
  `run_wrap`/`streaming_wrap`.
- `merge_fn` defaults to returning the list of chunk outputs.
- `split_spec` defines the canonical grid for cache chunking.

### run

```python
output, diagnostics = memo.run(params, exec_fn)
# Or run a subset
# output, diagnostics = memo.run(params, exec_fn, strat=["a"], s=[1, 2, 3])
# output, diagnostics = memo.run(params, exec_fn, axis_indices={"strat": range(0, 1), "s": slice(0, 3)})
```

Runs missing chunks, caches them, and returns merged output with diagnostics.

### run_streaming

```python
diagnostics = memo.run_streaming(params, exec_fn)
# Or run a subset
# diagnostics = memo.run_streaming(params, exec_fn, strat=["a"], s=[1, 2, 3])
# diagnostics = memo.run_streaming(params, exec_fn, axis_indices={"strat": range(0, 1), "s": slice(0, 3)})
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
- You can also pass `axis_indices` (same keys as `split_spec`) with ints, ranges,
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

## Parallel wrapper

The parallel wrapper is independent from memoization. It consumes cache metadata
and delegates missing work to `ProcessPoolExecutor`, while already-cached chunks
are handled locally.

```python
from swarm_memo import process_pool_parallel

status = exec_point.cache_status(
    params,
    axis_indices={"strat": range(0, 1), "s": slice(0, 3)},
    extra=2,
)

parallel_output, parallel_diag = process_pool_parallel(
    status,
    exec_fn,
    params=params,
)
```

Parallel wrapper notes:
- `process_pool_parallel` expects a `cache_status`-shaped dict.
- Missing chunks are executed in a ProcessPool using chunk indices.
- Cached chunks are executed locally to return the same nested output shape.

### process_pool_parallel

```python
process_pool_parallel(
    cache_status: Mapping[str, Any],
    exec_fn: Callable[..., Any],
    *,
    params: dict[str, Any],
    max_workers: int | None = None,
    chunk_indices_to_axes: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> tuple[list[Any], ParallelDiagnostics]
```

- `chunk_indices_to_axes` can be used to transform chunk index metadata into the
  axis values expected by your execution function.

## Caching behavior

- Each chunk file is named by a hash of `(params, chunk_key, cache_version)`.
- Chunks are sharded for disk efficiency, but lookups can return subsets of a
  chunk when you request fewer axis values.
- Changing split values creates new chunks automatically.
- Adding values to a list reuses existing chunks and computes only new ones.

## Examples

Run the example script:

```bash
python examples/basic.py
```

## Notes

- The exec function receives axis vectors for the current memo chunk.
- Memoization and parallel execution are decoupled so you can introduce new
  executors without changing `ChunkMemo`.
