# swarm-memo

swarm-memo is a small utility for evaluating a function over chunked blocks of
input values and caching the results on disk. It is designed for experiments
where you repeatedly evaluate a function over a grid of values and want to
avoid recomputing the parts you already ran.

## What problem does it solve?

Suppose you have:

- **Fixed parameters** (things that do not vary across the grid).
- **Split variables** (lists of values you want to sweep over).

You want to:

1. Evaluate a function for each point in the grid.
2. Store results so you can re-run with new values and only compute what is new.
3. You want to make use of chunking in 1 and 2 for greater efficiency.
For instance, you may not want to have a file saved for every point on the grid if there are many points, since disk read/write operations to collect function evaluation at all points would become expensive.

swarm-memo does this by breaking the grid into **memo chunks** and caching those
chunks on disk. If you add new split values, only the new chunks are computed.

## Key terms

- **Point**: one combination of split values (e.g., `("a", 2)` for
  `(strat="a", s=2)`.
- **Split spec**: a dictionary of lists that define the grid,
  e.g. `{"strat": ["a", "b"], "s": [1, 2, 3]}`.
- **Memo chunk**: a cached block of points created by chunking each axis list
  into fixed-size bins and taking the cartesian product of those bins. Each
  memo chunk is written to a single file.

## Core functionality

- **Chunked Memoization**: Optimally make user of previously cached data. Allow data to be
  saved in reasonably-sized file chunks that partition the split spec values.

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


memo = ChunkMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    exec_fn=exec_fn,
    merge_fn=merge_fn,
)

params = {"alpha": 0.4}
split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
output, diag = memo.run(params, split_spec)
print(output)
print(diag)
```

## API overview

### ChunkMemo

```python
ChunkMemo(
    cache_root: str | Path,
    memo_chunk_spec: dict[str, int | dict],
    exec_fn: Callable[..., Any],
    merge_fn: Callable[[list], Any] | None = None,
    memo_chunk_enumerator: Callable[[dict], Sequence[tuple]] | None = None,
    chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
    cache_version: str = "v1",
    axis_order: Sequence[str] | None = None,
    split_spec: dict[str, Any] | None = None,
    verbose: int = 1,
)
```

Notes:
- `memo_chunk_spec`: per-axis chunk sizes, e.g. `{"strat": 1, "s": 3}`.
- `exec_fn(params, **axes)`: chunk-level function; each axis receives the
  vector of values for that chunk.
- `merge_fn` defaults to returning the list of chunk outputs.
- `split_spec` can be provided to enable wrapper calls without passing a full
  split spec each time.

### run

```python
output, diagnostics = memo.run(params, split_spec)
```

Runs missing chunks, caches them, and returns merged output with diagnostics.

### Decorator wrappers

```python
@memo.run_wrap()
def exec_point(params, point, extra=1):
    ...

output, diag = exec_point(params, strat=["a"], s=[1, 2, 3], extra=2)
# Or use indices to select from split_spec
# output, diag = exec_point(params, axis_indices={"strat": [0], "s": [0, 1]}, extra=2)
```

- The wrapper accepts axis values directly (singletons or lists).
- You can also pass `axis_indices` (same keys as `split_spec`) with ints, ranges,
  or slices to select by index.
- `params` can be positional or keyword.
- Extra keyword arguments are merged into the memoization params and also passed
  to the exec function.

## Caching behavior

- Each chunk file is named by a hash of `(params, chunk_key, cache_version)`.
- Changing split values creates new chunks automatically.
- Adding values to a list reuses existing chunks and computes only new ones.

## Examples

Run the example script:

```bash
python examples/basic.py
```

## Notes

- The exec function receives axis vectors for the current memo chunk.
