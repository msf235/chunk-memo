# swarm-memo

swarm-memo is a small utility for evaluating a function over input points in
parallel chunks that tile a space of inputs, and caching the results in a different set of reusable chunks.
It is designed for experiments where you repeatedly evaluate a function over a grid of values and
want to avoid recomputing the parts you already ran, while also making use of
efficient parallel computation.

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

At the same time, swarm-memo breaks the needed computation into parallelized
**execution chunks**. The key use of this utility is to make the memo chunks and the execution
chunks play nicely together.

## Key terms

- **Point**: one combination of split values (e.g., `("a", 2)` for
  `strat="a", s=2`).
- **Split spec**: a dictionary of lists that define the grid,
  e.g. `{"strat": ["a", "b"], "s": [1, 2, 3]}`.
- **Memo chunk**: a cached block of points created by chunking each axis list
  into fixed-size bins and taking the cartesian product of those bins. Each
  memo chunk is written to a single file.
- **Exec chunk**: the batch size for parallel execution; it controls how many
  points are grouped per worker batch.

## Core functionality

- **Chunked Memoization**: Optimally make user of previously cached data. Allow data to be
  saved in reasonably-sized file chunks that partition the split spec values.
- **Chunked Parallelization**: For data that has not been cached, perform parallel execution. Avoid recomputing cached data
  while still dividing the remaining computation work into performant chunks.
- Optional: flush data from memory into the on-disk cache whenever possible, minimizing memory usage. 

## Installation

Local install from the repo:

```bash
pip install -e .
```

## Quick start

```python
from swarm_memo import SwarmMemo


def enumerate_points(params, split_spec):
    points = []
    for strat in split_spec["strat"]:
        for s in split_spec["s"]:
            points.append((strat, s))
    return points


def exec_fn(params, point):
    strat, s = point
    return {"strat": strat, "s": s, "value": len(strat) + s}


def collate_fn(outputs):
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


memo = SwarmMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    exec_chunk_size=2,
    enumerate_points=enumerate_points,
    exec_fn=exec_fn,
    collate_fn=collate_fn,
    merge_fn=merge_fn,
)

params = {"alpha": 0.4}
split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
output, diag = memo.run(params, split_spec)
print(output)
print(diag)
```

## API overview

### SwarmMemo

```python
SwarmMemo(
    cache_root: str | Path,
    memo_chunk_spec: dict[str, int | dict],
    exec_chunk_size: int,
    enumerate_points: Callable[[dict, dict], list],
    exec_fn: Callable[[dict, Any], Any],
    collate_fn: Callable[[list], Any],
    merge_fn: Callable[[list], Any],
    chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
    cache_version: str = "v1",
    max_workers: int | None = None,
    axis_order: Sequence[str] | None = None,
    verbose: int = 1,
)
```

Notes:
- `memo_chunk_spec`: per-axis chunk sizes, e.g. `{"strat": 1, "s": 3}`.
- `exec_chunk_size`: batching size for the process pool (reduces overhead).
- `exec_fn(params, point)`: per-point function; must be top-level for
  multiprocessing.

### run

```python
output, diagnostics = memo.run(params, split_spec)
```

Runs missing chunks, caches them, and returns merged output with diagnostics.

### run_streaming

```python
diagnostics = memo.run_streaming(params, split_spec)
```

Executes missing chunks and writes each completed chunk to disk as soon as
possible. No output is returned.

### Decorator wrappers

```python
@memo.run_wrap()
def exec_point(params, point, extra=1):
    ...

output, diag = exec_point(params, split_spec=split_spec, extra=2)
```

- `split_spec` must be passed as a keyword argument.
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

- For multiprocessing, the exec function must be a top-level function (not
  nested inside another function).
- `run_streaming` preserves point order within each memo chunk while flushing
  as soon as chunks are complete.
