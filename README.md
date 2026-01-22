# swarm-memo

swarm-memo provides a general-purpose, chunked memoization utility for parallel
experiments. It executes a per-point function across a grid of split variables,
stores chunk outputs to disk, and reuses cached chunks on subsequent runs. The
cache is keyed by a stable hash of fixed parameters plus the chunk’s axis bins,
so adding new split values reuses existing results while avoiding collisions
when values change.

## Core ideas

- **Axis-aware chunking**: chunk sizes are defined per axis (e.g., `strat=1`,
  `s=3`). Chunks are built by binning each axis list sequentially, then
  taking the cartesian product of bins.
- **Parallel execution**: point execution uses `ProcessPoolExecutor` with
  `exec_chunk_size` controlling batching overhead.
- **Streaming option**: `run_streaming` flushes memo chunks as soon as they are
  complete, writing to disk without returning the full output.       

## Installation

Only local install currently available. After downloading the repository, in the
root directory run:

```pip install -e . ```

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


memo = SwarmMemo(cache_root="./memo_cache", memo_chunk_spec={"strat": 1, "s": 3},
  exec_chunk_size=2, enumerate_points=enumerate_points, exec_fn=exec_fn,
  collate_fn=collate_fn, merge_fn=merge_fn,
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
  cache_root: str | Path, memo_chunk_spec: dict[str, int | dict],
  exec_chunk_size: int, enumerate_points: Callable[[dict, dict], list],
  exec_fn: Callable[[dict, Any], Any], collate_fn: Callable[[list], Any],
  merge_fn: Callable[[list], Any],
  chunk_hash_fn: Callable[[dict, tuple, str], str] | None = None,
  cache_version: str = "v1",
  max_workers: int | None = None,
  axis_order: Sequence[str] | None = None,
  verbose: int = 1,
)
```

- `memo_chunk_spec`: per-axis chunk sizes (e.g., `{"strat": 1, "s": 3}`
or `{"s": {"size": 3}}`)
-  `exec_chunk_size`: batching size used by `ProcessPoolExecutor` to reduce parallelization overhead (startup and concatenation of results)
- `exec_fn(params, point)`: per-point execution function (must be top-level for multiprocessing)                  .

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
@memo.run_wrap() def exec_point(params, point, extra=1): ...

output, diag = exec_point(params, split_spec=split_spec, extra=2)
```

`split_spec` must be passed as a keyword argument. `params` can be positional or keyword.
Additional keyword arguments are merged into the memoization params (and passed into the exec function)                                   .

## Caching behavior

- Each chunk file is named by a hash of `(params, chunk_key, cache_version)`.
- If split values change, new chunks are created automatically.
- If you add values to an axis list, existing chunks are reused and only new chunks are computed.

## Examples

Run the example script:

```bash
python examples/basic.py
```

## Notes

- For multiprocessing, the exec function must be a top-level function (not
nested).
- `run_streaming` preserves point order within chunks while flushing
as soon as chunks are complete.
- This utility was developed with heavy use of Chatgpt Codex 5.2.
  The conversation is included in `codex_conversation_1.md`.
