import shutil
import time
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from shard_memo import ChunkMemo, memo_parallel_run
from shard_memo.runners import run


def exec_fn(params, s):
    if not isinstance(s, (list, tuple)):
        s = [s]
    outputs = []
    for value in s:
        time.sleep(params["sleep_s"])
        outputs.append(value)
    return outputs


def run_case(root, n_points, sleep_s, exec_chunk_size, scenario):
    axis_values = {"s": list(range(n_points))}
    cache_root = root / f"exec_{exec_chunk_size}" / scenario
    memo = ChunkMemo(
        cache_root=str(cache_root),
        memo_chunk_spec={"s": 100},
        axis_values=axis_values,
    )

    params = {"sleep_s": sleep_s}
    if scenario == "half":
        run(
            memo,
            params,
            exec_fn=exec_fn,
            s=axis_values["s"][: n_points // 2],
        )
    elif scenario == "warm":
        run(memo, params, exec_fn)

    status = memo.cache_status(params, s=axis_values["s"])

    items = [{"s": value} for value in axis_values["s"]]
    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=8) as executor:
        memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=status,
            map_fn=executor.map,
            map_fn_kwargs={"chunksize": exec_chunk_size},
        )
    run_time = time.perf_counter() - start

    return run_time


def main():
    n_points = 10000
    sleep_s = 0.001

    print("Benchmark: n_points=10000, sleep=0.005s (varying exec_chunk_size)")
    print("max_workers=8, scenarios: cold/half/warm")
    root = Path("/tmp/shard_memo_bench")
    if root.exists():
        for child in root.iterdir():
            if child.is_file():
                child.unlink()
            else:
                shutil.rmtree(child)
    else:
        root.mkdir(parents=True, exist_ok=True)

    for exec_chunk_size in [1, 10]:
        for scenario in ["cold", "half", "warm"]:
            run_time = run_case(root, n_points, sleep_s, exec_chunk_size, scenario)
            print(
                f"scenario={scenario:4s} chunksize={exec_chunk_size:4d} "
                f"run_time_s: {run_time:10.3f}"
            )


if __name__ == "__main__":
    main()
