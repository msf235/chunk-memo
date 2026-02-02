import shutil
import time
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from shard_memo import ChunkCache, memo_parallel_run
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
    start_case = time.perf_counter()
    axis_values = {"s": list(range(n_points))}
    cache_root = root / f"exec_{exec_chunk_size}" / scenario
    memo = ChunkCache(
        cache_root=str(cache_root),
        cache_chunk_spec={"s": 100},
        axis_values=axis_values,
        verbose=0,
    )

    params = {"sleep_s": sleep_s}
    if scenario == "half":
        s_vals = axis_values["s"][: n_points // 2]
        run(memo, params, exec_fn, s=s_vals)
    elif scenario == "warm":
        run(memo, params, exec_fn)

    status = memo.cache_status(params, s=axis_values["s"])

    items = [{"s": value} for value in axis_values["s"]]
    load_time = time.perf_counter() - start_case
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
    end_time = time.perf_counter()
    run_time = end_time - start
    all_time = end_time - start_case

    return run_time, load_time, all_time


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
            run_time, load_time, all_time = run_case(
                root, n_points, sleep_s, exec_chunk_size, scenario
            )
            print(
                f"scenario={scenario:4s} chunksize={exec_chunk_size:4d} ",
                "\t",
                f"run_time_s: {run_time:7.3f}",
                "\t",
                f"load_time_s: {load_time:7.3f}",
            )


if __name__ == "__main__":
    main()
