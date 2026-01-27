import time
from pathlib import Path

from swarm_memo import ChunkMemo, process_pool_parallel


def exec_fn(params, s):
    outputs = []
    for value in s:
        time.sleep(params["sleep_s"])
        outputs.append(value)
    return outputs


def run_case(root, n_points, sleep_s, exec_chunk_size, scenario):
    split_spec = {"s": list(range(n_points))}
    memo = ChunkMemo(
        cache_root=str(root),
        memo_chunk_spec={"s": 100},
        exec_fn=exec_fn,
        split_spec=split_spec,
    )

    params = {"sleep_s": sleep_s}
    if scenario == "half":
        memo.run(params, {"s": split_spec["s"][: n_points // 2]})
        memo._set_split_spec(split_spec)
    elif scenario == "warm":
        memo.run(params, split_spec)

    status = memo.cache_status(params, s=split_spec["s"])

    start = time.perf_counter()
    process_pool_parallel(
        status,
        exec_fn,
        params=params,
        use_processes=True,
        exec_chunk_size=exec_chunk_size,
        max_workers=8,
    )
    run_time = time.perf_counter() - start

    return run_time


def main():
    n_points = 10000
    sleep_s = 0.001

    print("Benchmark: n_points=10000, sleep=0.005s (varying exec_chunk_size)")
    print("max_workers=8, scenarios: cold/half/warm")
    root = Path("/tmp/swarm_memo_bench")
    if root.exists():
        for child in root.iterdir():
            if child.is_file():
                child.unlink()
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
