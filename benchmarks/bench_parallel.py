import time
from pathlib import Path

from swarm_memo import ChunkMemo


def exec_fn(params, s):
    outputs = []
    for value in s:
        time.sleep(params["sleep_s"])
        outputs.append(value)
    return outputs


def run_case(root, n_points, sleep_s):
    memo = ChunkMemo(
        cache_root=str(root),
        memo_chunk_spec={"s": 100},
        exec_fn=exec_fn,
    )

    params = {"sleep_s": sleep_s}
    split_spec = {"s": list(range(n_points))}

    start = time.perf_counter()
    memo.run(params, split_spec)
    run_time = time.perf_counter() - start

    return run_time


def main():
    n_points = 10000
    sleep_s = 0.005

    print("Benchmark: n_points=10000, sleep=0.005s")
    root = Path("/tmp/swarm_memo_bench")
    if root.exists():
        for child in root.iterdir():
            if child.is_file():
                child.unlink()
    else:
        root.mkdir(parents=True, exist_ok=True)

    run_time = run_case(root, n_points, sleep_s)
    print(f"run_time_s: {run_time:10.3f}")


if __name__ == "__main__":
    main()
