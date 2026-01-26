import time
from pathlib import Path

from swarm_memo import SwarmMemo


def enumerate_points(params, split_spec):
    return [(s,) for s in split_spec["s"]]


def exec_fn(params, point):
    (s,) = point
    time.sleep(params["sleep_s"])
    return s


def collate_fn(outputs):
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def run_case(root, exec_chunk_size, max_workers, n_points, sleep_s):
    memo = SwarmMemo(
        cache_root=str(root),
        memo_chunk_spec={"s": 100},
        exec_chunk_size=exec_chunk_size,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
        max_workers=max_workers,
    )

    params = {"sleep_s": sleep_s}
    split_spec = {"s": list(range(n_points))}

    start = time.perf_counter()
    memo.run(params, split_spec)
    run_time = time.perf_counter() - start

    start = time.perf_counter()
    diag = memo.run_streaming(params, split_spec)
    stream_time = time.perf_counter() - start

    return run_time, stream_time, diag.max_in_memory_results


def main():
    n_points = 10000
    sleep_s = 0.005
    exec_sizes = [1, 10, 100, 1000]
    max_workers = 20

    print("Benchmark: n_points=10000, sleep=0.005s, max_workers=20")
    print("exec_chunk_size | run_time_s | stream_time_s | stream_max_in_mem")

    for exec_chunk_size in exec_sizes:
        root = Path("/tmp/swarm_memo_bench") / f"exec_{exec_chunk_size}"
        if root.exists():
            for child in root.iterdir():
                if child.is_file():
                    child.unlink()
        else:
            root.mkdir(parents=True, exist_ok=True)

        run_time, stream_time, max_in_mem = run_case(
            root, exec_chunk_size, max_workers, n_points, sleep_s
        )
        print(
            f"{exec_chunk_size:14d} | {run_time:10.3f} |"
            f" {stream_time:12.3f} | {max_in_mem:15d}"
        )


if __name__ == "__main__":
    main()
