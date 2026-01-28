import statistics
import tempfile
import time
from pathlib import Path

try:
    from joblib import Memory
except ImportError:  # pragma: no cover - optional dependency
    Memory = None

from swarm_memo import ChunkMemo


def exec_fn(params, s):
    scale = params["scale"]
    return [value * scale for value in s]


def run_warm_benchmark(*, n_points, chunk_size, repeats):
    split_spec = {"s": list(range(n_points))}
    params = {"scale": 2}
    with tempfile.TemporaryDirectory() as temp_dir:
        memo = ChunkMemo(
            cache_root=temp_dir,
            memo_chunk_spec={"s": chunk_size},
            exec_fn=exec_fn,
            split_spec=split_spec,
        )
        start = time.perf_counter()
        memo.run(params, split_spec)
        cold_time = time.perf_counter() - start

        run_times = []
        for _ in range(repeats):
            start = time.perf_counter()
            memo.run(params, split_spec)
            run_times.append(time.perf_counter() - start)

    return {
        "cold": cold_time,
        "min": min(run_times),
        "mean": statistics.mean(run_times),
        "max": max(run_times),
    }


def run_joblib_warm_benchmark(*, n_points, repeats, root):
    if Memory is None:
        return None
    points = list(range(n_points))
    params = {"scale": 2}
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root_path) as temp_dir:
        memory = Memory(location=temp_dir, verbose=0)

        @memory.cache
        def joblib_exec(params, point):
            scale = params["scale"]
            # return [value * scale for value in point]
            return point * params["scale"]

        start = time.perf_counter()
        for point in points:
            joblib_exec(params, point)
        cold_time = time.perf_counter() - start

        run_times = []
        for _ in range(repeats):
            start = time.perf_counter()
            for point in points:
                joblib_exec(params, point)
            run_times.append(time.perf_counter() - start)

    return {
        "cold": cold_time,
        "min": min(run_times),
        "mean": statistics.mean(run_times),
        "max": max(run_times),
    }


def main():
    n_points = 20000
    repeats = 5
    chunk_sizes = [1, 5, 10, 25, 50, 100]

    print("Memo warm cache benchmark (memo.run)")
    print(f"n_points={n_points}, repeats={repeats}")
    print("chunk_size  cold_s  min_s    mean_s   max_s")

    for chunk_size in chunk_sizes:
        stats = run_warm_benchmark(
            n_points=n_points,
            chunk_size=chunk_size,
            repeats=repeats,
        )
        print(
            f"{chunk_size:10d}  {stats['cold']:6.4f}  {stats['min']:6.4f}  {stats['mean']:6.4f}  {stats['max']:6.4f}"
        )

    joblib_stats = run_joblib_warm_benchmark(
        n_points=n_points,
        repeats=repeats,
        root=tempfile.gettempdir(),
    )
    if joblib_stats is None:
        print("joblib.Memory unavailable; install joblib to compare")
    else:
        print(
            f"joblib.Memory  {joblib_stats['cold']:6.4f}  {joblib_stats['min']:6.4f}  {joblib_stats['mean']:6.4f}  {joblib_stats['max']:6.4f}"
        )

    tmpfs_stats = run_joblib_warm_benchmark(
        n_points=n_points,
        repeats=repeats,
        root="/dev/shm",
    )
    if tmpfs_stats is None:
        print("joblib.Memory tmpfs unavailable; install joblib to compare")
    else:
        print(
            f"joblib.Memory(/dev/shm)  {tmpfs_stats['cold']:6.4f}  {tmpfs_stats['min']:6.4f}  {tmpfs_stats['mean']:6.4f}  {tmpfs_stats['max']:6.4f}"
        )


if __name__ == "__main__":
    main()
