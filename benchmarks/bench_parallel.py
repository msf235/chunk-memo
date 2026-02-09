import functools
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from shard_memo import ChunkCache, memo_parallel_run, run


def exec_fn(params, s):
    if not isinstance(s, (list, tuple)):
        s = [s]
    outputs = []
    for value in s:
        time.sleep(params["sleep_s"])
        outputs.append(value)
    return outputs


def run_case(root, n_points, sleep_s, exec_chunk_size, scenario):
    # breakpoint()
    axis_values = {"s": list(range(n_points))}
    cache_root = root / f"exec_{exec_chunk_size}" / scenario
    memo = ChunkCache(
        cache_root=str(cache_root),
        cache_chunk_spec={"s": 100},
        axis_values=axis_values,
        verbose=0,
    )

    params = {"sleep_s": sleep_s}
    memo.set_params(params)
    if scenario == "half":
        s_vals = axis_values["s"][: n_points // 2]
        run(memo, exec_fn, s=s_vals)
    elif scenario == "warm":
        run(memo, exec_fn)

    start_case = time.perf_counter()
    status = memo.cache_status(s=axis_values["s"])

    load_time = time.perf_counter() - start_case

    items = [{"s": value} for value in axis_values["s"]]
    start = time.perf_counter()
    parallel_kwargs = {
        "write_metadata": memo.write_metadata,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_axis_values": memo.build_item_maps_from_axis_values,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "reconstruct_output_from_items": memo.reconstruct_output_from_items,
        "collect_chunk_data": memo.collect_chunk_data,
        "item_hash": memo.item_hash,
        "context": memo,
    }
    with ProcessPoolExecutor(max_workers=8) as executor:
        memo_parallel_run(
            items,
            exec_fn=functools.partial(exec_fn, params),
            cache_status=status,
            map_fn=executor.map,
            map_fn_kwargs={"chunksize": exec_chunk_size},
            **parallel_kwargs,
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
