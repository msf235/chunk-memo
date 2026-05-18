import argparse
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

import chunk_memo.runners as runners_module
from chunk_memo import ChunkMemo


runners_module.tqdm = lambda iterable: iterable


def serial_map_fn(
    func: Callable[..., Any],
    items: list[Any],
    **kwargs: Any,
) -> list[Any]:
    workers = kwargs.pop("workers", 1)
    if workers != 1:
        raise ValueError("serial_map_fn only supports workers=1")
    return [func(item) for item in items]


def exec_bench(params: dict[str, Any], s: int) -> int:
    work = params["work"]
    value = s + params.get("offset", 0)
    for _ in range(work):
        value = ((value * 1664525) + 1013904223) & 0xFFFFFFFF
    return value


def build_runner(root: Path, *, axis_values: dict[str, list[int]], chunk_size: int):
    memo = ChunkMemo(
        root=root,
        chunk_spec={"s": chunk_size},
        axis_values=axis_values,
        verbose=0,
    )
    chunk_runner = memo.cache()(exec_bench)
    item_runner = memo.cache(
        map_fn=serial_map_fn,
        map_fn_kwargs={"workers": 1},
    )(exec_bench)
    return chunk_runner, item_runner


def scenario_axis_values(values: list[int], scenario: str) -> list[int]:
    if scenario == "cold":
        return values
    if scenario == "half":
        return values[: len(values) // 2]
    if scenario == "warm":
        return values
    raise ValueError(f"Unknown scenario: {scenario}")


def warm_cache(
    runner: Callable[..., Any],
    params: dict[str, Any],
    values: list[int],
    scenario: str,
) -> None:
    if scenario == "cold":
        return
    warm_values = values[: len(values) // 2] if scenario == "half" else values
    runner(params, s=warm_values)


def time_runner(
    runner: Callable[..., Any],
    params: dict[str, Any],
    values: list[int],
    scenario: str,
) -> tuple[float, int]:
    requested = scenario_axis_values(values, scenario)
    start = time.perf_counter()
    output, _diagnostics = runner(params, s=requested)
    elapsed = time.perf_counter() - start
    return elapsed, len(output)


def benchmark_mode(
    mode: str,
    *,
    n_points: int,
    chunk_size: int,
    work: int,
    scenario: str,
    repeats: int,
) -> dict[str, float]:
    values = list(range(n_points))
    axis_values = {"s": values}
    params = {"work": work, "offset": 7}
    timings: list[float] = []
    output_len: int | None = None

    for _ in range(repeats):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / mode
            chunk_runner, item_runner = build_runner(
                root,
                axis_values=axis_values,
                chunk_size=chunk_size,
            )
            runner = chunk_runner if mode == "chunk" else item_runner
            warm_cache(runner, params, values, scenario)
            elapsed, current_len = time_runner(runner, params, values, scenario)
            timings.append(elapsed)
            if output_len is None:
                output_len = current_len
            elif output_len != current_len:
                raise AssertionError(
                    f"Output length changed across repeats for mode={mode}"
                )

    return {
        "min": min(timings),
        "mean": statistics.mean(timings),
        "max": max(timings),
        "output_len": float(output_len or 0),
    }


def compare_outputs(*, n_points: int, chunk_size: int, work: int, scenario: str) -> None:
    values = list(range(n_points))
    axis_values = {"s": values}
    params = {"work": work, "offset": 7}

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        chunk_runner, _ = build_runner(
            root / "chunk",
            axis_values=axis_values,
            chunk_size=chunk_size,
        )
        _, item_runner = build_runner(
            root / "item",
            axis_values=axis_values,
            chunk_size=chunk_size,
        )
        warm_cache(chunk_runner, params, values, scenario)
        warm_cache(item_runner, params, values, scenario)
        requested = scenario_axis_values(values, scenario)
        chunk_output, _ = chunk_runner(params, s=requested)
        item_output, _ = item_runner(params, s=requested)
        if chunk_output != item_output:
            raise AssertionError(
                "Chunk-level and item-level serial outputs differ "
                f"for chunk_size={chunk_size}, work={work}, scenario={scenario}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare ChunkMemo.cache default runner against the item-level "
            "runner forced through a serial map_fn."
        )
    )
    parser.add_argument("--n-points", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--chunk-sizes", type=int, nargs="+", default=[10, 100])
    parser.add_argument("--workloads", type=int, nargs="+", default=[0, 200])
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=["cold", "half", "warm"],
        choices=["cold", "half", "warm"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("ChunkMemo.cache runner comparison")
    print(
        "default runner = runners.py chunk-level execution; "
        "serial item runner = runners_parallel.py with map_fn=serial_map_fn and workers=1"
    )
    print(
        f"n_points={args.n_points}, repeats={args.repeats}, "
        f"chunk_sizes={args.chunk_sizes}, workloads={args.workloads}, scenarios={args.scenarios}"
    )
    print()
    print(
        "work  scenario  chunk_size  chunk_mean_s  item_mean_s  ratio(item/chunk)"
    )

    for work in args.workloads:
        for scenario in args.scenarios:
            for chunk_size in args.chunk_sizes:
                compare_outputs(
                    n_points=args.n_points,
                    chunk_size=chunk_size,
                    work=work,
                    scenario=scenario,
                )
                chunk_stats = benchmark_mode(
                    "chunk",
                    n_points=args.n_points,
                    chunk_size=chunk_size,
                    work=work,
                    scenario=scenario,
                    repeats=args.repeats,
                )
                item_stats = benchmark_mode(
                    "item",
                    n_points=args.n_points,
                    chunk_size=chunk_size,
                    work=work,
                    scenario=scenario,
                    repeats=args.repeats,
                )
                ratio = item_stats["mean"] / chunk_stats["mean"]
                print(
                    f"{work:4d}  {scenario:8s}  {chunk_size:10d}  "
                    f"{chunk_stats['mean']:12.6f}  {item_stats['mean']:11.6f}  {ratio:17.3f}"
                )


if __name__ == "__main__":
    main()
