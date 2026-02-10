import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]

_thread_local = threading.local()
PROGRESS_REPORT_DIVISOR = 50


@dataclass
class _ProgressTracker:
    last_len: int = 0
    last_msg: str = ""

    def print_progress(self, message: str, *, final: bool) -> None:
        if final:
            print(message, end="\n", flush=True)
            self.last_len = 0
            self.last_msg = ""
            return
        pad = max(self.last_len - len(message), 0)
        print(message + (" " * pad), end="\r", flush=True)
        self.last_len = len(message)
        self.last_msg = message

    def print_detail(self, message: str) -> None:
        if self.last_len:
            print()
        print(message)
        if self.last_msg:
            print(self.last_msg, end="\r", flush=True)
            self.last_len = len(self.last_msg)


def format_axis_values(values: Any) -> str:
    if isinstance(values, (list, tuple)):
        if len(values) <= 4:
            inner = ", ".join(repr(value) for value in values)
            return f"[{inner}]"
        head = ", ".join(repr(value) for value in values[:2])
        tail = ", ".join(repr(value) for value in values[-2:])
        return f"[{head}, ..., {tail}]"
    return repr(values)


def format_params(params: Mapping[str, Any]) -> list[str]:
    lines = ["[ChunkCache] params:"]
    if not params:
        lines.append("  (none)")
        return lines
    for key in sorted(params):
        value = params[key]
        lines.append(f"  {key}={value!r}")
    return lines


def format_spec(axis_values: Mapping[str, Any], axis_order: Sequence[str]) -> list[str]:
    lines = ["[ChunkCache] spec:"]
    for axis in axis_order:
        values = axis_values.get(axis)
        lines.append(f"  {axis}={format_axis_values(values)}")
    return lines


def format_eta(seconds: float) -> str:
    if seconds < 0 or not math.isfinite(seconds):
        return "--:--:--"
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_rate_eta(
    label: str,
    processed: int,
    total: int,
    start_time: float,
    *,
    rate_processed: int | None = None,
    rate_total: int | None = None,
) -> str:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    if rate_processed is None:
        rate_processed = processed
    if rate_total is None:
        rate_total = total
    rate = rate_processed / elapsed
    percent = 100.0 if total <= 0 else (processed / total * 100.0)
    remaining = 0 if rate_total <= 0 else max(rate_total - rate_processed, 0)
    eta = remaining / rate if rate > 0 else float("inf")
    return (
        f"[ChunkCache] {label} {processed}/{total} "
        f"({percent:0.1f}%) rate={rate:0.1f}/s ETA={format_eta(eta)}"
    )


def print_progress(message: str, *, final: bool) -> None:
    if not hasattr(_thread_local, "tracker"):
        _thread_local.tracker = _ProgressTracker()
    _thread_local.tracker.print_progress(message, final=final)


def print_detail(message: str) -> None:
    if not hasattr(_thread_local, "tracker"):
        _thread_local.tracker = _ProgressTracker()
    _thread_local.tracker.print_detail(message)


def chunk_key_size(chunk_key: ChunkKey) -> int:
    if not chunk_key:
        return 0
    return math.prod(len(values) for _, values in chunk_key)


def build_plan_lines(
    params: Mapping[str, Any],
    axis_values: Mapping[str, Any],
    axis_order: Sequence[str],
    cached_count: int,
    execute_count: int,
) -> list[str]:
    lines: list[str] = []
    lines.extend(format_params(params))
    lines.extend(format_spec(axis_values, axis_order))
    lines.append(f"[ChunkCache] plan: cached={cached_count} execute={execute_count}")
    return lines


def prepare_progress(
    *,
    total_chunks: int,
    total_items: int,
    verbose: int,
    label: str = "planning",
) -> tuple[Callable[[int, bool], None], Callable[[int], None]]:
    progress_step = max(1, total_chunks // PROGRESS_REPORT_DIVISOR)
    start_time = time.monotonic()
    processed_items = 0

    def update_processed(count: int) -> None:
        nonlocal processed_items
        processed_items += count

    def report_progress(processed: int, final: bool = False) -> None:
        if verbose != 1:
            return
        if not final and processed % progress_step != 0 and processed != total_chunks:
            return
        message = format_rate_eta(
            label,
            processed_items,
            total_items,
            start_time,
        )
        print_progress(message, final=final)

    return report_progress, update_processed


def print_chunk_summary(
    diagnostics: Any,
    verbose: int,
) -> None:
    if verbose >= 2:
        partial_chunks = getattr(diagnostics, "partial_chunks", 0)
        partial_suffix = f" partial={partial_chunks}" if partial_chunks else ""
        print_detail(
            "[ChunkCache] summary "
            f"cached={diagnostics.cached_chunks} "
            f"executed={diagnostics.executed_chunks} "
            f"total={diagnostics.total_chunks}" + partial_suffix
        )
