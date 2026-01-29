import math
import time
from typing import Any, Mapping, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]

_progress_state = {"last_len": 0, "last_msg": ""}


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
    lines = ["[ShardMemo] params:"]
    if not params:
        lines.append("  (none)")
        return lines
    for key, value in params.items():
        lines.append(f"  {key}={value!r}")
    return lines


def format_spec(axis_values: Mapping[str, Any], axis_order: Sequence[str]) -> list[str]:
    lines = ["[ShardMemo] spec:"]
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
        f"[ShardMemo] {label} {processed}/{total} "
        f"({percent:0.1f}%) rate={rate:0.1f}/s ETA={format_eta(eta)}"
    )


def print_progress(message: str, *, final: bool) -> None:
    if final:
        print(message, end="\n", flush=True)
        _progress_state["last_len"] = 0
        _progress_state["last_msg"] = ""
        return
    pad = max(_progress_state["last_len"] - len(message), 0)
    print(message + (" " * pad), end="\r", flush=True)
    _progress_state["last_len"] = len(message)
    _progress_state["last_msg"] = message


def print_detail(message: str) -> None:
    last_len = _progress_state.get("last_len", 0)
    last_msg = _progress_state.get("last_msg", "")
    if last_len:
        print()
    print(message)
    if last_msg:
        print(last_msg, end="\r", flush=True)
        _progress_state["last_len"] = len(last_msg)


def chunk_key_size(chunk_key: ChunkKey) -> int:
    if not chunk_key:
        return 0
    return math.prod(len(values) for _, values in chunk_key)
