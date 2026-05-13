import math
from typing import Any, Mapping, Sequence, Tuple

ChunkKey = Tuple[Tuple[str, Tuple[Any, ...]], ...]

def format_axis_values(values: Any) -> str:
    if isinstance(values, (list, tuple)):
        if len(values) <= 4:
            inner = ", ".join(repr(value) for value in values)
            return f"[{inner}]"
        head = ", ".join(repr(value) for value in values[:2])
        tail = ", ".join(repr(value) for value in values[-2:])
        return f"[{head}, ..., {tail}]"
    return repr(values)


def format_cache_id(cache_id: str | None) -> list[str]:
    lines = ["[ChunkCache] cache_id:"]
    if not cache_id:
        lines.append("  (none)")
        return lines
    lines.append(f"  {cache_id}")
    return lines


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


def print_detail(message: str) -> None:
    print(message)


def chunk_key_size(chunk_key: ChunkKey) -> int:
    if not chunk_key:
        return 0
    return math.prod(len(values) for _, values in chunk_key)


def build_plan_lines(
    cache_id: str | None,
    params: Mapping[str, Any],
    axis_values: Mapping[str, Any],
    axis_order: Sequence[str],
    cached_count: int,
    execute_count: int,
) -> list[str]:
    lines: list[str] = []
    lines.extend(format_cache_id(cache_id))
    lines.extend(format_params(params))
    lines.extend(format_spec(axis_values, axis_order))
    lines.append(f"[ChunkCache] plan: cached={cached_count} execute={execute_count}")
    return lines


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
