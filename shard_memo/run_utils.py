import time
from typing import Any, Callable, Mapping, Protocol, Sequence

from ._format import (
    format_params,
    format_rate_eta,
    format_spec,
    print_detail,
    print_progress,
)

PROGRESS_REPORT_DIVISOR = 50


class DiagnosticsLike(Protocol):
    cached_chunks: int
    executed_chunks: int
    total_chunks: int


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
    lines.append(f"[ChunkMemo] plan: cached={cached_count} execute={execute_count}")
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


def print_chunk_summary(diagnostics: DiagnosticsLike, verbose: int) -> None:
    if verbose >= 2:
        print_detail(
            "[ChunkMemo] summary "
            f"cached={diagnostics.cached_chunks} "
            f"executed={diagnostics.executed_chunks} "
            f"total={diagnostics.total_chunks}"
        )
