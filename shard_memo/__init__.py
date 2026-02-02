from importlib.metadata import PackageNotFoundError, version

from .memo import ChunkMemo, ChunkCache, Diagnostics
from .runner_protocol import CacheStatus, MemoRunnerBackend
from .runners import memo_parallel_run, memo_parallel_run_streaming, run, run_streaming

auto_load = ChunkMemo.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "ChunkMemo",
    "Diagnostics",
    "ChunkCache",
    "__version__",
    "auto_load",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    "run",
    "run_streaming",
    "MemoRunnerBackend",
    "CacheStatus",
]
