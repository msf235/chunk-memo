from importlib.metadata import PackageNotFoundError, version

from .runners import memo_parallel_run, memo_parallel_run_streaming
from .memo import ShardMemo, ChunkCache, Diagnostics
from .runners import run, run_streaming

auto_load = ShardMemo.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "ShardMemo",
    "Diagnostics",
    "ChunkCache",
    "__version__",
    "auto_load",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    "run",
    "run_streaming",
]
