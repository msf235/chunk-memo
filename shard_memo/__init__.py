from importlib.metadata import PackageNotFoundError, version

from .runners import memo_parallel_run, memo_parallel_run_streaming
from .memo import ShardMemo, ShardMemoCache, Diagnostics
from .runners import run, run_streaming
from .memo import ShardMemo as _ShardMemo

auto_load = _ShardMemo.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "ShardMemo",
    "Diagnostics",
    "ShardMemoCache",
    "__version__",
    "auto_load",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    "run",
    "run_streaming",
]
