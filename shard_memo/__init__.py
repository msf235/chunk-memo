from importlib.metadata import PackageNotFoundError, version

from .memo import ChunkCache
from .runner_protocol import (
    CacheStatus,
    ChunkIndexEntry,
    MemoRunnerBackend,
    MinimalCacheStatus,
)
from .runners import (
    Diagnostics,
    memo_parallel_run,
    memo_parallel_run_streaming,
    run,
    run_streaming,
)

auto_load = ChunkCache.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "ChunkCache",
    "Diagnostics",
    "__version__",
    "auto_load",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    "run",
    "run_streaming",
    "MemoRunnerBackend",
    "CacheStatus",
    "ChunkIndexEntry",
    "MinimalCacheStatus",
]
