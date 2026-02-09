from importlib.metadata import PackageNotFoundError, version

from .cache import ChunkCache
from .memo import ChunkMemo
from .runner_protocol import CacheStatus, RunnerContext
from .runners import (
    Diagnostics,
    run,
    run_parallel,
    run_parallel_streaming,
    run_streaming,
)

auto_load = ChunkCache.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "ChunkCache",
    "ChunkMemo",
    "Diagnostics",
    "__version__",
    "auto_load",
    "run",
    "run_parallel",
    "run_parallel_streaming",
    "run_streaming",
    "RunnerContext",
    "CacheStatus",
]
