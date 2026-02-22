from importlib.metadata import PackageNotFoundError, version

from .cache import ChunkCache
from .memo import ChunkMemo
from .identity import params_to_cache_id
from .runner_protocol import CacheStatus, RunnerContext
from .runners import (
    Diagnostics,
    run,
    run_parallel,
    run_streaming,
)

auto_load = ChunkMemo.auto_load


try:
    __version__ = version("chunk-memo")
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
    "run_streaming",
    "RunnerContext",
    "CacheStatus",
    "params_to_cache_id",
]
