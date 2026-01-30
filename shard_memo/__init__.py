from importlib.metadata import PackageNotFoundError, version

from .bridge import BridgeDiagnostics, memo_parallel_run, memo_parallel_run_streaming
from .memo import ShardMemo, Diagnostics
from .memo import ShardMemo as _ShardMemo

auto_load = _ShardMemo.auto_load


try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "BridgeDiagnostics",
    "ShardMemo",
    "Diagnostics",
    "__version__",
    "auto_load",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
]
