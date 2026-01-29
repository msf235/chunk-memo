from importlib.metadata import PackageNotFoundError, version

from .bridge import BridgeDiagnostics, memo_parallel_run, memo_parallel_run_streaming
from .memo import ShardMemo, Diagnostics

try:
    __version__ = version("shard-memo")
except PackageNotFoundError:
    __version__ = "unknown"

# from .parallel import ParallelDiagnostics, memoized_map, process_pool_parallel

__all__ = [
    "BridgeDiagnostics",
    "ShardMemo",
    "Diagnostics",
    "__version__",
    # "ParallelDiagnostics",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    # "memoized_map",
    # "process_pool_parallel",
]
