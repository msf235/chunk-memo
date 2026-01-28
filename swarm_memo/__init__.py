from importlib.metadata import PackageNotFoundError, version

from .bridge import BridgeDiagnostics, memo_parallel_run, memo_parallel_run_streaming
from .memo import ChunkMemo, Diagnostics

try:
    __version__ = version("swarm-memo")
except PackageNotFoundError:
    __version__ = "unknown"

# from .parallel import ParallelDiagnostics, memoized_map, process_pool_parallel

__all__ = [
    "BridgeDiagnostics",
    "ChunkMemo",
    "Diagnostics",
    "__version__",
    # "ParallelDiagnostics",
    "memo_parallel_run",
    "memo_parallel_run_streaming",
    # "memoized_map",
    # "process_pool_parallel",
]
