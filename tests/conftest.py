import sys
import time

from shard_memo import ShardMemo as _ChunkMemo


class _TimestampedWriter:
    def __init__(self, stream, get_start):
        self._stream = stream
        self._get_start = get_start

    def write(self, text):
        if not text:
            return 0
        start = self._get_start()
        elapsed = time.perf_counter() - start if start is not None else 0.0
        prefix = f"[{elapsed:8.3f}s] "
        lines = text.splitlines(True)
        for line in lines:
            if line.strip():
                self._stream.write(prefix + line)
            else:
                self._stream.write(line)
        return len(text)

    def flush(self):
        self._stream.flush()


class _StartTime:
    def __init__(self, value=None):
        self.value = value


_ORIGINAL_INIT = _ChunkMemo.__init__
_ORIGINAL_RUN = _ChunkMemo.run


def _wrap_chunk_init(start_time):
    def wrapped_init(self, *args, **kwargs):
        memo_chunk_spec = kwargs.get("memo_chunk_spec")
        if memo_chunk_spec is None and len(args) > 1:
            memo_chunk_spec = args[1]
        elapsed = time.perf_counter() - start_time
        prefix = f"[{elapsed:8.3f}s] "
        if memo_chunk_spec is not None:
            print(f"{prefix}memo_chunk_spec {memo_chunk_spec}")
        return _ORIGINAL_INIT(self, *args, **kwargs)

    return wrapped_init


def _wrap_chunk_run(start_time):
    def wrapped_run(self, params, exec_fn, *args, **kwargs):
        elapsed = time.perf_counter() - start_time
        prefix = f"[{elapsed:8.3f}s] "
        split_spec = getattr(self, "_split_spec", None)
        print(f"{prefix}split_spec {split_spec}")
        return _ORIGINAL_RUN(self, params, exec_fn, *args, **kwargs)

    return wrapped_run


def pytest_runtest_setup(item):
    item._swarm_test_start = time.perf_counter()
    print(f"\n=== {item.name} ===")
    _ChunkMemo.__init__ = _wrap_chunk_init(item._swarm_test_start)
    _ChunkMemo.run = _wrap_chunk_run(item._swarm_test_start)


def pytest_runtest_teardown(item):
    _ChunkMemo.__init__ = _ORIGINAL_INIT
    _ChunkMemo.run = _ORIGINAL_RUN
    for attr in ("_swarm_stdout", "_swarm_stderr"):
        stream = getattr(item, attr, None)
        if stream is None:
            continue
        if attr == "_swarm_stdout":
            sys.stdout = stream
        else:
            sys.stderr = stream


def pytest_runtest_call(item):
    start = _StartTime(getattr(item, "_swarm_test_start", time.perf_counter()))
    item._swarm_stdout = sys.stdout
    item._swarm_stderr = sys.stderr
    sys.stdout = _TimestampedWriter(sys.stdout, lambda: start.value)
    sys.stderr = _TimestampedWriter(sys.stderr, lambda: start.value)
