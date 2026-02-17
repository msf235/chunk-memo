import pickle as pkl
import time
from pathlib import Path

try:
    from joblib import Memory
except ImportError:  # pragma: no cover - optional dependency
    Memory = None

try:
    from klepto import inf_cache, keymaps
    from klepto.archives import dir_archive
except ImportError:  # pragma: no cover - optional dependency
    inf_cache = None
    keymaps = None
    dir_archive = None

N = 20000
chunk_sizes = [1, 5, 10, 25, 50, 100]

DATA_ITEM = "a" * 10000
OUTPUT_ROOT = Path("output")


def reset_chunks(chunk_size):
    OUTPUT_ROOT.mkdir(exist_ok=True)
    filepath = OUTPUT_ROOT / f"chunk_{chunk_size}"
    if filepath.exists():
        for chunk_file in filepath.glob("*.pkl"):
            chunk_file.unlink()
    filepath.mkdir(exist_ok=True)
    n_chunks = N // chunk_size

    start = time.perf_counter()
    for chunk_idx in range(n_chunks):
        filename = f"chunk_{chunk_idx}.pkl"
        with open(filepath / filename, "wb") as handle:
            pkl.dump([DATA_ITEM] * chunk_size, handle)
    save_time = time.perf_counter() - start
    return filepath, n_chunks, save_time


def reset_nested_chunks(chunk_size, *, bucket_size=1000):
    OUTPUT_ROOT.mkdir(exist_ok=True)
    root = OUTPUT_ROOT / f"chunk_nested_{chunk_size}"
    if root.exists():
        for chunk_file in root.rglob("*.pkl"):
            chunk_file.unlink()
    root.mkdir(exist_ok=True)
    n_chunks = N // chunk_size

    start = time.perf_counter()
    for chunk_idx in range(n_chunks):
        bucket = f"bucket_{chunk_idx // bucket_size:04d}"
        bucket_path = root / bucket
        bucket_path.mkdir(exist_ok=True)
        filename = f"chunk_{chunk_idx}.pkl"
        with open(bucket_path / filename, "wb") as handle:
            pkl.dump([DATA_ITEM] * chunk_size, handle)
    save_time = time.perf_counter() - start
    return root, n_chunks, save_time


def reset_nested_single_item_chunks(*, bucket_size=1000):
    OUTPUT_ROOT.mkdir(exist_ok=True)
    root = OUTPUT_ROOT / "chunk_nested_single"
    if root.exists():
        for chunk_file in root.rglob("*.pkl"):
            chunk_file.unlink()
    root.mkdir(exist_ok=True)

    start = time.perf_counter()
    for chunk_idx in range(N):
        bucket = f"bucket_{chunk_idx // bucket_size:04d}"
        bucket_path = root / bucket
        bucket_path.mkdir(exist_ok=True)
        filename = f"chunk_{chunk_idx}.pkl"
        with open(bucket_path / filename, "wb") as handle:
            pkl.dump(DATA_ITEM, handle)
    save_time = time.perf_counter() - start
    return root, save_time


def load_chunks(filepath, chunk_indices):
    data_chunks = []
    for chunk_idx in chunk_indices:
        filename = f"chunk_{chunk_idx}.pkl"
        with open(filepath / filename, "rb") as handle:
            data_chunk = pkl.load(handle)
            data_chunks.append(data_chunk)
    return data_chunks


def load_nested_chunks(filepath, chunk_indices, *, bucket_size=1000):
    data_chunks = []
    for chunk_idx in chunk_indices:
        bucket = f"bucket_{chunk_idx // bucket_size:04d}"
        filename = f"chunk_{chunk_idx}.pkl"
        with open(filepath / bucket / filename, "rb") as handle:
            data_chunk = pkl.load(handle)
            data_chunks.append(data_chunk)
    return data_chunks


def load_nested_single_item_chunks(filepath, chunk_indices, *, bucket_size=1000):
    data_chunks = []
    for chunk_idx in chunk_indices:
        bucket = f"bucket_{chunk_idx // bucket_size:04d}"
        filename = f"chunk_{chunk_idx}.pkl"
        with open(filepath / bucket / filename, "rb") as handle:
            data_chunk = pkl.load(handle)
            data_chunks.append(data_chunk)
    return data_chunks


def timed_load(filepath, chunk_indices):
    start = time.perf_counter()
    load_chunks(filepath, chunk_indices)
    return time.perf_counter() - start


def timed_nested_load(filepath, chunk_indices, *, bucket_size=1000):
    start = time.perf_counter()
    load_nested_chunks(filepath, chunk_indices, bucket_size=bucket_size)
    return time.perf_counter() - start


def timed_nested_single_item_load(filepath, chunk_indices, *, bucket_size=1000):
    start = time.perf_counter()
    load_nested_single_item_chunks(filepath, chunk_indices, bucket_size=bucket_size)
    return time.perf_counter() - start


def run_joblib_benchmark():
    if Memory is None:
        return None

    params = {"scale": 2}
    OUTPUT_ROOT.mkdir(exist_ok=True)
    memory = Memory(location=str(OUTPUT_ROOT / "joblib_chunk_cache"), verbose=0)

    @memory.cache
    def joblib_exec(params, point):
        return params["scale"] * point

    start = time.perf_counter()
    for point in range(N):
        joblib_exec(params, point)
    save_time = time.perf_counter() - start

    all_indices = list(range(N))
    slice_count = max(1, N // 10)
    slice_indices = list(range(slice_count))

    start = time.perf_counter()
    for point in all_indices:
        joblib_exec(params, point)
    read_all_time = time.perf_counter() - start

    start = time.perf_counter()
    for point in slice_indices:
        joblib_exec(params, point)
    read_slice_time = time.perf_counter() - start

    return save_time, read_all_time, read_slice_time


def run_klepto_benchmark():
    if inf_cache is None or dir_archive is None or keymaps is None:
        return None

    OUTPUT_ROOT.mkdir(exist_ok=True)
    archive = dir_archive(str(OUTPUT_ROOT / "klepto_chunk_cache"), serialized=True)
    cache = inf_cache(cache=archive, keymap=keymaps.picklemap(flat=True))

    @cache
    def klepto_exec(params, point):
        return params["scale"] * point

    params = {"scale": 2}
    start = time.perf_counter()
    for point in range(N):
        klepto_exec(params, point)
    dump_cache = getattr(klepto_exec, "dump", None)
    if dump_cache is not None:
        dump_cache()
    save_time = time.perf_counter() - start

    all_indices = list(range(N))
    slice_count = max(1, N // 10)
    slice_indices = list(range(slice_count))

    start = time.perf_counter()
    for point in all_indices:
        klepto_exec(params, point)
    read_all_time = time.perf_counter() - start

    start = time.perf_counter()
    for point in slice_indices:
        klepto_exec(params, point)
    read_slice_time = time.perf_counter() - start

    return save_time, read_all_time, read_slice_time


def main():
    print("Chunk load benchmark")
    print(f"N={N}")
    print("chunk_size  save_s   read_all_s  read_slice_s")

    for chunk_size in chunk_sizes:
        filepath, n_chunks, save_time = reset_chunks(chunk_size)
        all_indices = list(range(n_chunks))
        slice_count = max(1, n_chunks // 10)
        slice_indices = list(range(slice_count))

        read_all_time = timed_load(filepath, all_indices)
        read_slice_time = timed_load(filepath, slice_indices)
        print(
            f"{chunk_size:10d}  {save_time:6.4f}  {read_all_time:10.4f}  {read_slice_time:11.4f}"
        )

    print("\nNested chunk layout (same chunk payloads)")
    print("chunk_size  save_s   read_all_s  read_slice_s")

    for chunk_size in chunk_sizes:
        filepath, n_chunks, save_time = reset_nested_chunks(chunk_size)
        all_indices = list(range(n_chunks))
        slice_count = max(1, n_chunks // 10)
        slice_indices = list(range(slice_count))

        read_all_time = timed_nested_load(filepath, all_indices)
        read_slice_time = timed_nested_load(filepath, slice_indices)
        print(
            f"{chunk_size:10d}  {save_time:6.4f}  {read_all_time:10.4f}  {read_slice_time:11.4f}"
        )

    print("\nNested chunk layout (1 item per file)")
    print("bucketed  save_s   read_all_s  read_slice_s")

    filepath, save_time = reset_nested_single_item_chunks()
    all_indices = list(range(N))
    slice_count = max(1, N // 10)
    slice_indices = list(range(slice_count))

    read_all_time = timed_nested_single_item_load(filepath, all_indices)
    read_slice_time = timed_nested_single_item_load(filepath, slice_indices)
    print(
        f"{str(True):7s}  {save_time:6.4f}  {read_all_time:10.4f}  {read_slice_time:11.4f}"
    )

    print("\njoblib.Memory (1 item per file)")
    print("joblib   save_s   read_all_s  read_slice_s")
    joblib_stats = run_joblib_benchmark()
    if joblib_stats is None:
        print("joblib    unavailable")
    else:
        save_time, read_all_time, read_slice_time = joblib_stats
        print(
            f"{str(True):7s}  {save_time:6.4f}  {read_all_time:10.4f}  {read_slice_time:11.4f}"
        )

    print("\nklepto (1 item per file)")
    print("klepto   save_s   read_all_s  read_slice_s")
    klepto_stats = run_klepto_benchmark()
    if klepto_stats is None:
        print("klepto    unavailable")
    else:
        save_time, read_all_time, read_slice_time = klepto_stats
        print(
            f"{str(True):7s}  {save_time:6.4f}  {read_all_time:10.4f}  {read_slice_time:11.4f}"
        )


if __name__ == "__main__":
    main()
