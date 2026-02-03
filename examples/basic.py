from pathlib import Path

from shard_memo import ChunkCache, ChunkMemo, memo_parallel_run
from shard_memo.runners import run


def exec_fn(params, strat, s):
    outputs = []
    for strat_value in strat:
        for s_value in s:
            outputs.append(
                {
                    "alpha": params["alpha"],
                    "strat": strat_value,
                    "s": s_value,
                    "value": len(strat_value) + s_value,
                }
            )
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def main():
    output_root = Path("output")
    output_root.mkdir(exist_ok=True)
    params = {"alpha": 0.4}
    axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4, 5, 6, 7, 8]}

    memo = ChunkCache(
        cache_root=output_root / "memo_run_cache",
        cache_chunk_spec={"strat": 1, "s": 3},
        axis_values=axis_values,
        merge_fn=merge_fn,
        verbose=1,
    )

    run_kwargs = {
        "prepare_run": memo.prepare_run,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "extract_items_from_map": memo.extract_items_from_map,
        "collect_chunk_data": memo.collect_chunk_data,
        "context": memo,
    }
    output, diag = run(params, exec_fn, **run_kwargs)
    print("Output:", output)
    print("Diagnostics:", diag)

    wrapper = ChunkMemo(memo)
    memoized_exec = wrapper.run_wrap()(exec_fn)
    wrapped_output, wrapped_diag = memoized_exec(params, strat=["aaa"], s=[1, 2, 3, 4])
    print("Wrapped output:", wrapped_output)
    print("Wrapped diagnostics:", wrapped_diag)

    indexed_output, indexed_diag = memoized_exec(
        params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
    )
    print("Indexed output:", indexed_output)
    print("Indexed diagnostics:", indexed_diag)

    status = memoized_exec.cache_status(  # type: ignore[attr-defined]
        params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
    )
    print("Cache status:", status)
    print("Cached indices:", status["cached_chunk_indices"])
    print("Missing indices:", status["missing_chunk_indices"])

    items = [("aaa", 1), ("bb", 4), ("aaa", 2)]
    parallel_kwargs = {
        "cache_status_fn": memo.cache_status,
        "write_metadata": memo.write_metadata,
        "chunk_hash": memo.chunk_hash,
        "resolve_cache_path": memo.resolve_cache_path,
        "load_payload": memo.load_payload,
        "write_chunk_payload": memo.write_chunk_payload,
        "update_chunk_index": memo.update_chunk_index,
        "build_item_maps_from_axis_values": memo.build_item_maps_from_axis_values,
        "build_item_maps_from_chunk_output": memo.build_item_maps_from_chunk_output,
        "reconstruct_output_from_items": memo.reconstruct_output_from_items,
        "collect_chunk_data": memo.collect_chunk_data,
        "item_hash": memo.item_hash,
        "context": memo,
    }
    bridge_output, bridge_diag = memo_parallel_run(
        items,
        exec_fn=exec_fn,
        **parallel_kwargs,
        params=params,
        strat=axis_values["strat"],
        s=axis_values["s"],
        map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        map_fn_kwargs={"chunksize": 1},
    )
    print("Bridge output:", bridge_output)
    print("Bridge diagnostics:", bridge_diag)

    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=4) as executor:
        pooled_output, pooled_diag = memo_parallel_run(
            items,
            exec_fn=exec_fn,
            **parallel_kwargs,
            params=params,
            strat=axis_values["strat"],
            s=axis_values["s"],
            map_fn=executor.map,
            map_fn_kwargs={"chunksize": 1},
        )
    print("Bridge pooled output:", pooled_output)
    print("Bridge pooled diagnostics:", pooled_diag)


if __name__ == "__main__":
    main()
