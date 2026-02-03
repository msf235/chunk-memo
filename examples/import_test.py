from pathlib import Path

from shard_memo import ChunkCache
from shard_memo.runners import run


def exec_fn(params, strat, s):
    outputs = []
    for strat_value in strat:
        for s_value in s:
            outputs.append(
                {"alpha": params["alpha"], "strat": strat_value, "s": s_value}
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
    memo = ChunkCache(
        cache_root=output_root / "memo_cache",
        cache_chunk_spec={"strat": 1, "s": 2},
        axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
        merge_fn=merge_fn,
    )

    params = {"alpha": 0.4}
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


if __name__ == "__main__":
    main()
