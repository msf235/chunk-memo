from pathlib import Path

from shard_memo import ShardMemo, memo_parallel_run


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

    memo = ShardMemo(
        cache_root=output_root / "memo_run_cache",
        memo_chunk_spec={"strat": 1, "s": 3},
        axis_values=axis_values,
        merge_fn=merge_fn,
        verbose=1,
    )

    output, diag = memo.run(params, exec_fn)
    print("Output:", output)
    print("Diagnostics:", diag)

    memoized_exec = memo.run_wrap()(exec_fn)
    wrapped_output, wrapped_diag = memoized_exec(params, strat=["aaa"], s=[1, 2, 3, 4])
    print("Wrapped output:", wrapped_output)
    print("Wrapped diagnostics:", wrapped_diag)

    indexed_output, indexed_diag = memoized_exec(
        params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
    )
    print("Indexed output:", indexed_output)
    print("Indexed diagnostics:", indexed_diag)

    status = memoized_exec.cache_status(
        params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
    )
    print("Cache status:", status)
    print("Cached indices:", status["cached_chunk_indices"])
    print("Missing indices:", status["missing_chunk_indices"])

    items = [("aaa", 1), ("bb", 4), ("aaa", 2)]
    bridge_output, bridge_diag = memo_parallel_run(
        memo,
        items,
        exec_fn=exec_fn,
        cache_status=memo.cache_status(
            params, strat=axis_values["strat"], s=axis_values["s"]
        ),
        map_fn=lambda func, items, **kwargs: [func(item) for item in items],
        map_fn_kwargs={"chunksize": 1},
    )
    print("Bridge output:", bridge_output)
    print("Bridge diagnostics:", bridge_diag)

    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=4) as executor:
        pooled_output, pooled_diag = memo_parallel_run(
            memo,
            items,
            exec_fn=exec_fn,
            cache_status=memo.cache_status(
                params, strat=axis_values["strat"], s=axis_values["s"]
            ),
            map_fn=executor.map,
            map_fn_kwargs={"chunksize": 1},
        )
    print("Bridge pooled output:", pooled_output)
    print("Bridge pooled diagnostics:", pooled_diag)


if __name__ == "__main__":
    main()
