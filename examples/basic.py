from swarm_memo import ChunkMemo


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
    params = {"alpha": 0.4}
    split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4, 5, 6, 7, 8]}

    memo = ChunkMemo(
        cache_root="./memo_run_cache",
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_fn=exec_fn,
        merge_fn=merge_fn,
        split_spec=split_spec,
    )

    output, diag = memo.run(params, split_spec)
    print("Output:", output)
    print("Diagnostics:", diag)

    memoized_exec = memo.run_wrap()(exec_fn)
    wrapped_output, wrapped_diag = memoized_exec(params, strat=["aaa"], s=[1, 2, 3])
    print("Wrapped output:", wrapped_output)
    print("Wrapped diagnostics:", wrapped_diag)

    indexed_output, indexed_diag = memoized_exec(
        params, axis_indices={"strat": range(0, 1), "s": slice(0, 3)}
    )
    print("Indexed output:", indexed_output)
    print("Indexed diagnostics:", indexed_diag)


if __name__ == "__main__":
    main()
