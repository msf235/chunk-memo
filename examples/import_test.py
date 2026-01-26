from swarm_memo import ChunkMemo


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
    memo = ChunkMemo(
        cache_root="./memo_cache",
        memo_chunk_spec={"strat": 1, "s": 2},
        exec_fn=exec_fn,
        merge_fn=merge_fn,
    )

    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3]}
    output, diag = memo.run(params, split_spec)

    print("Output:", output)
    print("Diagnostics:", diag)


if __name__ == "__main__":
    main()
