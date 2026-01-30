from pathlib import Path

from shard_memo import ShardMemo


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
    memo = ShardMemo(
        cache_root=output_root / "memo_cache",
        memo_chunk_spec={"strat": 1, "s": 2},
        axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
        merge_fn=merge_fn,
    )

    params = {"alpha": 0.4}
    output, diag = memo.run(params, exec_fn)

    print("Output:", output)
    print("Diagnostics:", diag)


if __name__ == "__main__":
    main()
