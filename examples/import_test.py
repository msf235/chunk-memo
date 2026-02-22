from pathlib import Path

from chunk_memo import ChunkCache, run


def exec_fn(params, strat, s):
    outputs = []
    for strat_value in strat:
        for s_value in s:
            outputs.append(
                {"alpha": params["alpha"], "strat": strat_value, "s": s_value}
            )
    return outputs


def collate_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def main():
    output_root = Path("output")
    output_root.mkdir(exist_ok=True)
    memo = ChunkCache(
        root=output_root / "memo_cache",
        chunk_spec={"strat": 1, "s": 2},
        axis_values={"strat": ["a", "b"], "s": [1, 2, 3]},
        collate_fn=collate_fn,
    )

    params = {"alpha": 0.4}
    memo.set_params(params)
    output, diag = run(memo, exec_fn)

    print("Output:", output)
    print("Diagnostics:", diag)


if __name__ == "__main__":
    main()
