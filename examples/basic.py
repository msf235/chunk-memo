from pathlib import Path

from chunk_memo import ChunkCache, ChunkMemo, run


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


def collate_fn(chunks):
    if not chunks:
        return []
    first = chunks[0]
    if isinstance(first, list):
        merged = []
        for chunk in chunks:
            merged.extend(chunk)
        return merged
    return list(chunks)


def main():
    output_root = Path("output")
    output_root.mkdir(exist_ok=True)
    params = {"alpha": 0.4}
    axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4, 5, 6, 7, 8]}

    memo = ChunkCache(
        root=output_root / "memo_run_cache",
        chunk_spec={"strat": 1, "s": 3},
        axis_values=axis_values,
        collate_fn=collate_fn,
        verbose=1,
    )
    memo.set_params(params)
    output, diag = run(memo, exec_fn)
    print("Output:", output)
    print("Diagnostics:", diag)

    wrapper = ChunkMemo(memo)
    memoized_exec = wrapper.run_wrap()(exec_fn)
    wrapped_output, wrapped_diag = memoized_exec(params, strat=["aaa"], s=[1, 2, 3, 4])
    print("Wrapped output:", wrapped_output)
    print("Wrapped diagnostics:", wrapped_diag)

    @wrapper.run_wrap()
    def exec_fn_2(alpha, strat, s):
        outputs = []
        for strat_value in strat:
            for s_value in s:
                outputs.append(
                    {
                        "alpha": alpha,
                        "strat": strat_value,
                        "s": s_value,
                        "value": len(strat_value) + s_value,
                    }
                )
        return outputs

    wrapped_output, wrapped_diag = exec_fn_2(alpha=0.4, strat=["aaa"], s=[1, 2, 3, 4])
    print("Wrapped output 2:", wrapped_output)
    print("Wrapped diagnostics 2:", wrapped_diag)


if __name__ == "__main__":
    main()
