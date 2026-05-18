import functools
from pathlib import Path

from chunk_memo import ChunkMemo, run


def exec_fn(params, strat, s):
    return {
        "alpha": params["alpha"],
        "strat": strat,
        "s": s,
        "value": len(strat) + s,
    }

def exec_fn2(params, **axis_val):
    dict = {'alpha': params["alpha"], **axis_val}
    dict["value"] = len(dict["strat"]) + dict["s"]
    return dict


def main():
    output_root = Path("output")
    output_root.mkdir(exist_ok=True)
    params = {"alpha": 0.4}
    axis_values = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4, 5, 6, 7, 8]}

    memo = ChunkMemo(
        root=output_root / "memo_run_cache",
        chunk_spec={"strat": 1, "s": 3},
        axis_values=axis_values,
        verbose=1,
    )
    cache = memo.cache_for_params(params)
    output, diag = run(cache, functools.partial(exec_fn, params))
    print("Output:", output)
    print("Diagnostics:", diag)

    exec_fn_wrapped = memo.cache(max_workers=1)(exec_fn2)
    output, diag = exec_fn_wrapped(
        params,
        **axis_values,
    )


if __name__ == "__main__":
    main()
