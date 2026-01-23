from swarm_memo import SwarmMemo
import subprocess


def enumerate_points(params, split_spec):
    points = []
    for strat in split_spec["strat"]:
        for s in split_spec["s"]:
            points.append((strat, s))
    return points


def exec_fn(params, point):
    strat, s = point
    return {"strat": strat, "s": s, "value": len(strat) + s}


def collate_fn(outputs):
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def main():
    params = {"alpha": 0.4}
    split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4, 5, 6, 7, 8]}

    print("Testing 'run'")
    print()
    memo = SwarmMemo(
        cache_root="./memo_run_cache",
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=2,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
    )

    output, diag = memo.run(params, split_spec)
    print("Output:", output)
    print("Diagnostics:", diag)

    print()
    print()
    print("Testing 'run_streaming'")
    print()
    memo = SwarmMemo(
        cache_root="./memo_stream_cache",
        memo_chunk_spec={"strat": 1, "s": 3},
        exec_chunk_size=2,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
    )

    diag = memo.run_streaming(params, split_spec)
    # print("Output:", output)
    result = subprocess.run(
        ["tree", "./memo_stream_cache"],
        capture_output=True,
        text=True,
        check=False,
    )

    print("Output (files written to disk): ")
    print(result.stdout)

    print("Diagnostics:", diag)


if __name__ == "__main__":
    main()
