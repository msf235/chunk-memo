from swarm_memo import SwarmMemo


def enumerate_points(params, split_spec):
    points = []
    for strat in split_spec["strat"]:
        for s in split_spec["s"]:
            points.append((strat, s))
    return points


def exec_fn(params, point):
    strat, s = point
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def collate_fn(outputs):
    return outputs


def merge_fn(chunks):
    merged = []
    for chunk in chunks:
        merged.extend(chunk)
    return merged


def main():
    memo = SwarmMemo(
        cache_root="./memo_cache",
        memo_chunk_spec={"strat": 1, "s": 2},
        exec_chunk_size=2,
        exec_fn=exec_fn,
        collate_fn=collate_fn,
        merge_fn=merge_fn,
        point_enumerator=enumerate_points,
    )

    params = {"alpha": 0.4}
    split_spec = {"strat": ["a", "b"], "s": [1, 2, 3]}
    output, diag = memo.run(params, split_spec)

    print("Output:", output)
    print("Diagnostics:", diag)


if __name__ == "__main__":
    main()
