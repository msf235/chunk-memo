from swarm_memo import SwarmMemo


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


memo = SwarmMemo(
    cache_root="./memo_cache",
    memo_chunk_spec={"strat": 1, "s": 3},
    exec_chunk_size=2,
    enumerate_points=enumerate_points,
    exec_fn=exec_fn,
    collate_fn=collate_fn,
    merge_fn=merge_fn,
)

params = {"alpha": 0.4}
split_spec = {"strat": ["aaa", "bb"], "s": [1, 2, 3, 4]}
output, diag = memo.run(params, split_spec)
print("Output:", output)
print("Diagnostics:", diag)
