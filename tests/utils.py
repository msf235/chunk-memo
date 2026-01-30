import itertools
from typing import Any


def exec_fn_grid(params: dict[str, Any], strat: Any, s: Any) -> Any:
    if isinstance(strat, (list, tuple)) and isinstance(s, (list, tuple)):
        outputs = []
        for strat_value, s_value in itertools.product(strat, s):
            outputs.append(
                {"alpha": params["alpha"], "strat": strat_value, "s": s_value}
            )
        return outputs
    return {"alpha": params["alpha"], "strat": strat, "s": s}


def item_dicts(axis_values: dict[str, list[Any]]) -> list[dict[str, Any]]:
    return [
        {"strat": strat, "s": s}
        for strat, s in itertools.product(axis_values["strat"], axis_values["s"])
    ]


def flatten_outputs(outputs: list[Any]) -> list[Any]:
    flattened: list[Any] = []
    for chunk in outputs:
        if isinstance(chunk, list):
            flattened.extend(chunk)
        else:
            flattened.append(chunk)
    return flattened


def observed_items(outputs: list[Any]) -> set[tuple[Any, Any]]:
    return {(item["strat"], item["s"]) for item in flatten_outputs(outputs)}


def item_from_index(
    item: tuple[int, int], axis_values: dict[str, list[Any]]
) -> dict[str, Any]:
    return {
        "strat": axis_values["strat"][item[0]],
        "s": axis_values["s"][item[1]],
    }
