import hashlib
from typing import Any, Mapping


def stable_serialize(value: Any) -> str:
    if isinstance(value, dict):
        items = ((k, stable_serialize(value[k])) for k in sorted(value))
        return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        inner = ",".join(stable_serialize(v) for v in value)
        return "[" + inner + "]"
    return repr(value)


def params_to_cache_id(params: Mapping[str, Any]) -> str:
    data = stable_serialize(dict(params))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()
