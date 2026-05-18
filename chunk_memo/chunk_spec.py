from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence


AxisChunks = tuple[tuple[Any, ...], ...]


def _materialize_axis_values(axis_values_obj: Any) -> list[Any]:
    if isinstance(axis_values_obj, Sequence) and not isinstance(
        axis_values_obj, (str, bytes, bytearray)
    ):
        return list(axis_values_obj)
    return list(axis_values_obj)


def _chunk_values(values: Sequence[Any], size: int) -> AxisChunks:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    chunks: list[tuple[Any, ...]] = []
    for start in range(0, len(values), size):
        end = min(start + size, len(values))
        chunks.append(tuple(values[start:end]))
    return tuple(chunks)


@dataclass(frozen=True)
class AxisChunkSpec:
    axis: str
    chunks: AxisChunks

    def __post_init__(self) -> None:
        if not self.chunks:
            object.__setattr__(self, "chunks", tuple())
        seen: set[Any] = set()
        for chunk in self.chunks:
            if not isinstance(chunk, tuple):
                raise TypeError("Axis chunks must be tuples")
            if not chunk:
                raise ValueError(f"Chunk for axis '{self.axis}' cannot be empty")
            for value in chunk:
                if value in seen:
                    raise ValueError(
                        f"Duplicate value {value!r} found in chunk spec for axis '{self.axis}'"
                    )
                seen.add(value)

    @property
    def size(self) -> int:
        if not self.chunks:
            return 0
        return max(len(chunk) for chunk in self.chunks)

    def axis_values(self) -> list[Any]:
        values: list[Any] = []
        for chunk in self.chunks:
            values.extend(chunk)
        return values

    def locate(self, value: Any) -> tuple[int, int]:
        for chunk_id, chunk in enumerate(self.chunks):
            for offset, chunk_value in enumerate(chunk):
                if chunk_value == value:
                    return chunk_id, offset
        raise KeyError(f"Value {value!r} not found in chunk spec for axis '{self.axis}'")

    def to_metadata(self) -> dict[str, Any]:
        return {
            "chunks": [list(chunk) for chunk in self.chunks],
            "size": self.size,
        }


class ChunkSpec(Mapping[str, dict[str, Any]]):
    def __init__(self, axes: Mapping[str, AxisChunkSpec]) -> None:
        self._axes = dict(axes)

    @classmethod
    def from_axis_values(
        cls,
        chunk_spec: Mapping[str, Any] | None,
        axis_values: Mapping[str, Any],
        *,
        axis_order: Sequence[str] | None = None,
    ) -> "ChunkSpec":
        ordered_axes = tuple(axis_order) if axis_order is not None else tuple(sorted(axis_values))
        axes: dict[str, AxisChunkSpec] = {}
        raw_spec = dict(chunk_spec or {})
        for axis in ordered_axes:
            if axis not in axis_values:
                raise KeyError(f"Missing axis '{axis}' in axis_values")
            values = _materialize_axis_values(axis_values[axis])
            axis_entry = raw_spec.get(axis)
            axes[axis] = cls._axis_spec_from_values(axis, axis_entry, values)
        for axis, axis_entry in raw_spec.items():
            if axis in axes:
                continue
            if not isinstance(axis_entry, Mapping) or "chunks" not in axis_entry:
                raise KeyError(
                    f"Axis '{axis}' is missing from axis_values; provide explicit chunks"
                )
            axes[axis] = cls._axis_spec_from_explicit(axis, axis_entry)
        return cls(axes)

    @classmethod
    def from_metadata(cls, payload: Mapping[str, Any]) -> "ChunkSpec":
        axes: dict[str, AxisChunkSpec] = {}
        for axis, axis_entry in payload.items():
            if not isinstance(axis_entry, Mapping):
                raise TypeError(f"Chunk spec for axis '{axis}' must be a mapping")
            axes[axis] = cls._axis_spec_from_explicit(axis, axis_entry)
        return cls(axes)

    @staticmethod
    def _axis_spec_from_values(
        axis: str,
        axis_entry: Any,
        axis_values: Sequence[Any],
    ) -> AxisChunkSpec:
        if isinstance(axis_entry, Mapping) and "chunks" in axis_entry:
            spec = ChunkSpec._axis_spec_from_explicit(axis, axis_entry)
            if spec.axis_values() != list(axis_values):
                raise ValueError(
                    f"Explicit chunks for axis '{axis}' do not match axis_values ordering"
                )
            return spec
        if axis_entry is None:
            size = len(axis_values) if axis_values else 1
        elif isinstance(axis_entry, Mapping):
            size = axis_entry.get("size")
            if size is None:
                raise KeyError(f"Missing size for axis '{axis}'")
        else:
            size = axis_entry
        return AxisChunkSpec(axis=axis, chunks=_chunk_values(list(axis_values), int(size)))

    @staticmethod
    def _axis_spec_from_explicit(axis: str, axis_entry: Mapping[str, Any]) -> AxisChunkSpec:
        raw_chunks = axis_entry.get("chunks")
        if raw_chunks is None:
            raise KeyError(f"Missing chunks for axis '{axis}'")
        chunks = tuple(tuple(chunk) for chunk in raw_chunks)
        return AxisChunkSpec(axis=axis, chunks=chunks)

    def for_axis(self, axis: str) -> AxisChunkSpec:
        if axis not in self._axes:
            raise KeyError(f"Missing chunk spec for axis '{axis}'")
        return self._axes[axis]

    def axis_values(self) -> dict[str, list[Any]]:
        return {axis: spec.axis_values() for axis, spec in self._axes.items()}

    def with_axis_values(
        self,
        axis_values: Mapping[str, Sequence[Any]],
        *,
        axis_order: Sequence[str] | None = None,
    ) -> "ChunkSpec":
        size_spec = {axis: self.for_axis(axis).size for axis in self._axes}
        return ChunkSpec.from_axis_values(
            size_spec,
            axis_values,
            axis_order=axis_order,
        )

    def to_metadata(self) -> dict[str, dict[str, Any]]:
        return {axis: spec.to_metadata() for axis, spec in self._axes.items()}

    def __getitem__(self, axis: str) -> dict[str, Any]:
        return self.for_axis(axis).to_metadata()

    def __iter__(self) -> Iterator[str]:
        return iter(self._axes)

    def __len__(self) -> int:
        return len(self._axes)

    def items(self) -> Iterable[tuple[str, dict[str, Any]]]:
        for axis in self._axes:
            yield axis, self[axis]
