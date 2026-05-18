
# -----------------------------------------------------------------------------
# Selection serialization
# -----------------------------------------------------------------------------

class SelectionCodec:
    """Serialize/deserialize IndexSelection objects."""

    @staticmethod
    def to_dict(selection: IndexSelection) -> dict:
        if isinstance(selection, EmptySet):
            return {"type": "EmptySet"}

        if isinstance(selection, IntervalSet):
            return {
                "type": "IntervalSet",
                "intervals": [list(interval) for interval in selection.intervals],
            }

        if isinstance(selection, StridedSet):
            return {
                "type": "StridedSet",
                "start": selection.start,
                "end": selection.end,
                "step": selection.step,
            }

        if isinstance(selection, ChunkSet):
            return {
                "type": "ChunkSet",
                "chunk_spec": {
                    "chunk_size": selection.chunk_spec.chunk_size,
                    "total_size": selection.chunk_spec.total_size,
                },
                "chunk_ids": sorted(selection.chunk_ids),
            }

        if isinstance(selection, UnionSet):
            return {
                "type": "UnionSet",
                "parts": [
                    SelectionCodec.to_dict(part)
                    for part in selection.parts
                ],
            }

        if isinstance(selection, IntersectionSet):
            return {
                "type": "IntersectionSet",
                "parts": [
                    SelectionCodec.to_dict(part)
                    for part in selection.parts
                ],
            }

        if isinstance(selection, DifferenceSet):
            return {
                "type": "DifferenceSet",
                "base": SelectionCodec.to_dict(selection.base),
                "remove": SelectionCodec.to_dict(selection.remove),
            }

        raise TypeError(f"Cannot serialize {type(selection).__name__}")

    @staticmethod
    def from_dict(
        data: dict,
        chunk_spec: ChunkSpec | None = None,
    ) -> IndexSelection:
        typ = data["type"]

        if typ == "EmptySet":
            return EmptySet()

        if typ == "IntervalSet":
            return IntervalSet(
                (int(start), int(end))
                for start, end in data["intervals"]
            )

        if typ == "StridedSet":
            return StridedSet(
                start=int(data["start"]),
                end=int(data["end"]),
                step=int(data["step"]),
            )

        if typ == "ChunkSet":
            if chunk_spec is None:
                spec_data = data["chunk_spec"]
                chunk_spec = ChunkSpec(
                    chunk_size=int(spec_data["chunk_size"]),
                    total_size=int(spec_data["total_size"]),
                )

            return ChunkSet(
                chunk_spec=chunk_spec,
                chunk_ids=data["chunk_ids"],
            )

        if typ == "UnionSet":
            return UnionSet(*[
                SelectionCodec.from_dict(
                    part,
                    chunk_spec=chunk_spec,
                )
                for part in data["parts"]
            ])

        if typ == "IntersectionSet":
            return IntersectionSet(*[
                SelectionCodec.from_dict(
                    part,
                    chunk_spec=chunk_spec,
                )
                for part in data["parts"]
            ])

        if typ == "DifferenceSet":
            return DifferenceSet(
                base=SelectionCodec.from_dict(
                    data["base"],
                    chunk_spec=chunk_spec,
                ),
                remove=SelectionCodec.from_dict(
                    data["remove"],
                    chunk_spec=chunk_spec,
                ),
            )

        raise ValueError(f"Unknown selection type {typ!r}")
