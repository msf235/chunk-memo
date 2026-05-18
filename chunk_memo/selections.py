from __future__ import annotations


SweepID = int
ChunkID = int
Interval = tuple[int, int]  # half-open [start, end)



Coord = tuple[int, ...]





# @dataclass(frozen=True)
# class ChunkSet(IndexSelection):
#     """Selection of whole chunks."""
#
#     chunk_spec: ChunkSpec
#     chunk_ids: frozenset[ChunkID]
#
#     def __init__(self, chunk_spec: ChunkSpec, chunk_ids: Iterable[ChunkID]) -> None:
#         object.__setattr__(self, "chunk_spec", chunk_spec)
#         object.__setattr__(self, "chunk_ids", frozenset(chunk_ids))
#
#     def contains(self, sweep_id: SweepID) -> bool:
#         return (
#             0 <= sweep_id < self.chunk_spec.total_size
#             and sweep_id // self.chunk_spec.chunk_size in self.chunk_ids
#         )
#
#     def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
#         if start >= end:
#             return []
#
#         result = IntervalSet()
#         first_chunk = start // self.chunk_spec.chunk_size
#         last_chunk = (end - 1) // self.chunk_spec.chunk_size
#
#         for chunk_id in range(first_chunk, last_chunk + 1):
#             if chunk_id not in self.chunk_ids:
#                 continue
#             a, b = self.chunk_spec.chunk_bounds(chunk_id)
#             lo, hi = max(a, start), min(b, end)
#             if lo < hi:
#                 result = result.with_range(lo, hi)
#
#         return list(result.intervals)
#
#     def bounds(self) -> Optional[Interval]:
#         if not self.chunk_ids:
#             return None
#         ranges = [self.chunk_spec.chunk_bounds(cid) for cid in self.chunk_ids]
#         return min(a for a, _ in ranges), max(b for _, b in ranges)
















if __name__ == "__main__":
    layout = RowMajorLayout([
        range(1, 9),    # x values
        range(0, 21),   # y values
        [3],            # z values
    ])

    selection = CoordUnion(
        CoordProduct((range(1, 9), 0, 3)),
        CoordProduct((1, range(0, 21), 3)),
    )

    index_selection = layout.compile(selection)

    chunk_bitmaps = selection_to_chunk_bitmaps(
        index_selection,
        chunk_size=4096,
        total_size=layout.size,
    )
    breakpoint()
