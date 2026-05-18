
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from chunk_memo.coord import CoordSelection
from chunk_memo.index import IndexSelection

SweepID = int
ChunkID = int
Interval = tuple[int, int]  # half-open [start, end)



Coord = tuple[int, ...]
@dataclass(frozen=True)
class ChunkSpec:
    chunk_size: int
    total_size: int

    @property
    def num_chunks(self) -> int:
        return (self.total_size + self.chunk_size - 1) // self.chunk_size

    def chunk_bounds(self, chunk_id: ChunkID) -> Interval:
        if not (0 <= chunk_id < self.num_chunks):
            raise ValueError(f"chunk_id {chunk_id} out of bounds")
        start = chunk_id * self.chunk_size
        end = min(start + self.chunk_size, self.total_size)
        return start, end

# -----------------------------------------------------------------------------
# Roaring bitmap materialization
# -----------------------------------------------------------------------------

def _new_bitmap():
    from pyroaring import BitMap
    return BitMap()


def _bitmap_add_range(bitmap, start: int, end: int) -> None:
    if start < end:
        bitmap.add_range(start, end)

def selection_to_chunk_bitmaps(
    selection: IndexSelection,
    chunk_size: int,
    *,
    total_size: Optional[int] = None,
) -> dict[ChunkID, object]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    chunks: dict[ChunkID, object] = {}

    for start, end in selection.iter_intervals():
        if total_size is not None:
            start = max(start, 0)
            end = min(end, total_size)

        i = start

        while i < end:
            chunk_id, offset_start = divmod(i, chunk_size)
            chunk_stop = min(end, (chunk_id + 1) * chunk_size)
            offset_stop = chunk_stop - chunk_id * chunk_size

            bitmap = chunks.get(chunk_id)
            if bitmap is None:
                bitmap = _new_bitmap()
                chunks[chunk_id] = bitmap

            _bitmap_add_range(bitmap, offset_start, offset_stop)

            i = chunk_stop

    return chunks

def compile_coord_selection_to_chunk_bitmaps(
    layout: RowMajorLayout,
    selection: CoordSelection,
    chunk_size: int,
    *,
    max_enumeration: int = 1_000_000,
) -> dict[ChunkID, object]:
    index_selection = layout.compile(
        selection,
        max_enumeration=max_enumeration,
    )

    return selection_to_chunk_bitmaps(
        index_selection,
        chunk_size,
        total_size=layout.size,
    )
