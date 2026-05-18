from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from chunk_memo.coord import (CoordDifference, CoordEmpty, CoordIntersection,
                              CoordProduct, CoordSelection, CoordUnion,
                              normalize_axes)
from chunk_memo.index import (DifferenceSet, EmptySet, IndexSelection,
                              IntersectionSet, IntervalSet, StridedSet,
                              UnionSet)

SweepID = int
ChunkID = int
Interval = tuple[int, int]  # half-open [start, end)



Coord = tuple[int, ...]


# -----------------------------------------------------------------------------
# Row-major coordinate -> flattened-index compiler
# -----------------------------------------------------------------------------

class IntervalBuilder:
    def __init__(self) -> None:
        self._intervals: list[Interval] = []

    def add_range(self, start: SweepID, end: SweepID) -> None:
        if start > end:
            raise ValueError("start must be <= end")
        if start < end:
            self._intervals.append((start, end))

    def add_point(self, x: SweepID) -> None:
        self.add_range(x, x + 1)

    def finish(self) -> IntervalSet:
        return IntervalSet(self._intervals)

def _row_major_strides(shape: Sequence[int]) -> tuple[int, ...]:
    if any(size <= 0 for size in shape):
        raise ValueError("all dimensions must be positive")

    strides: list[int] = []
    running = 1

    for size in reversed(shape[1:]):
        running *= size
        strides.append(running)

    return tuple(reversed(strides)) + (1,)

@dataclass(frozen=True)
class RowMajorLayout:
    axes: tuple[tuple[int, ...], ...]
    shape: tuple[int, ...]
    strides: tuple[int, ...]
    value_to_pos: tuple[dict[int, int], ...]

    def __init__(self, axes: Sequence[Sequence[int] | int]) -> None:
        norm_axes = normalize_axes(axes)
        shape = tuple(len(axis) for axis in norm_axes)
        strides = _row_major_strides(shape)

        value_to_pos = tuple(
            {value: i for i, value in enumerate(axis)}
            for axis in norm_axes
        )

        object.__setattr__(self, "axes", norm_axes)
        object.__setattr__(self, "shape", shape)
        object.__setattr__(self, "strides", strides)
        object.__setattr__(self, "value_to_pos", value_to_pos)

    @classmethod
    def from_shape(cls, shape: Sequence[int]) -> "RowMajorLayout":
        return cls(tuple(range(n) for n in shape))

    @property
    def rank(self) -> int:
        return len(self.axes)

    @property
    def size(self) -> int:
        sprod = 1
        for s in self.shape:
            sprod *= s
        return sprod

    def coord_to_index(self, coord: Coord) -> SweepID:
        if len(coord) != self.rank:
            raise ValueError(f"coord has rank {len(coord)}, expected {self.rank}")

        out = 0

        for value, mapping, stride in zip(coord, self.value_to_pos, self.strides):
            try:
                pos = mapping[value]
            except KeyError:
                raise ValueError(f"coord {coord} out of bounds") from None

            out += pos * stride

        return out

    def index_to_coord(self, sweep_id: SweepID) -> Coord:
        if not (0 <= sweep_id < self.size):
            raise ValueError("sweep_id out of bounds")

        out: list[int] = []
        rem = sweep_id

        for axis, stride in zip(self.axes, self.strides):
            pos = rem // stride
            rem %= stride
            out.append(axis[pos])

        return tuple(out)

    def compile(
        self,
        selection: CoordSelection,
        *,
        max_enumeration: int = 1_000_000,
    ) -> IndexSelection:
        selection = selection.simplify()

        if isinstance(selection, CoordEmpty):
            return EmptySet()

        if isinstance(selection, CoordProduct):
            return self.compile_product(selection)

        if isinstance(selection, CoordUnion):
            return UnionSet(
                *(
                    self.compile(part, max_enumeration=max_enumeration)
                    for part in selection.parts
                )
            )

        if isinstance(selection, CoordIntersection):
            return IntersectionSet(
                *(
                    self.compile(part, max_enumeration=max_enumeration)
                    for part in selection.parts
                )
            )

        if isinstance(selection, CoordDifference):
            return DifferenceSet(
                self.compile(selection.base, max_enumeration=max_enumeration),
                self.compile(selection.remove, max_enumeration=max_enumeration),
            )

        return self._compile_by_enumeration(
            selection,
            max_enumeration=max_enumeration,
        )

    def compile_product(self, product: CoordProduct) -> IndexSelection:
        allowed = self._allowed_positions(product)

        if allowed is None:
            return EmptySet()

        if self._is_whole_layout(allowed):
            return IntervalSet([(0, self.size)])

        strided = self._try_compile_fixed_fastest_axis(allowed)
        if strided is not None:
            return strided

        return self._compile_product_positions_to_intervals(allowed)

    def _allowed_positions(self, product: CoordProduct) -> list[tuple[int, ...]] | None:
        if len(product.axes) != self.rank:
            raise ValueError(
                f"product rank {len(product.axes)} does not match layout rank {self.rank}"
            )

        allowed: list[tuple[int, ...]] = []

        for axis_selection, mapping in zip(product.axes, self.value_to_pos):
            positions = tuple(
                sorted(
                    mapping[value]
                    for value in axis_selection.iter_values()
                    if value in mapping
                )
            )

            if not positions:
                return None

            allowed.append(positions)

        return allowed

    def _is_whole_layout(self, allowed: list[tuple[int, ...]]) -> bool:
        return all(len(indices) == size for indices, size in zip(allowed, self.shape))

    def _try_compile_fixed_fastest_axis(
        self,
        allowed: list[tuple[int, ...]],
    ) -> Optional[StridedSet]:
        """
        Compile selections like [:, :, fixed_fastest_axis].

        Example:
            shape = (3, 4)
            selection = [:, 2]
            flat IDs = 2, 6, 10
        """
        last = self.rank - 1

        for axis, (indices, size) in enumerate(zip(allowed, self.shape)):
            if axis == last:
                if len(indices) != 1:
                    return None
            elif len(indices) != size:
                return None

        selected_last = allowed[last][0]
        step = self.shape[last]
        start = selected_last
        end = self.size - step + selected_last + 1

        return StridedSet(start=start, end=end, step=step)

    def _compile_product_positions_to_intervals(
        self,
        allowed: list[tuple[int, ...]],
    ) -> IntervalSet:
        builder = IntervalBuilder()
        ndim = self.rank

        suffix_full = self._suffix_full_flags(allowed)
        suffix_sizes = self._suffix_sizes()

        def rec(axis: int, base_id: int) -> None:
            if axis == ndim:
                builder.add_point(base_id)
                return

            if suffix_full[axis]:
                builder.add_range(base_id, base_id + suffix_sizes[axis])
                return

            stride = self.strides[axis]

            for pos in allowed[axis]:
                rec(axis + 1, base_id + pos * stride)

        rec(0, 0)
        return builder.finish()

    def _suffix_full_flags(
        self,
        allowed: list[tuple[int, ...]],
    ) -> tuple[bool, ...]:
        out: list[bool] = []
        running = True

        for axis in reversed(range(self.rank)):
            running = running and len(allowed[axis]) == self.shape[axis]
            out.append(running)

        return tuple(reversed(out))

    def _suffix_sizes(self) -> tuple[int, ...]:
        out: list[int] = []
        running = 1

        for size in reversed(self.shape):
            running *= size
            out.append(running)

        return tuple(reversed(out))

    def _compile_by_enumeration(
        self,
        selection: CoordSelection,
        *,
        max_enumeration: int,
    ) -> IndexSelection:
        if self.size > max_enumeration:
            raise ValueError(
                f"Cannot compile {type(selection).__name__} symbolically, "
                f"and layout size {self.size} exceeds max_enumeration={max_enumeration}"
            )

        builder = IntervalBuilder()

        for sweep_id in range(self.size):
            coord = self.index_to_coord(sweep_id)
            if selection.contains_coord(coord):
                builder.add_point(sweep_id)

        return builder.finish()


#
# # -----------------------------------------------------------------------------
# # Flattening helper / compiler
# # -----------------------------------------------------------------------------
#
# @dataclass(frozen=True)
# class RowMajorLayout:
#     """Shape + row-major flattening rules."""
#
#     shape: tuple[int, ...]
#
#     def __init__(self, shape: Sequence[int]) -> None:
#         object.__setattr__(self, "shape", tuple(shape))
#         if any(n <= 0 for n in self.shape):
#             raise ValueError("all dimensions must be positive")
#
#     @property
#     def size(self) -> int:
#         out = 1
#         for n in self.shape:
#             out *= n
#         return out
#
#     @property
#     def strides(self) -> tuple[int, ...]:
#         strides: list[int] = []
#         running = 1
#         for n in reversed(self.shape[1:]):
#             running *= n
#             strides.append(running)
#         strides = list(reversed(strides))
#         strides.append(1)
#         return tuple(strides)
#
#     def coord_to_id(self, coord: tuple[int, ...]) -> SweepID:
#         if len(coord) != len(self.shape):
#             raise ValueError("coord has wrong rank")
#         for i, n in zip(coord, self.shape):
#             if not (0 <= i < n):
#                 raise ValueError(f"coord {coord} out of bounds for shape {self.shape}")
#         return sum(i * s for i, s in zip(coord, self.strides))
#
#     def id_to_coord(self, sweep_id: SweepID) -> tuple[int, ...]:
#         if not (0 <= sweep_id < self.size):
#             raise ValueError("sweep_id out of bounds")
#
#         out: list[int] = []
#         rem = sweep_id
#
#         for stride, dim in zip(self.strides, self.shape):
#             i = rem // stride
#             rem = rem % stride
#             if i >= dim:
#                 raise ValueError("invalid sweep_id")
#             out.append(i)
#
#         return tuple(out)
#
#     def compile_product(self, product: CoordProduct) -> IndexSelection:
#         allowed = product.allowed_indices(self.shape)
#
#         if all(len(a) == n for a, n in zip(allowed, self.shape)):
#             return IntervalSet([(0, self.size)])
#
#         simple_stride = self._try_compile_simple_last_axis_stride(allowed)
#         if simple_stride is not None:
#             return simple_stride
#
#         return self._compile_product_to_intervals(allowed)
#
#     def compile(self, selection: CoordSelection) -> IndexSelection:
#         if isinstance(selection, CoordProduct):
#             return self.compile_product(selection)
#
#         if isinstance(selection, CoordUnion):
#             return UnionSet(*(self.compile(part) for part in selection.parts))
#
#         if isinstance(selection, CoordIntersection):
#             return IntersectionSet(*(self.compile(part) for part in selection.parts))
#
#         if isinstance(selection, CoordDifference):
#             return DifferenceSet(
#                 self.compile(selection.base),
#                 self.compile(selection.remove),
#             )
#
#         return self._compile_by_enumeration(selection)
#
#     def _try_compile_simple_last_axis_stride(
#         self,
#         allowed: list[list[int]],
#     ) -> Optional[StridedSet]:
#         last = len(self.shape) - 1
#
#         for axis, (indices, size) in enumerate(zip(allowed, self.shape)):
#             if axis == last:
#                 if len(indices) != 1:
#                     return None
#             elif len(indices) != size:
#                 return None
#
#         selected_last = allowed[last][0]
#         step = self.shape[last]
#         start = selected_last
#         end = self.size - step + selected_last + 1
#         return StridedSet(start=start, end=end, step=step)
#
#     def _compile_product_to_intervals(
#         self,
#         allowed: list[list[int]],
#     ) -> IntervalSet:
#         intervals = IntervalSet()
#         ndim = len(self.shape)
#         strides = self.strides
#
#         def suffix_is_full(axis: int) -> bool:
#             return all(len(allowed[j]) == self.shape[j] for j in range(axis, ndim))
#
#         def suffix_size(axis: int) -> int:
#             out = 1
#             for n in self.shape[axis:]:
#                 out *= n
#             return out
#
#         def rec(axis: int, base_id: int) -> None:
#             nonlocal intervals
#
#             if axis == ndim:
#                 intervals = intervals.with_point(base_id)
#                 return
#
#             if suffix_is_full(axis):
#                 width = suffix_size(axis)
#                 intervals = intervals.with_range(base_id, base_id + width)
#                 return
#
#             for i in allowed[axis]:
#                 rec(axis + 1, base_id + i * strides[axis])
#
#         rec(0, 0)
#         return intervals
#
#     def _compile_by_enumeration(self, selection: CoordSelection) -> IndexSelection:
#         intervals = IntervalSet()
#
#         for sweep_id in range(self.size):
#             coord = self.id_to_coord(sweep_id)
#             if selection.contains_coord(coord):
#                 intervals = intervals.with_point(sweep_id)
#
#         return intervals
