from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from math import prod
from typing import Iterable, Iterator, Protocol, Sequence

Coord = tuple[int, ...]

SweepID = int
ChunkID = int
Interval = tuple[int, int]  # half-open [start, end)



Coord = tuple[int, ...]
# -----------------------------------------------------------------------------
# Coordinate-space selection algebra
# -----------------------------------------------------------------------------

AxisSpec = Sequence[int] | int


class AxisSelection(Protocol):
    def contains(self, value: int) -> bool: ...
    def iter_values(self) -> Iterator[int]: ...
    def size(self) -> int: ...
    def to_tuple(self) -> tuple[int, ...]: ...


@dataclass(frozen=True)
class IntSetAxis:
    values: tuple[int, ...]

    def __init__(self, values: Iterable[int]) -> None:
        norm = tuple(sorted(set(values)))
        if not norm:
            raise ValueError("axis cannot be empty")
        object.__setattr__(self, "values", norm)

    def contains(self, value: int) -> bool:
        return value in self.values

    def iter_values(self) -> Iterator[int]:
        yield from self.values

    def size(self) -> int:
        return len(self.values)

    def to_tuple(self) -> tuple[int, ...]:
        return self.values


@dataclass(frozen=True)
class RangeAxis:
    start: int
    stop: int

    def __post_init__(self) -> None:
        if self.stop <= self.start:
            raise ValueError("RangeAxis must be non-empty")

    def contains(self, value: int) -> bool:
        return self.start <= value < self.stop

    def iter_values(self) -> Iterator[int]:
        yield from range(self.start, self.stop)

    def size(self) -> int:
        return self.stop - self.start

    def to_tuple(self) -> tuple[int, ...]:
        return tuple(range(self.start, self.stop))


@dataclass(frozen=True)
class StridedAxis:
    start: int
    stop: int
    step: int

    def __post_init__(self) -> None:
        if self.step <= 0:
            raise ValueError("step must be positive")
        if self.stop <= self.start:
            raise ValueError("StridedAxis must be non-empty")
        if self.size() <= 0:
            raise ValueError("StridedAxis must contain at least one value")

    def contains(self, value: int) -> bool:
        return (
            self.start <= value < self.stop
            and (value - self.start) % self.step == 0
        )

    def iter_values(self) -> Iterator[int]:
        yield from range(self.start, self.stop, self.step)

    def size(self) -> int:
        return (self.stop - self.start + self.step - 1) // self.step

    def to_tuple(self) -> tuple[int, ...]:
        return tuple(range(self.start, self.stop, self.step))


AxisSpec = int | range | Sequence[int] | AxisSelection


def normalize_axis(axis: AxisSpec) -> AxisSelection:
    if isinstance(axis, int):
        return IntSetAxis((axis,))

    if isinstance(axis, range):
        if axis.step == 1:
            return RangeAxis(axis.start, axis.stop)
        return StridedAxis(axis.start, axis.stop, axis.step)

    if isinstance(axis, (IntSetAxis, RangeAxis, StridedAxis)):
        return axis

    return IntSetAxis(axis)


def normalize_axes(axes: Sequence[AxisSpec]) -> tuple[AxisSelection, ...]:
    return tuple(normalize_axis(axis) for axis in axes)


@dataclass(frozen=True)
class CoordSelection:
    def contains_coord(self, coord: Coord) -> bool:
        raise NotImplementedError

    def iter_coords(self) -> Iterator[Coord]:
        raise NotImplementedError

    def size(self) -> int:
        return sum(1 for _ in self.iter_coords())

    def simplify(self) -> "CoordSelection":
        return self


@dataclass(frozen=True)
class CoordEmpty(CoordSelection):
    def contains_coord(self, coord: Coord) -> bool:
        return False

    def iter_coords(self) -> Iterator[Coord]:
        yield from ()

    def size(self) -> int:
        return 0


@dataclass(frozen=True)
class CoordProduct(CoordSelection):
    axes: tuple[AxisSelection, ...]

    def __init__(self, axes: Sequence[AxisSpec]) -> None:
        object.__setattr__(self, "axes", normalize_axes(axes))

    def contains_coord(self, coord: Coord) -> bool:
        return (
            len(coord) == len(self.axes)
            and all(axis.contains(value) for value, axis in zip(coord, self.axes))
        )

    def iter_coords(self) -> Iterator[Coord]:
        yield from product(*(axis.iter_values() for axis in self.axes))

    def size(self) -> int:
        return prod(axis.size() for axis in self.axes)

    def as_tuple_axes(self) -> tuple[tuple[int, ...], ...]:
        return tuple(axis.to_tuple() for axis in self.axes)

    def simplify(self) -> CoordSelection:
        return self


def _intersect_products(a: CoordProduct, b: CoordProduct) -> CoordSelection:
    if len(a.axes) != len(b.axes):
        raise ValueError("Cannot intersect CoordProducts with different ranks")

    axes = []

    for ax, bx in zip(a.axes, b.axes):
        values = tuple(sorted(set(ax) & set(bx)))
        if not values:
            return CoordEmpty()
        axes.append(values)

    return CoordProduct(axes)

def _try_union_products(a: CoordProduct, b: CoordProduct) -> CoordSelection | None:
    """
    Merge two CoordProducts if they differ on at most one axis.

    Example:
        ([1], [2], [3]) ∪ ([4], [2], [3])
        -> ([1,4], [2], [3])

    Returns None if no simple product merge is possible.
    """
    if len(a.axes) != len(b.axes):
        raise ValueError("Cannot union CoordProducts with different ranks")

    differing_axes = 0
    merged_axes = []

    for ax, bx in zip(a.axes, b.axes):
        if ax == bx:
            merged_axes.append(ax)
        else:
            differing_axes += 1
            merged_axes.append(tuple(sorted(set(ax) | set(bx))))

            if differing_axes > 1:
                return None

    return CoordProduct(merged_axes)


@dataclass(frozen=True)
class CoordUnion(CoordSelection):
    parts: tuple[CoordSelection, ...]

    def __init__(self, *parts: CoordSelection) -> None:
        object.__setattr__(self, "parts", tuple(parts))

    def contains_coord(self, coord: tuple[int, ...]) -> bool:
        return any(part.contains_coord(coord) for part in self.parts)

    def iter_coords(self) -> Iterator[tuple[int, ...]]:
        seen: set[tuple[int, ...]] = set()

        for part in self.parts:
            for coord in part.iter_coords():
                if coord not in seen:
                    seen.add(coord)
                    yield coord

    def simplify(self) -> CoordSelection:
            flat = []

            for part in self.parts:
                part = part.simplify()

                if isinstance(part, CoordEmpty):
                    continue

                if isinstance(part, CoordUnion):
                    flat.extend(part.parts)
                else:
                    flat.append(part)

            # Remove exact duplicates while preserving order.
            deduped = []
            seen = set()
            for part in flat:
                if part not in seen:
                    seen.add(part)
                    deduped.append(part)

            if not deduped:
                return CoordEmpty()

            # Try pairwise product merging until stable.
            changed = True
            parts = deduped

            while changed:
                changed = False
                new_parts = []
                used = [False] * len(parts)

                for i, left in enumerate(parts):
                    if used[i]:
                        continue

                    merged = left

                    for j in range(i + 1, len(parts)):
                        if used[j]:
                            continue

                        right = parts[j]

                        if isinstance(merged, CoordProduct) and isinstance(right, CoordProduct):
                            candidate = _try_union_products(merged, right)
                            if candidate is not None:
                                merged = candidate
                                used[j] = True
                                changed = True

                    used[i] = True
                    new_parts.append(merged)

                parts = new_parts

            if len(parts) == 1:
                return parts[0]

            return CoordUnion(*parts)

def _intersect_many_products(parts: Sequence[CoordProduct]) -> CoordSelection:
    if not parts:
        raise ValueError("requires at least one CoordProduct")

    product: CoordSelection = parts[0]

    for part in parts[1:]:
        if not isinstance(product, CoordProduct):
            return CoordEmpty()

        product = _intersect_products(product, part)

        if isinstance(product, CoordEmpty):
            return CoordEmpty()

    return product

@dataclass(frozen=True)
class CoordIntersection(CoordSelection):
    parts: tuple[CoordSelection, ...]

    def __init__(self, *parts: CoordSelection) -> None:
        if not parts:
            raise ValueError("CoordIntersection requires at least one part")
        object.__setattr__(self, "parts", tuple(parts))

    def contains_coord(self, coord: tuple[int, ...]) -> bool:
        return all(part.contains_coord(coord) for part in self.parts)

    def iter_coords(self) -> Iterator[tuple[int, ...]]:
        # Iterate the smallest part first, then filter.
        parts_by_size = sorted(
            self.parts,
            key=lambda part: part.size(),
        )

        first, *rest = parts_by_size

        for coord in first.iter_coords():
            if all(part.contains_coord(coord) for part in rest):
                yield coord

    def simplify(self) -> CoordSelection:
        flat = []

        for part in self.parts:
            part = part.simplify()

            if isinstance(part, CoordEmpty):
                return CoordEmpty()

            if isinstance(part, CoordIntersection):
                flat.extend(part.parts)
            else:
                flat.append(part)

        if not flat:
            return CoordEmpty()

        product_parts = [part for part in flat if isinstance(part, CoordProduct)]
        others = [part for part in flat if not isinstance(part, CoordProduct)]

        if product_parts:
            product = _intersect_many_products(product_parts)

            if isinstance(product, CoordEmpty):
                return CoordEmpty()

            flat = [product, *others]
        else:
            flat = others

        # Remove exact duplicates.
        deduped = []
        seen = set()
        for part in flat:
            if part not in seen:
                seen.add(part)
                deduped.append(part)

        if not deduped:
            return CoordEmpty()

        if len(deduped) == 1:
            return deduped[0]

        return CoordIntersection(*deduped)

def _difference_products(
    base: CoordProduct,
    remove: CoordProduct,
) -> CoordSelection:
    """
    Compute base - remove for two non-empty CoordProducts of the same rank.

    Returns a CoordUnion of disjoint CoordProducts when needed.
    """
    if len(base.axes) != len(remove.axes):
        raise ValueError("Cannot subtract CoordProducts with different ranks")

    # Restrict remove to the part that overlaps base.
    overlap_axes: list[tuple[int, ...]] = []

    for base_axis, remove_axis in zip(base.axes, remove.axes):
        overlap = tuple(sorted(set(base_axis) & set(remove_axis)))

        if not overlap:
            return base

        overlap_axes.append(overlap)

    overlap_axes_tuple = tuple(overlap_axes)

    # remove covers base
    if overlap_axes_tuple == base.axes:
        return CoordEmpty()

    # Decompose base - overlap into disjoint slabs:
    #
    #   A x B x C - A' x B' x C'
    #
    # becomes:
    #
    #   (A - A') x B  x C
    #   A'       x (B - B') x C
    #   A'       x B' x (C - C')
    parts: list[CoordProduct] = []
    prefix_axes: list[tuple[int, ...]] = []

    for i, (base_axis, overlap_axis) in enumerate(
        zip(base.axes, overlap_axes_tuple)
    ):
        overlap_set = set(overlap_axis)
        remainder = tuple(v for v in base_axis if v not in overlap_set)

        if remainder:
            axes = (
                tuple(prefix_axes)
                + (remainder,)
                + base.axes[i + 1 :]
            )
            parts.append(CoordProduct(axes))

        prefix_axes.append(overlap_axis)

    if not parts:
        return CoordEmpty()

    if len(parts) == 1:
        return parts[0]

    return CoordUnion(*parts).simplify()

@dataclass(frozen=True)
class CoordDifference(CoordSelection):
    base: CoordSelection
    remove: CoordSelection

    def contains_coord(self, coord: tuple[int, ...]) -> bool:
        return self.base.contains_coord(coord) and not self.remove.contains_coord(coord)

    def iter_coords(self) -> Iterator[tuple[int, ...]]:
        for coord in self.base.iter_coords():
            if not self.remove.contains_coord(coord):
                yield coord

    def simplify(self) -> CoordSelection:
        base = self.base.simplify()
        remove = self.remove.simplify()

        if isinstance(base, CoordEmpty):
            return CoordEmpty()

        if isinstance(remove, CoordEmpty):
            return base

        if base == remove:
            return CoordEmpty()

        if isinstance(base, CoordProduct) and isinstance(remove, CoordProduct):
            return _difference_products(base, remove).simplify()

        if isinstance(base, CoordUnion):
            return CoordUnion(
                *(CoordDifference(part, remove).simplify() for part in base.parts)
            ).simplify()

        if isinstance(remove, CoordUnion):
            out = base
            for part in remove.parts:
                out = CoordDifference(out, part).simplify()
                if isinstance(out, CoordEmpty):
                    return CoordEmpty()
            return out.simplify()

        return CoordDifference(base, remove)
