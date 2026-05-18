
from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

SweepID = int
ChunkID = int
Interval = tuple[int, int]  # half-open [start, end)



Coord = tuple[int, ...]
# -----------------------------------------------------------------------------
# Flattened-index selections. These are efficient/compact representations of
# integers. At one extreme you may be able to represent a set of integers as
# a range [1,10]. At the other you may need to just keep a list of singletons.
# 
# -----------------------------------------------------------------------------

# Abstract base class
class IndexSelection:
    """Immutable subset of flattened integer sweep IDs."""

    def contains(self, sweep_id: SweepID) -> bool:
        raise NotImplementedError

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        """Return selected IDs inside [start, end) as intervals."""
        raise NotImplementedError

    def bounds(self) -> Optional[Interval]:
        """Return finite conservative bounds, or None if empty."""
        raise NotImplementedError

    def iter_intervals(self) -> Iterator[Interval]:
        bounds = self.bounds()
        if bounds is None:
            return iter(())
        start, end = bounds
        return iter(self.intersect_range(start, end))

    def __iter__(self) -> Iterator[SweepID]:
        for start, end in self.iter_intervals():
            yield from range(start, end)

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        if start >= end:
            return True
        intervals = self.intersect_range(start, end)
        return len(intervals) == 1 and intervals[0] == (start, end)

    def count_between(self, start: SweepID, end: SweepID) -> int:
        if start >= end:
            return 0
        return sum(b - a for a, b in self.intersect_range(start, end))

    def rank_between(self, start: SweepID, sweep_id: SweepID) -> int:
        """Number of selected IDs in [start, sweep_id)."""
        if sweep_id <= start:
            return 0
        return self.count_between(start, sweep_id)


@dataclass(frozen=True)
class EmptySet(IndexSelection):
    def contains(self, sweep_id: SweepID) -> bool:
        return False

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        return []

    def bounds(self) -> Optional[Interval]:
        return None


@dataclass(frozen=True)
class IntervalSet(IndexSelection):
    """Sorted, disjoint, half-open intervals."""

    intervals: tuple[Interval, ...] = ()

    def __init__(self, intervals: Iterable[Interval] = ()) -> None:
        normalized: tuple[Interval, ...] = ()
        for start, end in intervals:
            normalized = self._add_range(normalized, start, end)
        object.__setattr__(self, "intervals", normalized)

    def with_point(self, x: SweepID) -> "IntervalSet":
        return self.with_range(x, x + 1)

    def with_range(self, start: SweepID, end: SweepID) -> "IntervalSet":
        return IntervalSet(self._add_range(self.intervals, start, end))

    @staticmethod
    def _add_range(
        intervals: tuple[Interval, ...],
        start: SweepID,
        end: SweepID,
    ) -> tuple[Interval, ...]:
        if start > end:
            raise ValueError("start must be <= end")
        if start == end:
            return intervals

        new_start, new_end = start, end
        result: list[Interval] = []
        inserted = False

        for a, b in intervals:
            if b < new_start:
                result.append((a, b))
            elif new_end < a:
                if not inserted:
                    result.append((new_start, new_end))
                    inserted = True
                result.append((a, b))
            else:
                new_start = min(new_start, a)
                new_end = max(new_end, b)

        if not inserted:
            result.append((new_start, new_end))

        return tuple(result)

    def contains(self, sweep_id: SweepID) -> bool:
        i = bisect_right(self.intervals, (sweep_id, float("inf"))) - 1
        if i < 0:
            return False
        a, b = self.intervals[i]
        return a <= sweep_id < b

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        if start >= end:
            return []

        result: list[Interval] = []
        i = bisect_left(self.intervals, (start, -1))
        if i > 0:
            i -= 1

        while i < len(self.intervals):
            a, b = self.intervals[i]
            if a >= end:
                break
            if b > start:
                result.append((max(a, start), min(b, end)))
            i += 1

        return result

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        if start >= end:
            return True
        i = bisect_right(self.intervals, (start, float("inf"))) - 1
        if i < 0:
            return False
        a, b = self.intervals[i]
        return a <= start and end <= b

    def bounds(self) -> Optional[Interval]:
        if not self.intervals:
            return None
        return self.intervals[0][0], self.intervals[-1][1]


@dataclass(frozen=True)
class StridedSet(IndexSelection):
    """Arithmetic progression: start, start + step, ..., < end."""

    start: SweepID
    end: SweepID
    step: int

    def __post_init__(self) -> None:
        if self.step <= 0:
            raise ValueError("step must be positive")
        if self.start >= self.end:
            raise ValueError("start must be < end")

    def contains(self, sweep_id: SweepID) -> bool:
        return (
            self.start <= sweep_id < self.end
            and (sweep_id - self.start) % self.step == 0
        )

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        if start >= end:
            return []

        lo = max(start, self.start)
        hi = min(end, self.end)
        if lo >= hi:
            return []

        k = max(0, (lo - self.start + self.step - 1) // self.step)
        first = self.start + k * self.step

        if first >= hi:
            return []

        if self.step == 1:
            return [(first, hi)]

        return [(x, x + 1) for x in range(first, hi, self.step)]

    def count_between(self, start: SweepID, end: SweepID) -> int:
        if start >= end:
            return 0

        lo = max(start, self.start)
        hi = min(end, self.end)
        if lo >= hi:
            return 0

        k = max(0, (lo - self.start + self.step - 1) // self.step)
        first = self.start + k * self.step

        if first >= hi:
            return 0

        return ((hi - first) // self.step) + 1

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        if start >= end:
            return True
        if end - start == 1:
            return self.contains(start)
        return self.step == 1 and self.start <= start and end <= self.end

    def bounds(self) -> Optional[Interval]:
        return self.start, self.end

@dataclass(frozen=True)
class UnionSet(IndexSelection):
    parts: tuple[IndexSelection, ...]

    def __init__(self, *parts: IndexSelection) -> None:
        object.__setattr__(self, "parts", tuple(parts))

    def contains(self, sweep_id: SweepID) -> bool:
        return any(part.contains(sweep_id) for part in self.parts)

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        if start >= end:
            return []
        merged = IntervalSet()
        for part in self.parts:
            for a, b in part.intersect_range(start, end):
                merged = merged.with_range(a, b)
        return list(merged.intervals)

    def bounds(self) -> Optional[Interval]:
        bounds = [p.bounds() for p in self.parts]
        bounds = [b for b in bounds if b is not None]
        if not bounds:
            return None
        return min(a for a, _ in bounds), max(b for _, b in bounds)


@dataclass(frozen=True)
class IntersectionSet(IndexSelection):
    parts: tuple[IndexSelection, ...]

    def __init__(self, *parts: IndexSelection) -> None:
        if not parts:
            raise ValueError("IntersectionSet requires at least one part")
        object.__setattr__(self, "parts", tuple(parts))

    def contains(self, sweep_id: SweepID) -> bool:
        return all(part.contains(sweep_id) for part in self.parts)

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        if start >= end:
            return []
        current = IntervalSet([(start, end)])

        for part in self.parts:
            next_current = IntervalSet()
            for a, b in current.intervals:
                for x, y in part.intersect_range(a, b):
                    next_current = next_current.with_range(x, y)
            current = next_current

            if not current.intervals:
                break

        return list(current.intervals)

    def bounds(self) -> Optional[Interval]:
        bounds = [p.bounds() for p in self.parts]
        if any(b is None for b in bounds):
            return None

        lo = max(b[0] for b in bounds if b is not None)
        hi = min(b[1] for b in bounds if b is not None)

        if lo >= hi:
            return None

        return lo, hi


@dataclass(frozen=True)
class DifferenceSet(IndexSelection):
    base: IndexSelection
    remove: IndexSelection

    def contains(self, sweep_id: SweepID) -> bool:
        return self.base.contains(sweep_id) and not self.remove.contains(sweep_id)

    def intersect_range(self, start: SweepID, end: SweepID) -> list[Interval]:
        if start >= end:
            return []
        result = IntervalSet()

        for a, b in self.base.intersect_range(start, end):
            cursor = a
            for x, y in self.remove.intersect_range(a, b):
                if cursor < x:
                    result = result.with_range(cursor, x)
                cursor = max(cursor, y)

            if cursor < b:
                result = result.with_range(cursor, b)

        return list(result.intervals)

    def bounds(self) -> Optional[Interval]:
        return self.base.bounds()
