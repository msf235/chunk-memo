from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from math import prod
from typing import (Any, Dict, Iterable, List, Mapping, Optional, Sequence,
                    Tuple, Union)

# -----------------------------------------------------------------------------
# Basic types
# -----------------------------------------------------------------------------

SweepID = int
ChunkID = int
Coord = Tuple[int, ...]
Interval = Tuple[int, int]  # inclusive [start, end]
Point = Mapping[str, Any]


# -----------------------------------------------------------------------------
# SweepEncoder
# -----------------------------------------------------------------------------

class SweepEncoder:
    """
    Converts among:

        parameter point -> integer coordinate vector -> flattened sweep_id

    Example:

        space = {
            "lr": [0.001, 0.01],
            "depth": [2, 4, 8],
            "seed": [0, 1],
        }

        encoder = SweepEncoder(space, param_order=["lr", "depth", "seed"])

        point = {"lr": 0.01, "depth": 4, "seed": 1}
        coord = (1, 1, 1)
        sweep_id = 9
    """

    def __init__(
        self,
        values_by_param: Mapping[str, Sequence[Any]],
        param_order: Optional[Sequence[str]] = None,
    ) -> None:
        if param_order is None:
            # Stable if caller provides an ordered dict; explicit order is better.
            param_order = list(values_by_param.keys())

        self.param_order: Tuple[str, ...] = tuple(param_order)
        self.values_by_param: Dict[str, Tuple[Any, ...]] = {
            name: tuple(values_by_param[name]) for name in self.param_order
        }

        self.shape: Tuple[int, ...] = tuple(
            len(self.values_by_param[name]) for name in self.param_order
        )

        if any(n <= 0 for n in self.shape):
            raise ValueError("Each parameter must have at least one value.")

        self.strides: Tuple[int, ...] = self._make_row_major_strides(self.shape)
        self.size: int = prod(self.shape)

        self._value_to_index: Dict[str, Dict[Any, int]] = {
            name: {value: i for i, value in enumerate(values)}
            for name, values in self.values_by_param.items()
        }

    @staticmethod
    def _make_row_major_strides(shape: Sequence[int]) -> Tuple[int, ...]:
        strides: List[int] = []
        running = 1
        for n in reversed(shape[1:]):
            running *= n
            strides.append(running)
        strides = list(reversed(strides))
        strides.append(1)
        return tuple(strides)

    def point_to_coord(self, point: Point) -> Coord:
        coord: List[int] = []
        for name in self.param_order:
            value = point[name]
            try:
                coord.append(self._value_to_index[name][value])
            except KeyError as exc:
                raise KeyError(f"Invalid value {value!r} for parameter {name!r}") from exc
        return tuple(coord)

    def coord_to_point(self, coord: Coord) -> Dict[str, Any]:
        self._validate_coord(coord)
        return {
            name: self.values_by_param[name][i]
            for name, i in zip(self.param_order, coord)
        }

    def coord_to_id(self, coord: Coord) -> SweepID:
        self._validate_coord(coord)
        return sum(i * stride for i, stride in zip(coord, self.strides))

    def id_to_coord(self, sweep_id: SweepID) -> Coord:
        self._validate_id(sweep_id)
        remainder = sweep_id
        coord: List[int] = []
        for stride, dim_size in zip(self.strides, self.shape):
            i = remainder // stride
            remainder = remainder % stride
            if i >= dim_size:
                raise ValueError(f"Invalid sweep_id {sweep_id}")
            coord.append(i)
        return tuple(coord)

    def encode_point(self, point: Point) -> SweepID:
        return self.coord_to_id(self.point_to_coord(point))

    def decode_id(self, sweep_id: SweepID) -> Dict[str, Any]:
        return self.coord_to_point(self.id_to_coord(sweep_id))

    def compile_selection(self, selection: "CoordinateSelection") -> "IndexSelection":
        return selection.compile(self)

    def _validate_coord(self, coord: Coord) -> None:
        if len(coord) != len(self.shape):
            raise ValueError(f"Expected coord length {len(self.shape)}, got {len(coord)}")
        for i, n in zip(coord, self.shape):
            if not (0 <= i < n):
                raise ValueError(f"Coordinate {coord} is out of bounds for shape {self.shape}")

    def _validate_id(self, sweep_id: SweepID) -> None:
        if not (0 <= sweep_id < self.size):
            raise ValueError(f"sweep_id {sweep_id} is out of bounds [0, {self.size})")


# -----------------------------------------------------------------------------
# ChunkPolicy
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ChunkLocation:
    sweep_id: SweepID
    chunk_id: ChunkID
    offset: int
    path: str


class ChunkPolicy:
    """
    Converts flattened sweep IDs into storage chunks/files.
    """

    def __init__(
        self,
        chunk_size: int,
        total_size: int,
        path_template: str = "chunks/chunk_{chunk_id:06d}.dat",
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if total_size < 0:
            raise ValueError("total_size cannot be negative")
        self.chunk_size = chunk_size
        self.total_size = total_size
        self.path_template = path_template
        self.num_chunks = (total_size + chunk_size - 1) // chunk_size

    def locate(self, sweep_id: SweepID) -> ChunkLocation:
        self._validate_id(sweep_id)
        chunk_id = sweep_id // self.chunk_size
        offset = sweep_id % self.chunk_size
        return ChunkLocation(
            sweep_id=sweep_id,
            chunk_id=chunk_id,
            offset=offset,
            path=self.path_for(chunk_id),
        )

    def chunk_bounds(self, chunk_id: ChunkID) -> Interval:
        self._validate_chunk_id(chunk_id)
        start = chunk_id * self.chunk_size
        end = min(start + self.chunk_size, self.total_size) - 1
        return start, end

    def path_for(self, chunk_id: ChunkID) -> str:
        self._validate_chunk_id(chunk_id)
        return self.path_template.format(chunk_id=chunk_id)

    def _validate_id(self, sweep_id: SweepID) -> None:
        if not (0 <= sweep_id < self.total_size):
            raise ValueError(f"sweep_id {sweep_id} is out of bounds [0, {self.total_size})")

    def _validate_chunk_id(self, chunk_id: ChunkID) -> None:
        if not (0 <= chunk_id < self.num_chunks):
            raise ValueError(f"chunk_id {chunk_id} is out of bounds [0, {self.num_chunks})")


# -----------------------------------------------------------------------------
# IndexSelection interface and concrete implementations
# -----------------------------------------------------------------------------

class IndexSelection:
    """
    Abstract interface for subsets of flattened sweep IDs.

    Implementations can be interval-based, strided, chunk-based, unions,
    intersections, etc.
    """

    def contains(self, sweep_id: SweepID) -> bool:
        raise NotImplementedError

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        """Return selected IDs within [start, end] as inclusive intervals."""
        raise NotImplementedError

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        """True if every ID in [start, end] is selected."""
        if start > end:
            return True
        intervals = self.intersect_range(start, end)
        return len(intervals) == 1 and intervals[0] == (start, end)

    def count_between(self, start: SweepID, end: SweepID) -> int:
        """Count selected IDs in inclusive [start, end]."""
        if start > end:
            return 0
        return sum(b - a + 1 for a, b in self.intersect_range(start, end))

    def rank_between(self, start: SweepID, sweep_id: SweepID) -> int:
        """
        Number of selected IDs in [start, sweep_id), i.e. before sweep_id.

        This is the dense row index within a partial chunk, assuming sweep_id is present.
        """
        if sweep_id <= start:
            return 0
        return self.count_between(start, sweep_id - 1)


class IntervalSet(IndexSelection):
    """
    Sorted, disjoint, inclusive intervals.

    Good for mostly contiguous occupancy like:

        [0, 999], [1200, 1205], [1301, 1301]
    """

    def __init__(self, intervals: Iterable[Interval] = ()) -> None:
        self.intervals: List[Interval] = []
        for start, end in intervals:
            self.add_range(start, end)

    def add_point(self, x: SweepID) -> None:
        self.add_range(x, x)

    def add_range(self, start: SweepID, end: SweepID) -> None:
        if start > end:
            raise ValueError("start must be <= end")

        new_start, new_end = start, end
        result: List[Interval] = []
        inserted = False

        for a, b in self.intervals:
            if b + 1 < new_start:
                result.append((a, b))
            elif new_end + 1 < a:
                if not inserted:
                    result.append((new_start, new_end))
                    inserted = True
                result.append((a, b))
            else:
                new_start = min(new_start, a)
                new_end = max(new_end, b)

        if not inserted:
            result.append((new_start, new_end))

        self.intervals = result

    def contains(self, sweep_id: SweepID) -> bool:
        i = bisect_right(self.intervals, (sweep_id, float("inf"))) - 1
        if i < 0:
            return False
        start, end = self.intervals[i]
        return start <= sweep_id <= end

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        if start > end:
            return []

        result: List[Interval] = []
        # First possibly-overlapping interval.
        i = bisect_left(self.intervals, (start, -1))
        if i > 0:
            i -= 1

        while i < len(self.intervals):
            a, b = self.intervals[i]
            if a > end:
                break
            if b >= start:
                result.append((max(a, start), min(b, end)))
            i += 1

        return result

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        i = bisect_right(self.intervals, (start, float("inf"))) - 1
        if i < 0:
            return False
        a, b = self.intervals[i]
        return a <= start and end <= b

    def __repr__(self) -> str:
        return f"IntervalSet({self.intervals!r})"


class StridedSet(IndexSelection):
    """
    Represents arithmetic progressions:

        start, start + step, start + 2*step, ... <= end

    Useful for slices like seed == 1 in row-major layouts.
    """

    def __init__(self, start: SweepID, end: SweepID, step: int) -> None:
        if step <= 0:
            raise ValueError("step must be positive")
        if start > end:
            raise ValueError("start must be <= end")
        self.start = start
        self.end = end
        self.step = step

    def contains(self, sweep_id: SweepID) -> bool:
        return (
            self.start <= sweep_id <= self.end
            and (sweep_id - self.start) % self.step == 0
        )

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        if start > end:
            return []
        lo = max(start, self.start)
        hi = min(end, self.end)
        if lo > hi:
            return []

        # Find first selected ID >= lo.
        delta = lo - self.start
        k = (delta + self.step - 1) // self.step
        first = self.start + k * self.step
        if first > hi:
            return []

        # IntervalSet-compatible output. A pure stride usually becomes singletons
        # unless step == 1.
        if self.step == 1:
            return [(first, hi)]

        result: List[Interval] = []
        x = first
        while x <= hi:
            result.append((x, x))
            x += self.step
        return result

    def count_between(self, start: SweepID, end: SweepID) -> int:
        if start > end:
            return 0
        lo = max(start, self.start)
        hi = min(end, self.end)
        if lo > hi:
            return 0
        delta = lo - self.start
        k0 = (delta + self.step - 1) // self.step
        first = self.start + k0 * self.step
        if first > hi:
            return 0
        return ((hi - first) // self.step) + 1

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        # Only a unit stride can cover a whole contiguous range longer than one.
        if start > end:
            return True
        if start == end:
            return self.contains(start)
        return self.step == 1 and self.start <= start and end <= self.end

    def __repr__(self) -> str:
        return f"StridedSet(start={self.start}, end={self.end}, step={self.step})"


class ChunkSet(IndexSelection):
    """
    Represents a set of whole chunks under a given ChunkPolicy.

    This is useful when the selection is naturally chunk-aligned.
    """

    def __init__(self, chunk_policy: ChunkPolicy, chunk_ids: Iterable[ChunkID]) -> None:
        self.chunk_policy = chunk_policy
        self.chunk_ids = frozenset(chunk_ids)

    def contains(self, sweep_id: SweepID) -> bool:
        chunk_id = sweep_id // self.chunk_policy.chunk_size
        return chunk_id in self.chunk_ids

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        if start > end:
            return []

        result = IntervalSet()
        first_chunk = start // self.chunk_policy.chunk_size
        last_chunk = end // self.chunk_policy.chunk_size

        for chunk_id in range(first_chunk, last_chunk + 1):
            if chunk_id not in self.chunk_ids:
                continue
            a, b = self.chunk_policy.chunk_bounds(chunk_id)
            lo, hi = max(a, start), min(b, end)
            if lo <= hi:
                result.add_range(lo, hi)

        return result.intervals

    def covers_range(self, start: SweepID, end: SweepID) -> bool:
        return IntervalSet(self.intersect_range(start, end)).covers_range(start, end)

    def __repr__(self) -> str:
        return f"ChunkSet(chunk_ids={sorted(self.chunk_ids)!r})"


class UnionSet(IndexSelection):
    def __init__(self, *parts: IndexSelection) -> None:
        self.parts = parts

    def contains(self, sweep_id: SweepID) -> bool:
        return any(part.contains(sweep_id) for part in self.parts)

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        merged = IntervalSet()
        for part in self.parts:
            for a, b in part.intersect_range(start, end):
                merged.add_range(a, b)
        return merged.intervals


class IntersectionSet(IndexSelection):
    def __init__(self, *parts: IndexSelection) -> None:
        if not parts:
            raise ValueError("IntersectionSet requires at least one part")
        self.parts = parts

    def contains(self, sweep_id: SweepID) -> bool:
        return all(part.contains(sweep_id) for part in self.parts)

    def intersect_range(self, start: SweepID, end: SweepID) -> List[Interval]:
        current = IntervalSet([(start, end)])
        for part in self.parts:
            next_current = IntervalSet()
            for a, b in current.intervals:
                for x, y in part.intersect_range(a, b):
                    next_current.add_range(x, y)
            current = next_current
            if not current.intervals:
                break
        return current.intervals


# -----------------------------------------------------------------------------
# CoordinateSelection
# -----------------------------------------------------------------------------

class CoordinateSelection:
    """
    Abstract parameter-space selection.

    This is the semantic layer. It can compile itself to an IndexSelection.
    """

    def compile(self, encoder: SweepEncoder) -> IndexSelection:
        raise NotImplementedError


class CoordSlice(CoordinateSelection):
    """
    Simple parameter-space slice.

    Examples:

        CoordSlice(seed=1)
        CoordSlice(depth=[4, 8])
        CoordSlice(lr=0.01, seed=1)

    Values are parameter *values*, not integer coordinates.
    """

    def __init__(self, **constraints: Union[Any, Sequence[Any]]) -> None:
        self.constraints = constraints

    def compile(self, encoder: SweepEncoder) -> IndexSelection:
        allowed_indices: List[List[int]] = []

        for name in encoder.param_order:
            values = encoder.values_by_param[name]
            value_to_index = encoder._value_to_index[name]

            if name not in self.constraints:
                allowed_indices.append(list(range(len(values))))
                continue

            raw = self.constraints[name]

            # Treat strings/bytes as scalar values, not sequences.
            if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
                raw_values = list(raw)
            else:
                raw_values = [raw]

            indices: List[int] = []
            for value in raw_values:
                if value not in value_to_index:
                    raise KeyError(f"Invalid value {value!r} for parameter {name!r}")
                indices.append(value_to_index[value])

            allowed_indices.append(sorted(set(indices)))

        # General compiler: enumerate selected coordinate blocks and merge into intervals.
        # This is simple and correct. Later, specialized compilers can emit StridedSet
        # or ChunkSet when beneficial.
        interval_set = IntervalSet()

        def rec(axis: int, prefix: List[int]) -> None:
            if axis == len(encoder.shape):
                sweep_id = encoder.coord_to_id(tuple(prefix))
                interval_set.add_point(sweep_id)
                return
            for i in allowed_indices[axis]:
                prefix.append(i)
                rec(axis + 1, prefix)
                prefix.pop()

        rec(0, [])
        return interval_set


class CoordPredicate(CoordinateSelection):
    """
    General coordinate predicate.

    Useful for prototyping arbitrary selections. Compiles by enumeration, so this is
    not suitable for extremely large spaces unless optimized later.
    """

    def __init__(self, predicate) -> None:
        self.predicate = predicate

    def compile(self, encoder: SweepEncoder) -> IndexSelection:
        selected = IntervalSet()
        for sweep_id in range(encoder.size):
            coord = encoder.id_to_coord(sweep_id)
            point = encoder.coord_to_point(coord)
            if self.predicate(point, coord, sweep_id):
                selected.add_point(sweep_id)
        return selected


# -----------------------------------------------------------------------------
# SweepCache
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CacheLocation:
    point: Dict[str, Any]
    sweep_id: SweepID
    chunk_id: ChunkID
    offset: int
    row_index: int
    path: str
    fast_path: bool


class SweepCache:
    """
    Combines:

        SweepEncoder    parameter point <-> sweep_id
        ChunkPolicy     sweep_id <-> chunk/file location
        IndexSelection  which sweep_ids are present

    This class does not read/write result data itself. It maps parameter points to
    logical storage locations.
    """

    def __init__(
        self,
        encoder: SweepEncoder,
        chunk_policy: ChunkPolicy,
        occupancy: Optional[IndexSelection] = None,
    ) -> None:
        if chunk_policy.total_size != encoder.size:
            raise ValueError("ChunkPolicy.total_size must match SweepEncoder.size")
        self.encoder = encoder
        self.chunk_policy = chunk_policy
        self.occupancy = occupancy if occupancy is not None else IntervalSet()

    def has_id(self, sweep_id: SweepID) -> bool:
        self.encoder._validate_id(sweep_id)
        return self.occupancy.contains(sweep_id)

    def has_point(self, point: Point) -> bool:
        return self.has_id(self.encoder.encode_point(point))

    def add_id(self, sweep_id: SweepID) -> None:
        if not isinstance(self.occupancy, IntervalSet):
            raise TypeError("Can only mutate occupancy directly when it is an IntervalSet")
        self.encoder._validate_id(sweep_id)
        self.occupancy.add_point(sweep_id)

    def add_range(self, start_id: SweepID, end_id: SweepID) -> None:
        if not isinstance(self.occupancy, IntervalSet):
            raise TypeError("Can only mutate occupancy directly when it is an IntervalSet")
        self.encoder._validate_id(start_id)
        self.encoder._validate_id(end_id)
        self.occupancy.add_range(start_id, end_id)

    def add_point(self, point: Point) -> None:
        self.add_id(self.encoder.encode_point(point))

    def locate_id(self, sweep_id: SweepID) -> Optional[CacheLocation]:
        self.encoder._validate_id(sweep_id)

        if not self.occupancy.contains(sweep_id):
            return None

        loc = self.chunk_policy.locate(sweep_id)
        start, end = self.chunk_policy.chunk_bounds(loc.chunk_id)

        if self.occupancy.covers_range(start, end):
            row_index = loc.offset
            fast_path = True
        else:
            row_index = self.occupancy.rank_between(start, sweep_id)
            fast_path = False

        return CacheLocation(
            point=self.encoder.decode_id(sweep_id),
            sweep_id=sweep_id,
            chunk_id=loc.chunk_id,
            offset=loc.offset,
            row_index=row_index,
            path=loc.path,
            fast_path=fast_path,
        )

    def locate_point(self, point: Point) -> Optional[CacheLocation]:
        return self.locate_id(self.encoder.encode_point(point))

    def is_chunk_full(self, chunk_id: ChunkID) -> bool:
        start, end = self.chunk_policy.chunk_bounds(chunk_id)
        return self.occupancy.covers_range(start, end)

    def items_in_chunk(self, chunk_id: ChunkID) -> List[Interval]:
        start, end = self.chunk_policy.chunk_bounds(chunk_id)
        return self.occupancy.intersect_range(start, end)

    def slice(self, selection: CoordinateSelection) -> "SweepCache":
        index_selection = self.encoder.compile_selection(selection)
        return SweepCache(
            encoder=self.encoder,
            chunk_policy=self.chunk_policy,
            occupancy=IntersectionSet(self.occupancy, index_selection),
        )

    def chunk_summary(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for chunk_id in range(self.chunk_policy.num_chunks):
            start, end = self.chunk_policy.chunk_bounds(chunk_id)
            intervals = self.occupancy.intersect_range(start, end)
            count = sum(b - a + 1 for a, b in intervals)
            full = count == (end - start + 1)
            rows.append(
                {
                    "chunk_id": chunk_id,
                    "path": self.chunk_policy.path_for(chunk_id),
                    "bounds": (start, end),
                    "present": intervals,
                    "count": count,
                    "full": full,
                }
            )
        return rows


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    space = {
        "lr": [0.001, 0.01],
        "depth": [2, 4, 8],
        "seed": [0, 1],
    }

    encoder = SweepEncoder(space, param_order=["lr", "depth", "seed"])
    chunks = ChunkPolicy(
        chunk_size=4,
        total_size=encoder.size,
        path_template="chunks/chunk_{chunk_id:06d}.parquet",
    )

    occupancy = IntervalSet()
    cache = SweepCache(encoder, chunks, occupancy)

    # Add a full first chunk: sweep_id 0..3
    cache.add_range(0, 3)

    # Add a few arbitrary points.
    cache.add_id(6)
    cache.add_point({"lr": 0.01, "depth": 4, "seed": 1})  # sweep_id 9

    print("Full cache chunks:")
    for row in cache.chunk_summary():
        print(row)

    point = {"lr": 0.01, "depth": 4, "seed": 1}
    print("\nLocate point:")
    print(cache.locate_point(point))

    # A semantic slice: only seed == 1.
    seed_1_cache = cache.slice(CoordSlice(seed=1))

    print("\nSliced cache chunks, seed == 1:")
    for row in seed_1_cache.chunk_summary():
        print(row)
    breakpoint()

