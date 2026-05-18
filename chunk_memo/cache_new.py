from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from selections import (ANY, ChunkID, ChunkSpec, CoordIntersection,
                        CoordProduct, CoordSelection, IndexSelection,
                        IntersectionSet, Interval, IntervalSet, RowMajorLayout,
                        SelectionCodec, SweepID)

Point = Mapping[str, Any]
Coord = Tuple[int, ...]


# -----------------------------------------------------------------------------
# SweepEncoder
# -----------------------------------------------------------------------------

class SweepEncoder:
    """
    Maps between:

        parameter point <-> coordinate vector <-> flattened sweep_id

    selections.py already provides RowMajorLayout for coord <-> sweep_id.
    SweepEncoder adds parameter names and parameter values.
    """

    def __init__(
        self,
        values_by_param: Mapping[str, Sequence[Any]],
        param_order: Optional[Sequence[str]] = None,
    ) -> None:
        if param_order is None:
            param_order = list(values_by_param.keys())

        self.param_order: Tuple[str, ...] = tuple(param_order)

        self.values_by_param: Dict[str, Tuple[Any, ...]] = {
            name: tuple(values_by_param[name])
            for name in self.param_order
        }

        self.shape: Tuple[int, ...] = tuple(
            len(self.values_by_param[name])
            for name in self.param_order
        )

        self.layout = RowMajorLayout(self.shape)

        self._value_to_index: Dict[str, Dict[Any, int]] = {
            name: {
                value: i
                for i, value in enumerate(self.values_by_param[name])
            }
            for name in self.param_order
        }

    @property
    def size(self) -> int:
        return self.layout.size

    def point_to_coord(self, point: Point) -> Coord:
        coord = []

        for name in self.param_order:
            value = point[name]

            try:
                coord.append(self._value_to_index[name][value])
            except KeyError as exc:
                raise KeyError(
                    f"Invalid value {value!r} for parameter {name!r}"
                ) from exc

        return tuple(coord)

    def coord_to_point(self, coord: Coord) -> Dict[str, Any]:
        self._validate_coord(coord)

        return {
            name: self.values_by_param[name][i]
            for name, i in zip(self.param_order, coord)
        }

    def coord_to_id(self, coord: Coord) -> SweepID:
        return self.layout.coord_to_id(coord)

    def id_to_coord(self, sweep_id: SweepID) -> Coord:
        return self.layout.id_to_coord(sweep_id)

    def encode_point(self, point: Point) -> SweepID:
        return self.coord_to_id(self.point_to_coord(point))

    def decode_id(self, sweep_id: SweepID) -> Dict[str, Any]:
        return self.coord_to_point(self.id_to_coord(sweep_id))

    def point_slice(self, **constraints: Any) -> CoordProduct:
        """
        Build a coordinate-space selection using parameter *values*.

        Example:
            encoder.point_slice(seed=1)

        returns something equivalent to:
            CoordProduct((ANY, ANY, seed_index))
        """

        axes = []

        for name in self.param_order:
            if name not in constraints:
                axes.append(ANY)
                continue

            raw = constraints[name]

            if isinstance(raw, (list, tuple, set, frozenset)):
                indices = [
                    self._value_to_index[name][value]
                    for value in raw
                ]
                axes.append(indices)
            else:
                axes.append(self._value_to_index[name][raw])

        return CoordProduct(tuple(axes))

    def compile_selection(self, selection: CoordSelection) -> IndexSelection:
        return self.layout.compile(selection)

    def _validate_coord(self, coord: Coord) -> None:
        if len(coord) != len(self.shape):
            raise ValueError(
                f"Expected coord length {len(self.shape)}, got {len(coord)}"
            )

        for i, n in zip(coord, self.shape):
            if not (0 <= i < n):
                raise ValueError(
                    f"Coordinate {coord} is out of bounds for shape {self.shape}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SweepEncoder",
            "param_order": list(self.param_order),
            "values_by_param": {
                name: list(self.values_by_param[name])
                for name in self.param_order
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SweepEncoder":
        return cls(
            values_by_param=data["values_by_param"],
            param_order=data["param_order"],
        )


# -----------------------------------------------------------------------------
# ChunkPolicy
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ChunkLocation:
    sweep_id: SweepID
    chunk_id: ChunkID
    offset: int
    path: str


@dataclass(frozen=True)
class ChunkPolicy:
    """
    Defines how flattened sweep IDs map to chunk files.
    """

    chunk_spec: ChunkSpec
    path_template: str = "chunks/chunk_{chunk_id:06d}.dat"

    @classmethod
    def from_size(
        cls,
        *,
        chunk_size: int,
        total_size: int,
        path_template: str = "chunks/chunk_{chunk_id:06d}.dat",
    ) -> "ChunkPolicy":
        return cls(
            chunk_spec=ChunkSpec(
                chunk_size=chunk_size,
                total_size=total_size,
            ),
            path_template=path_template,
        )

    @property
    def chunk_size(self) -> int:
        return self.chunk_spec.chunk_size

    @property
    def total_size(self) -> int:
        return self.chunk_spec.total_size

    @property
    def num_chunks(self) -> int:
        return self.chunk_spec.num_chunks

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
        return self.chunk_spec.chunk_bounds(chunk_id)

    def path_for(self, chunk_id: ChunkID) -> str:
        self.chunk_spec.chunk_bounds(chunk_id)
        return self.path_template.format(chunk_id=chunk_id)

    def _validate_id(self, sweep_id: SweepID) -> None:
        if not (0 <= sweep_id < self.total_size):
            raise ValueError(
                f"sweep_id {sweep_id} is out of bounds [0, {self.total_size})"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ChunkPolicy",
            "chunk_size": self.chunk_size,
            "total_size": self.total_size,
            "path_template": self.path_template,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ChunkPolicy":
        return cls.from_size(
            chunk_size=data["chunk_size"],
            total_size=data["total_size"],
            path_template=data["path_template"],
        )


# -----------------------------------------------------------------------------
# SweepCache
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ResolvedLocation:
    """
    Fully resolved storage position.

    ChunkLocation gives logical chunk placement.
    ResolvedLocation additionally includes row_index, which depends on occupancy.
    """

    point: Dict[str, Any]
    sweep_id: SweepID
    chunk_id: ChunkID
    offset: int
    row_index: int
    path: str
    direct_indexing: bool


@dataclass(frozen=True)
class SweepCache:
    """
    Immutable parameter-sweep cache index.

    physical_occupancy:
        Which sweep_ids actually exist on disk.

    semantic_selection:
        Optional coordinate-space selection, e.g. seed == 1.

    effective_occupancy:
        physical_occupancy ∩ compiled semantic_selection
    """

    encoder: SweepEncoder
    chunk_policy: ChunkPolicy
    physical_occupancy: IndexSelection
    semantic_selection: Optional[CoordSelection] = None

    def __init__(
        self,
        encoder: SweepEncoder,
        chunk_policy: ChunkPolicy,
        physical_occupancy: Optional[IndexSelection] = None,
        semantic_selection: Optional[CoordSelection] = None,
    ) -> None:
        if chunk_policy.total_size != encoder.size:
            raise ValueError(
                "ChunkPolicy.total_size must match SweepEncoder.size"
            )

        object.__setattr__(self, "encoder", encoder)
        object.__setattr__(self, "chunk_policy", chunk_policy)
        object.__setattr__(
            self,
            "physical_occupancy",
            physical_occupancy if physical_occupancy is not None else IntervalSet(),
        )
        object.__setattr__(self, "semantic_selection", semantic_selection)

    @property
    def effective_occupancy(self) -> IndexSelection:
        if self.semantic_selection is None:
            return self.physical_occupancy

        compiled = self.encoder.compile_selection(self.semantic_selection)

        return IntersectionSet(
            self.physical_occupancy,
            compiled,
        )

    def has_id(self, sweep_id: SweepID) -> bool:
        self.encoder.layout.id_to_coord(sweep_id)
        return self.effective_occupancy.contains(sweep_id)

    def has_point(self, point: Point) -> bool:
        return self.has_id(self.encoder.encode_point(point))

    def with_id(self, sweep_id: SweepID) -> "SweepCache":
        """
        Return a new cache with one additional physically present sweep_id.

        This updates physical occupancy, not semantic selection.
        """
        self.encoder.layout.id_to_coord(sweep_id)

        new_physical = self._physical_as_interval_set().with_point(sweep_id)

        return SweepCache(
            encoder=self.encoder,
            chunk_policy=self.chunk_policy,
            physical_occupancy=new_physical,
            semantic_selection=self.semantic_selection,
        )

    def with_range(self, start_id: SweepID, end_id: SweepID) -> "SweepCache":
        """
        Return a new cache with a physically present ID range [start_id, end_id).
        """
        if not (0 <= start_id <= end_id <= self.encoder.size):
            raise ValueError(
                f"range [{start_id}, {end_id}) is out of bounds [0, {self.encoder.size})"
            )

        new_physical = self._physical_as_interval_set().with_range(
            start_id,
            end_id,
        )

        return SweepCache(
            encoder=self.encoder,
            chunk_policy=self.chunk_policy,
            physical_occupancy=new_physical,
            semantic_selection=self.semantic_selection,
        )

    def with_point(self, point: Point) -> "SweepCache":
        return self.with_id(self.encoder.encode_point(point))

    def slice(self, selection: CoordSelection) -> "SweepCache":
        """
        Return a semantic view of the cache.

        This does not rewrite physical occupancy or repack files.
        """
        if self.semantic_selection is None:
            new_selection = selection
        else:
            new_selection = CoordIntersection(
                self.semantic_selection,
                selection,
            )

        return SweepCache(
            encoder=self.encoder,
            chunk_policy=self.chunk_policy,
            physical_occupancy=self.physical_occupancy,
            semantic_selection=new_selection,
        )

    def slice_by_values(self, **constraints: Any) -> "SweepCache":
        """
        Convenience wrapper using parameter values.

        Example:
            cache.slice_by_values(seed=1)
            cache.slice_by_values(depth=[4, 8])
        """
        return self.slice(self.encoder.point_slice(**constraints))

    def locate_id(self, sweep_id: SweepID) -> Optional[ResolvedLocation]:
        self.encoder.layout.id_to_coord(sweep_id)

        occupancy = self.effective_occupancy

        if not occupancy.contains(sweep_id):
            return None

        chunk_loc = self.chunk_policy.locate(sweep_id)
        chunk_start, chunk_end = self.chunk_policy.chunk_bounds(
            chunk_loc.chunk_id
        )

        if occupancy.covers_range(chunk_start, chunk_end):
            row_index = chunk_loc.offset
            direct_indexing = True
        else:
            row_index = occupancy.rank_between(
                chunk_start,
                sweep_id,
            )
            direct_indexing = False

        return ResolvedLocation(
            point=self.encoder.decode_id(sweep_id),
            sweep_id=sweep_id,
            chunk_id=chunk_loc.chunk_id,
            offset=chunk_loc.offset,
            row_index=row_index,
            path=chunk_loc.path,
            direct_indexing=direct_indexing,
        )

    def locate_point(self, point: Point) -> Optional[ResolvedLocation]:
        return self.locate_id(self.encoder.encode_point(point))

    def is_chunk_full(self, chunk_id: ChunkID) -> bool:
        start, end = self.chunk_policy.chunk_bounds(chunk_id)
        return self.effective_occupancy.covers_range(start, end)

    def items_in_chunk(self, chunk_id: ChunkID) -> list[Interval]:
        start, end = self.chunk_policy.chunk_bounds(chunk_id)
        return self.effective_occupancy.intersect_range(start, end)

    def chunk_summary(self) -> list[Dict[str, Any]]:
        rows = []

        occupancy = self.effective_occupancy

        for chunk_id in range(self.chunk_policy.num_chunks):
            start, end = self.chunk_policy.chunk_bounds(chunk_id)
            intervals = occupancy.intersect_range(start, end)
            count = sum(b - a for a, b in intervals)
            capacity = end - start

            rows.append(
                {
                    "chunk_id": chunk_id,
                    "path": self.chunk_policy.path_for(chunk_id),
                    "bounds": (start, end),
                    "present": intervals,
                    "count": count,
                    "capacity": capacity,
                    "full": count == capacity,
                }
            )

        return rows

    def _physical_as_interval_set(self) -> IntervalSet:
        return IntervalSet(
            self.physical_occupancy.intersect_range(
                0,
                self.encoder.size,
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Minimal serialization.

        This does not serialize semantic_selection yet, because coordinate
        selection serialization should probably live in selections.py.
        """
        if self.semantic_selection is not None:
            raise NotImplementedError(
                "semantic_selection serialization is not implemented yet"
            )

        return {
            "type": "SweepCache",
            "version": 1,
            "encoder": self.encoder.to_dict(),
            "chunk_policy": self.chunk_policy.to_dict(),
            "physical_occupancy": SelectionCodec.to_dict(
                self.physical_occupancy
            ),
            "semantic_selection": None,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SweepCache":

        encoder = SweepEncoder.from_dict(data["encoder"])
        chunk_policy = ChunkPolicy.from_dict(data["chunk_policy"])

        physical_occupancy = SelectionCodec.from_dict(
            data["physical_occupancy"],
            chunk_spec=chunk_policy.chunk_spec,
        )

        if data.get("semantic_selection") is not None:
            raise NotImplementedError(
                "semantic_selection deserialization is not implemented yet"
            )

        return cls(
            encoder=encoder,
            chunk_policy=chunk_policy,
            physical_occupancy=physical_occupancy,
            semantic_selection=None,
        )


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

    chunks = ChunkPolicy.from_size(
        chunk_size=4,
        total_size=encoder.size,
        path_template="chunks/chunk_{chunk_id:06d}.parquet",
    )

    empty = SweepCache(encoder, chunks)

    cache = (
        empty
        .with_range(0, 4)
        .with_id(6)
        .with_point({"lr": 0.01, "depth": 4, "seed": 1})
    )

    seed_1 = cache.slice_by_values(seed=1)

    print(list(seed_1.effective_occupancy))
    # [1, 3, 9]

    print(seed_1.locate_point({"lr": 0.01, "depth": 4, "seed": 1}))
    breakpoint()
