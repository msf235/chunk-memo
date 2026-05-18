import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "chunk_memo" / "selections.py"
_SPEC = spec_from_file_location("chunk_memo.selections", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

ChunkSpec = _MODULE.ChunkSpec
DifferenceSet = _MODULE.DifferenceSet
IntervalSet = _MODULE.IntervalSet
StridedSet = _MODULE.StridedSet


def test_interval_set_uses_exclusive_upper_bounds():
    selection = IntervalSet([(2, 5)]).with_range(5, 7)

    assert list(selection) == [2, 3, 4, 5, 6]
    assert selection.intervals == ((2, 7),)
    assert selection.intersect_range(3, 6) == [(3, 6)]
    assert selection.count_between(0, 10) == 5
    assert selection.rank_between(2, 5) == 3


def test_strided_set_returns_half_open_singletons():
    selection = StridedSet(start=1, end=8, step=2)

    assert selection.intersect_range(0, 8) == [(1, 2), (3, 4), (5, 6), (7, 8)]
    assert selection.count_between(0, 8) == 4
    assert selection.contains(7)
    assert not selection.contains(8)


def test_difference_set_preserves_exclusive_bounds():
    base = IntervalSet([(0, 10)])
    remove = IntervalSet([(2, 4), (6, 9)])

    assert DifferenceSet(base, remove).intersect_range(0, 10) == [(0, 2), (4, 6), (9, 10)]


def test_chunk_spec_chunk_bounds_are_half_open():
    spec = ChunkSpec(chunk_size=4, total_size=10)

    assert spec.chunk_bounds(0) == (0, 4)
    assert spec.chunk_bounds(2) == (8, 10)
