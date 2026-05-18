import tempfile

from chunk_memo import ChunkCache, ChunkSpec


def make_cache(*, chunk_spec=None, axis_values=None):
    return ChunkCache(
        root=tempfile.gettempdir(),
        cache_id="slice-test",
        chunk_spec=chunk_spec or {"strat": 1, "s": 2},
        axis_values=axis_values
        or {"strat": ["a", "b"], "s": [1, 2, 3, 4]},
        verbose=0,
    )


def test_chunk_spec_normalizes_size_spec_to_explicit_chunks():
    cache = make_cache()

    assert cache.chunk_spec.to_metadata() == {
        "s": {"chunks": [[1, 2], [3, 4]], "size": 2},
        "strat": {"chunks": [["a"], ["b"]], "size": 1},
    }


def test_chunk_spec_accepts_explicit_chunks():
    spec = ChunkSpec.from_axis_values(
        {
            "s": {"chunks": [[1, 2], [3, 4]]},
            "strat": {"chunks": [["a"], ["b"]]},
        },
        {"strat": ["a", "b"], "s": [1, 2, 3, 4]},
        axis_order=("strat", "s"),
    )

    assert spec.to_metadata() == {
        "strat": {"chunks": [["a"], ["b"]], "size": 1},
        "s": {"chunks": [[1, 2], [3, 4]], "size": 2},
    }


def test_slice_records_partial_chunk_requests():
    cache = make_cache()

    sliced = cache.slice(s=[2, 3])

    assert sliced.resolved_chunk_keys() == [
        (("s", (1, 2)), ("strat", ("a",))),
        (("s", (1, 2)), ("strat", ("b",))),
        (("s", (3, 4)), ("strat", ("a",))),
        (("s", (3, 4)), ("strat", ("b",))),
    ]
    assert sliced.requested_items_by_chunk() == {
        (("s", (1, 2)), ("strat", ("a",))): [(2, "a")],
        (("s", (1, 2)), ("strat", ("b",))): [(2, "b")],
        (("s", (3, 4)), ("strat", ("a",))): [(3, "a")],
        (("s", (3, 4)), ("strat", ("b",))): [(3, "b")],
    }
    assert sliced.chunk_spec.to_metadata() == {'s': {'chunks': [[2, 3]], 'size': 2}, 'strat': {'chunks': [['a'], ['b']], 'size': 1}}

    breakpoint()

def test_slice_records_complete_chunk_requests():
    cache = make_cache()

    sliced = cache.slice(s=[1, 2], strat=["a"])

    assert sliced.resolved_chunk_keys() == [(("s", (1, 2)), ("strat", ("a",)))]
    assert sliced.requested_items_by_chunk() == {
        (("s", (1, 2)), ("strat", ("a",))): [(1, "a"), (2, "a")]
    }

if __name__ == "__main__":
    test_slice_records_partial_chunk_requests()
