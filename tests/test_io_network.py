import muxpack
import ibis
import tempfile
from collections.abc import Generator
import pytest

@pytest.fixture()
def simple_mps() -> Generator[muxpack.MultiplexSeries]:
    edges = ibis.memtable({
        "src": [1, 2, 2, 1],
        "dst": [2, 3, 4, 2],
        "period": [2020, 2020, 2021, 2021],
        "layer": ["A", "B", "A", "B"],
        "relationtype": [1,2, 1, 2]
    })
    vertices = ibis.memtable({
        "id": [1, 2, 3, 4],
        "period": [2020, 2020, 2021, 2021]
    })
    m = muxpack.MultiplexSeries(edges, vertices)
    yield m

def test_load_data():
    mp = muxpack.load_network("data")
    assert mp is not None

def test_save_data(simple_mps):
    with tempfile.TemporaryDirectory() as tmpdir:
        mp = muxpack.MultiplexSeries(simple_mps.edges, simple_mps.vertices)
        mp.save(tmpdir)
        for layer in mp.layers():
            assert(layer in simple_mps.layers())

        for period in mp.periods():
            assert(period in simple_mps.periods())
        
        E1 = mp.edges.to_pandas()
        E2 = simple_mps.edges.to_pandas()
        V1 = mp.vertices.to_pandas()
        V2 = simple_mps.vertices.to_pandas()

        assert(E1.ndim == E2.ndim)
        assert(len(E1) == len(E2))
        
        assert(V1.ndim == V2.ndim)
        assert(len(V1) == len(V2))
        # assert(mp.edges.to_pandas().count() == mp2.edges.to_pandas().count())