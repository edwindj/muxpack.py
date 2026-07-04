import muxpack
import ibis
import tempfile
from collections.abc import Generator
import pytest
from muxpack import io as muxio


@pytest.fixture()
def simple_mps() -> Generator[muxpack.MultiplexSeries]:
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            "period": [2020, 2020, 2021, 2021],
            "layer": ["A", "B", "A", "B"],
            "relationtype": [1, 2, 1, 2],
        }
    )
    vertices = ibis.memtable({"id": [1, 2, 3, 4], "period": [2020, 2020, 2021, 2021]})
    m = muxpack.MultiplexSeries(edges, vertices)
    yield m


# def test_load_data():
#     mp = muxpack.load_network("data")
#     assert mp is not None


def test_save_data(simple_mps):
    with tempfile.TemporaryDirectory() as tmpdir:
        mp = muxpack.MultiplexSeries(simple_mps.edges, simple_mps.vertices)
        mp.save(tmpdir)
        for layer in mp.layers():
            assert layer in simple_mps.layers()

        for period in mp.periods():
            assert period in simple_mps.periods()

        E1 = mp.edges.to_pandas()
        E2 = simple_mps.edges.to_pandas()
        V1 = mp.vertices.to_pandas()
        V2 = simple_mps.vertices.to_pandas()

        assert E1.ndim == E2.ndim
        assert len(E1) == len(E2)

        assert V1.ndim == V2.ndim
        assert len(V1) == len(V2)
        # assert(mp.edges.to_pandas().count() == mp2.edges.to_pandas().count())


def test_save_multiplex_period_none_preserves_all_rows():
    edges = ibis.memtable(
        {
            "src": [1, 2, 2],
            "dst": [2, 3, 4],
            "period": [2020, 2020, 2021],
            "layer": ["A", "B", "A"],
            "relationtype": [1, 2, 1],
        }
    )
    vertices = ibis.memtable(
        {"id": [1, 2, 3, 4], "period": [2020, 2020, 2021, 2021]}
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        saved_edges, saved_vertices = muxio.save_multiplex(
            edges=edges,
            vertices=vertices,
            dir=tmpdir,
            period=None,
        )
        assert saved_edges.count().execute() == edges.count().execute()
        assert saved_vertices.count().execute() == vertices.count().execute()


def test_relationtypes_roundtrip(simple_mps):
    with tempfile.TemporaryDirectory() as tmpdir:
        mp = muxpack.MultiplexSeries(simple_mps.edges, simple_mps.vertices)
        mp.update_relationtypes()
        mp.save(tmpdir)

        loaded = muxpack.read_multiplexseries(tmpdir)
        assert loaded.relationtypes is not None
        rt = loaded.relationtypes.to_pandas()
        assert set(rt["layer"]) == {"A", "B"}
        assert set(rt["relationtype"]) == {1, 2}
