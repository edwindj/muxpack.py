import ibis
import pytest
import tempfile
import os
import muxpack
from muxpack import Multiplex


def test_layers():
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            "layer": ["A", "B", "A", "B"],
            "relationtype": [1, 2, 1, 2],
        }
    )
    vertices = ibis.memtable(
        {
            "id": [1, 2, 3, 4],
        }
    )
    m = Multiplex(edges, vertices, period=2020)
    assert len(m.layers()) == 2
    for layer in m.layers():
        assert layer in ["A", "B"]
    assert m.period == 2020


def test_outdegree():
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            "layer": ["A", "B", "A", "B"],
            "relationtype": [1, 2, 1, 2],
        }
    )
    vertices = ibis.memtable(
        {
            "id": [1, 2, 3, 4],
        }
    )
    m = Multiplex(edges, vertices)
    outdegree = m.outdegree()
    assert sorted(outdegree["id"].to_list()) == [1, 2]
    assert outdegree["outdegree"].to_list() == [2, 2]


def test_save_with_period():
    edges = ibis.memtable(
        {
            "src": [1, 2],
            "dst": [2, 3],
            "period": [2020, 2020],
            "layer": ["A", "B"],
            "relationtype": [1, 2],
        }
    )
    vertices = ibis.memtable({"id": [1, 2, 3], "period": [2020, 2020, 2020]})
    m = Multiplex(edges, vertices, period=2020)

    with tempfile.TemporaryDirectory() as tmpdir:
        m.save(tmpdir)
        assert os.path.exists(f"{tmpdir}/vertices.parquet")
        assert os.path.exists(f"{tmpdir}/edges")
        assert m.edges.count().execute() == 2
        assert m.vertices.count().execute() == 3


def test_cli_main_version_flag():
    with pytest.raises(SystemExit) as exc:
        muxpack.main(["--version"])
    assert exc.value.code == 0
