
import pytest
import duckdb
import ibis

def test_multiplex():
    from muxpack.multiplex import Multiplex
    ddb = ibis.duckdb.connect()
    edges = ddb.read_parquet("data/*/edges/**/*.parquet")
    # edges = ibis.read_parquet("data/**/*.parquet")
    assert edges is not None
    src = edges.select(id="src", year="year").distinct()
    dst = edges.select(id="dst", year="year").distinct()

    vertices = src.union(dst).distinct().execute()


def test_layers():
    from muxpack.multiplex import Multiplex
    edges = ibis.memtable({
        "src": [1, 2, 2, 1],
        "dst": [2, 3, 4, 2],
        "year": [2020, 2020, 2021, 2021],
        "layer": ["A", "B", "A", "B"],
        "relationtype": [1,2, 1, 2]
    })
    vertices = ibis.memtable({
        "id": [1, 2, 3, 4],
        "year": [2020, 2020, 2021, 2021]
    })
    m = Multiplex(edges, vertices)
    assert len(m.layers()) == 2
    for l in m.layers():
        assert l in ["A", "B"]
    assert len(m.years()) == 2
    for y in m.years():
        assert y in [2020, 2021]