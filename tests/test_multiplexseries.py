import ibis
from muxpack import MultiplexSeries


def test_multiplexseries():
    ddb = ibis.duckdb.connect()
    edges = ddb.read_parquet("data/*/edges/**/*.parquet")
    mp = MultiplexSeries(edges=edges)
    assert mp.edges is not None


def test_layers_and_periods():
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
    m = MultiplexSeries(edges, vertices)
    assert len(m.layers()) == 2
    for l in m.layers():
        assert l in ["A", "B"]

    assert len(m.periods()) == 2
    for y in m.periods():
        assert y in [2020, 2021]


def test_get_multiplex():
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
    m = MultiplexSeries(edges, vertices)
    m.update_vertices()
    mp_2020 = m.get_multiplex(2020)
    assert mp_2020.period == 2020


def test_update_relationtypes():
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
    m = MultiplexSeries(edges, vertices)
    m.update_relationtypes()
    rt = m.relationtypes.to_pandas()
    assert len(rt) == 2
    assert set(rt["label"]) == {"A_1", "B_2"}
    assert set(rt["relationtype"]) == {1, 2}
    assert set(rt["layer"]) == {"A", "B"}
