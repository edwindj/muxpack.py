import ibis
from muxpack import MultiplexSeries

def test_filter_edges():
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
    m = MultiplexSeries(edges, vertices)

    m1 = m.filter_edges(years=[2021])
    assert(len(m1.periods()) == 1)
    assert(m1.periods() == [2021])

    m2 = m.filter_edges(layers=["A"])
    assert(len(m2.layers()) == 1)
    assert(m2.layers() == ["A"])

    m3 = m.filter_edges(years=[2021], layers=["B"])
    assert(len(m3.periods()) == 1)
    assert(m3.periods() == [2021])
    assert(len(m3.layers()) == 1)
    assert(m3.layers() == ["B"])

