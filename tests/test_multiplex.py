
import ibis
from muxpack import Multiplex

def test_layers():
    edges = ibis.memtable({
        "src": [1, 2, 2, 1],
        "dst": [2, 3, 4, 2],
        "layer": ["A", "B", "A", "B"],
        "relationtype": [1,2, 1, 2]
    })
    vertices = ibis.memtable({
        "id": [1, 2, 3, 4],
    })
    m = Multiplex(edges, vertices, period=2020)
    assert len(m.layers()) == 2
    for layer in m.layers():
        assert layer in ["A", "B"]
    assert m.period == 2020