import ibis
from muxpack import check_edges



def test_layers():
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            # "layer": ["A", "B", "A", "B"],
            "relationtype": [1, 2, 1, 2],
            "weight": [0.1, 1.2, 1.4, 1.5],
        }
    )
    # layer is missing
    assert not check_edges(edges)
