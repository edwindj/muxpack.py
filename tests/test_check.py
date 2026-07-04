import ibis
from muxpack import check_edges, check_vertices



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


def test_vertices_require_period_by_default():
    vertices = ibis.memtable(
        {
            "id": [1, 2, 3, 4],
        }
    )

    assert not check_vertices(vertices)


def test_vertices_accept_without_period_when_disabled():
    vertices = ibis.memtable(
        {
            "id": [1, 2, 3, 4],
        }
    )

    assert check_vertices(vertices, check_period=False)


def test_edges_wrong_type_fails_validation():
    edges = ibis.memtable(
        {
            "src": [1, 2],
            "dst": [2, 3],
            "period": [2020, 2020],
            "layer": ["A", 42],
            "relationtype": [1, 2],
        }
    )

    assert not check_edges(edges)
