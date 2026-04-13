import ibis
from muxpack import check_edges
import pytest

def test_check():
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            "relationtype": [1, 2, 1, 2],
        }
    )
    vertices = ibis.memtable(
        {
            "id": [1, 2, 3, 4],
        }
    )

    check_edges(edges, check_period=False)