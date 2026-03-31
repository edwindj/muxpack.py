import ibis
from muxpack import Bipartite
import pandas as pd


def make_table():
    return ibis.memtable(
        {
            "a": [1, 2, 2, 3],
            "b": ["x", "x", "z", "z"],
            "relationtype": ["r", "r", "r2", "r2"],
        },
    )


def test_bipartite():
    edges = make_table()
    V_a = edges.select(id="a").distinct()
    V_b = edges.select(id="b").distinct()
    bp = Bipartite(edges, role_src="a", role_dst="b")

    assert bp.role_dst == "b"
    assert bp.role_src == "a"
    assert bp.relationtype == "relationtype"


def test_project_to_src():
    edges = make_table()
    bp = Bipartite(edges, role_src="a", role_dst="b")
    E = bp.project_to_src()

    d = E.to_pandas()

    d_exp = pd.DataFrame(
        {
            "src": [1, 2, 2, 3],
            "dst": [2, 3, 1, 2],
            "relationtype": ["r", "r2", "r", "r2"],
        }
    )
    pd.testing.assert_frame_equal(d, d_exp)
