
import pytest

def test_true():
    assert 1 == 1

import ibis

def test_multiplex():
    from muxpack.multiplex import Multiplex
    edges = ibis.read_parquet("data/**/*.parquet")
    assert edges is not None
    a = edges.distinct("src")
    print(a.to_pandas())
    # m = Multiplex(edges)