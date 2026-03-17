
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
    # print(f"{edges.get_backend()}")
    # ddb.to_parquet(vertices, "data/vertices.parquet")
    # m = Multiplex(edges)