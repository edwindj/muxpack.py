
import pytest
import duckdb

def test_true():
    assert 1 == 1

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
    vertices["value"] = True
    vertices = vertices.pivot(index="id", columns="year").fillna(False).reset_index()
    # print(f"{edges.get_backend()}")
    print(f"vertices: {vertices}")
    vertices.to_parquet("data/vertices.parquet")
    # ddb.to_parquet(vertices, "data/vertices.parquet")
    # m = Multiplex(edges)