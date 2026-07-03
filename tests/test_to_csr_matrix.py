import pytest
import muxpack
from collections.abc import Generator
from typing import Tuple
import ibis
from scipy.sparse import csr_matrix


@pytest.fixture()
def edges_vertices() -> Generator[Tuple[ibis.Table, ibis.Table]]:
    E = ibis.memtable({"src": [100, 100, 200], "dst": [200, 300, 300]})

    V = ibis.memtable({"id": [100, 200, 300]})
    yield E, V


def test_to_csr_matrix(edges_vertices):
    edges, vertices = edges_vertices
    m = muxpack.to_csr_matrix(edges, vertices)

    assert isinstance(m, csr_matrix)
    print(m.toarray())


def test_to_weighted_csr_matrix(edges_vertices):
    edges, vertices = edges_vertices
    edges = edges.mutate(weight=[1.0, 2.0, 3.0])
    # m = muxpack.to_csr_matrix(edges, vertices, use_weight="weight")
    m = muxpack.to_csr_matrix(edges, vertices)
    assert isinstance(m, csr_matrix)
    print(m.toarray())
