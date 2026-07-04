import pytest
import muxpack
from collections.abc import Generator
from typing import Tuple
import ibis
from scipy.sparse import csr_matrix
from muxpack.to_csr_matrix import to_period_csr_matrix


@pytest.fixture()
def edges_vertices() -> Generator[Tuple[ibis.Table, ibis.Table]]:
    E = ibis.memtable(
        {
            "src": [100, 100, 200],
            "dst": [200, 300, 300],
            "layer": ["A", "A", "A"],
            "relationtype": [1, 1, 1],
        }
    )

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


def test_multiplex_to_csr_matrix(edges_vertices):
    edges, vertices = edges_vertices
    m = muxpack.Multiplex(edges=edges, vertices=vertices)
    csr = m.to_csr_matrix()
    print(csr.toarray())
    assert isinstance(csr, csr_matrix)
    assert csr.shape == (3, 3)
    assert csr.toarray().tolist() == [[0, 1, 1], [0, 0, 1], [0, 0, 0]]


def test_to_period_csr_matrix_all_periods():
    edges = ibis.memtable(
        {
            "src": [100, 100, 200],
            "dst": [200, 300, 300],
            "period": [2020, 2020, 2021],
            "layer": ["A", "A", "A"],
            "relationtype": [1, 1, 1],
        }
    )
    vertices = ibis.memtable({"id": [100, 200, 300]})

    matrices = list(to_period_csr_matrix(edges, vertices))
    periods = [period for _, period in matrices]
    assert len(periods) == 2
    assert set(periods) == {2020, 2021}
    for m, _ in matrices:
        assert isinstance(m, csr_matrix)
        assert m.shape == (3, 3)


def test_to_period_csr_matrix_explicit_period_subset():
    edges = ibis.memtable(
        {
            "src": [100, 100, 200],
            "dst": [200, 300, 300],
            "period": [2020, 2020, 2021],
            "layer": ["A", "A", "A"],
            "relationtype": [1, 1, 1],
        }
    )
    vertices = ibis.memtable({"id": [100, 200, 300]})

    matrices = list(to_period_csr_matrix(edges, vertices, periods=[2021]))
    assert len(matrices) == 1
    assert matrices[0][1] == 2021
