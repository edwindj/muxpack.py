import pytest
import muxpack
import ibis
from scipy.sparse import csr_matrix

@pytest.fixture()
def edges_vertices():
    print("Create edges")
    E = ibis.memtable({
        "src": [100,200,300],
        "dst": [200,100,200]
    })

    V = ibis.memtable({
        "id": [100, 200, 300]
    })
    yield E, V
    print("end")  

def test_to_csr_matrix(edges_vertices):
    edges, vertices = edges_vertices
    m = muxpack.to_csr_matrix(edges, vertices)

    assert(isinstance(m, csr_matrix))