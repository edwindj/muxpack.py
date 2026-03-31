import networkx as nx
import ibis
from .to_csr_matrix import to_csr_matrix


def to_MultiDiGraph(edges: ibis.Table, vertices: ibis.Table) -> nx.MultiDiGraph:
    """
    Convert an edge list and vertex table to a NetworkX MultiDiGraph.

    Args:
        - edges: table with ``src`` and ``dst`` columns.
        - vertices: table with an ``id`` column.

    Returns:
        - NetworkX MultiDiGraph built from the CSR matrix representation of the edges.
    """
    # problem: this generates
    csr = to_csr_matrix(edges, vertices)
    mdg = nx.MultiDiGraph(csr)
    return mdg
