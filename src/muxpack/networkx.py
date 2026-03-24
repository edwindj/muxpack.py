import networkx as nx
import ibis
from .to_csr_matrix import to_csr_matrix


def to_MultiDiGraph(edges: ibis.Table, vertices: ibis.Table) -> nx.MultiDiGraph:
    # problem: this generates
    csr = to_csr_matrix(edges, vertices)
    mdg = nx.MultiDiGraph(csr)
    return mdg
