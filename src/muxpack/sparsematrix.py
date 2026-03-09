from ibis import Table
from .multiplex import Multiplex

def sparsify(mp: Multiplex) -> ibis.Table:
    """
    Turn the edge list into a 
    """
    v = mp.vertices
    e = mp.edges
    pass