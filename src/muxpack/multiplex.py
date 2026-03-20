import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io
import logging
from typing import Tuple
from scipy.sparse import csr_matrix

logger = logging.getLogger(__name__)

class Multiplex:
    """
    A multiplex is a graph with multiple layers. 
    Each layer represents a different type of relationship between the same set of vertices, during one year.
    For example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    For multiple years, use MultiplexSeries.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "layer" and "relationtype".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id" and optional additional columns.
    vertices: ibis.Table

    year: int | None

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None, year: int | None = None) -> None:
        if not check_edges(edges, check_year=False):
            raise ValueError("Invalid edges table")
        
        if vertices is not None and not check_vertices(vertices, check_year=False):
            raise ValueError("Invalid vertices table")

        self.year = year 
        self.edges = edges
        #TODO derive vertices from edges if not provided
        self.vertices = vertices
    
    def layers(self) -> list[str]:
        """
        Get the list of layers in the multiplex.
        """
        layers = self.edges[["layer"]].distinct().to_pandas().layer.tolist()
        return layers
    
    def update_vertices(self) -> None:
        """
        Update the vertices table based on the edges table. This is useful if the vertices table was not provided at initialization.
        """
        src = self.edges.select(id="src").distinct()
        dst = self.edges.select(id="dst").distinct()

        V = src.union(dst, distinct=True).to_pyarrow()
        self.vertices = ibis.memtable(V)

    def to_csr_matrix(self) -> csr_matrix[bool]:
        """
        Transform the multiplex into a sparse matrix (csr_matrix)
        Note that this will collapse all layers into one. If you want to keep the layers separate, use `to_csr_matrices` instead.
        """
        from .to_csr_matrix import to_row_col_idx, idx_to_csr_matrix
        idx = to_row_col_idx(self.edges, self.vertices)
        M = idx_to_csr_matrix(idx, self.vertices)
        return M
    
    def to_csr_matrices(self) -> dict[str, csr_matrix]:
        """
        Transform the multiplex into a dictionary of sparse matrices (csr_matrix), one for each layer.
        """
        from .to_csr_matrix import to_row_col_idx, idx_to_csr_matrix
        layers = self.layers()
        matrices = {}
        for layer in layers:
            idx = to_row_col_idx(self.edges.filter(self.edges.layer == layer), self.vertices)
            M = idx_to_csr_matrix(idx, self.vertices)
            matrices[layer] = M
        return matrices

    def save(self, dir: Path | str, **kw_args):
        """
        Save a multiplex to disk, using the specification.
        Note that saving does create the directory if it does not exist, and it
        will overwrite existing files in the directory.
        """
        edges = self.edges
        vertices = self.vertices
        if vertices is None:
            mp = Multiplex(edges = self.edges)
            mp.update_vertices()
            vertices = mp.vertices
        year = self.year if self.year is not None else 0
        edges = edges.with_column("year", ibis.literal(year))
        vertices = vertices.with_column("year", ibis.literal(year))
        io.save_network(edges, vertices, dir = dir, **kw_args)