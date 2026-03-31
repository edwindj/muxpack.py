import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io
import logging
from scipy.sparse import csr_matrix

logger = logging.getLogger(__name__)

class Multiplex:
    """
    A multiplex is a graph with multiple layers. 
    Each layer represents a different type of relationship between the same set of vertices, during one period.
    For example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    For multiple periods, use MultiplexSeries.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "layer" and "relationtype".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id" and optional additional columns.
    vertices: ibis.Table

    period: int | None

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None, period: int | None = None) -> None:
        """
        Initialize a multiplex with the given edges and vertices tables.

        Args:
            - edges: table with columns ``src``, ``dst``, ``layer``, and ``relationtype``.
            - vertices: table with column ``id`` and optional additional columns.
            - period: the period this multiplex belongs to, or ``None`` if not applicable.

        Raises:
            - ValueError: if the edges table does not satisfy the required schema.
            - ValueError: if the vertices table does not satisfy the required schema.
        """
        if not check_edges(edges, check_period=False):
            raise ValueError("Invalid edges table")
        
        if vertices is not None and not check_vertices(vertices, check_period=False):
            raise ValueError("Invalid vertices table")

        self.period = period 
        self.edges = edges
        #TODO derive vertices from edges if not provided
        self.vertices = vertices
    
    def layers(self) -> list[str]:
        """
        Get the list of layers present in the multiplex.

        Returns:
            - List of layer names.
        """
        layers = self.edges[["layer"]].distinct().to_pandas().layer.tolist()
        return layers
    
    def update_vertices(self) -> None:
        """
        Update the vertices table by deriving it from the edges table.
        This is useful when the vertices table was not provided at initialization.
        ``self.vertices`` is updated in place.
        """
        src = self.edges.select(id="src").distinct()
        dst = self.edges.select(id="dst").distinct()

        V = src.union(dst, distinct=True).to_pyarrow()
        self.vertices = ibis.memtable(V)

    def to_csr_matrix(self) -> csr_matrix[bool]:
        """
        Transform the multiplex into a sparse matrix, collapsing all layers into one.
        To keep layers separate, use ``to_csr_matrices`` instead.

        Returns:
            - Sparse boolean matrix of shape ``(n_vertices, n_vertices)``.
        """
        from .to_csr_matrix import to_row_col_idx, idx_to_csr_matrix
        idx = to_row_col_idx(self.edges, self.vertices)
        M = idx_to_csr_matrix(idx, self.vertices)
        return M
    
    def to_csr_matrices(self) -> dict[str, csr_matrix]:
        """
        Transform the multiplex into a dictionary of sparse matrices, one per layer.

        Returns:
            - Dictionary mapping layer name to a sparse boolean matrix of shape ``(n_vertices, n_vertices)``.
        """
        from .to_csr_matrix import to_row_col_idx, idx_to_csr_matrix
        layers = self.layers()
        matrices = {}
        for layer in layers:
            idx = to_row_col_idx(self.edges.filter(self.edges.layer == layer), self.vertices)
            M = idx_to_csr_matrix(idx, self.vertices)
            matrices[layer] = M
        return matrices

    def save(self, dir: Path | str, **kw_args) -> None:
        """
        Save the multiplex to disk.
        The directory is created if it does not exist; existing files are overwritten.
        Saving also evaluates the lazy ``edges`` and ``vertices`` expressions and
        updates them to point at the saved files, which can improve subsequent performance.

        Args:
            - dir: path to the directory where the Multiplex will be saved.
            - **kw_args: additional keyword arguments forwarded to ``io.save_multiplex``.
        """
        edges = self.edges
        vertices = self.vertices
        if vertices is None:
            self.update_vertices()
            vertices = self.vertices
        period = self.period 
        edges, vertices = io.save_multiplex(edges, vertices, period, dir = dir, **kw_args)
        self.edges = edges
        self.vertices = vertices