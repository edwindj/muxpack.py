import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io
import logging
from scipy.sparse import csr_matrix
import networkx as nx

logger = logging.getLogger(__name__)


class Multiplex:
    """
    A multiplex is a graph with multiple layers.
    Each layer represents a different type of relationship between the same set of vertices, during one period.
    For example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    For multiple periods, use MultiplexSeries.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "layer","relationtype" and optionally weight.
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id" and optional additional columns.
    vertices: ibis.Table

    period: int | None

    def __init__(
        self, edges: ibis.Table, vertices: ibis.Table = None, period: int | None = None
    ) -> None:
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
        # TODO derive vertices from edges if not provided
        self.vertices = vertices

    def layers(self) -> list[str]:
        """
        Get the list of layers present in the multiplex.

        Returns:
            - List of layer names.
        """
        layers = self.edges[["layer"]].distinct().layer.to_list()
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

    def to_csr_matrix(self, use_weight: bool | str | ibis.Value = False) -> csr_matrix[bool] | csr_matrix[float]:
        """
        Transform the multiplex into a sparse matrix, collapsing all layers into one.
        To keep layers separate, use ``to_csr_matrices`` instead.

        Args:
            - use_weight: optional column in the edges table to use as weights for the adjacency matrix. If False, the adjacency matrix will be unweighted (boolean).
            if True, the method will look for a column named "weight" in the edges table. If a string is provided, it will be used as the name of the weight column.
              If not provided, the adjacency matrix will be unweighted (boolean).
        
        Returns:
            - Sparse boolean matrix of shape ``(n_vertices, n_vertices)``.
        """
        from .to_csr_matrix import to_row_col_idx, idx_to_csr_matrix

        E = self.edges
        V = self.vertices

        if use_weight is True:
            weight = "weight"
        elif isinstance(use_weight, str):
            E[["weight"]] = E[[use_weight]]
        elif isinstance(use_weight, ibis.Value):
            weight = "weight"
            E = E.mutate(weight=weight)
        else: 
            E = E.drop(["weight"], errors="ignore")

        if (use_weight is not False) and (weight not in E.columns):
            raise ValueError(f"Weight column '{weight}' not found in edges table")
        
        idx = to_row_col_idx(E, V)
        M = idx_to_csr_matrix(idx, V)
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
            idx = to_row_col_idx(
                self.edges.filter(self.edges.layer == layer), self.vertices
            )
            M = idx_to_csr_matrix(idx, self.vertices)
            matrices[layer] = M
        return matrices
    
    def outdegree(self, by_layer: bool = False) -> ibis.Table:
        """
        Compute the out-degree of each vertex in the multiplex.

        Args:
            - by_layer: if True, compute the out-degree separately for each layer.

        Returns:
            - by_layer=False: Table with columns "id" and "out_degree", where "id" is the vertex id and "out_degree" is the total number of outgoing edges from that vertex across all layers.
            - by_layer=True: Table with columns "id", "layer", and "out_degree", where "id" is the vertex id, "layer" is the layer name, and "out_degree" is the number of outgoing edges from that vertex in that layer.
        """
        E = self.edges

        gb = ["src"]
        if by_layer:
            gb.append("layer")

        outdegree = (
            E.group_by(gb)
            .aggregate(outdegree=E.count())
            .rename(id = "src")
        )
        return outdegree

    def to_networkx(self) -> nx.MultiDiGraph:
        """
        Convert the multiplex to a NetworkX MultiDiGraph.

        Returns:
            - NetworkX MultiDiGraph built from the CSR matrix representation of the edges.
        """
        from .networkx import to_MultiDiGraph

        return to_MultiDiGraph(self.edges, self.vertices)

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
        edges, vertices = io.save_multiplex(edges, vertices, period, dir=dir, **kw_args)
        self.edges = edges
        self.vertices = vertices
