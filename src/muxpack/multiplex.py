import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io

class Multiplex:
    """
    A multiplex is a graph with multiple layers. Each layer represents a
    different type of relationship between the same set of vertices. For
    example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "year", "layer" and "relationtype".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id","year" and optional additional columns.
    vertices: ibis.Table

    #
    all_vertices: ibis.Table

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None) -> None:
        if not check_edges(edges):
            raise ValueError("Invalid edges table")
        
        if vertices is not None and not check_vertices(vertices):
            raise ValueError("Invalid vertices table")
        
        self.edges = edges
        #TODO derive vertices from edges if not provided
        self.vertices = vertices
        if not vertices is None:
            self.all_vertices = vertices[["id"]].distinct() 
    
    def years(self) -> list[int]:
        """
        Get the list of years in the multiplex.
        """
        years = self.edges[["year"]].distinct().to_pandas().year.tolist()
        return years
    
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
        src = self.edges.select(id="src",year = "year").distinct()
        dst = self.edges.select(id="dst",year = "year").distinct()

        V = src.union(dst, distinct=True).to_pyarrow()
        V_all = V[[V.id]].to_pyarrow()
        self.vertices = ibis.memtable(V)
        self.all_vertices = ibis.memtable(V_all)

    def save(self, dir: Path | str, **kw_args):
        """
        Save a multiplex to disk, using the specification.
        Note that saving does 
        """
        edges = self.edges
        vertices = self.vertices
        if vertices is None:
            mp = Multiplex(edges = self.edges)
            mp.update_vertices()
            vertices = mp.vertices
        
        io.save_network(edges, vertices, dir = dir, **kw_args)