import ibis

from .check import check_edges, check_vertices
class Multiplex:
    """
    A multiplex is a graph with multiple layers. Each layer represents a
    different type of relationship between the same set of vertices. For
    example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "year", and "layer".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id" and optional additional columns.
    vertices: ibis.Table

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None) -> None:
        if not check_edges(edges):
            raise ValueError("Invalid edges table")
        
        if vertices is not None and not check_vertices(vertices):
            raise ValueError("Invalid vertices table")
        
        self.edges = edges
        #TODO derive vertices from edges if not provided
        self.vertices = vertices
    
    def years(self) -> list[int]:
        """
        Get the list of years in the multiplex.
        """
        years = self.edges.distinct("year").execute()
        return years
    
    def layers(self) -> list[str]:
        """
        Get the list of layers in the multiplex.
        """
        layers = self.edges.distinct("layer").execute()
        return layers
    
    def update_vertices(self) -> None:
        """
        Update the vertices table based on the edges table. This is useful if the vertices table was not provided at initialization.
        """
        src = self.edges.select(id="src").distinct()
        dst = self.edges.select(id="dst").distinct()
        self.vertices = src.union(dst).distinct().execute()
    