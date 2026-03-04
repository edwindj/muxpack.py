import ibis

class Multiplex:
    """
    A multiplex is a graph with multiple layers. Each layer represents a
    different type of relationship between the same set of vertices. For
    example, in a social network, one layer could represent friendships, while
    another layer could represent professional connections.
    """
    _edges: ibis.Table
    _vertices: ibis.Table

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None) -> None:
        self._edges = edges

        if (vertices is None) and (edges is not None):
            self._vertices = edges.distinct("src").union(
                edges.distinct("dst")
            )
        self._vertices = vertices
    
    def years(self) -> list[int]:
        years = self._edges.distinct("year").execute()
        return years
    
    def layers(self) -> list[str]:
        layers = self._edges.distinct("layer").execute()
        return layers
    
    def edges(self) -> ibis.Table:
        # This is a placeholder for the actual implementation
        return self._edges
