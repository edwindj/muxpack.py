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

    def __init__(self, *args, **kwargs):
        pass
    
    def years(self, t: ibis.Table) -> list[int]:
        years = t.distinct("year").execute()
        return years
    
    def layers(self, t: ibis.Table) -> list[str]:
        layers = t.distinct("layer").execute()
        return layers
    
    def edges(self) -> ibis.Table:
        # This is a placeholder for the actual implementation
        return self._edges
