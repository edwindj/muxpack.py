import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io
from .multiplex import Multiplex
import logging
from typing import Tuple, Generator

logger = logging.getLogger(__name__)

class MultiplexSeries:
    """
    A multiplexseries is a series of Multiplex graph with multiple layers, spanning multiple years. 
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "year", "layer" and "relationtype".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id","year" and optional additional columns.
    vertices: ibis.Table

    #
    vertex_ids: ibis.Table

    def __init__(self, edges: ibis.Table, vertices: ibis.Table = None) -> None:
        if not check_edges(edges):
            raise ValueError("Invalid edges table")
        
        if vertices is not None and not check_vertices(vertices):
            raise ValueError("Invalid vertices table")
        
        self.edges = edges
        #TODO derive vertices from edges if not provided
        self.vertices = vertices
        if not vertices is None:
            logger.info("Vertices table provided, using it as is.")
            self.vertex_ids = vertices[["id"]].distinct() 
    
    def years(self) -> list[int]:
        """
        Get the list of years in the multiplex.
        """
        years = (
            self.edges
            .select(self.edges.year)
            .distinct()
            .order_by("year")
            .to_pyarrow()
            .column("year")
            .to_pylist()
        )
        # years = self.edges[["year"]].distinct().to_pandas().year.tolist()
        return years
    
    def layers(self) -> list[str]:
        """
        Get the list of layers in the multiplex.
        """
        layers = (
            self.edges
            .select(self.edges.layer)
            .distinct()
            .order_by("layer")
            .to_pyarrow()
            .column("layer")
            .to_pylist()
        )
        return layers
    
    def update_vertices(self) -> None:
        """
        Update the vertices table based on the edges table. This is useful if the vertices table was not provided at initialization.
        """
        src = self.edges.select(id="src",year = "year").distinct()
        dst = self.edges.select(id="dst",year = "year").distinct()

        V = src.union(dst, distinct=True)
        V_all = V.select(V.id)
        self.vertices = ibis.memtable(V.to_pyarrow())
        self.vertex_ids = ibis.memtable(V_all.to_pyarrow())

    def get_multiplex(self, year: int) -> Multiplex:
        """
        Get a multiplex for a specific year.
        """
        E_y = self.edges.filter(self.edges.year == year)
        if self.vertices is not None:
            V_y = self.vertices.filter(self.vertices.year == year)
        else:
            V_y = None
        return Multiplex(edges=E_y, vertices=V_y, year = year)
    
    def multiplexes(self) -> list[Tuple[int,Multiplex]]:
        """
        Get a list of multiplexes for all years in the multiplex series.
        """
        years = self.years()
        return [(year, self.get_multiplex(year)) for year in years]
    
    def collapse(self) -> Multiplex:
        """
        Collapse the multiplex series into a single multiplex, by ignoring the year information. This is useful for analyses that do not require temporal.
        """
        E = self.edges.select(src="src", dst="dst", layer="layer", relationtype="relationtype").distinct()
        if self.vertices is not None:
            V = self.vertices[["id"]].distinct()
        else:
            V = None
        return Multiplex(edges=E, vertices=V, year = None)
    
    def save(self, dir: Path | str, **kw_args):
        """
        Save a multiplex to disk, using the specification.
        Note that saving does create the directory if it does not exist, and it
        will overwrite existing files in the directory.
        """
        edges = self.edges
        vertices = self.vertices
        if vertices is None:
            mp = MultiplexSeries(edges = self.edges)
            mp.update_vertices()
            vertices = mp.vertices
        io.save_network(edges, vertices, dir = dir, **kw_args)