import ibis

from .check import check_edges, check_vertices
from pathlib import Path
from . import io
from .multiplex import Multiplex
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


class MultiplexSeries:
    """
    A multiplexseries is a series of Multiplex graphs with multiple layers, spanning multiple periods.
    """

    #: The edges of the multiplex. This is a table with columns "src", "dst", "period", "layer" and "relationtype".
    edges: ibis.Table

    #: The vertices of the multiplex. This is a table with a column "id","period" and optional additional columns.
    vertices: ibis.Table | None

    #
    vertex_ids: ibis.Table

    relationtypes: ibis.Table | None

    def __init__(
        self,
        edges: ibis.Table,
        vertices: ibis.Table = None,
        relationtypes: ibis.Table = None,
    ) -> None:
        """
        Initialize a multiplex series with the given edges and vertices tables.
        The edges table must have columns "src", "dst", "period", "layer" and
        "relationtype". The vertices table must have a column "id" and optional
        additional columns, and must have a column "period" if the edges table has
        a column "period". The relationtypes table must have a column
        "relationtype", "layer", "label" and optional additional columns.
        """
        if not check_edges(edges):
            raise ValueError("Invalid edges table")

        if vertices is not None and not check_vertices(vertices):
            raise ValueError("Invalid vertices table")

        self.edges = edges
        # TODO derive vertices from edges if not provided
        self.vertices = vertices
        self.relationtypes = relationtypes

        if not vertices is None:
            logger.info("Vertices table provided, using it as is.")
            self.vertex_ids = vertices[["id"]].distinct()

    def periods(self) -> list[int]:
        """
        Get the list of periods in the multiplex.
        """
        periods = (
            self.edges.select(self.edges.period)
            .distinct()
            .order_by("period")
            .to_pyarrow()
            .column("period")
            .to_pylist()
        )
        # periods = self.edges[["period"]].distinct().to_pandas().period.tolist()
        return periods

    def layers(self) -> list[str]:
        """
        Get the list of layers in the multiplex.
        """
        layers = (
            self.edges.select(self.edges.layer)
            .distinct()
            .order_by("layer")
            .to_pyarrow()
            .column("layer")
            .to_pylist()
        )
        return layers

    def update_vertices(self) -> None:
        """
        Update the vertices table based on the edges table. This is useful if
        the vertices table was not provided at initialization.
        """
        src = self.edges.select(id="src", period="period").distinct()
        dst = self.edges.select(id="dst", period="period").distinct()

        V = src.union(dst, distinct=True)
        V_all = V.select(V.id)
        self.vertices = ibis.memtable(V.to_pyarrow())
        self.vertex_ids = ibis.memtable(V_all.to_pyarrow())

    def update_relationtypes(self) -> None:
        """
        Update the relationtypes table based on the edges table. This is useful
        if the relationtypes table was not provided at initialization.
        """
        relationtypes = (
            self.edges.select(self.edges.relationtype, self.edges.layer)
            .distinct()
            .order_by("layer", "relationtype")
            .to_pandas()
            .assign(
                label=lambda df: (
                    df["layer"].astype(str) + "_" + df["relationtype"].astype(str)
                )
            )
        )
        logger.debug(
            f"Updated relationtypes table with {len(relationtypes)} unique relationtypes."
        )
        self.relationtypes = ibis.memtable(relationtypes)

    def get_multiplex(self, period: int) -> Multiplex:
        """
        Get a multiplex for a specific period.
        """
        E_y = self.edges.filter(self.edges.period == period)
        if self.vertices is not None:
            V_y = self.vertices.filter(self.vertices.period == period)
        else:
            V_y = None
        return Multiplex(edges=E_y, vertices=V_y, period=period)

    def multiplexes(self) -> list[Tuple[int, Multiplex]]:
        """
        Get a list of multiplexes for all periods in the multiplex series.
        """
        periods = self.periods()
        return [(period, self.get_multiplex(period)) for period in periods]

    def add_filter(
        self,
        periods: list[int] = None,
        layers: list[str] = None,
        relationtypes: list[int] = None,
        src: list[int] = None,
        dst: list[int] = None,
    ):
        """
        Apply a filter to the multiplex network. 
        Note that an empty list means no filtering and that filtering
        is lazy, the filter will only be executed when saving or converting
        to another format.

        Advanced filtering can be accomplished by modifying the `edges`
        property directly using `ibis` expressions.

        Args:
            - periods: list of periods to filter on
            - layers: list of layers to filter on
            - relationtype: list of relationtypes to filter on
            - src: list of vertex ids (ego)
            - dst: list of vertex ids (non-ego)

        Return:
            - MultiplexSeries object
        """
        E = self.edges

        flt: list[ibis.BooleanValue] = []

        if periods is not None and len(periods) > 0:
            flt.append(E.period.isin(periods))

        if layers is not None and len(layers) > 0:
            flt.append(E.layer.isin(layers))

        if relationtypes is not None and len(relationtypes) > 0:
            flt.append(E.relationtype.isin(relationtypes))

        if src is not None and len(src) > 0:
            vid = ibis.memtable({"id": src})
            # we use semi join because we expect the vertex list to be large
            E = E.semi_join(vid, E.src == vid.id)

        if dst is not None and len(dst) > 0:
            vid = ibis.memtable({"id": dst})
            # we use semi join because we expect the vertex list to be large
            E = E.semi_join(vid, E.dst == vid.id)

        logger.debug("Filter: f{flt}")
        if len(flt):
            E = E.filter(flt)

        self.edges = E

    def __copy__(self):
        return MultiplexSeries(self.edges, self.vertices)

    def collapse(self) -> Multiplex:
        """
        Collapse the multiplex series into a single multiplex, by ignoring the
        period information. This is useful for analyses that do not require
        temporal information
        """
        E = self.edges.select(["src", "dst", "layer", "relationtype"]).distinct()
        if self.vertices is not None:
            V = self.vertices.select("id").distinct()
        else:
            V = None
        return Multiplex(edges=E, vertices=V, period=None)

    def save(self, dir: Path | str, **kw_args):
        """
        Save a multiplex to disk, using the specification.
        Note that saving does create the directory if it does not exist, and it
        will overwrite existing files in the directory.
        Saving might also improve performance, because it will
        evaluate the `edges` and `vertices` expressions of this MultiplexSeries
        """
        edges = self.edges
        vertices = self.vertices
        if vertices is None:
            mp = MultiplexSeries(edges=self.edges)
            mp.update_vertices()
            vertices = mp.vertices
        E, V = io.save_network(edges, vertices, dir=dir, **kw_args)
        self.edges = E
        self.vertices = V
