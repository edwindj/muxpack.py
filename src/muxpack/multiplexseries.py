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

        Args:
            - edges: table with columns ``src``, ``dst``, ``period``, ``layer``, and ``relationtype``.
            - vertices: table with column ``id``, ``period``, and optional additional columns.
              Must have a ``period`` column because the edges table has one.
            - relationtypes: table with columns ``relationtype``, ``layer``, ``label``,
              and optional additional columns.

        Raises:
            - ValueError: if the edges table does not satisfy the required schema.
            - ValueError: if the vertices table does not satisfy the required schema.
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
        Get the list of periods present in the multiplex series.

        Returns:
            - Sorted list of period values.
        """
        periods = (
            self.edges.select("period")
            .distinct()
            .order_by("period")
            .period
            .to_list()
        )
        # periods = self.edges[["period"]].distinct().to_pandas().period.tolist()
        return periods

    def layers(self) -> list[str]:
        """
        Get the list of layers present in the multiplex series.

        Returns:
            - Sorted list of layer names.
        """
        layers = (
            self.edges.select("layer")
            .distinct()
            .order_by("layer")
            .layer
            .to_list()
        )
        return layers

    def update_vertices(self) -> None:
        """
        Update the vertices table by deriving it from the edges table.
        This is useful when the vertices table was not provided at initialization.
        Both ``self.vertices`` and ``self.vertex_ids`` are updated in place.
        """
        src = self.edges.select(id="src", period="period").distinct()
        dst = self.edges.select(id="dst", period="period").distinct()

        V = src.union(dst, distinct=True)
        V_all = V.select(V.id)
        self.vertices = ibis.memtable(V.to_pyarrow())
        self.vertex_ids = ibis.memtable(V_all.to_pyarrow())

    def update_relationtypes(self) -> None:
        """
        Update the relationtypes table by deriving it from the edges table.
        This is useful when the relationtypes table was not provided at initialization.
        A ``label`` column is constructed as ``"<layer>_<relationtype>"``.
        ``self.relationtypes`` is updated in place.
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
        Return the multiplex for a specific period.

        Args:
            - period: the period to retrieve.

        Returns:
            - Multiplex object containing only the edges and vertices for the given period.
        """
        E_y = self.edges.filter(self.edges.period == period)
        if self.vertices is not None:
            V_y = self.vertices.filter(self.vertices.period == period)
        else:
            V_y = None
        return Multiplex(edges=E_y, vertices=V_y, period=period)

    def multiplexes(self) -> list[Tuple[int, Multiplex]]:
        """
        Return all multiplexes in the series, one per period.

        Returns:
            - List of ``(period, Multiplex)`` tuples, ordered by period.
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
    ) -> None:
        """
        Apply a filter to the multiplex series in place.
        Filtering is lazy: the filter is only executed when saving or converting
        to another format. Passing ``None`` or an empty list for any argument
        means no filtering is applied for that dimension.

        For advanced filtering, modify the ``edges`` property directly using
        ibis expressions.

        Args:
            - periods: list of periods to keep.
            - layers: list of layer names to keep.
            - relationtypes: list of relationtype values to keep.
            - src: list of source vertex ids (ego) to keep.
            - dst: list of destination vertex ids (non-ego) to keep.
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

    def __copy__(self) -> "MultiplexSeries":
        """
        Return a shallow copy of this MultiplexSeries.

        Returns:
            - A new MultiplexSeries sharing the same ``edges`` and ``vertices`` tables.
        """
        return MultiplexSeries(self.edges, self.vertices)

    def collapse(self) -> Multiplex:
        """
        Collapse the multiplex series into a single Multiplex by discarding period
        information. Duplicate edges across periods are removed. This is useful
        for analyses that do not require temporal information.

        Returns:
            - Multiplex containing all distinct edges across all periods, with ``period=None``.
        """
        E = self.edges.select(["src", "dst", "layer", "relationtype"]).distinct()
        if self.vertices is not None:
            V = self.vertices.select("id").distinct()
        else:
            V = None
        return Multiplex(edges=E, vertices=V, period=None)

    def collapse_to(self, dir: Path | str) -> None:
        """
        Collapse the multiplex series and save the result to disk.
        This is a convenience method equivalent to calling ``collapse()`` followed
        by ``Multiplex.save()``.

        Args:
            - dir: path to the directory where the collapsed Multiplex will be saved.
        """
        m = self.collapse()
        return m.save(dir=dir)

    def save(self, dir: Path | str, **kw_args) -> None:
        """
        Save the multiplex series to disk.
        The directory is created if it does not exist; existing files are overwritten.
        Saving also evaluates the lazy ``edges`` and ``vertices`` expressions and
        updates them to point at the saved files, which can improve subsequent
        performance.

        Args:
            - dir: path to the directory where the MultiplexSeries will be saved.
            - **kw_args: additional keyword arguments forwarded to ``io.save_network``.
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
