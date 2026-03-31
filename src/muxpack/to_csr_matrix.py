from ibis import row_number, Table
import ibis
from scipy.sparse import csr_matrix
from muxpack.multiplex import Multiplex
from typing import Tuple, Generator

import logging

logger = logging.getLogger(__name__)
# from collections.abc import Generator


def to_row_col_idx(edges: Table, vertices: Table) -> Table:
    """
    Turn an edge list into a row/column index table based on the given vertices table.

    Args:
        - edges: table with ``src`` and ``dst`` columns.
        - vertices: table with an ``id`` column; edges not referencing a vertex in this
          table are filtered out.

    Returns:
        - Table with columns ``data``, ``row``, and ``col`` containing the boolean edge
          indicator and the row/column indices corresponding to vertex positions in
          ``vertices``. Can be passed directly to ``idx_to_csr_matrix``.
    """
    v = vertices.select("id").mutate(idx=row_number())
    row = v.select(src="id", row="idx")
    col = v.select(dst="id", col="idx")

    # may sum the number of columns
    idx_edges = (
        edges[["src", "dst"]]
        .distinct()
        .inner_join(row, "src")
        .inner_join(col, "dst")
        .mutate(data=True)
        .select("data", "row", "col")
    )
    logger.debug(
        f"Created row-col index table with {idx_edges.count().execute()} edges."
    )
    return idx_edges


def idx_to_csr_matrix(idx: Table, vertices: Table) -> csr_matrix:
    """
    Convert a row-column index table to a CSR sparse matrix.

    Args:
        - idx: table with columns ``data``, ``row``, and ``col``, as produced by
          ``to_row_col_idx``.
        - vertices: table with an ``id`` column; its row count determines the matrix size.

    Returns:
        - Square CSR sparse matrix of shape ``(n_vertices, n_vertices)``.
    """
    # TODO maybe to_parquet()?
    coo = idx.execute()
    logger.debug(f"COO matrix data: {coo}")

    n = vertices.count().execute()
    logger.debug(f"Number of vertices: {n}")
    M = csr_matrix((coo["data"], (coo["row"], coo["col"])), shape=(n, n))
    return M


def to_csr_matrix(edges: Table, vertices: Table | None) -> csr_matrix:
    """
    Transform an edge list into a sparse matrix (csr_matrix).

    Args:
        - edges: table with ``src`` and ``dst`` columns.
        - vertices: table with an ``id`` column; edges are filtered to vertices present
          in this table. Pass ``None`` to derive vertices from the edges table.

    Returns:
        - Square CSR sparse matrix of shape ``(n_vertices, n_vertices)``.
    """
    # vertices may contain multiple periods
    if vertices is not None:
        vertices = vertices[["id"]].distinct()
    edges_row_col = to_row_col_idx(edges, vertices=vertices)
    M = idx_to_csr_matrix(edges_row_col, vertices=vertices)
    return M


def to_period_csr_matrix(
    edges: Table, vertices: Table | None, periods: list[int] = []
) -> Generator[Tuple[csr_matrix, int]]:
    """
    Generate a sparse matrix for each period.

    Args:
        - edges: table with columns ``src``, ``dst``, and ``period``.
        - vertices: table with columns ``id`` and ``period``, or ``None`` to derive
          vertices from the edges table for each period.
        - periods: list of periods to generate matrices for. If empty, all periods
          present in ``edges`` are used.

    Returns:
        - Generator of ``(csr_matrix, period)`` tuples, one per period.
    """
    if len(periods) == 0:
        periods = edges[["period"]].distinct().to_pandas().period.tolist()
    for period in periods:
        E_y = edges.filter(edges.period == period)
        if vertices is not None:
            V_y = vertices.filter(vertices.period == period)
        else:
            V_y = None

        yield to_csr_matrix(E_y, V_y), period


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import pandas as pd

    edges = pd.DataFrame({"src": [100, 100], "dst": [300, 200]})
    vertices = pd.DataFrame({"id": [100, 200, 300]})

    E = ibis.memtable(edges)
    V = ibis.memtable(vertices)

    V1 = V.filter(V.id < 250)
    idx = to_row_col_idx(E, V1)
    M1 = idx_to_csr_matrix(idx, V1)
    print(f"M1 = {M1}")

    M = to_csr_matrix(E, V)
    print(M)
