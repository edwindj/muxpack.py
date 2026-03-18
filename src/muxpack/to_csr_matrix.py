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
    Turn the edge list into a row col index table, based on the 
    vertices table that is given.

    Args:
        edges : needs `src` and `dst` field
        vertices : needs an `id` field, edges will be filtered on id.
 
    Returns:
        lazy edges table that only contains edges with id in vertices. 
        the row and column refer to the row number of the vertices Table.
        The result can be used by `to_csr_matrix`
    """
    v = vertices.select("id").mutate(idx = row_number())
    row = v.select(src = "id", row = "idx")
    col = v.select(dst = "id", col = "idx")

    idx_edges = (
        edges[["src", "dst"]]
        .distinct()
        .inner_join(row, "src")
        .inner_join(col, "dst")
        .mutate(data = True)
        .select("data", "row", "col")
    )
    logger.debug(f"Created row-col index table with {idx_edges.count().execute()} edges.")
    return idx_edges

def idx_to_csr_matrix(idx: Table, vertices: Table) -> csr_matrix:
    # TODO maybe to_parquet()?
    coo = idx.execute()
    logger.debug(f"COO matrix data: {coo}")

    n = vertices.count().execute()
    logger.debug(f"Number of vertices: {n}")
    M = csr_matrix((coo["data"], (coo["row"], coo["col"])), shape=(n,n))
    return M

def to_csr_matrix(edges: Table, vertices: Table | None) -> csr_matrix:
    """
    Transform an edge list into a sparse matrix (csr_matrix)

    Parameters:
        edges: `src`, `dst` fields
        vertices: Table representing nodes/vertices, needs `id` field. `edges` will
        be filtered on whether they exist in vertices 

    Returns:
        sparse matrix as `csr_matrix` object
    """
    # vertices may contain multiple years
    vertices = vertices[["id"]].distinct()
    edges_row_col = to_row_col_idx(edges, vertices=vertices)
    M = idx_to_csr_matrix(edges_row_col, vertices=vertices)
    return M

def to_year_csr_matrix(edges: Table, vertices: Table | None, years: list[int]= []) -> Generator[Tuple[csr_matrix, int]]:
    """
    Generates a sparse matrix for all years given

    Parameters:
        edges: Table with the edges/links needs `src`, `dst`, `year`
        vertices: Optional Table with vertex information, needs a `id` and `year`
    """
    if len(years) == 0:
        years = (
            edges[["year"]]
            .distinct()
            .to_pandas()
            .year
            .tolist()
        )
    for year in years:
        E_y = edges.filter(edges.year == year)
        if vertices is not None:
            V_y = vertices.filter(vertices.year == year)
        else:
            V_y = None

        yield to_csr_matrix(E_y, V_y), year


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import pandas as pd
    edges = pd.DataFrame({
        "src" : [100,100],
        "dst" : [300,200]
    })
    vertices = pd.DataFrame({
        "id": [100,200,300]
    })

    E = ibis.memtable(edges)
    V = ibis.memtable(vertices)
    
    V1 = V.filter(V.id < 250)
    idx = to_row_col_idx(E, V1)
    M1 = idx_to_csr_matrix(idx, V1)
    print(f"M1 = {M1}")

    M = to_csr_matrix(E, V)
    print(M)