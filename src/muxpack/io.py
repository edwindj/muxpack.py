"""Input and output helpers for the muxpack on-disk layout.

This module provides low-level read/write functions used by high-level classes
such as :class:`muxpack.Multiplex` and :class:`muxpack.MultiplexSeries`.
"""

import ibis

from muxpack.bipartite import Bipartite
from .multiplexseries import MultiplexSeries
from pathlib import Path
import os
import logging
from typing import Tuple
from ibis import _

logger = logging.getLogger(__name__)


def read_multiplexseries(dir: Path) -> MultiplexSeries:
    """
    Load a multiplex series from a directory containing Parquet files.

    The expected directory structure is::

        dir/
            <period>/
                edges/
                    <layer>/
                        *.parquet
                vertices.parquet

    Args:
        - dir: path to the root directory containing the Parquet files.

    Returns:
        - MultiplexSeries loaded from the directory.
    """
    logger.info("Loading data from {dir}...")
    con = ibis.duckdb.connect()

    logger.info("Loading edges...")
    edges = con.read_parquet(f"{dir}/*/edges/**/*.parquet", table_name="edges")

    logger.info("Loading vertices")
    try:
        vertices = ibis.read_parquet(f"{dir}/*/vertices.parquet", table_name="vertices")
    except Exception as e:
        logger.info(f"No vertices found: {e}")
        vertices = None

    relationtypes = None
    relationtypes_file = Path(dir) / "relationtypes.parquet"
    legacy_relationtypes_file = Path(dir) / "relationtypes.csv"
    try:
        if relationtypes_file.exists():
            relationtypes = con.read_parquet(
                str(relationtypes_file), table_name="relationtypes"
            )
        elif legacy_relationtypes_file.exists():
            relationtypes = con.read_csv(
                str(legacy_relationtypes_file), table_name="relationtypes"
            )
    except Exception as e:
        logger.info(f"No relationtypes found: {e}")
        relationtypes = None

    m = MultiplexSeries(edges=edges, vertices=vertices, relationtypes=relationtypes)
    return m


# def save_multiplexseries(
#     edges: ibis.Table,
#     vertices: ibis.Table,
#     dir: Path | str,
#     existing_data_behavior="delete_matching",
#     **kwargs,
# ) -> Tuple[ibis.Table, ibis.Table]:
#     """
#     Save edges and vertices to disk following the muxpack directory structure.
#     The directory and all sub-directories are created if they do not exist.
#     Edges and vertices are not validated for consistency.

#     Args:
#         - edges: edge table to save.
#         - vertices: vertex table to save.
#         - dir: root path where the network will be saved.
#         - existing_data_behavior: passed through to ``pyarrow.dataset.write_dataset``.
#         - **kwargs: additional keyword arguments forwarded to ``pyarrow.dataset.write_dataset``.

#     Returns:
#         - Tuple of ``(edges, vertices)`` table objects pointing to the saved files.
#     """
#     E = edges
#     V = vertices
#     dir = Path(dir)

#     logger.info(f"Saving network to {dir}...")

#     # We do a manual partitioning to have maximum control.
#     # alternative and potentially more efficient would be partitioning using
#     # duckdb, however, that would pose some problems:
#     # - Hive naming convention does not follow the muxpack specification
#     # - Hive partitioning removes columns that are partitioned.
#     periods = E[["period"]].distinct().period.to_list()

#     for period in periods:
#         period_dir = dir / f"{period}"
#         os.makedirs(period_dir, exist_ok=True)

#         # writing vertices
#         vertices_file = period_dir / "vertices.parquet"
#         V_period = V.filter(V.period == period)
#         V_period.to_parquet(vertices_file)

#         # writing edges
#         edges_dir = period_dir / "edges"
#         os.makedirs(edges_dir, exist_ok=True)
#         E_period = E.filter(E.period == period)
#         layers = E_period[["layer"]].distinct().layer.to_list()
#         logger.info(f"layers: {layers}")
#         for layer in layers:
#             layer_dir = edges_dir / f"{layer}"
#             # TODO further partition?
#             os.makedirs(layer_dir, exist_ok=True)
#             E_period_layer = E_period.filter(E_period.layer == layer).order_by(
#                 ["src", "relationtype", "dst"]
#             )
#             E_period_layer.to_parquet_dir(
#                 layer_dir, existing_data_behavior=existing_data_behavior, **kwargs
#             )
#             logger.info(f"\t\tSaved layer {layer}")
#         logger.info(f"\tFinished saving period {period}")
#     logger.info(f"Finished saving network to {dir}.")

#     con = ibis.duckdb.connect()
#     edges = con.read_parquet(f"{dir}/*/edges/**/*.parquet", table_name="edges")
#     vertices = con.read_parquet(f"{dir}/*/vertices.parquet", table_name="vertices")
#     return edges, vertices


def save_multiplex(
    edges: ibis.Table,
    vertices: ibis.Table,
    dir: Path | str,
    period: int | None,
    existing_data_behavior="delete_matching",
    **kwargs,
) -> Tuple[ibis.Table, ibis.Table]:
    """
    Save a single-period multiplex to disk following the muxpack directory structure.
    The directory and all sub-directories are created if they do not exist.
    Edges and vertices are not validated for consistency.

    Parameters
    ----------
    edges
        Edge table to save.
    vertices
        Vertex table to save.
    dir
        Root path where the multiplex will be saved.
    period
        Period for this multiplex. If ``None``, all rows in ``edges`` are written.
    existing_data_behavior
        Passed through to ``pyarrow.dataset.write_dataset``.
    kwargs
        Additional keyword arguments forwarded to
        ``pyarrow.dataset.write_dataset``.

    Returns:
        - Tuple of ``(edges, vertices)`` table objects pointing to the saved files.
    """
    E = edges
    V = vertices
    dir = Path(dir)

    logger.info(f"Saving multiplex to {dir}...")

    # We do a manual partitioning to have maximum control.
    # alternative and potentially more efficient would be partitioning using
    # duckdb, however, that would pose some problems:
    # - Hive naming convention does not follow the muxpack specification
    # - Hive partitioning removes columns that are partitioned.
    os.makedirs(dir, exist_ok=True)

    # writing vertices
    vertices_file = dir / "vertices.parquet"
    if period is not None:
        # test if period column is there, if not add it to
        V = V.filter(_.period == period)
    V.to_parquet(vertices_file)

    # writing edges
    edges_dir = dir / "edges"

    os.makedirs(edges_dir, exist_ok=True)
    E_period = E
    if period is not None:
        E_period = E.filter(_.period == period)
    layers = E_period[["layer"]].distinct().layer.to_list()
    logger.info(f"layers: {layers}")
    for layer in layers:
        layer_dir = edges_dir / f"{layer}"
        # TODO further partition?
        os.makedirs(layer_dir, exist_ok=True)
        E_period_layer = E_period.filter(_.layer == layer).order_by(
            ["src", "relationtype", "dst"]
        )
        E_period_layer.to_parquet_dir(
            layer_dir, existing_data_behavior=existing_data_behavior, **kwargs
        )
        logger.info(f"\t\tSaved layer {layer}")
    logger.info("\tFinished saving")

    con = ibis.duckdb.connect()
    edges = con.read_parquet(f"{dir}/edges/**/*.parquet", table_name="edges")
    vertices = con.read_parquet(f"{dir}/vertices.parquet", table_name="vertices")
    return edges, vertices


def save_multiplexseries(
    edges: ibis.Table,
    vertices: ibis.Table,
    dir: Path | str,
    relationtypes: ibis.Table | None = None,
    existing_data_behavior="delete_matching",
    **kwargs,
) -> Tuple[ibis.Table, ibis.Table]:
    """
    Save edges and vertices to disk following the muxpack directory structure.
    The directory and all sub-directories are created if they do not exist.
    Edges and vertices are not validated for consistency.

    Args:

        - edges: edge table to save.
        - vertices: vertex table to save.
        - relationtypes: optional relationtype metadata table to save at root level.
        - dir: root path where the network will be saved.
        - existing_data_behavior: passed through to ``pyarrow.dataset.write_dataset``.
        - **kwargs: additional keyword arguments forwarded to ``pyarrow.dataset.write_dataset``.

    Returns:
        - Tuple of ``(edges, vertices)`` table objects pointing to the saved files.
    """

    dir = Path(dir)
    os.makedirs(dir, exist_ok=True)
    periods: list[str] = (
        edges.select("period").distinct().order_by("period").period.to_list()
    )
    for period in periods:
        E = edges.filter(edges.period == period)
        V = vertices.filter(vertices.period == period)
        speriod = f"{period}"
        save_multiplex(
            edges=E,
            vertices=V,
            dir=dir / speriod,
            period=period,
            existing_data_behavior=existing_data_behavior,
            **kwargs,
        )

    if relationtypes is not None:
        relationtypes.to_parquet(dir / "relationtypes.parquet")

    mp = read_multiplexseries(dir)
    return mp.edges, mp.vertices


def save_bipartite(
    edges: ibis.Table, role_src: str, role_dst: str, relationtype: str, dir: Path | str
) -> None:
    """
    Save a bipartite graph to disk as a Parquet file plus a JSON metadata file.

    Args:
        - edges: edge table to save.
        - role_src: column name used for the source role.
        - role_dst: column name used for the destination role.
        - relationtype: column name used for the relation type.
        - dir: path to the directory where the files will be saved.
    """
    dir = Path(dir)
    os.makedirs(dir, exist_ok=True)
    edges.to_parquet(dir / "edges.parquet")
    json_content = {
        "role_src": role_src,
        "role_dst": role_dst,
        "relationtype": relationtype,
    }
    with open(dir / "metadata.json", "w") as f:
        import json

        json.dump(json_content, f)


def read_bipartite(dir: Path | str) -> Bipartite:
    """
    Load a bipartite graph from disk.

    Args:
        - dir: path to the directory containing ``edges.parquet`` and ``metadata.json``.

    Returns:
        - BiPartite object with edges and metadata loaded from disk.
    """
    dir = Path(dir)
    edges = ibis.read_parquet(dir / "edges.parquet")
    with open(dir / "metadata.json", "r") as f:
        import json

        metadata = json.load(f)
    role_src = metadata["role_src"]
    role_dst = metadata["role_dst"]
    relationtype = metadata["relationtype"]
    return Bipartite(
        edges=edges, role_src=role_src, role_dst=role_dst, relationtype=relationtype
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = read_multiplexseries("data")

    save_multiplexseries(edges=m.edges, vertices=m.vertices, dir="data2")
