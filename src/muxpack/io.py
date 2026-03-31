import ibis

from muxpack.bipartite import BiPartite
from .multiplexseries import MultiplexSeries
from pathlib import Path
import os
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

def load_network(dir: Path) -> MultiplexSeries:
    """
    Load a multiplex network from a directory containing Parquet files.

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

    try:
        relationtypes = ibis.read_parquet(f"{dir}/*/relationtypes.csv")
    except Exception as e:
        logger.info(f"No relationtypes found: {e}")
        relationtypes = None

    m = MultiplexSeries(edges=edges, vertices=vertices, relationtypes=relationtypes)
    return m

def save_network(
    edges: ibis.Table,
    vertices: ibis.Table,
    dir: Path | str,
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
        - dir: root path where the network will be saved.
        - existing_data_behavior: passed through to ``pyarrow.dataset.write_dataset``.
        - **kwargs: additional keyword arguments forwarded to ``pyarrow.dataset.write_dataset``.

    Returns:
        - Tuple of ``(edges, vertices)`` table objects pointing to the saved files.
    """
    E = edges
    V = vertices
    dir = Path(dir)

    logger.info(f"Saving network to {dir}...")

    # We do a manual partitioning to have maximum control.
    # alternative and potentially more efficient would be partitioning using
    # duckdb, however, that would pose some problems:
    # - Hive naming convention does not follow the muxpack specification
    # - Hive partitioning removes columns that are partitioned.
    periods = E[["period"]].distinct().to_pandas().period

    for period in periods:
        period_dir = dir / f"{period}"
        os.makedirs(period_dir, exist_ok=True)

        # writing vertices
        vertices_file = period_dir / "vertices.parquet"
        V_period = V.filter(V.period == period)
        V_period.to_parquet(vertices_file)

        # writing edges
        edges_dir = period_dir / "edges"
        os.makedirs(edges_dir, exist_ok=True)
        E_period = E.filter(E.period == period)
        layers = E_period[["layer"]].distinct().to_pandas().layer
        logger.info(f"layers: {layers}")
        for layer in layers:
            layer_dir = edges_dir / f"{layer}"
            # TODO further partition?
            os.makedirs(layer_dir, exist_ok=True)
            E_period_layer = E_period.filter(E_period.layer == layer).order_by(
                ["src", "relationtype", "dst"]
            )
            E_period_layer.to_parquet_dir(
                layer_dir, existing_data_behavior=existing_data_behavior, **kwargs
            )
            logger.info(f"\t\tSaved layer {layer}")
        logger.info(f"\tFinished saving period {period}")
    logger.info(f"Finished saving network to {dir}.")
    
    con = ibis.duckdb.connect()
    edges = con.read_parquet(f"{dir}/*/edges/**/*.parquet", table_name="edges")
    vertices = con.read_parquet(f"{dir}/*/vertices.parquet", table_name="vertices")
    return edges,vertices

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

    Args:
        - edges: edge table to save.
        - vertices: vertex table to save.
        - period: the period for this multiplex, or ``None`` to skip period filtering.
        - dir: root path where the multiplex will be saved.
        - existing_data_behavior: passed through to ``pyarrow.dataset.write_dataset``.
        - **kwargs: additional keyword arguments forwarded to ``pyarrow.dataset.write_dataset``.

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
        V = V.filter(V.period == period)
    V.to_parquet(vertices_file)

    # writing edges
    edges_dir = dir / "edges"

    os.makedirs(edges_dir, exist_ok=True)
    E_period = E.filter(E.period == period)
    layers = E_period[["layer"]].distinct().to_pandas().layer
    logger.info(f"layers: {layers}")
    for layer in layers:
        layer_dir = edges_dir / f"{layer}"
        # TODO further partition?
        os.makedirs(layer_dir, exist_ok=True)
        E_period_layer = E_period.filter(E_period.layer == layer).order_by(
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
    return edges,vertices

def save_multiplexseries(edges: ibis.Table, vertices: ibis.Table, dir: Path | str) -> None:
    """
    Save a multiplex series to disk by writing each period as a separate sub-directory.

    Args:
        - edges: edge table with a ``period`` column.
        - vertices: vertex table with a ``period`` column.
        - dir: root path where the multiplex series will be saved.
    """
    dir = Path(dir)
    periods = (edges.select("period").distinct().to_pyarrow().column("period").to_pylist())
    for period in periods:
        E = edges.filter(edges.period == period)
        V = vertices.filter(vertices.period == period)
        save_multiplex(edges=E, vertices=V, dir = dir / period)

def save_bipartite(edges: ibis.Table, 
                   role_src: str, role_dst: str, relationtype:str, 
                   dir: Path | str) -> None:
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
        "relationtype": relationtype
    }
    with open(dir / "metadata.json", "w") as f:
        import json
        json.dump(json_content, f)

def read_bipartite(dir: Path | str) -> BiPartite:
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
    return BiPartite(edges=edges, role_src=role_src, role_dst=role_dst, relationtype=relationtype)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = load_network("data")

    save_network(edges=m.edges, vertices=m.vertices, dir="data2")
