import ibis
from .multiplexseries import MultiplexSeries
from pathlib import Path
import os
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

def load_network(dir: Path) -> MultiplexSeries:
    """
    Load a multiplex network from a directory containing Parquet files.

    Parameters
    ----------
    dir : Path
        The directory containing the Parquet files. The expected structure is:
        dir/
            period1/
                edges/
                    layer1/
                        *.parquet
                    layer2/
                        *.parquet
            period2/
                edges/
                    layer1/
                        *.parquet
                    layer2/

    periods and layers are also in the parquet files, but the directory structure is expected to follow this pattern.

    Returns
    -------
    Multiplex
        The loaded multiplex network.
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
    Save edges and vertices to dir.
    Note that:
        - dir and sub directories will be created if not present
        - the edges and vertices will be saved in the correct structure, but
        won't be checked for consistency. You should use a Multiplex object
        for that.

    Parameters:
        edges: Table
        vertices: Table
        dir: root path where the network is saved
        kwargs: passed through to pyarrow.dataset_write()

    Return:
       (edges,vertices): Table objects pointing to the saved files.
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
    Save edges and vertices to dir as multiplex. Assumption is that `period` is unique
    Note that:
        - dir and sub directories will be created if not present
        - the edges and vertices will be saved in the correct structure, but
        won't be checked for consistency. You should use a Multiplex object
        for that.

    Parameters:
        edges: Table
        vertices: Table
        period: unique period
        dir: root path where the network is saved
        kwargs: passed through to pyarrow.dataset_write()

    Return:
       (edges,vertices): Table objects pointing to the saved files.
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

def save_multiplexseries(edges: ibis.Table, vertices: ibis.Table, dir: Path | str) -> MultiplexSeries:
    dir = Path(dir)
    periods = (edges.select("period").distinct().to_pyarrow().column("period").to_pylist())
    for period in periods:
        E = edges.filter(edges.period == period)
        V = vertices.filter(vertices.period == period)
        save_multiplex(edges=E, vertices=V, dir = dir / period)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = load_network("data")

    save_network(edges=m.edges, vertices=m.vertices, dir="data2")
