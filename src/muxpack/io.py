import ibis
from muxpack.multiplex import Multiplex
from pathlib import Path
import os
import logging

from .multiplexseries import MultiplexSeries

logger = logging.getLogger(__name__)

def load_network(dir: Path) -> MultiplexSeries:
    """
    Load a multiplex network from a directory containing Parquet files.

    Parameters
    ----------
    dir : Path
        The directory containing the Parquet files. The expected structure is:
        dir/
            year1/
                edges/
                    layer1/
                        *.parquet
                    layer2/
                        *.parquet
            year2/
                edges/
                    layer1/
                        *.parquet
                    layer2/
    
    years and layers are also in the parquet files, but the directory structure is expected to follow this pattern.
    
    Returns
    -------
    MultiplexSeries
        The loaded multiplex network.
    """
    logger.info(f"Loading data from {dir}...")
    edges = ibis.read_parquet(f"{dir}/*/edges/**/*.parquet", table_name="edges")
    
    try:
        vertices = ibis.read_parquet(f"{dir}/*/vertices.parquet")
    except:
        vertices = None
    
    m = MultiplexSeries(edges=edges, vertices=vertices)
    return m

def save_network(edges: ibis.Table, vertices: ibis.Table, dir: Path | str, 
                 existing_data_behavior = "delete_matching", **kwargs):
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
    years = E[["year"]].distinct().to_pandas().year
    
    for year in years:
        logger.info(f"\tSaving year {year}...")
        year_dir  = dir / f"{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        # writing vertices
        vertices_file = year_dir / "vertices.parquet"
        V_year = V.filter(V.year == year)
        V_year.to_parquet(vertices_file)

        # writing edges
        edges_dir = year_dir / "edges"
        os.makedirs(edges_dir, exist_ok=True)
        E_year = E.filter(E.year == year)
        layers = E_year[["layer"]].distinct().to_pandas().layer
        logger.info(f"\t\tLayers: {layers}")
        for layer in layers:
            layer_dir = edges_dir / f"{layer}"
            # TODO further partition?
            os.makedirs(layer_dir, exist_ok=True)
            E_year_layer = (
                E_year
                .filter(E_year.layer == layer)
                .order_by(["src", "relationtype", "dst"])
            )
            E_year_layer.to_parquet_dir(layer_dir, existing_data_behavior=existing_data_behavior, **kwargs)
            logger.info(f"\t\tSaved layer {layer}")
        logger.info(f"\tFinished saving year {year}")
    logger.info(f"Finished saving network to {dir}.")    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = load_network("data")

    save_network(edges = m.edges, vertices=m.vertices, dir = "data2")