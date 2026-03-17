import ibis
from muxpack.multiplex import Multiplex
from pathlib import Path
import os

def load_network(dir: Path) -> Multiplex:
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
    Multiplex
        The loaded multiplex network.
    """
    print("Loading data...")
    edges = ibis.read_parquet(f"{dir}/*/edges/**/*.parquet", table_name="edges")
    print(edges)
    m = Multiplex(edges=edges)
    return m

def save_network(mp: Multiplex, dir: Path, **kwargs):
    """

    Parameters:
        mp: multiplex object
        dir: root path where the network is saved
        kwargs: passed through to pyarrow.dataset_write()
    """
    if mp.vertices is None:
        mp.update_vertices()
    
    E = mp.edges
    V = mp.vertices

    years = E.distinct("year").to_pandas()["year"]
    
    for year in years:
        year_dir  = dir / f"{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        # writing vertices
        vertices_file = year_dir / "vertices.parquet"
        V_year = V.filter(V.year == year)
        V_year.to_parquet(vertices_file)

        # writing edges
        edges_dir = year_dir / "edges"
        E_year = E.filter(E.year == year)
        layers = E_year.distinct("layer").to_pandas()["layer"]
        for layer in layers:
            layer_dir = edges_dir / f"{layer}"
            # TODO further partition?
            os.makedirs(layer_dir, exist_ok=True)
            E_year_layer = E_year.filter(E_year.layer == layer)
            E_year_layer.to_parquet_dir(layer_dir, **kwargs)

if __name__ == "__main__":
    m = load_network("data/network")
    print(m.edges)