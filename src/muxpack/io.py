import ibis
from muxpack.multiplex import Multiplex

def load_network(dir: str) -> Multiplex:
    """
    Load a multiplex network from a directory containing Parquet files.

    Parameters
    ----------
    dir : str
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


if __name__ == "__main__":
    m = load_network("data/network")
    print(m.edges)