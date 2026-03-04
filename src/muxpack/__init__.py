import ibis
from .multiplex import Multiplex

def main() -> None:
    print("Hello from muxpack-py!")


def load_network(dir: str) -> Multiplex:
    """
    Load a multiplex network from a directory containing Parquet files.

    Parameters
    ----------
    dir : str
        The directory containing the Parquet files.

    Returns
    -------
    Multiplex
        The loaded multiplex network.
    """
    print("Loading data...")
    d = ibis.read_parquet(dir)
    m = Multiplex()
    return m
