"""Public package API for working with multiplex network data.

This module re-exports the main classes and helper functions so users can
import common functionality directly from :mod:`muxpack`.
"""

from importlib.metadata import PackageNotFoundError, version
import argparse

from .check import check_edges, check_vertices
from .io import read_multiplexseries, save_multiplexseries
from .multiplexseries import MultiplexSeries
from .multiplex import Multiplex
from .to_csr_matrix import to_csr_matrix
from .bipartite import Bipartite

from .muxpack_json import read_muxpackseries_json
from .muxpack_json import read_muxpack_json

try:
    __version__ = version("muxpack")
except PackageNotFoundError:
    __version__ = "0+unknown"


def main(argv: list[str] | None = None) -> int:
    """Minimal CLI entrypoint for package metadata and help output."""
    parser = argparse.ArgumentParser(
        prog="muxpack", description="Tools to handle multiplex network data."
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "--read-multiplex-json", metavar="PATH", help="Read a multiplex JSON file."
    )
    parser.add_argument(
        "--read-multiplexseries-json", metavar="PATH", help="Read a multiplex series JSON file."
    )
    a = parser.parse_args(argv)
    if a.read_multiplex_json:
        m = read_muxpack_json(a.read_multiplex_json)
        print(m.layers())
        print(m.outdegree(False).to_pandas())
        return 0

    if a.read_multiplexseries_json:
        m = read_muxpackseries_json(a.read_multiplexseries_json)
        print(m.layers())
        print(m.periods())
        return 0
    
    parser.print_help()
    return 0

__all__ = [
    "check_edges",
    "check_vertices",
    "read_multiplexseries",
    "Multiplex",
    "MultiplexSeries",
    "save_multiplexseries",
    "to_csr_matrix",
    "Bipartite",
    "main",
]
