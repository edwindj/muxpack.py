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
    parser.parse_args(argv)
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
