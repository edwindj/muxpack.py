"""Public package API for working with multiplex network data.

This module re-exports the main classes and helper functions so users can
import common functionality directly from :mod:`muxpack`.
"""

from .check import check_edges, check_vertices
from .io import read_multiplexseries, save_multiplexseries
from .multiplexseries import MultiplexSeries
from .multiplex import Multiplex
from .to_csr_matrix import to_csr_matrix
from .bipartite import Bipartite

__all__ = [
    "check_edges",
    "check_vertices",
    "read_multiplexseries",
    "Multiplex",
    "MultiplexSeries",
    "save_multiplexseries",
    "to_csr_matrix",
    "Bipartite",
]
