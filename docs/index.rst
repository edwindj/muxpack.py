.. muxpack.py documentation master file, created by
   sphinx-quickstart on Thu Mar  5 09:47:06 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

muxpack.py documentation
========================

Muxpack is a Python package for working with large multi-layer or multiplex networks, 
for multiple periods e.g years. The main classes work on an `edgelist` representation of the network, and provide methods for creating,
manipulating, and analyzing multiplex networks. It is designed to keep the data
on disk and only load the necessary parts into memory, making it suitable for large networks that do not fit in memory.


.. toctree::
   :maxdepth: 4
   :caption: Contents:
   
   usage
   api