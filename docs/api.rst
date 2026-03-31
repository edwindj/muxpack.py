API Reference
=============

.. contents:: Modules
   :local:
   :depth: 1


MultiplexSeries
---------------

.. autoclass:: muxpack.MultiplexSeries
   :members:
   :special-members: __init__


Multiplex
---------

.. autoclass:: muxpack.Multiplex
   :members:
   :special-members: __init__


BiPartite
---------

.. autoclass:: muxpack.bipartite.BiPartite
   :members:
   :special-members: __init__


Validation
----------

.. autofunction:: muxpack.check_edges
.. autofunction:: muxpack.check_vertices
.. autofunction:: muxpack.check.check_column_type


Input / Output
--------------

.. autofunction:: muxpack.load_network
.. autofunction:: muxpack.save_network
.. autofunction:: muxpack.io.save_multiplex
.. autofunction:: muxpack.io.save_multiplexseries
.. autofunction:: muxpack.io.save_bipartite
.. autofunction:: muxpack.io.read_bipartite


Sparse matrices
---------------

.. autofunction:: muxpack.to_csr_matrix
.. autofunction:: muxpack.to_csr_matrix.to_period_csr_matrix


NetworkX
--------

.. autofunction:: muxpack.networkx.to_MultiDiGraph

