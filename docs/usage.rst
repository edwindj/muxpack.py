Usage
=====

Muxpack is designed to work with large multiplex networks that do not fit in
memory. The main classes work on an ``edgelist`` representation of the network
and provide methods for creating, filtering, and analyzing multiplex networks.
Data is kept on disk and only the necessary parts are loaded into memory.
It also provides a idiomatic directory structure for storing multiplex networks on disk, which is used by the
:func:`muxpack.read_multiplexseries` function to load a multiplex series from disk.
See https://codeberg.org/CBS-Networktools/muxpack.py

Basic Examples
--------------

Read a ``MultiplexSeries``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Use :func:`muxpack.read_multiplexseries` to load a multiplex series from a
directory. The directory is expected to contain a folder for each period with
``edges/**/*.parquet`` files and optional ``vertices.parquet`` files.

.. code-block:: python

	from pathlib import Path
	import muxpack as mp

	data_dir = Path("example/data")
	ms = mp.read_multiplexseries(data_dir)

	print(ms)
	print(ms.periods())
	print(ms.layers())


Add a filter
^^^^^^^^^^^^

Filters are applied lazily on ``ms.edges``. You can filter by periods, layers,
relation types, and source/destination vertices.

.. code-block:: python

	import muxpack as mp

	ms = mp.read_multiplexseries("example/data")

	# Keep period 2020 and two layers.
	# None means: keep all relation types for that layer.
	ms.add_filter(periods=[2020], layers={"household": None, "school": None})

	print(ms.periods())
	print(ms.layers())


Select a multiplex for one period
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get a single-period :class:`muxpack.Multiplex` object from a
``MultiplexSeries`` with :meth:`muxpack.MultiplexSeries.get_multiplex`.

.. code-block:: python

	import muxpack as mp

	ms = mp.read_multiplexseries("example/data")
	m2020 = ms.get_multiplex(2020)

	print(m2020.period)
	print(m2020.layers())

You can also iterate all available multiplexes:

.. code-block:: python

	for period, m in ms.multiplexes():
		 print(period, m)