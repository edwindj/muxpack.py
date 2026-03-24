import ibis
from muxpack import MultiplexSeries
import pytest
from collections.abc import Generator
from copy import copy


@pytest.fixture()
def simple() -> Generator[MultiplexSeries]:
    edges = ibis.memtable(
        {
            "src": [1, 2, 2, 1],
            "dst": [2, 3, 4, 2],
            "period": [2020, 2020, 2021, 2021],
            "layer": ["A", "B", "A", "B"],
            "relationtype": [1, 2, 1, 2],
        }
    )
    vertices = ibis.memtable({"id": [1, 2, 3, 4], "period": [2020, 2020, 2021, 2021]})
    m = MultiplexSeries(edges, vertices)
    yield m


def test_add_filter(simple):
    m = copy(simple)

    m.add_filter(periods=[2021])
    assert len(m.periods()) == 1
    assert m.periods() == [2021]

    m2 = copy(simple)
    m2.add_filter(layers=["A"])
    assert len(m2.layers()) == 1
    assert m2.layers() == ["A"]

    m3 = copy(simple)
    m3.add_filter(periods=[2021], layers=["B"])
    assert len(m3.periods()) == 1
    assert m3.periods() == [2021]
    assert len(m3.layers()) == 1
    assert m3.layers() == ["B"]


def test_add_filter_src_dst(simple):
    m = copy(simple)
    m.add_filter(src=[1])
    assert len(m.periods()) == 2

    E = m.edges.to_pandas()
    assert len(E) == 2
    # assert(E[["src"]] == [1,1])
