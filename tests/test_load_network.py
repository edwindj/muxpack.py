import pytest

def test_load_data():
    from muxpack import load_network
    data = load_network("data/network")
    assert data is not None