import pytest
import muxpack

def test_load_data():
    data = muxpack.load_network("data")
    assert data is not None