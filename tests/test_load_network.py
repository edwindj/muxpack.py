import pytest
import muxpack

def test_load_data():
    mp = muxpack.load_network("data")
    assert mp is not None

    mp.save("data2")