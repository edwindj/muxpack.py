
#import pytest

def test_true():
    assert 1 == 1

def test_false():
    assert 1 == 0

import ibis
def test_ibis():
    con = ibis.duckdb.connect('test.parquet')
