import pytest
import numpy as np
import hither2 as hi
from .functions import functions as fun

def do_test_pipeline():
    a = fun.ones.run(shape=(3, 5))
    b = fun.add.run(x=a, y=a)
    b = b.wait()
    assert np.array_equal(b, np.ones((3, 5)) * 2)

def test_pipeline(general):
    do_test_pipeline()

@pytest.mark.container
def test_pipeline_in_container(general):
    with hi.config(container=True):
        do_test_pipeline()