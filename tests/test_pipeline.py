import pytest
import numpy as np
import hither as hi
from .functions import functions as fun

def do_test_pipeline():
    a = fun.ones.run(shape=(3, 5))
    b = fun.add.run(x=a, y=a)
    b = b.wait()
    assert np.array_equal(b, np.ones((3, 5)) * 2)

def test_pipeline(general):
    do_test_pipeline()

def test_cancel_job(general):
    pjh = hi.ParallelJobHandler(num_workers=4)
    ok = False
    with hi.Config(job_handler=pjh):
        a = fun.do_nothing.run(delay=20)
        a.wait(0.1)
        a.cancel()
        try:
            a.wait(10)
        except:
            print('Got the expected exception')
            ok = True
    if not ok:
        raise Exception('Did not get the expected exception.')

@pytest.mark.container
def test_pipeline_in_container(general):
    with hi.Config(container=True):
        do_test_pipeline()