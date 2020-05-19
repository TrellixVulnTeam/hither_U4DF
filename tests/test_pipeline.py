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

def do_test_cancel_job(*, container):
    pjh = hi.ParallelJobHandler(num_workers=4)
    ok = False
    with hi.Config(job_handler=pjh, container=container):
        a = fun.do_nothing.run(delay=10)
        a.wait(0.3)
        a.cancel()
        try:
            a.wait(4)
        except hi.JobCancelledException:
            print('Got the expected exception')
            ok = True
    if not ok:
        raise Exception('Did not get the expected exception.')

@pytest.mark.current
def test_cancel_job(general):
    do_test_cancel_job(container=False)

@pytest.mark.current
def test_cancel_job_in_container(general):
    do_test_cancel_job(container=True)

@pytest.mark.container
def test_pipeline_in_container(general):
    with hi.Config(container=True):
        do_test_pipeline()