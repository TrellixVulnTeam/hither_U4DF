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
    with hi.Config(job_handler=pjh, container=container):
        try:
            a = fun.do_nothing.run(delay=20)
            a.wait(1)
            a.cancel()
            # NOTE: If this wait time is too short, the job may resolve before it picks up the
            # cancellation. In that event the test may fail for the wrong reasons.
            a.wait(40)
            assert False, "Call to cancelled Job succeeded but should have failed."
        except hi.JobCancelledException:
            print('Got the expected exception')

def test_cancel_job(general):
    do_test_cancel_job(container=False)

def test_cancel_job_in_container(general):
    do_test_cancel_job(container=True)

@pytest.mark.container
def test_pipeline_in_container(general):
    with hi.Config(container=True):
        do_test_pipeline()