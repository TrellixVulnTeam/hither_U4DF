import os
import pytest
import numpy as np
import hither as hi
from .functions import functions as fun
from .load_kachery_text import load_kachery_text

@pytest.mark.remote
def test_remote_0(general, kachery_p2p_daemon, compute_resource):
    import kachery_p2p as kp
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        random_text = 'some-random-text-0901'
        x = kp.store_text(random_text)
        with hi.Config(job_handler=jh, container=True, jhparams={'cr_partition': 'partition1'}):
            with hi.Config(required_files=[x]):
                txt = load_kachery_text.run(uri=x).wait()
            assert txt == random_text

@pytest.mark.remote
def test_remote_1(general, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        for passnum in [1, 2]: # do it twice so we can cover the job cache code on the compute resource
            with hi.Config(job_handler=jh, container=True):
                job = fun.ones.run(shape=(4, 3))
                a = job.wait()
                assert np.array_equal(a, np.ones((4, 3)))
                assert jh._internal_counts.num_jobs == 1 * passnum, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'
                job.print_console_out()

@pytest.mark.remote
def test_remote_2(general, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True):
            a = fun.ones.run(shape=(4, 3))
            b = fun.add.run(x=a, y=a)
            b = b.wait()
            assert np.array_equal(b, 2* np.ones((4, 3)))
            assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_3(general, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True):
            a = fun.ones.run(shape=(4, 3))
        
        b = fun.add.run(x=a, y=a)
        b = b.wait()
        assert np.array_equal(b, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_4(general, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True):
            a = fun.ones.run(shape=(4, 3))
            b = fun.ones.run(shape=(4, 3))
            hi.wait()
        
        # two implicit jobs should be created here
        c = fun.add.run(x=a, y=b)
        c = c.wait()
        assert np.array_equal(c, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_5(general, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(compute_resource_uri=compute_resource.compute_resource_uri) as jh:
        ok = False
        with hi.Config(job_handler=jh, container=True):
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