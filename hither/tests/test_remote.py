import pytest
import numpy as np
import hither as hi
from .functions import functions as fun
from .fixtures import MONGO_PORT, DATABASE_NAME, KACHERY_P2P_DAEMON_API_PORT

@pytest.mark.remote
@pytest.mark.current
def test_remote_1(general, mongodb, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(uri=compute_resource.compute_resource_uri) as jh:
        for passnum in [1, 2]: # do it twice so we can cover the job cache code on the compute resource
            with hi.Config(job_handler=jh, container=True):
                job = fun.ones.run(shape=(4, 3))
                a = job.wait()
                assert np.array_equal(a, np.ones((4, 3)))
                assert jh._internal_counts.num_jobs == 1 * passnum, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'
                job.print_console_out()

@pytest.mark.remote
def test_remote_2(general, mongodb, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True):
            a = fun.ones.run(shape=(4, 3))
            b = fun.add.run(x=a, y=a)
            b = b.wait()
            assert np.array_equal(b, 2* np.ones((4, 3)))
            assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_3(general, mongodb, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True):
            a = fun.ones.run(shape=(4, 3))
        
        b = fun.add.run(x=a, y=a)
        b = b.wait()
        assert np.array_equal(b, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_4(general, mongodb, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(uri=compute_resource.compute_resource_uri) as jh:
        with hi.Config(job_handler=jh, container=True, download_results=True):
            a = fun.ones.run(shape=(4, 3))
            b = fun.ones.run(shape=(4, 3))
            hi.wait()
        
        # two implicit jobs should be created here
        c = fun.add.run(x=a, y=b)
        c = c.wait()
        assert np.array_equal(c, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 4, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_5(general, mongodb, kachery_p2p_daemon, compute_resource):
    with hi.RemoteJobHandler(uri=compute_resource.compute_resource_uri) as jh:
        ok = False
        with hi.Config(job_handler=jh, container=True, download_results=True):
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