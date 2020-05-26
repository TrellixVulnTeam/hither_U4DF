import pytest
import numpy as np
import hither as hi
from .functions import functions as fun
from .fixtures import MONGO_PORT, DATABASE_NAME, COMPUTE_RESOURCE_ID, EVENT_STREAM_SERVER_CONFIG

@pytest.mark.remote
@pytest.mark.current
def test_remote_1(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        a = a.wait()
        assert np.array_equal(a, np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_1b(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        hi.wait()
        # we should get an implicit 'identity' job here
        a = a.wait()
        assert np.array_equal(a, np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_2(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        b = fun.add.run(x=a, y=a)
        b = b.wait()
        assert np.array_equal(b, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_3(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
    
    b = fun.add.run(x=a, y=a)
    b = b.wait()
    assert np.array_equal(b, 2* np.ones((4, 3)))
    assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_4(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
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
def test_remote_5(general, mongodb, event_stream_server, kachery_server, compute_resource):
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    jh = hi.RemoteJobHandler(event_stream_client=event_stream_client, compute_resource_id=COMPUTE_RESOURCE_ID)
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