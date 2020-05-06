import pytest
import numpy as np
import hither2 as hi
from .functions import functions as fun
from .fixtures import MONGO_PORT, DATABASE_NAME, COMPUTE_RESOURCE_ID

@pytest.mark.remote
def test_remote_1(general, mongodb, kachery_server, compute_resource):
    print("Entered test")
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    jh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        a = a.wait()
        assert np.array_equal(a, np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_1b(general, mongodb, kachery_server, compute_resource):
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    jh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        hi.wait()
        # we should get an implicit 'identity' job here
        a = a.wait()
        assert np.array_equal(a, np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_2(general, mongodb, kachery_server, compute_resource):
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    jh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
        b = fun.add.run(x=a, y=a)
        b = b.wait()
        assert np.array_equal(b, 2* np.ones((4, 3)))
        assert jh._internal_counts.num_jobs == 2, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_3(general, mongodb, kachery_server, compute_resource):
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    jh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True):
        a = fun.ones.run(shape=(4, 3))
    
    b = fun.add.run(x=a, y=a)
    b = b.wait()
    assert np.array_equal(b, 2* np.ones((4, 3)))
    assert jh._internal_counts.num_jobs == 1, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'

@pytest.mark.remote
def test_remote_4(general, mongodb, kachery_server, compute_resource):
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    jh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
    with hi.Config(job_handler=jh, container=True, download_results=True):
        a = fun.ones.run(shape=(4, 3))
        b = fun.ones.run(shape=(4, 3))
        hi.wait()
    
    # two implicit jobs should be created here
    c = fun.add.run(x=a, y=b)
    c = c.wait()
    assert np.array_equal(c, 2* np.ones((4, 3)))
    assert jh._internal_counts.num_jobs == 4, f'Unexpected number of jobs: {jh._internal_counts.num_jobs}'