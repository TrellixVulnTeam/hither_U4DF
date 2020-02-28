from hither2.jobcache import JobCache
from sys import stdout
import os
import time
import shlex
import subprocess
import hither2 as hi
import pytest
import multiprocessing
import numpy as np
import shutil
import kachery as ka
from .misc_functions import make_zeros_npy, add_one_npy, readnpy, intentional_error

MONGO_PORT = 27027
COMPUTE_RESOURCE_ID = 'test_compute_resource_001'
DATABASE_NAME = 'test_database_001'
KACHERY_PORT = 3602

HOST_IP = os.getenv('HOST_IP')
print(F'Using HOST_IP={HOST_IP}')
KACHERY_CONFIG = dict(
    url=f'http://{HOST_IP}:{KACHERY_PORT}',
    channel="test-channel",
    password="test-password"
)

def run_service_compute_resource(*, db, kachery_storage_dir):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    with hi.ConsoleCapture(label='[compute-resource]'):
        pjh = hi.ParallelJobHandler(num_workers=4)
        jc = hi.JobCache(database=db)
        CR = hi.ComputeResource(database=db, job_handler=pjh, compute_resource_id=COMPUTE_RESOURCE_ID, kachery=KACHERY_CONFIG, job_cache=jc)
        CR.clear()
        CR.run()

@pytest.fixture()
def compute_resource(tmp_path):
    print('Starting compute resource')
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    kachery_storage_dir_compute_resource = str(tmp_path / 'kachery-storage-compute-resource')
    os.mkdir(kachery_storage_dir_compute_resource)
    process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(db=db, kachery_storage_dir=kachery_storage_dir_compute_resource))
    process.start()
    yield process
    print('Terminating compute resource')
    process.terminate()
    shutil.rmtree(kachery_storage_dir_compute_resource)
    print('Terminated compute resource')

@pytest.fixture()
def mongodb(tmp_path):
    print('Starting mongo database')
    with open(str(tmp_path / 'mongodb_out.txt'), 'w') as logf:
        dbpath = str(tmp_path / 'db')
        os.mkdir(dbpath)
        ss = hi.ShellScript(f"""
        #!/bin/bash
        set -ex

        exec mongod --dbpath {dbpath} --quiet --port {MONGO_PORT} --bind_ip localhost > /dev/null
        """)
        ss.start()
        yield ss
        print('Terminating mongo database')
        ss.stop()
        shutil.rmtree(dbpath)

def run_service_kachery_server(*, kachery_dir):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    with hi.ConsoleCapture(label='[kachery-server]'):
        ss = hi.ShellScript(f"""
        #!/bin/bash
        set -ex

        docker kill kachery-fixture > /dev/null 2>&1 || true
        docker rm kachery-fixture > /dev/null 2>&1 || true
        exec docker run --name kachery-fixture -v {kachery_dir}:/storage -p {KACHERY_PORT}:8080 -v /etc/passwd:/etc/passwd -u `id -u`:`id -g` -i magland/kachery2
        """, redirect_output_to_stdout=True)
        ss.start()
        ss.wait()

@pytest.fixture()
def kachery(tmp_path):
    print('Starting kachery server')
    thisdir = os.path.dirname(os.path.realpath(__file__))
    kachery_dir = str(tmp_path / 'kachery')
    os.mkdir(kachery_dir)
    shutil.copyfile(thisdir + '/kachery.json', kachery_dir + '/kachery.json')

    ss_pull = hi.ShellScript("""
    #!/bin/bash
    set -ex

    exec docker pull magland/kachery2
    """)
    ss_pull.start()
    ss_pull.wait()

    process = multiprocessing.Process(target=run_service_kachery_server, kwargs=dict(kachery_dir=kachery_dir))
    process.start()
    yield process
    print('Terminating kachery server')

    process.terminate()
    ss2 = hi.ShellScript(f"""
    #!/bin/bash

    set -ex

    docker kill kachery-fixture || true
    docker rm kachery-fixture
    """)
    ss2.start()
    ss2.wait()
    shutil.rmtree(kachery_dir)

@pytest.fixture()
def local_kachery_storage(tmp_path):
    old_kachery_storage_dir = os.getenv('KACHERY_STORAGE_DIR', None)
    kachery_storage_dir = str(tmp_path / 'local-kachery-storage')
    os.mkdir(kachery_storage_dir)
    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    yield kachery_storage_dir
    if old_kachery_storage_dir is not None:
        os.environ['KACHERY_STORAGE_DIR'] = old_kachery_storage_dir

def _run_pipeline(*, delay=None, shape=(6, 3)):
    f = make_zeros_npy.run(shape=shape, delay=delay)
    g = add_one_npy.run(x=f)
    A = readnpy.run(x=g)
    A.wait(0.1) # For code coverage
    a = A.wait()
    print('===========================================================')
    print(a)
    print('===========================================================')
    assert a.shape == shape
    assert np.allclose(a, np.ones(shape))

def test_1(compute_resource, mongodb, local_kachery_storage):
    _run_pipeline()
    with hi.ConsoleCapture(label='[test_1]') as cc:
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        jc = hi.JobCache(database=db)
        with hi.config(container=True, job_cache=jc):
            for num in range(2):
                timer = time.time()
                _run_pipeline()
                elapsed = time.time() - timer
                print(f'Elapsed for pass {num}: {elapsed}')
                if num == 1:
                    assert elapsed < 2
        cc.runtime_info() # for code coverage

def test_2(compute_resource, mongodb, kachery, local_kachery_storage):
    with hi.ConsoleCapture(label='[test_2]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for num in range(2):
                timer = time.time()
                _run_pipeline(delay=0.2)
                elapsed = time.time() - timer
                print(f'Elapsed for pass {num}: {elapsed}')
                if num == 1:
                    assert elapsed < 2
            with hi.config(download_results=True):
                _run_pipeline(shape=(6, 3))
        hi.wait() # for code coverage
            
def test_file_lock(tmp_path):
    # For code coverage
    with hi.ConsoleCapture(label='[test_file_lock]'):
        path = str(tmp_path)
        with hi.FileLock(path + '/testfile.txt', exclusive=False):
            pass
        with hi.FileLock(path + '/testfile.txt', exclusive=True):
            pass

def test_misc():
    # For code coverage
    import pytest
    with hi.ConsoleCapture(label='[test_misc]'):
        f = make_zeros_npy.run(shape=(3, 4), delay=0)
        with pytest.raises(Exception):
            f.result()
        f.wait()
        f.result()

def test_job_error(compute_resource, mongodb, kachery, local_kachery_storage):
    import pytest
    
    with hi.ConsoleCapture(label='[test_job_error]'):
        x = intentional_error.run()
        with pytest.raises(Exception):
            a = x.wait()
        assert str(x.exception()) == 'intentional-error'

        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for _ in range(2):
                x = intentional_error.run()
                with pytest.raises(Exception):
                    a = x.wait()
                assert str(x.exception()) == 'intentional-error'
        jc = JobCache(database=db, cache_failing=True)
        with hi.config(job_cache=jc, container=True):
            for _ in range(2):
                x = intentional_error.run()
                with pytest.raises(Exception):
                    a = x.wait()
                assert str(x.exception()) == 'intentional-error'