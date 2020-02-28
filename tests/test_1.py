from hither2.jobcache import JobCache
from sys import stdout
import os
import time
import random
import hither2 as hi
import pytest
import multiprocessing
import numpy as np
import shutil
import kachery as ka
from .misc_functions import make_zeros_npy, add_one_npy, readnpy, intentional_error, do_nothing, bad_container, additional_file, local_module, identity

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
    kachery_storage_dir_compute_resource = str(tmp_path / f'kachery-storage-compute-resource-{_random_string(10)}')
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
        dbpath = str(tmp_path / f'db-{_random_string(10)}')
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

    # important for clearing the http request cache of the kachery client
    ka.reset()

    thisdir = os.path.dirname(os.path.realpath(__file__))
    kachery_dir = str(tmp_path / f'kachery-{_random_string(10)}')
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
    time.sleep(2)
    
    # Not sure why the following is causing a problem....
    # # make sure it's working before we proceed
    # txt0 = 'abcdefg'
    # p = ka.store_text(txt0, to=KACHERY_CONFIG)
    # txt = ka.load_text(p, fr=KACHERY_CONFIG, from_remote_only=True)
    # assert txt == txt0

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
    # important for clearing the http request cache of the kachery client
    ka.reset()

    old_kachery_storage_dir = os.getenv('KACHERY_STORAGE_DIR', None)
    kachery_storage_dir = str(tmp_path / f'local-kachery-storage-{_random_string(10)}')
    os.mkdir(kachery_storage_dir)
    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    yield kachery_storage_dir
    # do not remove the kachery storage directory here because it might be used by other things which are not yet shut down
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

def test_1(mongodb, local_kachery_storage):
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

@pytest.mark.compute_resource
def test_2(compute_resource, mongodb, kachery, local_kachery_storage):
    with hi.ConsoleCapture(label='[test_2]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for num in range(2):
                timer = time.time()
                _run_pipeline(delay=1)
                elapsed = time.time() - timer
                print(f'Elapsed for pass {num}: {elapsed}')
                if num == 1:
                    assert elapsed < 2
            with hi.config(download_results=True):
                _run_pipeline(shape=(6, 3))
        hi.wait() # for code coverage

def test_file_lock(tmp_path, local_kachery_storage):
    # For code coverage
    with hi.ConsoleCapture(label='[test_file_lock]'):
        path = str(tmp_path)
        with hi.FileLock(path + '/testfile.txt', exclusive=False):
            pass
        with hi.FileLock(path + '/testfile.txt', exclusive=True):
            pass

def test_misc(local_kachery_storage):
    # For code coverage
    import pytest
    with hi.ConsoleCapture(label='[test_misc]'):
        f = make_zeros_npy.run(shape=(3, 4), delay=0)
        with pytest.raises(Exception):
            f.result()
        f.wait()
        f.result()

@pytest.mark.compute_resource
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

@pytest.mark.compute_resource
def test_bad_container(compute_resource, mongodb, kachery, local_kachery_storage):
    import pytest
    
    with hi.ConsoleCapture(label='[test_bad_container]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)

        bad_container.run().wait()

        with hi.config(container=True):
            x = bad_container.run()
            with pytest.raises(Exception):
                x.wait()
        
        with hi.config(job_handler=rjh, container=True):
            x = bad_container.run()
            with pytest.raises(Exception):
                x.wait()

@pytest.mark.compute_resource
def test_job_arg_error(compute_resource, mongodb, kachery, local_kachery_storage):
    import pytest
    
    with hi.ConsoleCapture(label='[test_job_arg_error]'):
        x = intentional_error.run()
        a = do_nothing.run(x=x)
        with pytest.raises(Exception):
            a.wait()

def test_wait(local_kachery_storage):
    pjh = hi.ParallelJobHandler(num_workers=4)
    with hi.config(job_handler=pjh):
        a = do_nothing.run(x=None, delay=0.2)
        hi.wait(0.1)
        hi.wait()
        assert a.result() == None

def test_extras(local_kachery_storage):
    with hi.config(container='docker://jupyter/scipy-notebook:678ada768ab1'):
        a = additional_file.run()
        assert isinstance(a.wait(), np.ndarray)

        a = local_module.run()
        assert a.wait() == True

@pytest.mark.compute_resource
@pytest.mark.focus
def test_missing_input_file(compute_resource, mongodb, kachery, local_kachery_storage):
    with hi.ConsoleCapture(label='[test_missing_input_file]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        path = ka.store_text('test-text')
        false_path = path.replace('0', '1')
        assert path != false_path

        with hi.config(container=True):
            a = do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-1')
            a.wait()
            b = do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-2')
            with pytest.raises(Exception):
                b.wait()
        
        with hi.config(job_handler=rjh, container=True):
            a = do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-remotely-1')
            a.wait()
            b = do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-remotely-2')
            with pytest.raises(Exception):
                b.wait()

@pytest.mark.compute_resource
@pytest.mark.focus
def test_identity(compute_resource, mongodb, kachery, local_kachery_storage):
    with hi.ConsoleCapture(label='[test_identity]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        path = ka.store_text('test-text-2')

        with hi.config(container=True):
            a = ([dict(file=hi.File(path))],)
            b = identity.run(x=a).wait()
            assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)
        
        with hi.config(job_handler=rjh, container=True, download_results=True):
            a = ([dict(file=hi.File(path))],)
            b = identity.run(x=a).wait()
            assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)

def _random_string(num: int):
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))