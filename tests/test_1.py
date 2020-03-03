from hither2.slurmjobhandler import SlurmJobHandler
from hither2.jobcache import JobCache
from sys import stdout
import os
import time
import random
import json
import hither2 as hi
import pytest
import multiprocessing
import numpy as np
import shutil
import kachery as ka
from .misc_functions import misc_functions as mf

MONGO_PORT = 27027
COMPUTE_RESOURCE_ID = 'test_compute_resource_001'
DATABASE_NAME = 'test_database_001'
KACHERY_PORT = 3602

KACHERY_CONFIG = dict(
    url=f'http://localhost:{KACHERY_PORT}',
    channel="test-channel",
    password="test-password"
)

def run_service_compute_resource(*, db, kachery_storage_dir, compute_resource_id, kachery):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    with hi.ConsoleCapture(label='[compute-resource]'):
        pjh = hi.ParallelJobHandler(num_workers=4)
        jc = hi.JobCache(database=db)
        CR = hi.ComputeResource(database=db, job_handler=pjh, compute_resource_id=compute_resource_id, kachery=kachery, job_cache=jc)
        CR.clear()
        CR.run()

@pytest.fixture()
def compute_resource(tmp_path):
    print('Starting compute resource')
    db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    kachery_storage_dir_compute_resource = str(tmp_path / f'kachery-storage-compute-resource-{_random_string(10)}')
    os.mkdir(kachery_storage_dir_compute_resource)
    process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(db=db, kachery_storage_dir=kachery_storage_dir_compute_resource, compute_resource_id=COMPUTE_RESOURCE_ID, kachery=KACHERY_CONFIG))
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

    os.environ['RUNNING_PYTEST'] = 'TRUE'

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
    old_kachery_storage_dir = os.getenv('KACHERY_STORAGE_DIR', None)
    kachery_storage_dir = str(tmp_path / f'local-kachery-storage-{_random_string(10)}')
    os.mkdir(kachery_storage_dir)
    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    yield kachery_storage_dir
    # do not remove the kachery storage directory here because it might be used by other things which are not yet shut down
    if old_kachery_storage_dir is not None:
        os.environ['KACHERY_STORAGE_DIR'] = old_kachery_storage_dir

@pytest.fixture()
def general(local_kachery_storage):
    # important to clear all the running or queued jobs
    hi.reset()
    # important for clearing the http request cache of the kachery client
    ka.reset()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    x = dict()
    yield x

def _run_pipeline(*, delay=None, shape=(6, 3)):
    f = mf.zeros.run(shape=(6, 3))
    g = mf.add.run(x=f, y=1)
    with hi.config(download_results=True):
        A = mf.identity.run(x=g)
    A.wait(0.1) # For code coverage
    a = A.wait().array()
    print('===========================================================')
    print(a)
    print('===========================================================')
    assert a.shape == shape
    assert np.allclose(a, np.ones(shape))

def _run_short_pipeline(*, delay=None, shape=(6, 3)):
    f = mf.zeros.run(shape=(6, 3))
    with hi.config(download_results=True):
        A = mf.identity.run(x=f)
    A.wait(0.1) # For code coverage
    a = A.wait().array()
    assert a.shape == shape
    assert np.allclose(a, np.zeros(shape))

def test_1(general, mongodb):
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

@pytest.mark.focus
@pytest.mark.compute_resource
def test_2(general, compute_resource, mongodb, kachery):
    with hi.ConsoleCapture(label='[test_2]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for num in range(2):
                timer = time.time()
                _run_short_pipeline(delay=1)
                elapsed = time.time() - timer
                print(f'Elapsed for pass {num}: {elapsed}')
                if num == 1:
                    assert elapsed < 2
            with hi.config(download_results=True):
                _run_pipeline(shape=(6, 3))
        hi.wait() # for code coverage

def test_file_lock(general, tmp_path):
    # For code coverage
    with hi.ConsoleCapture(label='[test_file_lock]'):
        path = str(tmp_path)
        with hi.FileLock(path + '/testfile.txt', exclusive=False):
            pass
        with hi.FileLock(path + '/testfile.txt', exclusive=True):
            pass

def test_misc(general):
    # For code coverage
    import pytest
    with hi.ConsoleCapture(label='[test_misc]'):
        f = mf.zeros.run(shape=(3, 4), delay=0)
        with pytest.raises(Exception):
            f.result()
        f.wait()
        f.result()

@pytest.mark.compute_resource
def test_job_error(general, compute_resource, mongodb, kachery):
    import pytest
    
    with hi.ConsoleCapture(label='[test_job_error]'):
        x = mf.intentional_error.run()
        with pytest.raises(Exception):
            a = x.wait()
        assert str(x.exception()) == 'intentional-error'

        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for _ in range(2):
                x = mf.intentional_error.run()
                with pytest.raises(Exception):
                    a = x.wait()
                assert str(x.exception()) == 'intentional-error'
        jc = JobCache(database=db, cache_failing=True)
        with hi.config(job_cache=jc, container=True):
            for _ in range(2):
                x = mf.intentional_error.run()
                with pytest.raises(Exception):
                    a = x.wait()
                assert str(x.exception()) == 'intentional-error'

@pytest.mark.compute_resource
def test_bad_container(general, compute_resource, mongodb, kachery):
    import pytest
    
    with hi.ConsoleCapture(label='[test_bad_container]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)

        mf.bad_container.run().wait()

        with hi.config(container=True):
            x = mf.bad_container.run()
            with pytest.raises(Exception):
                x.wait()
        
        with hi.config(job_handler=rjh, container=True):
            x = mf.bad_container.run()
            with pytest.raises(Exception):
                x.wait()

@pytest.mark.compute_resource
def test_job_arg_error(general, compute_resource, mongodb, kachery):
    import pytest
    
    with hi.ConsoleCapture(label='[test_job_arg_error]'):
        x = mf.intentional_error.run()
        a = mf.do_nothing.run(x=x)
        with pytest.raises(Exception):
            a.wait()

def test_wait(general):
    pjh = hi.ParallelJobHandler(num_workers=4)
    with hi.config(job_handler=pjh):
        a = mf.do_nothing.run(x=None, delay=0.2)
        hi.wait(0.1)
        hi.wait()
        assert a.result() == None

def test_extras(general):
    with hi.config(container='docker://jupyter/scipy-notebook:678ada768ab1'):
        a = mf.additional_file.run()
        assert a.wait().array().shape == (2, 3)

        a = mf.local_module.run()
        assert a.wait() == True

@pytest.mark.compute_resource
def test_missing_input_file(general, compute_resource, mongodb, kachery):
    with hi.ConsoleCapture(label='[test_missing_input_file]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        path = ka.store_text('test-text')
        false_path = path.replace('0', '1')
        assert path != false_path

        with hi.config(container=True):
            a = mf.do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-1')
            a.wait()
            b = mf.do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-2')
            with pytest.raises(Exception):
                b.wait()
        
        with hi.config(job_handler=rjh, container=True):
            a = mf.do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-remotely-1')
            a.wait()
            b = mf.do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-remotely-2')
            with pytest.raises(Exception):
                b.wait()

@pytest.mark.focus
@pytest.mark.compute_resource
def test_identity(general, compute_resource, mongodb, kachery):
    with hi.ConsoleCapture(label='[test_identity]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        path = ka.store_text('test-text-2')

        with hi.config(container=True):
            a = ([dict(file=hi.File(path))],)
            b = mf.identity.run(x=a).wait()
            assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)
        
        with hi.config(job_handler=rjh, container=True, download_results=True):
            a = ([dict(file=hi.File(path))],)
            b = mf.identity.run(x=a).wait()
            assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)

def test_slurm_job_handler(general, tmp_path):
    slurm_working_dir = str(tmp_path / 'slurm-job-handler')
    sjh = SlurmJobHandler(
        working_dir=slurm_working_dir,
        use_slurm=False,
        num_workers_per_batch=3,
        num_cores_per_job=2,
        time_limit_per_batch=2400,  # number of seconds or None
        max_simultaneous_batches=5,
        additional_srun_opts=[]
    )
    with hi.ConsoleCapture(label='[slurm_job_handler]'):
        with hi.config(container=True, job_handler=sjh):
            shapes = [(j, 3) for j in range(1, 8)]
            results = []
            for shape in shapes:
                f = mf.zeros(shape=shape, delay=0.1)
                g = mf.add.run(x=f, y=1)
                A = mf.identity.run(x=g)
                results.append(A)
            for i in range(len(shapes)):
                assert shapes[i] == results[i].wait().array().shape
                print(f'Checked: {shapes[i]} {results[i].wait().array().shape}')

@pytest.fixture()
def remote_compute_resource(tmp_path):
    if os.getenv('SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD', None):
        print('Starting remote compute resource')
        db = hi.Database.preset('spikeforest_readwrite')
        kachery_storage_dir_remote_compute_resource = str(tmp_path / f'kachery-storage-remote-compute-resource-{_random_string(10)}')
        os.mkdir(kachery_storage_dir_remote_compute_resource)
        kachery = 'default_readwrite'
        process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(db=db, kachery_storage_dir=kachery_storage_dir_remote_compute_resource, compute_resource_id='spikeforest1', kachery=kachery))
        process.start()
    else:
        print('Not starting remote compute resource because environment variable not set: SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD')
        process = None
        
    yield process

    if process is not None:
        print('Terminating remote compute resource')
        process.terminate()
        shutil.rmtree(kachery_storage_dir_remote_compute_resource)
        print('Terminated remote compute resource')

@pytest.mark.compute_resource
def test_combo_local_remote(general, compute_resource, mongodb, kachery):
    with hi.ConsoleCapture(label='[test_combo_local_remote]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        
        A = np.ones((5, 5))

        with hi.config(container=True, job_handler=rjh):
            with hi.config(download_results=True):
                B = mf.add.run(x=A, y=1)
        b = B.wait().array()
        assert np.allclose(A + 1, b)

# def test_spikeforest_remote_compute_resource(general, remote_compute_resource):
def test_spikeforest_remote_compute_resource(general):
    if not os.getenv('SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD', None):
        print('Skipping remote compute resource test because environment variable not set: SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD')
        return

    db = hi.Database.preset('spikeforest_readwrite')
    rjh = hi.RemoteJobHandler(database=db, compute_resource_id='spikeforest1')
    with hi.config(container=True, job_handler=rjh):
        _run_pipeline()

def test_preset_config(general):
    db = hi.Database.preset('spikeforest_readonly')
    assert db is not None

def test_bin(general, tmp_path, mongodb, kachery):
    working_dir = str(tmp_path / 'compute-resource')
    os.mkdir(working_dir)
    ss1 = hi.ShellScript(f"""
    #!/bin/bash
    set -ex

    cd {working_dir}
    hither2-compute-resource init
    """)
    ss1.start()
    ss1.wait()
    config_fname = working_dir + '/compute_resource.json'
    with open(config_fname, 'r') as f:
        config = json.load(f)
    config['compute_resource_id'] = 'test_resource_1'
    database_config = dict(
        mongo_url = f'mongodb://localhost:{MONGO_PORT}',
        database='test_database'
    )
    config['database'] = database_config
    config['kachery'] = KACHERY_CONFIG
    with open(config_fname, 'w') as f:
        json.dump(config, f)
    ss2 = hi.ShellScript(f"""
    #!/bin/bash
    set -ex

    cd {working_dir}
    hither2-compute-resource start
    """)
    ss2.start()
    ss2.wait(1)

    db = hi.Database(**database_config)
    rjh = hi.RemoteJobHandler(
        database=db,
        compute_resource_id='test_resource_1',
    )
    with hi.config(container=True, job_handler=rjh):
        _run_short_pipeline()
        hi.wait()
    ss2.kill()

def _random_string(num: int):
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))