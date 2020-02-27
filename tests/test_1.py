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
from .misc_functions import make_zeros_npy, add_one_npy, readnpy

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

def start_compute_resource(*, db, kachery_storage_dir):
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
    process = multiprocessing.Process(target=start_compute_resource, kwargs=dict(db=db, kachery_storage_dir=kachery_storage_dir_compute_resource))
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

    ss = hi.ShellScript(f"""
    #!/bin/bash
    set -ex

    exec docker run --name kachery-fixture -v {kachery_dir}:/storage -p {KACHERY_PORT}:8080 -v /etc/passwd:/etc/passwd -u `id -u`:`id -g` -it magland/kachery2
    """)
    ss.start()
    time.sleep(4)
    yield ss
    print('Terminating kachery server')
    ss2 = hi.ShellScript(f"""
    #!/bin/bash

    set -ex

    docker kill kachery-fixture
    docker rm kachery-fixture
    """)
    ss2.start()
    ss2.wait()
    ss.wait()
    shutil.rmtree(kachery_dir)

@pytest.fixture()
def local_kachery_storage(tmp_path):
    old_kachery_storage_dir = str(os.getenv('KACHERY_STORAGE_DIR'))
    kachery_storage_dir = str(tmp_path / 'local-kachery-storage')
    os.mkdir(kachery_storage_dir)
    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    yield kachery_storage_dir
    shutil.rmtree(kachery_storage_dir)
    os.environ['KACHERY_STORAGE_DIR'] = old_kachery_storage_dir

def _run_pipeline(*, delay=None):
    f = make_zeros_npy.run(shape=(6, 3), delay=delay)
    g = add_one_npy.run(x=f)
    a = readnpy.run(x=g).wait()
    print('===========================================================')
    print(a)
    print('===========================================================')
    assert a.shape == (6, 3)
    assert np.allclose(a, np.ones((6, 3)))

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
        print('Runtime info for test_1:', cc.runtime_info()) # for code coverage

def test_2(compute_resource, mongodb, kachery, local_kachery_storage):
    with hi.ConsoleCapture(label='[test_1]'):
        db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
        rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        with hi.config(job_handler=rjh, container=True):
            for num in range(2):
                timer = time.time()
                _run_pipeline(delay=3)
                elapsed = time.time() - timer
                print(f'Elapsed for pass {num}: {elapsed}')
                if num == 1:
                    assert elapsed < 2
            with hi.config(download_results=True):
                _run_pipeline()
        hi.wait() # for code coverage
            
def test_3(tmp_path):
    # For code coverage
    path = str(tmp_path)
    with hi.FileLock(path + '/testfile.txt', exclusive=False):
        pass
    with hi.FileLock(path + '/testfile.txt', exclusive=True):
        pass