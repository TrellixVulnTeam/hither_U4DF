import os
import pytest
import multiprocessing
import shutil
import hither as hi
import kachery as ka
from ._config import MONGO_PORT, DATABASE_NAME, KACHERY_P2P_DAEMON_API_PORT
from ._common import _random_string
from ._util import _wait_for_compute_resource_to_start, _wait_for_kachery_p2p_daemon_to_start
from ._kachery_p2p_daemon import run_service_kachery_p2p_daemon
import kachery_p2p as kp

@pytest.fixture()
def compute_resource(tmp_path):
    print('Starting compute resource')
    # db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    kachery_storage_dir_compute_resource = str(tmp_path) + f'/kachery-storage-compute-resource-{_random_string(10)}'
    kachery_p2p_config_dir_compute_resource = str(tmp_path) + f'/kachery-p2p-config-compute-resource-{_random_string(10)}'
    os.mkdir(kachery_storage_dir_compute_resource)
    os.mkdir(kachery_p2p_config_dir_compute_resource)
    api_port = 29013
    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir_compute_resource
    os.environ['KACHERY_P2P_CONFIG_DIR'] = kachery_p2p_config_dir_compute_resource
    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)
    
    kp2p_process = multiprocessing.Process(target=run_service_kachery_p2p_daemon, kwargs=dict(
        api_port=api_port,
        kachery_p2p_config_dir=kachery_p2p_config_dir_compute_resource,
        kachery_storage_dir=kachery_storage_dir_compute_resource
    ))
    kp2p_process.start()
    
    _wait_for_kachery_p2p_daemon_to_start(api_port=api_port)
    
    feed = kp.create_feed()
    compute_resource_uri = feed.get_uri()

    process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(
        api_port=api_port,
        kachery_p2p_config_dir=kachery_p2p_config_dir_compute_resource,
        kachery_storage_dir=kachery_storage_dir_compute_resource,
        compute_resource_uri=compute_resource_uri,
        compute_resource_dir=kachery_storage_dir_compute_resource
    ))
    process.start()
    _wait_for_compute_resource_to_start(compute_resource_uri)

    setattr(process, 'compute_resource_uri', compute_resource_uri)

    yield process
    print('Terminating compute resource')

    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)
    kp.stop_daemon()

    kp2p_process.terminate()
    process.terminate()
    shutil.rmtree(kachery_storage_dir_compute_resource)
    shutil.rmtree(kachery_p2p_config_dir_compute_resource)
    print('Terminated compute resource')


def run_service_compute_resource(*, api_port, kachery_p2p_config_dir, kachery_storage_dir, compute_resource_uri, compute_resource_dir):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    try:
        ka.set_config(use_hard_links=True)
    except:
        print('WARNING: You should update your version of kachery so that the "use_hard_links" configuration option is available.')

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    os.environ['KACHERY_P2P_CONFIG_DIR'] = kachery_p2p_config_dir
    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)

    with hi.ConsoleCapture(label='[compute-resource]'):
        pjh = hi.ParallelJobHandler(num_workers=4)
        jc = hi.JobCache(use_tempdir=True)
        CR = hi.ComputeResource(compute_resource_uri=compute_resource_uri, job_handler=pjh, job_cache=jc, compute_resource_dir=compute_resource_dir)
        CR.run()
