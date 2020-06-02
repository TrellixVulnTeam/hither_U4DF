import os
import pytest
import multiprocessing
import shutil
import hither as hi
import kachery as ka
from ._config import MONGO_PORT, DATABASE_NAME, COMPUTE_RESOURCE_ID, KACHERY_CONFIG, EVENT_STREAM_SERVER_CONFIG
from ._common import _random_string
from ._util import _wait_for_compute_resource_to_start, _wait_for_event_stream_server_to_start, _wait_for_kachery_server_to_start

@pytest.fixture()
def compute_resource(tmp_path):
    print('Starting compute resource')
    # db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    kachery_storage_dir_compute_resource = str(tmp_path / f'kachery-storage-compute-resource-{_random_string(10)}')
    os.mkdir(kachery_storage_dir_compute_resource)
    _wait_for_kachery_server_to_start()
    _wait_for_event_stream_server_to_start()
    process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(event_stream_client=event_stream_client, kachery_storage_dir=kachery_storage_dir_compute_resource, compute_resource_id=COMPUTE_RESOURCE_ID, kachery_config=KACHERY_CONFIG))
    process.start()
    _wait_for_compute_resource_to_start()

    yield process
    print('Terminating compute resource')
    process.terminate()
    shutil.rmtree(kachery_storage_dir_compute_resource)
    print('Terminated compute resource')


def run_service_compute_resource(*, event_stream_client, kachery_storage_dir, compute_resource_id, kachery_config):
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
    with hi.ConsoleCapture(label='[compute-resource]'):
        pjh = hi.ParallelJobHandler(num_workers=4)
        # jc = hi.JobCache(database=db)
        # CR = hi.ComputeResource(event_stream_client=event_stream_client, job_handler=pjh, compute_resource_id=compute_resource_id, kachery_config=kachery, job_cache=jc)
        jc = hi.JobCache(use_tempdir=True)
        CR = hi.ComputeResource(event_stream_client=event_stream_client, job_handler=pjh, compute_resource_id=compute_resource_id, kachery_config=kachery_config, job_cache=jc)
        CR.clear()
        CR.run()
