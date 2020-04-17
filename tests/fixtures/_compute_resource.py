import os
import pytest
import multiprocessing
import shutil
import hither2 as hi
from ._config import MONGO_PORT, DATABASE_NAME, COMPUTE_RESOURCE_ID, KACHERY_CONFIG
from ._common import _random_string

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
