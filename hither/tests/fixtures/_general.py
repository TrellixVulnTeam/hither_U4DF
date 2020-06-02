import os
import pytest
import hither as hi
import kachery as ka
from ._common import _random_string

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