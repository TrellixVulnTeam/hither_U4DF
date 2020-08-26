import os
import pytest
import hither as hi
import kachery as ka
from ._common import _random_string

@pytest.fixture()
def general(tmp_path):
    pytest_local_kachery_storage_dir = str(tmp_path / f'local-kachery-storage-{_random_string(10)}')
    pytest_local_kachery_p2p_config_dir = pytest_local_kachery_storage_dir + '/kachery-p2p-config'
    pytest_cr_kachery_storage_dir = str(tmp_path / f'cr-kachery-storage-{_random_string(10)}')
    pytest_cr_kachery_p2p_config_dir = pytest_cr_kachery_storage_dir + '/kachery-p2p-config'
    os.mkdir(pytest_local_kachery_storage_dir)
    os.mkdir(pytest_local_kachery_p2p_config_dir)
    os.mkdir(pytest_cr_kachery_storage_dir)
    os.mkdir(pytest_cr_kachery_p2p_config_dir)
    
    os.environ['PYTEST_LOCAL_KACHERY_STORAGE_DIR'] = pytest_local_kachery_storage_dir
    os.environ['PYTEST_LOCAL_KACHERY_P2P_CONFIG_DIR'] = pytest_local_kachery_p2p_config_dir
    os.environ['PYTEST_LOCAL_KACHERY_P2P_API_PORT'] = '29101'
    os.environ['PYTEST_CR_KACHERY_STORAGE_DIR'] = pytest_cr_kachery_storage_dir
    os.environ['PYTEST_CR_KACHERY_P2P_CONFIG_DIR'] = pytest_cr_kachery_p2p_config_dir
    os.environ['PYTEST_CR_KACHERY_P2P_API_PORT'] = '29102'

    os.environ['KACHERY_STORAGE_DIR'] = pytest_local_kachery_storage_dir
    os.environ['KACHERY_P2P_CONFIG_DIR'] = pytest_local_kachery_p2p_config_dir
    os.environ['KACHERY_P2P_API_PORT'] = os.environ['PYTEST_LOCAL_KACHERY_P2P_API_PORT']

    # important to clear all the running or queued jobs
    hi.reset()
    # important for clearing the http request cache of the kachery client
    ka.reset()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    x = dict()
    yield x