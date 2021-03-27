import os
import shutil
import multiprocessing
import pytest
import hither as hi
import kachery_p2p as kp
from urllib import request
from ._common import _random_string
from ._util import _wait_for_kachery_p2p_daemon_to_start

@pytest.fixture()
def kachery_p2p_daemon():
    import kachery_p2p as kp
    print('Starting kachery_p2p_daemon')

    kachery_storage_dir = os.environ['PYTEST_LOCAL_KACHERY_STORAGE_DIR']
    kachery_p2p_config_dir = os.environ['PYTEST_LOCAL_KACHERY_P2P_CONFIG_DIR']
    api_port = int(os.environ['PYTEST_LOCAL_KACHERY_P2P_API_PORT'])

    process = multiprocessing.Process(target=run_service_kachery_p2p_daemon, kwargs=dict(
        kachery_storage_dir=kachery_storage_dir,
        kachery_p2p_config_dir=kachery_p2p_config_dir,
        api_port=api_port
    ))
    process.start()

    _wait_for_kachery_p2p_daemon_to_start(api_port=api_port)

    yield process
    print('Terminating kachery p2p daemon')

    kp.stop_daemon(api_port=api_port)

    process.terminate()
    shutil.rmtree(kachery_p2p_config_dir)

def run_service_kachery_p2p_daemon(*, kachery_storage_dir, kachery_p2p_config_dir, api_port):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    os.environ['KACHERY_P2P_CONFIG_DIR'] = kachery_p2p_config_dir
    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)

    with hi.ConsoleCapture(label='[kachery-p2p-daemon]'):
        ss = kp.ShellScript(f"""
        #!/bin/bash

        exec kachery-p2p-start-daemon --channel test1 --nobootstrap --method npx
        """, redirect_output_to_stdout=False)
        ss.start()

        _wait_for_kachery_p2p_daemon_to_start(api_port)

        import kachery_p2p as kp

