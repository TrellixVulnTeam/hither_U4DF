import os
import shutil
import multiprocessing
import pytest
import hither as hi
from urllib import request
from ._config import KACHERY_P2P_DAEMON_API_PORT
from ._common import _random_string
from ._util import _wait_for_kachery_p2p_daemon_to_start
import kachery_p2p as kp

def run_service_kachery_p2p_daemon(*, kachery_storage_dir, kachery_p2p_config_dir, api_port):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    with hi.ConsoleCapture(label='[kachery-p2p-daemon]'):
        ss = hi.ShellScript(f"""
        #!/bin/bash

        export KACHERY_P2P_API_PORT={api_port}
        export KACHERY_STORAGE_DIR={kachery_storage_dir}
        export KACHERY_P2P_CONFIG_DIR={kachery_p2p_config_dir}
        mkdir -p $KACHERY_STORAGE_DIR

        exec kachery-p2p-start-daemon --channel test1 --method npx --verbose {api_port}
        """, redirect_output_to_stdout=True)
        ss.start()
        ss.wait()

@pytest.fixture()
def kachery_p2p_daemon(tmp_path):
    print('Starting kachery_p2p_daemon')

    daemon_dir = str(tmp_path / f'kachery-p2p-daemon-{_random_string(10)}')
    kachery_storage_dir = daemon_dir + '/kachery-storage'
    kachery_p2p_config_dir = daemon_dir + '/config'
    api_port = KACHERY_P2P_DAEMON_API_PORT

    os.mkdir(daemon_dir)
    os.mkdir(kachery_storage_dir)
    os.mkdir(kachery_p2p_config_dir)

    process = multiprocessing.Process(target=run_service_kachery_p2p_daemon, kwargs=dict(
        kachery_storage_dir=kachery_storage_dir,
        kachery_p2p_config_dir=kachery_p2p_config_dir,
        api_port=api_port
    ))
    process.start()
    
    _wait_for_kachery_p2p_daemon_to_start(api_port=api_port)

    yield process
    print('Terminating kachery p2p daemon')

    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)
    kp.stop_daemon()

    process.terminate()
    shutil.rmtree(daemon_dir)