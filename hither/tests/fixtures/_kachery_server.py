import os
import shutil
import multiprocessing
import pytest
import hither as hi
from ._config import KACHERY_PORT
from ._common import _random_string
from ._util import _wait_for_kachery_server_to_start

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
def kachery_server(tmp_path):
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
    _wait_for_kachery_server_to_start()
    
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