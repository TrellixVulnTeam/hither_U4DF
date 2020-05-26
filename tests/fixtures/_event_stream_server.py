import os
import shutil
import multiprocessing
import pytest
import hither as hi
from urllib import request
from ._config import EVENT_STREAM_SERVER_PORT
from ._common import _random_string
from ._util import _wait_for_event_stream_server_to_start

def run_service_event_stream_server(*, server_dir):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    with hi.ConsoleCapture(label='[event-stream-server]'):
        ss = hi.ShellScript(f"""
        #!/bin/bash
        set -ex

        docker kill event-stream-server-fixture > /dev/null 2>&1 || true
        docker rm event-stream-server-fixture > /dev/null 2>&1 || true
        exec docker run --name event-stream-server-fixture -v {server_dir}:/event-stream-server -p {EVENT_STREAM_SERVER_PORT}:8080 -v /etc/passwd:/etc/passwd -u `id -u`:`id -g` -i magland/eventstreamserver:latest
        """, redirect_output_to_stdout=True)
        ss.start()
        ss.wait()

@pytest.fixture()
def event_stream_server(tmp_path):
    print('Starting event stream server')

    thisdir = os.path.dirname(os.path.realpath(__file__))
    server_dir = str(tmp_path / f'eventstreamserver-{_random_string(10)}')
    os.mkdir(server_dir)
    shutil.copyfile(thisdir + '/eventstreamserver.json', server_dir + '/eventstreamserver.json')

    ss_pull = hi.ShellScript("""
    #!/bin/bash
    set -ex

    exec docker pull magland/eventstreamserver:latest
    """)
    ss_pull.start()
    ss_pull.wait()

    process = multiprocessing.Process(target=run_service_event_stream_server, kwargs=dict(server_dir=server_dir))
    process.start()
    
    _wait_for_event_stream_server_to_start()

    yield process
    print('Terminating event stream server')

    process.terminate()
    ss2 = hi.ShellScript(f"""
    #!/bin/bash

    set -ex

    docker kill event-stream-server-fixture || true
    docker rm event-stream-server-fixture
    """)
    ss2.start()
    ss2.wait()
    shutil.rmtree(server_dir)