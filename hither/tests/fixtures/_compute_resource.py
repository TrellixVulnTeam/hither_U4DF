import os
import pytest
import multiprocessing
import shutil
import hither as hi
import kachery as ka
import kachery_p2p as kp
from ._common import _random_string
from ._util import _wait_for_compute_resource_feed, _wait_for_kachery_p2p_daemon_to_start
from ._kachery_p2p_daemon import run_service_kachery_p2p_daemon
import time

@pytest.fixture()
def compute_resource(tmp_path):
    kachery_storage_dir_compute_resource = os.environ['PYTEST_CR_KACHERY_STORAGE_DIR']
    kachery_p2p_config_dir_compute_resource = os.environ['PYTEST_CR_KACHERY_P2P_CONFIG_DIR']
    api_port_compute_resource = int(os.environ['PYTEST_CR_KACHERY_P2P_API_PORT'])

    kp2p_process = multiprocessing.Process(target=run_service_kachery_p2p_daemon, kwargs=dict(
        api_port=api_port_compute_resource,
        kachery_p2p_config_dir=kachery_p2p_config_dir_compute_resource,
        kachery_storage_dir=kachery_storage_dir_compute_resource
    ))
    kp2p_process.start()
    _wait_for_kachery_p2p_daemon_to_start(api_port=api_port_compute_resource)
    
    x = dict(
        KACHERY_STORAGE_DIR=kachery_storage_dir_compute_resource,
        KACHERY_P2P_CONFIG_DIR=kachery_p2p_config_dir_compute_resource,
        KACHERY_P2P_API_PORT=str(api_port_compute_resource)
    )
    old_environ = {}
    for k in x.keys():
        old_environ[k] = os.environ.get(k, None)
        os.environ[k] = x[k]
    feed = kp.create_feed()
    compute_resource_uri = feed.get_uri()

    local_node_id = kp.get_node_id(api_port=int(os.environ['PYTEST_LOCAL_KACHERY_P2P_API_PORT']))
    process = multiprocessing.Process(target=run_service_compute_resource, kwargs=dict(
        api_port=api_port_compute_resource,
        kachery_p2p_config_dir=kachery_p2p_config_dir_compute_resource,
        kachery_storage_dir=kachery_storage_dir_compute_resource,
        compute_resource_uri=compute_resource_uri,
        nodes_with_access=[dict(node_id=local_node_id)]
    ))
    process.start()

    x = dict(
        KACHERY_STORAGE_DIR=os.environ['PYTEST_LOCAL_KACHERY_STORAGE_DIR'],
        KACHERY_P2P_CONFIG_DIR=os.environ['PYTEST_LOCAL_KACHERY_P2P_CONFIG_DIR'],
        KACHERY_P2P_API_PORT=os.environ['PYTEST_LOCAL_KACHERY_P2P_API_PORT']
    )
    for k in x.keys():
        os.environ[k] = x[k]

    time.sleep(1)
    _wait_for_compute_resource_feed(compute_resource_uri)

    for k in x.keys():
        if old_environ[k] is not None:
            os.environ[k] = old_environ[k]
        else:
            del os.environ[k]

    setattr(process, 'compute_resource_uri', compute_resource_uri)

    yield process
    print('Terminating compute resource')

    kp.stop_daemon(api_port=api_port_compute_resource)

    kp2p_process.terminate()
    process.terminate()
    print('Terminated compute resource')


def run_service_compute_resource(*, api_port, kachery_p2p_config_dir, kachery_storage_dir, compute_resource_uri, nodes_with_access):
    # The following cleanup is needed because we terminate this compute resource process
    # See: https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
    from pytest_cov.embed import cleanup_on_sigterm
    cleanup_on_sigterm()

    os.environ['RUNNING_PYTEST'] = 'TRUE'

    os.environ['KACHERY_STORAGE_DIR'] = kachery_storage_dir
    os.environ['KACHERY_P2P_CONFIG_DIR'] = kachery_p2p_config_dir
    os.environ['KACHERY_P2P_API_PORT'] = str(api_port)
    os.environ['KACHERY_P2P_API_PORT_TEST'] = str(api_port)

    import kachery_p2p as kp

    try:
        ka.set_config(use_hard_links=True)
    except:
        print('WARNING: You should update your version of kachery so that the "use_hard_links" configuration option is available.')

    with hi.ConsoleCapture(label='[compute-resource]'):
        pjh = hi.ParallelJobHandler(num_workers=4)
        pjh_p1 = hi.ParallelJobHandler(num_workers=4)
        import kachery_p2p as kp
        CR = hi.ComputeResource(
            compute_resource_uri=compute_resource_uri,
            nodes_with_access=nodes_with_access,
            job_handlers=dict(
                default=pjh,
                partition1=pjh_p1
            )
        )
        CR.run()
