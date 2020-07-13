from urllib import request
import time
import json
import hither as hi
from ._config import KACHERY_P2P_DAEMON_API_PORT
import kachery_p2p as kp

def _wait_for_kachery_p2p_daemon_to_start(api_port):
    max_retries = 90
    num_retries = 0
    delay_between_retries = 0.3
    while True:
        print(f'Probing kachery-p2p daemon. Try {num_retries + 1}')
        url = f'http://localhost:{api_port}/probe'
        try:
            req = request.urlopen(url)
        except: # pragma: no cover
            req = None
        if req is not None:
            obj = json.load(req)
            assert obj['success'] == True
            return
        if num_retries >= max_retries:
            raise Exception('Problem waiting for kachery-p2p daemon to start.')
        num_retries += 1
        time.sleep(delay_between_retries)

def _wait_for_compute_resource_to_start(compute_resource_uri):
    f = kp.load_feed(compute_resource_uri).get_subfeed('job_handler_registry')
    msg = f.get_next_message(wait_msec=10000)
    assert (msg is not None) and (msg['type'] == 'COMPUTE_RESOURCE_STARTED'), 'Unexpected problem waiting for compute resource to start'