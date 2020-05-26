from urllib import request
import time
import json
import hither as hi
from ._config import KACHERY_PORT, EVENT_STREAM_SERVER_PORT, COMPUTE_RESOURCE_ID, EVENT_STREAM_SERVER_CONFIG

def _wait_for_kachery_server_to_start():
    max_retries = 50
    num_retries = 0
    delay_between_retries = 0.2
    while True:
        print(f'Probing kachery server. Try {num_retries + 1}')
        url = f'http://localhost:{KACHERY_PORT}/probe'
        try:
            req = request.urlopen(url)
        except: # pragma: no cover
            req = None
        if req is not None:
            obj = json.load(req)
            assert obj['success'] == True
            return
        if num_retries >= max_retries:
            raise Exception('Problem waiting for kachery to start.')
        num_retries += 1
        time.sleep(delay_between_retries)

def _wait_for_event_stream_server_to_start():
    max_retries = 50
    num_retries = 0
    delay_between_retries = 0.2
    while True:
        print(f'Probing kachery server. Try {num_retries + 1}')
        url = f'http://localhost:{EVENT_STREAM_SERVER_PORT}/probe'
        try:
            req = request.urlopen(url)
        except: # pragma: no cover
            req = None
        if req is not None:
            obj = json.load(req)
            assert obj['success'] == True
            return
        if num_retries >= max_retries:
            raise Exception('Problem waiting for kachery to start.')
        num_retries += 1
        time.sleep(delay_between_retries)

def _wait_for_compute_resource_to_start():
    event_stream_client = hi.EventStreamClient(**EVENT_STREAM_SERVER_CONFIG)
    s = event_stream_client.get_stream(dict(
        name='hither_compute_resource',
        compute_resource_id=COMPUTE_RESOURCE_ID
    ))
    events = s.read_events(15)
    for e in events:
        if e['type'] == 'compute-resource-started':
            return
    raise Exception('Problem waiting for compute resource to start')