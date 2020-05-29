import os
from typing import Optional, List, Union
import json
import hashlib
import time
import urllib.request as request


class EventStreamClient:
    def __init__(self, url: str, channel: str, password: Union[dict, str]):
        self._url = url
        self._channel = channel
        self._password = password

    def _resolve_password(self):
        return _resolve_password(self._password)

    def get_stream(self, stream_id: Union[dict, str], start_at_end: bool=False):
        return _EventStream(stream_id=stream_id, client=self, start_at_end=start_at_end)


class _EventStream:
    def __init__(self, stream_id: dict, client: EventStreamClient, start_at_end: bool=False):
        self._url = client._url
        self._channel = client._channel
        self._password = client._password
        self._stream_id_hash = _sha1_of_object(stream_id)
        self._position = 0
        if start_at_end:
            self.goto_end()
    
    def goto_end(self):
        num_events = self.get_num_events()
        self.set_position(num_events)

    def set_position(self, position: int):
        self._position = position

    def read_events(self, wait_sec: float=0) -> List[dict]:
        signature = _sha1_of_object(dict(
            # keys in alphabetical order
            password=_resolve_password(self._password),
            streamId=self._stream_id_hash,
            taskName='readEvents'
        ))
        url = f'''{self._url}/readEvents/{self._stream_id_hash}/{self._position}'''
        result = _http_post_json(url, dict(channel=self._channel, signature=signature, waitMsec=wait_sec * 1000))
        assert result is not None, f'Error loading json from: {url}'
        assert result.get('success', False), 'Error reading from event stream.'
        self._position = result['newPosition']
        return result['events']
    
    def get_num_events(self) -> int:
        signature = _sha1_of_object(dict(
            # keys in alphabetical order
            password=_resolve_password(self._password),
            streamId=self._stream_id_hash,
            taskName='getNumEvents'
        ))
        url = f'''{self._url}/getNumEvents/{self._stream_id_hash}'''
        result = _http_post_json(url, dict(channel=self._channel, signature=signature))
        assert result is not None, f'Error loading json from: {url}'
        assert result.get('success', False), 'Error getting num results.'
        return result['numEvents']

    def write_event(self, event: dict):
        self.write_events([event])

    def write_events(self, events: List[dict]):
        signature = _sha1_of_object(dict(
            # keys in alphabetical order
            password=_resolve_password(self._password),
            streamId=self._stream_id_hash,
            taskName='writeEvents'
        ))
        url = f'''{self._url}/writeEvents/{self._stream_id_hash}'''
        result = _http_post_json(url, dict(channel=self._channel, signature=signature, events=events))
        assert result is not None, f'Error loading json from: {url}'
        assert result.get('success', False), 'Error writing to event stream.'


def _resolve_password(x):
    if type(x) == str:
        return x
    elif type(x) == dict:
        if 'env' in x:
            env0 = x['env']
            if env0 in os.environ:
                return os.environ[env0]
            else:
                raise Exception(
                    'You need to set the {} environment variable'.format(env0))
        else:
            raise Exception('Unexpected password config')


def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret


def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)


def _http_post_json(url: str, data: dict, verbose: Optional[bool] = None) -> dict:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_post_json::: ' + url)
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    req = requests.post(url, json=data)
    if req.status_code != 200:
        return dict(
            success=False,
            error='Error posting json: {} {}'.format(
                req.status_code, req.content.decode('utf-8'))
        )
    if verbose:
        print('Elapsed time for _http_post_json: {}'.format(time.time() - timer))
    return json.loads(req.content)
