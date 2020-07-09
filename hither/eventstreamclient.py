from typing import List, Union
import json
import hashlib
import urllib.request as request
import kachery_p2p as kp


class EventStreamClient:
    def __init__(self, uri: str):
        self._uri = uri

    def get_stream(self, stream_id: Union[dict, str], start_at_end: bool=False):
        if isinstance(stream_id, str):
            stream_id = dict(name=stream_id)
        return _EventStream(stream_id=stream_id, client=self, start_at_end=start_at_end)


class _EventStream:
    def __init__(self, stream_id: dict, client: EventStreamClient, start_at_end: bool=False):
        self._uri = client._uri
        self._stream_id_hash = _sha1_of_object(stream_id)
        self._subfeed = kp.load_feed(client._uri).get_subfeed(stream_id_hash)

        if start_at_end:
            self._subfeed.goto_end()

    def set_position(self, position: int):
        self._subfeed.set_position(position)

    def read_events(self, wait_sec: float=0) -> List[dict]:
        return [msg for msg in self._subfeed.get_messages(wait_msec= wait_sec*1000)]
    
    def get_num_events(self) -> int:
        return self._subfeed.get_num_messages()

    def write_event(self, event: dict):
        self._subfeed.append_message(event)

    def write_events(self, events: List[dict]):
        self._subfeed.append_messages(events)


def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret


def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)
