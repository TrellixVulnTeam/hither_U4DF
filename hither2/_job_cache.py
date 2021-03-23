import hashlib
import json
from typing import Any, Dict, Union
from ._job import JobResult

job_cache_version = '0.1.1'

class JobCache:
    def __init__(self, *, feed_name: Union[str, None]=None, feed_uri: Union[str, None]=None):
        import kachery_p2p as kp
        if (feed_name is not None) and (feed_uri is not None):
            raise Exception('You cannot specify both feed_name and feed_id')
        if feed_name is not None:
            feed = kp.load_feed(feed_name, create=True)
        elif feed_uri is not None:
            feed = kp.load_feed(feed_uri)
        else:
            raise Exception('You must specify a feed_name or a feed_uri')
        self._feed = feed
    def _cache_job_result(self, job_hash: str, job_result: JobResult):
        import kachery_p2p as kp
        cached_result = {
            'jobCacheVersion': job_cache_version,
            'jobHash': job_hash,
            'jobResult': job_result.to_cache_dict()
        }

        obj = cached_result
        sf = self._feed.get_subfeed({'jobHash': job_hash})
        sf.append_message(obj)

    def _fetch_cached_job_result(self, job_hash:str) -> Union[JobResult, None]:
        import kachery_p2p as kp
        sf = self._feed.get_subfeed({'jobHash': job_hash})
        messages =  sf.get_next_messages(wait_msec=0)
        if len(messages) > 0:
            obj = messages[-1] # last message
            if obj.get('jobCacheVersion', None) != job_cache_version:
                print('Warning: incorrect job cache version')
                return None
            try:
                return JobResult.from_cache_dict(
                    obj['jobResult']
                )
            except Exception as e:
                print('Warning: problem retrieving cached result:', e)
                return None
        else:
            return None

def _hash_kwargs(kwargs: Any):
    if _is_jsonable(kwargs):
        return _get_object_hash(kwargs)
    else:
        import pickle
        pickle_data = pickle.dumps(kwargs)
        return hashlib.sha1(pickle_data).hexdigest()

def _is_jsonable(x: Any) -> bool:
    import json
    try:
        json.dumps(x)
        return True
    except:
        return False

def _compute_job_hash(
    function_name: str,
    function_version: str,
    kwargs: dict
):
    kwargs_hash = _hash_kwargs(kwargs)
    hash_object: Dict[str, Any] = {
        'job_hash_version': '0.1.0',
        'function_name': function_name,
        'function_Version': function_version
    }
    if _is_jsonable(kwargs):
        hash_object['kwargs'] = kwargs
    else:
        hash_object['kwargs_hash'] = _hash_kwargs(kwargs)
    return _get_object_hash(hash_object)

def _get_object_hash(hash_object: dict):
    return _sha1_of_object(hash_object)

def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret

def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)