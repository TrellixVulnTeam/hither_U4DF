from os import wait
from typing import Union
from ._job import Job, JobResult
from ._safe_pickle import _safe_unpickle, _safe_pickle

job_cache_version = '0.1.0'

class JobCache:
    def __init__(self, *, feed_name: Union[str, None]=None, feed_uri: Union[str, None]=None):
        import kachery_p2p as kp
        if (feed_name is not None) and (feed_uri is not None):
            raise Exception('You cannot specify both feed_name and feed_id')
        if feed_name is not None:
            feed = kp.load_feed(feed_name)
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
        messages =  sf.get_next_messages(wait_msec=100)
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
                print(obj)
                print('Warning: problem retrieving cached result:', e)
                return None
        else:
            return None