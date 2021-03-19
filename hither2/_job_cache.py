from typing import Union
from ._job import Job, JobResult
from ._safe_pickle import _safe_unpickle, _safe_pickle

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
            'jobHash': job_hash,
            'jobStatus': job_result.get_status(),
            'returnValue': None,
            'errorMessage': None
        }
        if job_result.get_status() == 'finished':
            rv = job_result.get_return_value()
            if rv is not None:
                cached_result['returnValue'] = kp.store_pkl(rv)
        elif job_result.get_status() == 'error':
            cached_result['errorMessage'] = str(job_result.get_error())

        obj = cached_result
        sf = self._feed.get_subfeed({'jobHash': job_hash})
        sf.append_message(obj)

    def _fetch_cached_job_result(self, job_hash:str):
        import kachery_p2p as kp
        sf = self._feed.get_subfeed({'jobHash': job_hash})
        m = sf.get_next_messages(wait_msec=100)
        if len(m) > 0:
            obj = m[-1]
            try:
                return JobResult(
                    status=obj['jobStatus'],
                    return_value=obj['returnValue'],
                    error=Exception(obj['error_message']) if obj['errorMessage'] is not None else None
                )
            except Exception as e:
                print('Warning retrieving cached result:', e)
                return None
        else:
            return None