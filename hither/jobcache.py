import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Union

import kachery_p2p as kp

from ._enums import CachedJobResultKeys, JobStatus
from ._filelock import FileLock
from ._util import _deserialize_item, _serialize_item
from .job import Job


class JobCache:
    def __init__(self,
        use_tempdir: Union[bool, None]=None,
        path: Union[str, None]=None,
        feed_uri: Union[str, None]=None,
        readonly: bool=False
    ):
        """Cache for storing a retrieving results of hither jobs.

        Provide one of the following arguments:
            use_tempdir, path

        Keyword Arguments:
            use_tempdir {Union[bool, None]} -- Whether to use a directory inside /tmp (or wherever tempdir is configured) (default: {None})
            path {Union[str, None]} -- Path to directory on local disk (default: {None})
            feed_uri {Union[str, None]} -- URI of kachery-p2p feed to use
        """
        self._readonly = readonly

        set_parameters = 0
        errmsg = "You must provide exactly one of: use_tempdir, path, feed_uri"
        for param in [path, use_tempdir, feed_uri]:
            if param is None: continue
            set_parameters += 1
        assert set_parameters == 1, errmsg

        if feed_uri is not None:
            self._cache_provider = FeedJobCache(feed_uri)
        else:
            if path is None:
                path = f'{tempfile.gettempdir()}/hither_job_cache'
            if not os.path.exists(path):
                # Query: do we want to create a specified path, too, if it doesn't exist?
                # probably, right?
                os.makedirs(path)
            self._cache_provider = DiskJobCache(path)
        assert self._cache_provider is not None, errmsg

    def fetch_cached_job_results(self, job: Job) -> bool:
        """Replaces completed Jobs with their result from cache, and returns whether the cache
        hit or missed.

        Arguments:
            job {Job} -- Job to look for in the job cache.

        Returns:
            bool -- True if an acceptable cached result was found. False if the Job has not run,
            is unknown, or returned an error (and we're set to rerun errored Jobs).
        """
        if job._force_run:
            return False
        job_dict = self._fetch_cached_job_result(job.compute_hash())
        if job_dict is None:
            return False

        status_str = job_dict[CachedJobResultKeys.STATUS]
        if status_str == 'finished':
            status = JobStatus.FINISHED
        elif status_str == 'error':
            status = JobStatus.ERROR
        else:
            raise Exception(f'Unexpected: cached job status {status_str} not in complete statuses.') # pragma: no cover

        job_description = f"{job._label} ({job._function_name} {job._function_version})"
        if status == JobStatus.FINISHED:
            if CachedJobResultKeys.RESULT in job_dict:
                serialized_result = job_dict[CachedJobResultKeys.RESULT]
            elif CachedJobResultKeys.RESULT_URI in job_dict:
                x = kp.load_object(job_dict[CachedJobResultKeys.RESULT_URI], p2p=False)
                if x is None:
                    print(f'Found result in cache, but result does not exist locally: {job_description}')  # TODO: Make log
                    return False
                if 'result' not in x:
                    print(f'Unexpected, result not in object obtained from result_uri: {job_description}')  # TODO: Make log
                    return False
                serialized_result = x['result']
            else:
                print('Neither result nor result_uri found in cached job')
                return False
            result = _deserialize_item(serialized_result)
            # todo: we need to be able to check whether the files actually exist in the kachery storage
            #       not 100% sure how to do that
            job._result = result
            job._exception = None
            print(f'Using cached result for job: {job_description}') # TODO: Make log
        elif status == JobStatus.ERROR:
            exception = job_dict[CachedJobResultKeys.EXCEPTION]
            if job._cache_failing and (not job._rerun_failing):
                job._result = None
                job._exception = Exception(exception)
                print(f'Using cached error for job: {job_description}') # TODO: Make log
            else:
                return False
        job._set_status(status)
        job._runtime_info = job_dict[CachedJobResultKeys.RUNTIME_INFO]
        return True

    def cache_job_result(self, job:Job):
        if self._readonly:
            return
        if job._status == JobStatus.ERROR and not job._cache_failing:
            return 
        job_hash = job.compute_hash()
        self._cache_provider._cache_job_result(job_hash, job)
    
    def readonly(self):
        return self._readonly
    
    def _fetch_cached_job_result(self, job_hash) -> Union[Dict[str, Any], None]:
        return self._cache_provider._fetch_cached_job_result(job_hash)

class DiskJobCache:
    def __init__(self, path):
        self._path = path
    
    def _cache_job_result(self, job_hash: str, job:Job):
        from .computeresource._result_small_enough_to_store_directly import \
            _result_small_enough_to_store_directly
        cached_result = {
            CachedJobResultKeys.JOB_HASH: job_hash,
            CachedJobResultKeys.RUNTIME_INFO: job.get_runtime_info()
        }
        if job.get_status() == JobStatus.FINISHED:
            serialized_result = _serialize_item(job.get_result())
            if _result_small_enough_to_store_directly(serialized_result):
                cached_result[CachedJobResultKeys.RESULT] = serialized_result
            else:
                cached_result[CachedJobResultKeys.RESULT_URI] = kp.store_json(dict(result=serialized_result))
            cached_result[CachedJobResultKeys.STATUS] = 'finished'
        elif job.get_status() == JobStatus.ERROR:
            cached_result[CachedJobResultKeys.EXCEPTION] = '{}'.format(job.get_exception())
            cached_result[CachedJobResultKeys.STATUS] = 'error'

        obj = cached_result
        p = self._get_cache_file_path(job_hash=job_hash, create_dir_if_needed=True)
        with FileLock(p + '.lock', exclusive=True):
            with open(p, 'w') as f:
                try:
                    json.dump(obj, f)
                except:
                    print(obj)
                    print('WARNING: problem dumping json when caching result.')

    def _fetch_cached_job_result(self, job_hash:str):
        p = self._get_cache_file_path(job_hash=job_hash, create_dir_if_needed=False)
        if not os.path.exists(p):
            return None
        with FileLock(p + '.lock', exclusive=False):
            with open(p, 'r') as f:
                try:
                    return json.load(f)
                except:
                    print('Warning: problem parsing json when retrieving cached result')
                    return None

    def _get_cache_file_path(self, job_hash:str, create_dir_if_needed:bool):
        dirpath = f'{self._path}/{job_hash[0]}{job_hash[1]}/{job_hash[2]}{job_hash[3]}/{job_hash[4]}{job_hash[5]}'
        if create_dir_if_needed:
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
        return f'{dirpath}/{job_hash}.json'

class FeedJobCache:
    def __init__(self, feed_uri):
        self._feed_uri = feed_uri
    
    def _cache_job_result(self, job_hash: str, job:Job):
        import kachery_p2p as kp

        # todo: this is duplicated code
        from .computeresource._result_small_enough_to_store_directly import \
            _result_small_enough_to_store_directly
        cached_result = {
            CachedJobResultKeys.JOB_HASH: job_hash,
            CachedJobResultKeys.RUNTIME_INFO: job.get_runtime_info()
        }
        if job.get_status() == JobStatus.FINISHED:
            serialized_result = _serialize_item(job.get_result())
            if _result_small_enough_to_store_directly(serialized_result):
                cached_result[CachedJobResultKeys.RESULT] = serialized_result
            else:
                cached_result[CachedJobResultKeys.RESULT_URI] = kp.store_json(dict(result=serialized_result))
            cached_result[CachedJobResultKeys.STATUS] = 'finished'
        elif job.get_status() == JobStatus.ERROR:
            cached_result[CachedJobResultKeys.EXCEPTION] = '{}'.format(job.get_exception())
            cached_result[CachedJobResultKeys.STATUS] = 'error'

        obj = cached_result
        f = kp.load_feed(self._feed_uri)
        sf = f.get_subfeed(job_hash)
        sf.append_messages([obj])

    def _fetch_cached_job_result(self, job_hash:str):
        import kachery_p2p as kp
        f = kp.load_feed(self._feed_uri)
        sf = f.get_subfeed(job_hash)
        m = sf.get_next_message(wait_msec=100)
        if m is not None:
            return m
