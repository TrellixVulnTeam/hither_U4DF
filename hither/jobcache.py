from typing import Dict, List, Union, Any, Optional
import tempfile
import os
import json
import kachery_p2p as kp

from .database import Database
from ._util import _deserialize_item
from ._enums import JobStatus, JobKeys
from .job import Job
from ._filelock import FileLock

class JobCache:
    def __init__(self,
        database: Union[Database, None]=None,
        use_tempdir: Union[bool, None]=None,
        path: Union[str, None]=None
    ):
        """Cache for storing a retrieving results of hither jobs.

        Provide one of the following arguments:
            database, use_tempdir, path

        Keyword Arguments:
            database {Union[Database, None]} -- A Mongo database object (default: {None})
            use_tempdir {Union[bool, None]} -- Whether to use a directory inside /tmp (or wherever tempdir is configured) (default: {None})
            path {Union[str, None]} -- Path to directory on local disk (default: {None})
        """
        set_parameters = 0
        errmsg = "You must provide exactly one of: database, use_tempdir, path"
        for param in [database, path, use_tempdir]:
            if param is None: continue
            set_parameters += 1
        assert set_parameters == 1, errmsg

        if database is not None:
            self._cache_provider = DatabaseJobCache(database)
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
        job_dict = self._fetch_cached_job_result(job._compute_hash())
        if job_dict is None:
            return False

        status:JobStatus = JobStatus(job_dict[JobKeys.STATUS])
        if status not in JobStatus.complete_statuses():
            raise Exception('Unexpected: cached job status not in complete statuses.') # pragma: no cover

        job_description = f"{job._label} ({job._function_name} {job._function_version})"
        if status == JobStatus.FINISHED:
            if JobKeys.RESULT in job_dict:
                serialized_result = job_dict[JobKeys.RESULT]
            elif JobKeys.RESULT_URI in job_dict:
                x = kp.load_object(job_dict[JobKeys.RESULT_URI], p2p=False)
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
            if not job._result_files_are_available_locally(results=result):
                print(f'Found result in cache, but files do not exist locally: {job_description}')  # TODO: Make log
                return False
            job._result = result
            job._exception = None
            print(f'Using cached result for job: {job_description}') # TODO: Make log
        elif status == JobStatus.ERROR:
            exception = job_dict[JobKeys.EXCEPTION]
            if job._cache_failing and (not job._rerun_failing):
                job._result = None
                job._exception = Exception(exception)
                print(f'Using cached error for job: {job_description}') # TODO: Make log
            else:
                return False
        job._status = status
        job._runtime_info = job_dict[JobKeys.RUNTIME_INFO]
        return True

    def cache_job_result(self, job:Job):
        if job._status == JobStatus.ERROR and not job._cache_failing:
            return 
        job_hash = job._compute_hash()
        self._cache_provider._cache_job_result(job_hash, job)
    
    def _fetch_cached_job_result(self, job_hash) -> Union[Dict[str, Any], None]:
        return self._cache_provider._fetch_cached_job_result(job_hash)

class DiskJobCache:
    def __init__(self, path):
        self._path = path
    
    def _cache_job_result(self, job_hash: str, job:Job):
        obj = job._as_cached_result()
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

class DatabaseJobCache:
    def __init__(self, db):
        self._database = db

    def _cache_job_result(self, job_hash:str, job:Job):
        self._database._cache_job_result(job_hash, job)

    def _fetch_cached_job_result(self, job_hash:str):
        return self._database._fetch_cached_job_result(job_hash)



        