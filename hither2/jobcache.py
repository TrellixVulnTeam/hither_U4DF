from typing import Dict, List, Union, Any

import kachery as ka
from .database import Database
from ._util import _deserialize_item, _flatten_nested_collection
from ._enums import JobStatus
from .file import File

# TODO: provide wrapper for calls to Database
# TODO: Handle checking for locality of files (may need to pull out that function from Job.py)
class JobCache:
    def __init__(self, database: Database, cache_failing=False, rerun_failing=False, force_run=False):
        self._database = database
        self._cache_failing = cache_failing
        self._rerun_failing = rerun_failing
        self._force_run = force_run

    def check_job(self, job) -> bool:
        if self._force_run:
            return False
        hash0 = self._compute_job_hash(job)
        query = dict(
            hash=hash0
        )
        db = self._database.collection('cached_job_results')
        doc = db.find_one(query)
        if doc is None:
            return False
        if 'status' not in doc or doc['status'] not in JobStatus.complete_statuses():
            return False

        if doc['status'] == JobStatus.FINISHED:
            result0 = _deserialize_item(doc['result'])
            if not _check_file_results_exist_locally(result0):
                print(f'Found result in cache, but files do not exist locally: {job._label}')
                # TODO: Is there a way we could recover from this situation? Like... try to download it?
                return False
            job._result = result0 # TODO: Can combine this with below? See what happens if not set?
            job._exception = None
            print(f'Using cached result for job: {job._label} ({job._function_name} {job._function_version})')
        elif doc['status'] == JobStatus.ERROR:
            if self._cache_failing and (not self._rerun_failing):
                job._result = None
                job._exception = Exception(doc['exception']) # TODO: Can combine with above? What if unset?
                print(f'Using cached error for job: {job._label} ({job._function_name} {job._function_version})')
            else:
                return False
        job._status = doc['status']
        job._runtime_info = doc['runtime_info']
        return True


    def cache_job_result(self, job):
        from .core import _serialize_item
        assert isinstance(job._status, JobStatus)
        if job._status == JobStatus.ERROR:
            if not self._cache_failing:
                return
        hash0 = self._compute_job_hash(job)
        db = self._database.collection('cached_job_results')
        query = dict(
            hash=hash0
        )
        update = {
            '$set': dict(
                hash=hash0,
                status=job._status.value,
                result=_serialize_item(job._result),
                runtime_info=job._runtime_info,
                exception='{}'.format(job._exception)
            )
        }
        db.update_one(query, update, upsert=True)

    def _compute_job_hash(self, job):
        from .core import _serialize_item
        hash_object = dict(
            function_name=job._function_name,
            function_version=job._function_version,
            kwargs=_serialize_item(job._wrapped_function_arguments)
        )
        if job._no_resolve_input_files:
            hash_object['no_resolve_input_files'] = True
        return ka.get_object_hash(hash_object)

def _check_file_results_exist_locally(x):
    files = _flatten_nested_collection(x, _type=File)
    for f in files:
        local_path = ka.get_file_info(f._sha1_path, fr=None)
        if local_path is None: return False
    return True