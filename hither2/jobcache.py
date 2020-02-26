import kachery as ka
from .database import Database
from ._util import _deserialize_item
from .file import File

class JobCache:
    def __init__(self, database: Database, cache_failing=False, rerun_failing=False, force_run=False):
        self._database = database
        self._cache_failing = cache_failing
        self._rerun_failing = rerun_failing
        self._force_run = force_run
    def check_job(self, job):
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
        if doc['status'] == 'finished':
            result0 = _deserialize_item(doc['result'])
            if not _check_files_all_exist_locally_in_item(result0):
                print(f'Found result in cache, but files do not exist locally: {job._label}')
                return False
            job._status = 'finished'
            job._result = result0
            job._runtime_info = doc['runtime_info']
            job._exception = None
            print(f'Using cached result for job: {job._label}')
            return True
        elif doc['status'] == 'error':
            if self._cache_failing and (not self._rerun_failing):
                job._status = 'error'
                job._result = None
                job._runtime_info = doc['runtime_info']
                job._exception = Exception(doc['exception'])
                print(f'Using cached error for job: {job._label}')
                return True
        return False
    def cache_job_result(self, job):
        from .core import _serialize_item
        if job._status == 'error':
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
                status=job._status,
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
            kwargs=_serialize_item(job._kwargs)
        )
        return ka.get_object_hash(hash_object)

def _check_files_all_exist_locally_in_item(x):
    if isinstance(x, File):
        a = ka.get_file_info(x._sha1_path, fr=None)
        if a is None:
            return False
    elif type(x) == dict:
        for val in x.values():
            if not _check_files_all_exist_locally_in_item(val):
                return False
    elif type(x) == list:
        for val in x:
            if not _check_files_all_exist_locally_in_item(val):
                return False
    elif type(x) == tuple:
        for val in x:
            if not _check_files_all_exist_locally_in_item(val):
                return False
    else:
        pass
    return True