from types import SimpleNamespace
import time
from typing import Dict, Any
import kachery as ka
from ._basejobhandler import BaseJobHandler
from .database import Database
from ._enums import JobStatus, JobKeys
from .file import File
from ._load_config import _load_preset_config_from_github
from ._util import _random_string, _deserialize_item, _flatten_nested_collection, _get_poll_interval

class RemoteJobHandler(BaseJobHandler):
    def __init__(self, *, database: Database, compute_resource_id):
        self.is_remote = True
        
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._handler_id = _random_string(15)
        self._jobs: Dict = {}
        self._kachery = None

        self._timestamp_database_poll = 0
        self._timestamp_last_action = time.time()

        self._internal_counts = SimpleNamespace(
            num_jobs=0,
            num_finished_jobs=0,
            num_errored_jobs=0
        )
        self._kachery = database._get_active_compute_resource_kachery_handle(compute_resource_id)

    @staticmethod
    def preset(name):
        db = Database.load_preset_config(name)
        config = _load_preset_config_from_github(url='https://raw.githubusercontent.com/flatironinstitute/hither/config/config/2020a.json', name=name)
        return RemoteJobHandler(database=db, compute_resource_id=config['compute_resource_id'])

    def handle_job(self, job):
        super(RemoteJobHandler, self).handle_job(job)
        self._internal_counts.num_jobs += 1
        self._report_active()

        for f in _flatten_nested_collection(job._wrapped_function_arguments, _type=File):
            self._send_file_as_needed(f)

        job_serialized = job._serialize(generate_code=True)
        # the CODE member is a big block of code text. Send it to kachery & replace with a hash ref
        job_serialized[JobKeys.CODE] = ka.store_object(job_serialized[JobKeys.CODE], to=self._kachery)
        self._database.add_pending_job(compute_resource_id=self._compute_resource_id,
                                    handler_id=self._handler_id,
                                    job_id = job._job_id,
                                    job_serialized=job_serialized)
        self._jobs[job._job_id] = job
        self._report_action()
    
    def cancel_job(self, job_id):
        print('Warning: not yet able to cancel job of remotejobhandler')
    
    def iterate(self) -> None:
        elapsed_database_poll = time.time() - self._timestamp_database_poll
        if elapsed_database_poll <= _get_poll_interval(self._timestamp_last_action):
            return

        self._timestamp_database_poll = time.time()
        self._report_active()

        # self._iterate_timer = time.time() # Never actually used
        for doc in self._database._fetch_remote_modified_jobs(
                compute_resource_id=self._compute_resource_id,
                handler_id = self._handler_id):
            self._report_action()
            job_id = doc[JobKeys.JOB_ID]
            if job_id not in self._jobs:
                continue    # Is it worth interrogating this if it happens?
            compute_resource_status = JobStatus(doc[JobKeys.COMPUTE_RESOURCE_STATUS])
            if compute_resource_status not in JobStatus.local_statuses():
                raise Exception(f'Unexpected compute resource status: {compute_resource_status}')
            print(f"Job {compute_resource_status.value}: {job_id}") # TODO: Make formal log?
            if compute_resource_status == JobStatus.FINISHED:
                self._handle_finished_job(doc)
            elif compute_resource_status == JobStatus.ERROR:
                self._handle_error_job(doc)

    def _handle_finished_job(self, serialized_job:Dict[str, Any]) -> None:
        job_id = serialized_job[JobKeys.JOB_ID]
        job = self._jobs[job_id]
        self._internal_counts.num_finished_jobs += 1
        job._runtime_info = serialized_job[JobKeys.RUNTIME_INFO]
        job._status = JobStatus.FINISHED
        job._result = _deserialize_item(serialized_job[JobKeys.RESULT])
        for f in _flatten_nested_collection(job._result, _type=File):
            setattr(f, '_remote_job_handler', self)
        del self._jobs[job_id]

    def _handle_error_job(self, serialized_job:Dict[str, Any]) -> None:
        job_id = serialized_job[JobKeys.JOB_ID]
        job = self._jobs[job_id]
        self._internal_counts.num_errored_jobs += 1
        job._runtime_info = serialized_job['runtime_info']
        job._status = JobStatus.ERROR
        job._exception = Exception(serialized_job['exception'])
        del self._jobs[job_id]

    def _load_file(self, sha1_path):
        return ka.load_file(sha1_path, fr=self._kachery)

    def _send_file_as_needed(self, x:File) -> None:
        if self._kachery is None: return # We have no file store; nothing we can do.

        remote_handler = getattr(x, '_remote_job_handler', None)
        if remote_handler is None:
            if self._compute_resource_id is None: return
            ka.store_file(x.path, to=self._kachery)
        else: 
            #  If we're the remote handler, we don't need to do anything.
            if remote_handler._compute_resource_id == self._compute_resource_id:
                return
            raise Exception('This case not yet supported (we need to transfer data from one compute resource to another)')
        
    def _report_active(self):
        self._database._report_job_handler_active(self._handler_id)
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

    def cleanup(self):
        pass