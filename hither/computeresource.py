import time
from typing import Union, Optional, Dict, Any

import kachery as ka
from .core import _serialize_item, _prepare_container
from ._util import _random_string, _utctime
from .database import Database, JobKeys
from ._enums import JobStatus
from ._exceptions import DeserializationException
from .file import File
from .job import Job

# TODO: Functionalize, tighten.
# TODO: Consider filtering db query/filter/update statements through an interface function.
# TODO: Inject a JobManager into this instead of relying on redirection through core._prepare_container?

class ComputeResource:
    def __init__(self, *, database: Database, compute_resource_id, kachery, job_handler, job_cache=None):
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._kachery = kachery
        self._instance_id = _random_string(15)
        self._database = database
        self._timestamp_database_poll = 0
        self._timestamp_last_action = time.time()
        self._job_handler = job_handler
        self._job_cache = job_cache
        self._jobs: Dict[str, Job] = dict()

    def clear(self):
        self._database._clear_jobs_for_compute_resource(self._compute_resource_id)

    def run(self):
        while True:
            self._iterate()
            time.sleep(0.02) # TODO: alternative to busy-wait?

    def _iterate(self):
        self._iterate_timer = time.time()

        elapsed_database_poll = time.time() - self._timestamp_database_poll
        if elapsed_database_poll > self._poll_interval():
            self._handle_pending_jobs()
        
        # Handle jobs
        job_ids = list(self._jobs.keys())
        for job_id in job_ids:
            job = self._jobs[job_id]
            if job._status == JobStatus.RUNNING:
                self._mark_job_as_running(job=job)
            elif job._status == JobStatus.FINISHED:
                self._handle_finished_job(job)
                del self._jobs[job._job_id]
            elif job._status == JobStatus.ERROR:
                self._mark_job_as_error(job_id=job_id, runtime_info=job._runtime_info, exception=job._exception)
                del self._jobs[job._job_id]
            elif job._status == JobStatus.WAITING:
                pass
            elif job._status == JobStatus.PENDING:
                pass # Local status will remain PENDING until changed by remote. This is expected.
            elif job._status == JobStatus.QUEUED:
                pass # TODO: Can this happen?
            elif job._status == JobStatus.CANCELED:
                pass # TODO: What to do here? Are server-only statuses possible?
            else:
                raise Exception(f"Job {job_id} has unidentified status in compute resource.")

            self._report_action()
            self._clear_jobs_with_inactive_handlers()
        self._job_handler.iterate()


    def _clear_jobs_with_inactive_handlers(self):
        active_handler_ids = self._database._get_active_job_handler_ids()
        for job_id in self._jobs:
            handler_id = self._jobs[job_id]._handler_id
            if handler_id not in active_handler_ids:
                print(f'Removing job because client handler is no longer active: {job_id}')
                self._job_handler.cancel_job(job_id)
                self._database._delete_job(job_id, self._compute_resource_id)
                del self._jobs[job_id]

    def _handle_pending_jobs(self):
        self._timestamp_database_poll = time.time()
        self._database._report_compute_resource_active(self._compute_resource_id, self._kachery)

        for doc in self._database._fetch_pending_jobs(_compute_resource_id=self._compute_resource_id):
            self._report_action()
            self._handle_pending_job(doc)

    def _handle_pending_job(self, doc):
        job_id, handler_id, job_serialized = JobKeys._unpack_serialized_job(doc)
        label = job_serialized[JobKeys.LABEL]
        print(f'Queuing job: {label}') # TODO: Convert to log statement
        
        if not (self._hydrate_code_for_serialized_job(job_id, job_serialized)
                and self._hydrate_container_for_serialized_job(job_id, job_serialized)):
            return
       
        job = Job._deserialize(job_serialized)
        if self._job_cache:
            self._job_cache.fetch_cached_job_results(job)
            if job._status == JobStatus.FINISHED:
                # TODO NOTE: These print statements are not correct; there's no check for a cache miss
                print(f'Found job in cache: {label}') # TODO: Convert to log statement
                self._handle_finished_job(job)
                return
            elif job._status == JobStatus.ERROR:
                print(f'Found error job in cache: {label}') # TODO: Convert to log statement; also might not be true
                self._mark_job_as_error(job_id=job_id, exception=job._exception, runtime_info=job._runtime_info)
                return
        # No finished or errored version of the Job was found in the cache. Thus, queue it.
        self._queue_job(job, handler_id)

    def _queue_job(self, job:Job, handler_id:str) -> None:
        try:
            job.download_parameter_files_if_needed(kachery=self._kachery)
        except Exception as e:
            print(f"Error downloading input files for job: {job._label}\n{e}")
            self._mark_job_as_error(job_id=job._job_id, exception=e, runtime_info=None)
            return
        self._jobs[job._job_id] = job
        self._job_handler.handle_job(job)
        job._reported_status = JobStatus.QUEUED
        job._handler_id = handler_id
        self._database._mark_job_as_queued(job._job_id, self._compute_resource_id)

    def _hydrate_code_for_serialized_job(self, job_id:str, serialized_job:Dict[str, Any]) -> bool:
        """Prepare contents of 'code' field for serialized Job.

        Arguments:
            job_id {str} -- Id of serialized Hither Job.
            serialized_job {Dict[str, Any]} -- Dictionary corresponding to a serialized Hither Job.

        Raises:
            DeserializationException: Thrown if the serialized Job contains no code object known to
            Kachery.

        Returns:
            bool -- True if processing may continue; False in the event of fatal error loading
            serialized code object.
        """
        code = serialized_job[JobKeys.CODE]
        label = serialized_job[JobKeys.LABEL]
        if code is None:
            return True
        try:
            code_obj = ka.load_object(code, fr=self._kachery)
            if code_obj is None:
                raise DeserializationException("Kachery returned no serialized code for function.")
            serialized_job[JobKeys.CODE] = code_obj
        except Exception as e:
            exc = f'Error loading code for function {label}: {code} ({str(e)})'
            print(exc)
            self._mark_job_as_error(job_id=job_id, exception=Exception(exc), runtime_info=None)
            return False
        return True

    def _hydrate_container_for_serialized_job(self, job_id:str, serialized_job:Dict[str, Any]) -> bool:
        """Prepare container for serialized job, with error checking.

        Arguments:
            job_id {str} -- Id of serialized Hither Job.
            serialized_job {Dict[str, Any]} -- Dictionary corresponding to a serialized Hither Job.

        Returns:
            bool -- True if successful or not needed; False if a fatal error occurred.
        """
        container = serialized_job[JobKeys.CONTAINER]
        label = serialized_job[JobKeys.LABEL]

        if container is None:
            exc = f'Cannot run serialized job outside of container: {label}'
            print(exc)
            self._mark_job_as_error(job_id=job_id, exception=Exception(exc), runtime_info=None)
            return False
        try:
            _prepare_container(container)
        except Exception as e:
            print(f"Error preparing container for pending job: {label}\n{e}")
            self._mark_job_as_error(job_id=job_id, exception=e, runtime_info=None)
            return False
        return True
 
    def _handle_finished_job(self, job):
        print(f'Job finished: {job._job_id}') # TODO: Change to formal log statement?
        job.kache_results_if_needed(kachery=self._kachery)
        self._mark_job_as_finished(job=job)
        if self._job_cache is not None:
            self._job_cache.cache_job_result(job)

    def _mark_job_as_running(self, *, job: Job) -> None:
        if job._reported_status == job._status:
            return  # we know it's still running, nothing to see here
        print(f"Job now running: {job._job_id}") # TODO: Formal log statement?
        self._database._mark_job_as_running(job._job_id, self._compute_resource_id)
        job._reported_status = JobStatus.RUNNING

    def _mark_job_as_finished(self, *, job: Job) -> None:
        serialized_result = _serialize_item(job._result)
        self._database._mark_job_as_finished(job._job_id, self._compute_resource_id,
            runtime_info=job._runtime_info, result=serialized_result)
        
    def _mark_job_as_error(self, *,
            job_id: str, runtime_info: Optional[dict], exception: Optional[Exception]) -> None:
        print(f"Job error: {job_id}\n{exception}") # TODO: Change to formal log statement?
        self._database._mark_job_as_error(job_id, self._compute_resource_id,
            runtime_info=runtime_info, exception=exception)
    
    def _report_action(self):
        self._timestamp_last_action = time.time()
    
    def _poll_interval(self):
        elapsed_since_last_action = time.time() - self._timestamp_last_action
        if elapsed_since_last_action < 3:
            return 0.1
        elif elapsed_since_last_action < 20:
            return 1
        elif elapsed_since_last_action < 60:
            return 3
        else:
            return 6

def _print_console_out(x):
    for a in x['lines']:
        t = _fmt_time(a['timestamp'])
        txt = a['text']
        print(f'{t}: {txt}')

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()