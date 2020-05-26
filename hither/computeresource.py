import time
from typing import Optional, Dict, Any

import kachery as ka
from ._containermanager import ContainerManager
from ._util import _serialize_item, _random_string, _get_poll_interval
from .database import Database
from ._enums import JobStatus, JobKeys
from ._exceptions import DeserializationException
from .file import File
from .job import Job

class ComputeResourceActionTypes:
    # sent from job handler to compute resource
    ADD_JOB_HANDLER = 'ADD_JOB_HANDLER'
    ADD_JOB = 'ADD_JOB'
    CANCEL_JOB = 'CANCEL_JOB'
    REPORT_ALIVE = 'REPORT_ALIVE'

    # sent from compute resource to job handler
    JOB_FINISHED = 'JOB_FINISHED'
    JOB_ERROR = 'JOB_ERROR'
    SET_KACHERY_CONFIG = 'SET_KACHERY_CONFIG'

HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE = 'HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE'
HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER = 'HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER'

class ConnectedClient:
    def __init__(self, compute_resource, handler_id):
        # pointer to parent compute resource -- and get some useful variables
        self._compute_resource = compute_resource
        self._job_handler = compute_resource._job_handler
        self._job_cache = compute_resource._job_cache
        self._kachery = compute_resource._kachery

        # for polling
        self._timestamp_event_poll = 0
        self._timestamp_last_action = time.time()

        # handler id for the connected client
        self._handler_id = handler_id

        # list of jobs for this particular connected client
        self._jobs: Dict[str, Job] = dict()

        # stream of incoming messages/events
        self._stream_incoming = compute_resource._event_stream_client.get_stream(dict(
            name=HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE,
            compute_resource_id=compute_resource._compute_resource_id,
            handler_id=handler_id
        ))

        # stream of outgoing messages/events
        self._stream_outgoing = compute_resource._event_stream_client.get_stream(dict(
            name=HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER,
            compute_resource_id=compute_resource._compute_resource_id,
            handler_id=handler_id
        ))

        # write initial message sharing the kachery config
        self._stream_outgoing.write_event(dict(
            type=ComputeResourceActionTypes.SET_KACHERY_CONFIG,
            kachery_config=compute_resource._kachery
        ))

        # timestamp for when the client last reported being alive
        self._timestamp_client_report_alive = time.time()

    def iterate(self):
        # read and process all incoming messages (polling)
        elapsed_event_poll = time.time() - self._timestamp_event_poll
        if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
            self._timestamp_event_poll = time.time()
            actions = self._stream_incoming.read_events()
            for action in actions:
                self._process_action(action)
        
        # Handle jobs
        job_ids = list(self._jobs.keys())
        for job_id in job_ids:
            job = self._jobs[job_id]
            if job._status == JobStatus.RUNNING:
                pass
            elif job._status == JobStatus.FINISHED:
                self._handle_finished_job(job)
                del self._jobs[job._job_id]
                self._report_action
            elif job._status == JobStatus.ERROR:
                self._mark_job_as_error(job_id=job_id, runtime_info=job._runtime_info, exception=job._exception)
                del self._jobs[job._job_id]
                self._report_action
            elif job._status == JobStatus.WAITING:
                pass
            elif job._status == JobStatus.PENDING:
                pass # Local status will remain PENDING until changed by remote. This is expected.
            elif job._status == JobStatus.QUEUED:
                pass
            else:
                raise Exception(f"Job {job_id} has unexpected status in compute resource: {job._status} {type(job._status)} {JobStatus.ERROR}")

    def cancel_all_jobs(self):
        for job in self._jobs.values():
            if job.get_status() in [JobStatus.RUNNING, JobStatus.QUEUED]:
                self._job_handler.cancel_job(job._job_id)

    def _process_action(self, action):
        # process an incoming message/action
        _type = action['type']
        if _type == ComputeResourceActionTypes.CANCEL_JOB:
            # cancel a job... get the job id and forward the request to the job handler on this end
            job_id = action['job_id']
            self._job_handler.cancel_job(job_id)
            self._report_action()
        elif _type == ComputeResourceActionTypes.REPORT_ALIVE:
            # the client has reported they are alive
            self._timestamp_client_report_alive = time.time()
        elif _type == ComputeResourceActionTypes.ADD_JOB:
            # Add a job
            action['handler_id'] = self._handler_id
            self._add_job(action)
            self._report_action()
        else:
            print(f'Unexpected action type: {_type}')
    
    def _add_job(self, action):
        # Add a job that was sent from the client
        job_id, handler_id, job_serialized = JobKeys._unpack_serialized_job(action)
        label = job_serialized[JobKeys.LABEL]
        print(f'Queuing job: {label}') # TODO: Convert to log statement
        
        if not (self._hydrate_code_for_serialized_job(job_id, job_serialized)
                and self._hydrate_container_for_serialized_job(job_id, job_serialized)):
            return
       
        job = Job._deserialize(job_serialized)
        if self._job_cache and self._job_cache.fetch_cached_job_results(job):
            # Cache fetch will return false if the Job needs to be rerun, or else update the Job
            # to match the cached Job's values (including status).
            # The Job must have been in status PENDING to enter this code, so the only way for the
            # status to be FINISHED or ERROR is if a cached version was found.
            if job._status == JobStatus.FINISHED:
                print(f'Found job in cache: {label}') # TODO: Convert to log statement
                self._handle_finished_job(job)
                return
            elif job._status == JobStatus.ERROR:
                print(f'Found error job in cache: {label}') # TODO: Convert to log statement
                self._mark_job_as_error(job_id=job_id, exception=job._exception, runtime_info=job._runtime_info)
                return
        # No finished or errored version of the Job was found in the cache. Thus, queue it.
        self._queue_job(job, handler_id)
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

        try:
            if container is None:
                raise Exception("Cannot run serialized job outside of container, but none was set.")
            ContainerManager.prepare_container(container)
        except Exception as e:
            print(f"Error preparing container for pending job: {label}\n{e}") # TODO: log
            self._mark_job_as_error(job_id=job_id, exception=e, runtime_info=None)
            return False
        return True
    def _handle_finished_job(self, job):
        print(f'Job finished: {job._job_id}') # TODO: Change to formal log statement?
        job.kache_results_if_needed(kachery=self._kachery)
        self._mark_job_as_finished(job=job)
        if self._job_cache is not None:
            self._job_cache.cache_job_result(job)
    def _queue_job(self, job:Job, handler_id:str) -> None:
        try:
            job.download_parameter_files_if_needed(kachery=self._kachery)
        except Exception as e:
            print(f"Error downloading input files for job: {job._label}\n{e}")
            self._mark_job_as_error(job_id=job._job_id, exception=e, runtime_info=None)
            return
        self._jobs[job._job_id] = job
        self._job_handler.handle_job(job)
        job._handler_id = handler_id
        # self._database._mark_job_as_queued(job._job_id, self._compute_resource_id)

    def _mark_job_as_finished(self, *, job: Job) -> None:
        serialized_result = _serialize_item(job._result)
        action = {
            "type": ComputeResourceActionTypes.JOB_FINISHED,
            "job_id": job._job_id, # change this to JobJeys.JOB_ID if safe
            JobKeys.RUNTIME_INFO: job.get_runtime_info(),
            JobKeys.RESULT: serialized_result
        }
        self._stream_outgoing.write_event(action)
        # self._database._mark_job_as_finished(job._job_id, self._compute_resource_id,
        #     runtime_info=job._runtime_info, result=serialized_result)
        
    def _mark_job_as_error(self, *,
            job_id: str, runtime_info: Optional[dict], exception: Optional[Exception]) -> None:
        print(f"Job error: {job_id}\n{exception}") # TODO: Change to formal log statement?
        # fix the following to conform to above method:
        action = dict([
            ("type", ComputeResourceActionTypes.JOB_ERROR),
            ("job_id", job_id),
            (JobKeys.RUNTIME_INFO, runtime_info),
            (JobKeys.EXCEPTION, str(exception))
        ])
        self._stream_outgoing.write_event(action)
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

class ComputeResource:
    def __init__(self, *, event_stream_client, compute_resource_id, kachery, job_handler, job_cache=None):
        self._event_stream_client = event_stream_client
        self._compute_resource_id = compute_resource_id
        self._kachery = kachery
        self._instance_id = _random_string(15)
        self._timestamp_event_poll = 0
        self._timestamp_last_action = time.time()
        self._job_handler = job_handler
        self._job_cache = job_cache
        self._connected_clients = dict()

        self._stream = self._event_stream_client.get_stream(dict(
            name='hither_compute_resource',
            compute_resource_id=self._compute_resource_id
        ))
        self._stream.goto_end()

    def clear(self):
        print('Deprecated. It is no longer needed to call clear().')
        pass

    def run(self):
        self._stream.write_event(dict(
            type='compute-resource-started'
        ))
        while True:
            self._iterate()
            time.sleep(0.02) # TODO: alternative to busy-wait?

    def _process_action(self, action):
        _type = action['type']
        if _type == ComputeResourceActionTypes.ADD_JOB_HANDLER:
            handler_id = action['handler_id']
            self._connected_clients[handler_id] = ConnectedClient(self, handler_id)
            self._report_action()
        else:
            print(f'Unexpected action type: {_type}')

    def _iterate(self):
        # self._iterate_timer = time.time() # Never actually used

        elapsed_event_poll = time.time() - self._timestamp_event_poll
        if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
            self._timestamp_event_poll = time.time()
            actions = self._stream.read_events()
            for action in actions:
                self._process_action(action)
        
            clients = self._connected_clients.values()
            for client in clients:
                client.iterate()
                elapsed_since_client_alive = client._timestamp_client_report_alive - time.time()
                if elapsed_since_client_alive > 20:
                    print(f'Closing job handler: {client._handler_id}')
                    client.cancel_all_jobs()
                    del self._connected_clients[client._handler_id]
        
        self._job_handler.iterate()
    
    def _report_action(self):
        self._timestamp_last_action = time.time()
    
def _print_console_out(x):
    for a in x['lines']:
        t = _fmt_time(a['timestamp'])
        txt = a['text']
        print(f'{t}: {txt}')

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()