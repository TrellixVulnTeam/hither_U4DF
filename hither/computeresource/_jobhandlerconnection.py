from typing import Dict
import time
import kachery_p2p as kp
import numbers
from ..job import Job, JobStatus, _compute_job_hash
from .._util import _serialize_item
from .._workerprocess import WorkerProcess
from .._enums import SerializedJobKeys
from ._compute_resource_enums import MessageTypes, MessageKeys, InternalJobAttributeKeys
from ._jobhandlerconnectionworker import JobHandlerConnectionWorker
from ._result_small_enough_to_store_directly import _result_small_enough_to_store_directly

# JobHandlerConnection represents a connection to a job handler
class JobHandlerConnection:
    def __init__(
        self,
        compute_resource_uri, # uri of this compute resource
        job_handler_uri, # uri of the job handler feed
        job_manager # The job manager object
    ):
        self._compute_resource_uri = compute_resource_uri
        self._job_handler_uri = job_handler_uri
        self._job_manager = job_manager

        self._last_keepalive_timestamp = time.time() - 0

        # active jobs (by jh_job_id)
        self._active_jobs: Dict[str, Job] = {}

        # subfeed for outgoing messages to the job handler
        self._compute_resource_feed = kp.load_feed(self._compute_resource_uri)
        self._outgoing_subfeed = self._compute_resource_feed.get_subfeed(job_handler_uri)

        # worker process associated with this job handler connection
        # listening for messages from the job handler
        self._worker_process = WorkerProcess(JobHandlerConnectionWorker, (
            self._job_handler_uri
        ))
        # handle messages from the worker
        self._worker_process.on_message_from_process(self._handle_message_from_worker)
    def start(self):
        # start the worker process
        self._worker_process.start()
    def stop(self):
        # stop the worker process
        self._worker_process.stop()
        # cancel the jobs
        jobs = list(self._active_jobs.values()) # do it this way because a job may get deleted during this iteration
        for job in jobs:
            if job._status not in [JobStatus.FINISHED, JobStatus.ERROR]:
                job.cancel()
    def _handle_message_from_worker(self, message):
        # message from the worker = message from job handler
        if message[MessageKeys.TYPE] == MessageTypes.ADD_JOB:
            # add a job
            jh_job_id = message[MessageKeys.JOB_ID]
            job_serialized = message[MessageKeys.JOB_SERIALIZED]
            if jh_job_id in self._active_jobs:
                print('WARNING: cannot add job. Job with this ID is already active.')
                return
            
            function_name = job_serialized[SerializedJobKeys.FUNCTION_NAME]
            function_version = job_serialized[SerializedJobKeys.FUNCTION_VERSION]
            args = job_serialized[SerializedJobKeys.WRAPPED_ARGS]
            job_hash = _compute_job_hash(
                function_name=function_name,
                function_version=function_version,
                serialized_args=args
            )

            if not job_serialized[SerializedJobKeys.FORCE_RUN]:
                # check to see if the job with this hash has been previously run and finished
                # with the result stored in the feed (filed under the job hash)
                job_subfeed = self._compute_resource_feed.get_subfeed(job_hash)
                n = job_subfeed.get_num_messages()
                if n > 0:
                    job_subfeed.set_position(n - 1)
                    msg = job_subfeed.get_next_message(wait_msec=0)
                    if msg is not None:
                        if msg[MessageKeys.TYPE] == MessageTypes.JOB_FINISHED:
                            # todo: we also need to check whether or not any associated files still exist in kachery storage
                            #       not 100% sure how to do that
                            # important to swap out the job id
                            msg[MessageKeys.JOB_ID] = jh_job_id
                            self._send_message_to_job_handler(msg)
                            return

            # add job to the job manager
            # note: the job manager will determine if job is already being processed based on the hash,
            # and if so will not create a new one (that's why we don't create the job object here)
            # IMPORTANT: the jh_job_id is not necessarily the same as the job._job_id
            job: Job = self._job_manager.add_job(job_hash=job_hash, job_serialized=job_serialized)
            setattr(job, InternalJobAttributeKeys.CR_JOB_HASH, job_hash)
            # add to the active jobs
            self._active_jobs[jh_job_id] = job
            # handle job status changes
            job.on_status_changed(lambda: self._handle_job_status_changed(jh_job_id)) # todo
            # it's possible that the job status was already running, finished, or error
            # in that case we should report the job status to the job handler right now
            self._handle_job_status_changed(jh_job_id)
        elif message[MessageKeys.TYPE] == MessageTypes.CANCEL_JOB:
            # the job handler wants to cancel a job
            jh_job_id = message[MessageKeys.JOB_ID]
            if jh_job_id not in self._active_jobs:
                print('WARNING: cannot cancel job. Job with this ID is not active.')
                return
            job = self._active_jobs[jh_job_id]
            # this will eventually generate an error (I believe)
            job.cancel()
        elif message[MessageKeys.TYPE] == MessageTypes.KEEP_ALIVE:
            self._last_keepalive_timestamp = time.time()
    def iterate(self):
        self._worker_process.iterate()
    
    def is_alive(self):
        elapsed_since_keepalive = time.time() - self._last_keepalive_timestamp
        return (elapsed_since_keepalive <= 80)

    def _handle_job_status_changed(self, jh_job_id: str):
        # The status of the job has changed
        if jh_job_id not in self._active_jobs:
            return
        job = self._active_jobs[jh_job_id]
        status = job.get_status()
        if status == JobStatus.QUEUED:
            # notify the job handler that we have queued the job
            self._send_message_to_job_handler({
                MessageKeys.TYPE: MessageTypes.JOB_QUEUED,
                MessageKeys.TIMESTAMP: time.time() - 0,
                MessageKeys.JOB_ID: jh_job_id,
                MessageKeys.LABEL: job._label
            })
        elif status == JobStatus.RUNNING:
            # notify the job handler that the job has started
            msg = {
                MessageKeys.TYPE: MessageTypes.JOB_STARTED,
                MessageKeys.TIMESTAMP: time.time() - 0,
                MessageKeys.JOB_ID: jh_job_id,
                MessageKeys.LABEL: job._label
            }
            self._send_message_to_job_handler(msg)
        elif status == JobStatus.FINISHED:
            # notify the job handler that the job has finished
            msg = {
                MessageKeys.TYPE: MessageTypes.JOB_FINISHED,
                MessageKeys.TIMESTAMP: time.time() - 0,
                MessageKeys.JOB_ID: jh_job_id, # important to use jh_job_id here
                MessageKeys.LABEL: job._label,
                MessageKeys.RUNTIME_INFO: job.get_runtime_info()
            }
            # serialize the result
            serialized_result = _serialize_item(job._result)
            # decide whether to store the result or a result_uri
            if _result_small_enough_to_store_directly(serialized_result):
                msg[MessageKeys.RESULT] = serialized_result
            else:
                msg[MessageKeys.RESULT_URI] = kp.store_json(dict(result=serialized_result))
            self._send_message_to_job_handler(msg)
            job_subfeed = self._compute_resource_feed.get_subfeed(getattr(job, InternalJobAttributeKeys.CR_JOB_HASH))
            job_subfeed.append_message(msg)
            del self._active_jobs[jh_job_id]
        elif status == JobStatus.ERROR:
            # notify the job handler that the job has an error
            msg = {
                MessageKeys.TYPE: MessageTypes.JOB_ERROR,
                MessageKeys.TIMESTAMP: time.time() - 0,
                MessageKeys.JOB_ID: jh_job_id,
                MessageKeys.LABEL: job._label,
                MessageKeys.RUNTIME_INFO: job.get_runtime_info(),
                MessageKeys.EXCEPTION: str(job._exception)
            }
            self._send_message_to_job_handler(msg)
            del self._active_jobs[jh_job_id]
        else:
            print(f'WARNING: unexpected job status: {status}')
    def _send_message_to_job_handler(self, message):
        # append the job to the outgoing subfeed
        self._outgoing_subfeed.append_message(message)