from typing import List, Dict, Set
from enum import Enum
import time
import kachery_p2p as kp
import numbers
import kachery as ka
from ..job import Job, JobStatus, _compute_job_hash
from .._util import _serialize_item
from .._workerprocess import WorkerProcess
from .._enums import SerializedJobKeys
from .._preventkeyboardinterrupt import PreventKeyboardInterrupt
from ._computeresourcejobmanager import ComputeResourceJobManager

# types of messages between job handler and compute resource
# these are communicated via feeds
class MessageTypes:
    # job handler registry
    COMPUTE_RESOURCE_STARTED = 'COMPUTE_RESOURCE_STARTED'
    ADD_JOB_HANDLER = 'ADD_JOB_HANDLER'
    REMOVE_JOB_HANDLER = 'REMOVE_JOB_HANDLER'

    # incoming messages from job handler
    ADD_JOB = 'ADD_JOB'
    CANCEL_JOB = 'CANCEL_JOB'
    KEEP_ALIVE = 'KEEP_ALIVE'

    # outgoing messages to job handler
    JOB_QUEUED = 'JOB_QUEUED'
    JOB_STARTED = 'JOB_STARTED'
    JOB_FINISHED = 'JOB_FINISHED'
    JOB_ERROR = 'JOB_ERROR'
    # KEEP_ALIVE - as above

# keys (or field names) of messages between job handler and compute resource
class MessageKeys:
    TYPE = 'type'
    TIMESTAMP = 'timestamp'
    JOB_ID = 'job_id'
    JOB_SERIALIZED = 'job_serialized'
    LABEL = 'label'
    RUNTIME_INFO = 'runtime_info'
    EXCEPTION = 'exception'
    JOB_HANDLER_URI = 'job_handler_uri'
    RESULT = 'result'
    RESULT_URI = 'result_uri'

class InternalJobAttributeKeys:
    CR_JOB_HASH = '_cr_job_hash'

# A couple of names of subfeeds used for the communication
class SubfeedNames:
    JOB_HANDLER_REGISTRY = 'job_handler_registry'
    MAIN = 'main'

class ComputeResource:
    def __init__(
        self, *,
        compute_resource_uri: str, # feed uri for this compute resource
        node_ids_with_access: List[str], # ids of nodes that have privileges of writing to this compute resource
        job_handler
    ):
        self._compute_resource_uri = compute_resource_uri
        self._node_ids_with_access = node_ids_with_access
        self._active_job_handlers: Dict[str, JobHandlerConnection] = {} # by feed uri
        self._pending_job_handler_uris: Set[str] = set()
        # the manager for all the jobs
        self._job_manager = ComputeResourceJobManager(job_handler=job_handler)

        # the worker process - listening for incoming messages on the job handler registry feed
        self._worker_process = WorkerProcess(ComputeResourceWorker, (
            self._compute_resource_uri,
            self._node_ids_with_access
        ))
        # handle messages from the worker
        self._worker_process.on_message_from_process(self._handle_message_from_worker)
    def run(self):
        # start the worker process
        self._worker_process.start()
        try:
            while True:
                self.iterate()
                time.sleep(0.05)
        except:
            with PreventKeyboardInterrupt():
                self.cleanup()
            raise
    def cleanup(self):
        # stop the worker process
        self._worker_process.stop()
        # stop the active job handlers
        for ajh in self._active_job_handlers.values():
            ajh.stop()
    def _handle_message_from_worker(self, message):
        # handle a message from the worker
        # these are messages that come from the job handler registry feed
        message_type = message[MessageKeys.TYPE]
        if message_type == MessageTypes.ADD_JOB_HANDLER:
            # add a job handler
            job_handler_uri = message[MessageKeys.JOB_HANDLER_URI]
            if job_handler_uri in self._active_job_handlers:
                print('WARNING: job handler is already active')
                return
            # add to the pending job handler uris
            # we do it this way, because as we read through the entire
            # feed at startup, we will probably remove many handlers
            # and we don't want the overhead of creating new handler
            # connections for them
            self._pending_job_handler_uris.add(job_handler_uri)
        elif message_type == MessageTypes.REMOVE_JOB_HANDLER:
            # remove a job handler
            job_handler_uri = message[MessageKeys.JOB_HANDLER_URI]
            if job_handler_uri in self._active_job_handlers:
                # stop and delete the active job handler
                self._active_job_handlers[job_handler_uri].stop()
                del self._active_job_handlers[job_handler_uri]
            if job_handler_uri in self._pending_job_handler_uris:
                self._pending_job_handler_uris.remove(job_handler_uri)
    def iterate(self):
        # iterate the worker process
        self._worker_process.iterate()
        # handle the pending job handlers
        while self._pending_job_handler_uris:
            uri = self._pending_job_handler_uris.pop()
            assert uri not in self._active_job_handlers
            # create a new job handler connection
            X = JobHandlerConnection(compute_resource_uri=self._compute_resource_uri, job_handler_uri=uri, job_manager=self._job_manager)
            self._active_job_handlers[uri] = X
            X.start()
        # iterate the active job handlers
        for ajh in self._active_job_handlers.values():
            ajh.iterate()
        # iterate the job manager
        self._job_manager.iterate()

# The compute resource worker lives in a worker process
class ComputeResourceWorker:
    def __init__(
        self,
        compute_resource_uri, # uri of this compute resource feed
        node_ids_with_access # ids of nodes that have privileges of writing to this compute resource
    ):
        self._compute_resource_uri = compute_resource_uri
        self._node_ids_with_access = node_ids_with_access

        # Load the job handler registry feed and set the access permissions
        feed = kp.load_feed(self._compute_resource_uri)
        subfeed = feed.get_subfeed(SubfeedNames.JOB_HANDLER_REGISTRY)
        subfeed.set_access_rules(dict(
            rules = [
                dict(
                    nodeId=node_id,
                    write=True
                )
                for node_id in self._node_ids_with_access
            ]
        ))
        self._subfeed = subfeed
        self._subfeed.append_message({
            MessageKeys.TYPE: MessageTypes.COMPUTE_RESOURCE_STARTED,
            MessageKeys.TIMESTAMP: time.time() - 0
        })
    def handle_message_from_parent(self, message):
        pass
    def iterate(self):
        # listen for messages on the job handler registry subfeed
        try:
            messages = self._subfeed.get_next_messages(wait_msec=3000)
        except:
            # perhaps the daemon is down
            messages = None
        if messages is not None:
            for message in messages:
                self.send_message_to_parent(message)
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass

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
        for job in self._active_jobs.values():
            if job._status not in [JobStatus.FINISHED, JobStatus.ERROR]:
                job.cancel() # todo
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
            if job._status not in [JobStatus.RUNNING, JobStatus.FINISHED, JobStatus.ERROR]:
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
    def iterate(self):
        self._worker_process.iterate()
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
                msg[MessageKeys.RESULT_URI] = kp.store_object(dict(result=serialized_result))
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


# The worker associated with the job handler connection
# It listens for incoming messages from the job handler
class JobHandlerConnectionWorker:
    def __init__(self, job_handler_uri):
        self._job_handler_uri = job_handler_uri
        # Load the subfeed of incoming messages from the job handler
        self._incoming_subfeed = kp.load_feed(job_handler_uri).get_subfeed(SubfeedNames.MAIN)
    def handle_message_from_parent(self, message):
        pass
    def iterate(self):
        # Listen for messages from the job handler and send to parent
        try:
            messages = self._incoming_subfeed.get_next_messages(wait_msec=3000)
        except:
            # perhaps the daemon is down
            messages = None
        if messages is not None:
            for message in messages:
                self.send_message_to_parent(message)
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass

def _result_small_enough_to_store_directly(x, allow_small_dicts=True, allow_small_lists=True):
    if isinstance(x, numbers.Number):
        return True
    if isinstance(x, str):
        if len(x) <= 1000:
            return True
    if allow_small_dicts and isinstance(x, dict):
        if len(x.keys()) <= 3:
            for k, v in x.items():
                if not _result_small_enough_to_store_directly(k, allow_small_dicts=False, allow_small_lists=False):
                    return False
                if not _result_small_enough_to_store_directly(v, allow_small_dicts=False, allow_small_lists=False):
                    return False
            return True
    if allow_small_lists and (isinstance(x, tuple) or isinstance(x, list)):
        if len(x) <= 3:
            for v in x:
                if not _result_small_enough_to_store_directly(v, allow_small_dicts=False, allow_small_lists=False):
                    return False
            return True
    return False