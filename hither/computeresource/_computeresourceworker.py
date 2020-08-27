from typing import List, Dict, Set
import time
import kachery_p2p as kp
import numbers
from .._workerprocess import WorkerProcess
from .._preventkeyboardinterrupt import PreventKeyboardInterrupt
from ._computeresourcejobmanager import ComputeResourceJobManager
from ._jobhandlerconnection import JobHandlerConnection
from ._result_small_enough_to_store_directly import _result_small_enough_to_store_directly

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
        nodes_with_access: List[dict], # nodes that have privileges of writing to this compute resource. Each is dict(node_id=...)
        job_handlers
    ):
        self._compute_resource_uri = compute_resource_uri
        self._nodes_with_access = nodes_with_access
        self._active_job_handlers: Dict[str, JobHandlerConnection] = {} # by feed uri
        self._pending_job_handler_uris: Set[str] = set()
        # the manager for all the jobs
        self._job_manager = ComputeResourceJobManager(compute_resource_job_handlers=job_handlers)

        # the worker process - listening for incoming messages on the job handler registry feed
        self._worker_process = WorkerProcess(ComputeResourceWorker, (
            self._compute_resource_uri,
            self._nodes_with_access
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
            if MessageKeys.JOB_HANDLER_URI in message:
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
            else:
                print(f'WARNING: no {MessageKeys.JOB_HANDLER_URI} in ADD_JOB_HANDLER message')
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
        active_job_handler_uris = list(self._active_job_handlers.keys())
        for job_handler_uri in active_job_handler_uris:
            ajh = self._active_job_handlers[job_handler_uri]
            ajh.iterate()
            if not ajh.is_alive():
                print(f'Stopping job handler: {job_handler_uri}')
                ajh.stop()
                del self._active_job_handlers[job_handler_uri]

        # iterate the job manager
        self._job_manager.iterate()

# The compute resource worker lives in a worker process
class ComputeResourceWorker:
    def __init__(
        self,
        compute_resource_uri, # uri of this compute resource feed
        nodes_with_access # ids of nodes that have privileges of writing to this compute resource
    ):
        self._compute_resource_uri = compute_resource_uri
        self._nodes_with_access = nodes_with_access

        # Load the job handler registry feed and set the access permissions
        feed = kp.load_feed(self._compute_resource_uri)
        subfeed = feed.get_subfeed(SubfeedNames.JOB_HANDLER_REGISTRY)
        subfeed.set_access_rules(dict(
            rules = [
                dict(
                    nodeId=n['node_id'],
                    write=True
                )
                for n in self._nodes_with_access
            ]
        ))
        self._subfeed = subfeed
        # move to the end of the registry subfeed
        self._subfeed.set_position(self._subfeed.get_num_messages())
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