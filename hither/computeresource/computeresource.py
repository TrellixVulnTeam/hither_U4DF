from typing import List, Dict, Set
import time
import numbers
from .._workerprocess import WorkerProcess
from .._preventkeyboardinterrupt import PreventKeyboardInterrupt
from ._computeresourcejobmanager import ComputeResourceJobManager
from ._jobhandlerconnection import JobHandlerConnection
from ._computeresourceworker import ComputeResourceWorker
from ._compute_resource_enums import MessageTypes, MessageKeys

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
            if MessageKeys.JOB_HANDLER_URI in message:
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
            else:
                print('WARNING: no JOB_HANDLER_URI in ADD_JOB_HANDLER message')
        elif message_type == MessageTypes.REMOVE_JOB_HANDLER:
            if MessageKeys.JOB_HANDLER_URI in message:
                # remove a job handler
                job_handler_uri = message[MessageKeys.JOB_HANDLER_URI]
                if job_handler_uri in self._active_job_handlers:
                    # stop and delete the active job handler
                    self._active_job_handlers[job_handler_uri].stop()
                    del self._active_job_handlers[job_handler_uri]
                if job_handler_uri in self._pending_job_handler_uris:
                    self._pending_job_handler_uris.remove(job_handler_uri)
            else:
                print('WARNING: no JOB_HANDLER_URI in REMOVE_JOB_HANDLER message')
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