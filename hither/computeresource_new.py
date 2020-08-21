from typing import List, Dict, Set
from enum import Enum
import time
import multiprocessing
import kachery_p2p as kp
import numbers
from .job import Job, JobKeys, JobStatus
from ._util import _serialize_item

class MessageTypes(Enum):
    # job handler registry
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

class WorkerProcess:
    def __init__(self, WorkerClass, args):
        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        self._pipe_to_process = pipe_to_child
        self._process = multiprocessing.Process(
            target=_worker_process_target,
            args=(pipe_to_parent, WorkerClass, args)
        )
        self._on_message_from_process_callbacks = []
    def start(self):
        self._process.start()
    def stop(self):
        self._pipe_to_process.send({'type': 'exit'})
    def send_message_to_process(self, message):
        self._pipe_to_process.send({'type': 'message', 'message': message})
    def on_message_from_process(self, cb):
        self._on_message_from_process_callbacks.append(cb)
    def iterate(self):
        while self._pipe_to_process.poll():
            x = self._pipe_to_process.recv()
            if x['type'] == 'exit':
                self.stop()
            elif x['type'] == 'message':
                for cb in self._on_message_from_process_callbacks:
                    cb(x['message'])

def _worker_process_target(pipe_to_parent, WorkerClass, args):
    worker = WorkerClass(*args)
    def send_message_to_parent(message):
        pipe_to_parent.send({'type': 'message', 'message': message})
    def handle_exit():
        pipe_to_parent.send({'type': 'exit'})
    worker.send_message_to_parent = send_message_to_parent
    worker.exit = handle_exit
    while True:
        while pipe_to_parent.poll():
            x = pipe_to_parent.recv()
            if x['type'] == 'exit':
                return
            elif x['type'] == 'message':
                worker.handle_message_from_parent(x['message'])
        worker.iterate()
        time.sleep(0.1)

class ComputeResourceNew:
    def __init__(
        self, *,
        compute_resource_uri: str, # feed uri for this compute resource
        node_ids_with_access: List[str] # ids of nodes that have privileges of writing to this compute resource
    ):
        self._compute_resource_uri = compute_resource_uri
        self._node_ids_with_access = node_ids_with_access
        self._active_job_handlers: Dict[str, JobHandlerConnection] = {} # by feed uri
        self._pending_job_handler_uris: Set[str] = set()
        self._job_manager = ComputeResourceJobManager()

        self._worker_process = WorkerProcess(ComputeResourceWorker, (
            self._compute_resource_uri,
            self._node_ids_with_access
        ))
        self._worker_process.on_message_from_process(self._process_message_from_worker)
    def start(self):
        self._worker_process.start()
    def stop(self):
        self._worker_process.stop()
        for ajh in self._active_job_handlers.values():
            ajh.stop()
    def _process_message_from_worker(self, message):
        if message['type'] == MessageTypes.ADD_JOB_HANDLER:
            job_handler_uri = message['uri']
            if job_handler_uri in self._active_job_handlers:
                print('WARNING: job handler is already active')
                return
            self._pending_job_handler_uris.add(job_handler_uri)
        elif message['type'] == MessageTypes.REMOVE_JOB_HANDLER:
            job_handler_uri = message['uri']
            if job_handler_uri in self._active_job_handlers:
                self._active_job_handlers[job_handler_uri].stop()
                del self._active_job_handlers[job_handler_uri]
            if job_handler_uri in self._pending_job_handler_uris:
                self._pending_job_handler_uris.remove(job_handler_uri)
    def iterate(self):
        self._worker_process.iterate()
        while self._pending_job_handler_uris:
            uri = self._pending_job_handler_uris.pop()
            assert uri not in self._active_job_handlers
            X = JobHandlerConnection(compute_resource=self, job_handler_uri=uri, job_manager=self._job_manager)
            self._active_job_handlers[uri] = X
            X.start()
        for ajh in self._active_job_handlers.values():
            ajh.iterate()

class ComputeResourceWorker:
    def __init__(self, compute_resource_uri, node_ids_with_access):
        self._compute_resource_uri = compute_resource_uri
        self._node_ids_with_access = node_ids_with_access

        feed = kp.load_feed(self._compute_resource_uri)
        subfeed = feed.get_subfeed('job_handler_registry')
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
    def on_message_from_parent(self, message):
        pass
    def iterate(self):
        messages = self._subfeed.get_next_messages(wait_msec=3000)
        for message in messages:
            self.send_message_to_parent(message)
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass

class ComputeResourceJobManager:
    def __init__(self):
        self._jobs = dict(
            pending={},
            queued={},
            running={},
            finished={},
            error={}
        )
    def add_job(self, job_id, job_serialized):
        Job._deserialize(job_serialized)
        pass

class JobHandlerConnection:
    def __init__(self, compute_resource, job_handler_uri, job_manager):
        self._compute_resource = compute_resource
        self._job_handler_uri = job_handler_uri
        self._job_manager = job_manager
        self._active_jobs: Dict[str, Job] = {}
        self._outgoing_subfeed = kp.load_feed(compute_resource._compute_resource_uri).get_subfeed(job_handler_uri)

        self._worker_process = WorkerProcess(JobHandlerConnectionWorker, (
            self._job_handler_uri
        ))
        self._worker_process.on_message_from_process(self._process_message_from_worker)
    def start(self):
        self._worker_process.start()
    def stop(self):
        # todo: stop the running jobs
        self._worker_process.stop()
    def _process_message_from_worker(self, message):
        if message['type'] == MessageTypes.ADD_JOB:
            job_id = message['job_id']
            job_serialized = message['job_serialized']
            if job_id in self._active_jobs:
                print('WARNING: cannot add job. Job with this ID is already active.')
                return
            job = self._job_manager.add_job(job_id=job_id, job_serialized=job_serialized)
            self._send_message_to_job_handler({
                'type': MessageTypes.JOB_QUEUED,
                'job_id': job_id,
                'label': job._label
            })
            self._active_jobs[job_id] = job
            job.on_status_change(self._handle_job_status_change)
        elif message['type'] == MessageTypes.CANCEL_JOB:
            job_id = message['job_id']
            if job_id not in self._active_jobs:
                print('WARNING: cannot cancel job. Job with this ID is not active.')
                return
            job = self._active_jobs[job_id]
            job.cancel() # todo
    def iterate(self):
        pass
    def _handle_job_status_change(self, job: Job):
        status = job.status # todo
        if status == JobStatus.FINISHED:
            msg = {
                'type': MessageTypes.JOB_FINISHED,
                'timestamp': time.time() - 0,
                'job_id': job._job_id, # change this to JobKeys.JOB_ID if safe
                'label': job._label,
                JobKeys.RUNTIME_INFO: job.get_runtime_info()
            }
            serialized_result = _serialize_item(job._result)
            # decide whether to store the result or a result_uri
            if _result_small_enough_to_store_directly(serialized_result):
                msg[JobKeys.RESULT] = serialized_result
            else:
                msg[JobKeys.RESULT_URI] = kp.store_object(dict(result=serialized_result))
            self._send_message_to_job_handler(msg)
        elif status == JobStatus.ERROR:
            msg = {
                'type': MessageTypes.JOB_ERROR,
                'timestamp': time.time() - 0,
                'job_id': job._job_id, # change this to JobKeys.JOB_ID if safe
                'label': job._label,
                JobKeys.RUNTIME_INFO: job.get_runtime_info(),
                JobKeys.EXCEPTION: str(job._exception)
            }
            self._send_message_to_job_handler(msg)
        elif status == JobStatus.RUNNING:
            msg = {
                'type': MessageTypes.JOB_STARTED,
                'timestamp': time.time() - 0,
                'job_id': job._job_id,
                'label': job._label
            }
            self._send_message_to_job_handler(msg)
        else:
            print(f'WARNING: unexpected job status: {status}')
    def _send_message_to_job_handler(self, message):
        self._outgoing_subfeed.append_message(message)


class JobHandlerConnectionWorker:
    def __init__(self, job_handler_uri):
        self._job_handler_uri = job_handler_uri
        
        self._incoming_subfeed = kp.load_feed(job_handler_uri).get_subfeed('main')
    def on_message_from_parent(self, message):
        pass
    def iterate(self):
        messages = self._incoming_subfeed.get_next_messages(wait_msec=3000)
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