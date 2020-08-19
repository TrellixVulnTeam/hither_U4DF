import time
from typing import Dict, Any
import kachery as ka
import kachery_p2p as kp
from ._basejobhandler import BaseJobHandler
from .database import Database
from ._enums import JobStatus, JobKeys
from .file import File
from ._util import _random_string, _deserialize_item, _flatten_nested_collection, _get_poll_interval
from .computeresource import ComputeResourceActionTypes
from .computeresource import HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER, HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE
import multiprocessing
from multiprocessing.connection import Connection

class RemoteJobHandler(BaseJobHandler):
    def __init__(self, *, uri):
        super().__init__()
        self.is_remote = True
        
        self._compute_resource_uri = uri
        self._compute_resource_node_id = None # not known yet
        self._timestamp_initialized = None
        self._is_initialized = False
        self._job_handler_registered = False

        self._job_handler_feed = None
        self._outgoing_feed = None
        self._compute_resource_feed = None
        self._registry_feed = None
        self._incoming_feed = None
    
    def _initialize(self):
        self._is_initialized = True

        self._jobs: Dict = {}
        self._timestamp_last_action = time.time()
        self._timestamp_report_alive = 0

        self._job_handler_feed = kp.create_feed()
        self._outgoing_feed = self._job_handler_feed.get_subfeed('main')
        self._compute_resource_feed = kp.load_feed(self._compute_resource_uri)
        self._registry_feed = self._compute_resource_feed.get_subfeed('job_handler_registry')
        self._incoming_feed = self._compute_resource_feed.get_subfeed(self._job_handler_feed.get_uri())

        # register self with compute resource
        print('Registering job handler with remote compute resource...')
        try:
            self._registry_feed.submit_message(dict(
                type=ComputeResourceActionTypes.REGISTER_JOB_HANDLER,
                timestamp=time.time() - 0,
                uri=self._job_handler_feed.get_uri()
            ))
        except:
            raise Exception('Unable to register job handler with remote compute resource. Perhaps you do not have permission to access this resource.')
            
        self._report_action()

        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        self._pipe_to_worker_process = pipe_to_child
        self._worker_process = multiprocessing.Process(
            target=_rjh_worker,
            args=(pipe_to_parent, self._compute_resource_uri, self._job_handler_feed.get_uri())
        )
        self._worker_process.start()

        self._timestamp_initialized = time.time()

        # wait for the compute resource to ackowledge us
        print('Waiting for remote compute resource to respond...')
    
    def cleanup(self):
        if self._is_initialized:
            self._outgoing_feed.append_message(dict(
                type=ComputeResourceActionTypes.JOB_HANDLER_FINISHED,
                timestamp=time.time() - 0
            ))
            self._job_handler_feed = None
            self._outgoing_feed = None
            self._compute_resource_feed = None
            self._registry_feed = None
            self._incoming_feed = None
            self._is_initialized = False
            self._pipe_to_worker_process.send('exit')

    def handle_job(self, job):
        super(RemoteJobHandler, self).handle_job(job)

        if not self._is_initialized:
            try:
                self._initialize()
            except Exception as err:
                job._runtime_info = None
                job._status = JobStatus.ERROR
                job._exception = Exception(f'Error initializing remote job handler: {str(err)}')
                return

        job_serialized = job._serialize(generate_code=True)
        # the CODE member is a big block of code text.
        # Make sure it is stored in the local kachery database so it can be retrieved through the kachery-p2p network
        job_serialized[JobKeys.CODE] = ka.store_object(job_serialized[JobKeys.CODE])
        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.ADD_JOB,
            timestamp=time.time() - 0,
            job_id=job._job_id,
            label=job._label,
            job_serialized=job_serialized
        ))
        self._jobs[job._job_id] = job

        self._report_action()
    
    def cancel_job(self, job_id):
        if not self._is_initialized:
            self._initialize()

        if job_id not in self._jobs:
            print(f'Warning: RemoteJobHandler -- cannot cancel job {job_id}. Job with this ID not found.')
            return
        if self._outgoing_feed is None:
            return
        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.CANCEL_JOB,
            timestamp=time.time() - 0,
            job_id=job_id,
            label=self._jobs[job_id]._label
        ))
        self._report_action()
    
    def compute_resource_node_id(self):
        return self._compute_resource_node_id
    
    def _process_job_finished_action(self, action):
        job_id = action[JobKeys.JOB_ID]
        if job_id not in self._jobs:
            print(f'Warning: Job with id not found: {job_id}')
            return
        job = self._jobs[job_id]
        job._runtime_info = action[JobKeys.RUNTIME_INFO]
        if JobKeys.RESULT in action:
            serialized_result = action[JobKeys.RESULT]
        elif JobKeys.RESULT_URI in action:
            x = kp.load_object(action[JobKeys.RESULT_URI], from_node=self._compute_resource_node_id)
            if x is None:
                job._status = JobStatus.ERROR
                job._exception = Exception(f'Unable to load result for uri: {action[JobKeys.RESULT_URI]}')
                return
            if 'result' not in x:
                job._status = JobStatus.ERROR
                job._exception = Exception(f'result field not in object obtained from uri: {action[JobKeys.RESULT_URI]}')
                return
            serialized_result = x['result']
        else:
            job._status = JobStatus.ERROR
            job._exception = Exception(f'Neither result nor result_uri in job finished action')
            return
        try:
            job._result = _deserialize_item(serialized_result)
        except:
            job._status = JobStatus.ERROR
            job._exception = Exception(f'Problem deserializing result')
            return
        job._status = JobStatus.FINISHED
        for f in _flatten_nested_collection(job._result, _type=File):
            setattr(f, '_remote_job_handler', self)
        del self._jobs[job_id]
    
    def _process_job_error_action(self, action):
        job_id = action[JobKeys.JOB_ID]
        if job_id not in self._jobs:
            print(f'Warning: Job with id not found: {job_id}')
            return
        job = self._jobs[job_id]
        job._runtime_info = action[JobKeys.RUNTIME_INFO]
        job._status = JobStatus.ERROR
        job._exception = Exception(action[JobKeys.EXCEPTION])
        del self._jobs[job_id]
    
    def _process_incoming_action(self, action):
        _type = action['type']

        if not self._job_handler_registered:
            if _type == ComputeResourceActionTypes.JOB_HANDLER_REGISTERED:
                print('Got response from compute resource.')
                print(f'{bcolors.HEADER}To monitor this job handler:{bcolors.ENDC}')
                print(f'{bcolors.OKBLUE}hither-compute-resource monitor --uri {self._compute_resource_uri} --job-handler {self._job_handler_feed.get_uri()}{bcolors.ENDC}')
                self._job_handler_registered = True
                self._compute_resource_node_id = action.get('compute_resource_node_id', None)
                if self._compute_resource_node_id is None:
                    print('WARNING: did not get compute_resource_node_id info in JOB_HANDLER_REGISTERED message')
            else:
                raise Exception(f'Got unexpected message ({_type}) from compute resource prior to JOB_HANDLER_REGISTERED message.')
            return

        if _type == ComputeResourceActionTypes.JOB_FINISHED:
            self._report_action()
            self._process_job_finished_action(action)
        elif _type == ComputeResourceActionTypes.JOB_ERROR:
            self._report_action()
            self._process_job_error_action(action)
        elif _type == ComputeResourceActionTypes.LOG:
            pass
        else:
            raise Exception(f'Unexpected action type from compute resource: {_type}')
    
    def iterate(self) -> None:
        if not self._is_initialized:
            return
        if not self._job_handler_registered:
            elapsed_since_initialized = time.time() - self._timestamp_initialized
            if elapsed_since_initialized > 30:
                self.cleanup()
                raise Exception('Timeout while waiting for compute resource to respond.')
        elapsed_report_alive = time.time() - self._timestamp_report_alive
        if elapsed_report_alive >= 15:
            self._timestamp_report_alive = time.time()    
            self._report_alive()
        while self._pipe_to_worker_process.poll():
            action = self._pipe_to_worker_process.recv()
            self._process_incoming_action(action)
    
    def _report_alive(self):
        if self._outgoing_feed is None:
            return
        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.REPORT_ALIVE,
            timestamp=time.time() - 0
        ))
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

def _rjh_worker(pipe_to_parent: Connection, compute_resource_uri: str, job_handler_feed_uri) -> None:
    compute_resource_feed = kp.load_feed(compute_resource_uri)
    incoming_subfeed = compute_resource_feed.get_subfeed(job_handler_feed_uri)
    while True:
        if pipe_to_parent.poll():
            x = pipe_to_parent.recv()
            return
        
        actions = incoming_subfeed.get_next_messages(wait_msec=6000)
        if actions is not None:
            for action in actions:
                pipe_to_parent.send(action)
        time.sleep(0.3)

# Thanks: https://stackoverflow.com/questions/287871/how-to-print-colored-text-in-terminal-in-python
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
