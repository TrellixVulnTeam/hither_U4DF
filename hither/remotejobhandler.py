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

class RemoteJobHandler(BaseJobHandler):
    def __init__(self, *, compute_resource_uri):
        super().__init__()
        self.is_remote = True
        
        self._compute_resource_uri = compute_resource_uri
        self._is_initialized = False
    
    def _initialize(self):
        self._is_initialized = True

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

        self._jobs: Dict = {}
        self._timestamp_last_action = time.time()
        self._timestamp_event_poll = 0

        # wait for the compute resource to ackowledge us
        print('Waiting for remote compute resource to respond...')
        msg = self._incoming_feed.get_next_message(wait_msec=10000)
        assert msg is not None, 'Timeout while waiting for compute resource to respond.'
        assert msg['type'] == ComputeResourceActionTypes.JOB_HANDLER_REGISTERED, 'Unexpected message from compute resource'
        print('Got response from compute resource.')
            
        self._report_action()

    def handle_job(self, job):
        super(RemoteJobHandler, self).handle_job(job)

        if not self._is_initialized:
            self._initialize()

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
        if job_id not in self._jobs:
            print(f'Warning: RemoteJobHandler -- cannot cancel job {job_id}. Job with this id not found.')
            return
        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.CANCEL_JOB,
            timestamp=time.time() - 0,
            job_id=job_id,
            label=self._jobs[job_id]._label
        ))

        self._report_action()
    
    def _process_job_finished_action(self, action):
        job_id = action[JobKeys.JOB_ID]
        if job_id not in self._jobs:
            print(f'Warning: Job with id not found: {job_id}')
            return
        job = self._jobs[job_id]
        job._runtime_info = action[JobKeys.RUNTIME_INFO]
        job._status = JobStatus.FINISHED
        job._result = _deserialize_item(action[JobKeys.RESULT])
        for f in _flatten_nested_collection(job._result, _type=File):
            setattr(f, '_remote_job_handler', self)
        del self._jobs[job_id]

        self._report_action()
    
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

        self._report_action()
    
    def _process_incoming_action(self, action):
        _type = action['type']
        if _type == ComputeResourceActionTypes.JOB_FINISHED:
            self._process_job_finished_action(action)
        elif _type == ComputeResourceActionTypes.JOB_ERROR:
            self._process_job_error_action(action)
        elif _type == ComputeResourceActionTypes.LOG:
            pass
        else:
            raise Exception(f'Unexpected action type from compute resource: {_type}')
    
    def iterate(self) -> None:
        elapsed_event_poll = time.time() - self._timestamp_event_poll
        if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
            self._timestamp_event_poll = time.time()    
            self._report_alive()
            actions = self._incoming_feed.get_next_messages(wait_msec=100)
            for action in actions:
                self._process_incoming_action(action)
    
    def _report_alive(self):
        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.REPORT_ALIVE,
            timestamp=time.time() - 0
        ))
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

    def cleanup(self):
        if hasattr(self, '_outgoing_feed'):
            self._outgoing_feed.append_message(dict(
                type=ComputeResourceActionTypes.JOB_HANDLER_FINISHED,
                timestamp=time.time() - 0
            ))