import time
from typing import Dict, Any
import kachery as ka
from ._basejobhandler import BaseJobHandler
from .database import Database
from ._enums import JobStatus, JobKeys
from .file import File
from ._load_config import _load_preset_config_from_github
from ._util import _random_string, _deserialize_item, _flatten_nested_collection, _get_poll_interval
from .eventstreamclient import EventStreamClient
from .computeresource import ComputeResourceActionTypes
from .computeresource import HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER, HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE

class RemoteJobHandler(BaseJobHandler):
    def __init__(self, *, event_stream_client, compute_resource_id):
        self.is_remote = True
        
        self._event_stream_Client = event_stream_client
        self._compute_resource_id = compute_resource_id
        self._handler_id = _random_string(15)
        self._jobs: Dict = {}
        self._kachery_config = None
        self._timestamp_last_action = time.time()
        self._timestamp_event_poll = 0

        # The event streams for communication between this client and the remote compute resource
        self._event_stream_outgoing = event_stream_client.get_stream(dict(
            name=HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE,
            compute_resource_id=self._compute_resource_id,
            handler_id=self._handler_id
        ))
        self._event_stream_incoming = event_stream_client.get_stream(dict(
            name=HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER,
            compute_resource_id=self._compute_resource_id,
            handler_id=self._handler_id
        ))

        # notify the compute resource of the existence of this handler
        stream = event_stream_client.get_stream(dict(
            name='hither_compute_resource',
            compute_resource_id=self._compute_resource_id
        ))
        stream.write_event(dict(type=ComputeResourceActionTypes.ADD_JOB_HANDLER, handler_id=self._handler_id))

        # wait for the compute resource to ackowledge us
        print('Waiting for remote compute resource to respond...')
        actions = self._event_stream_incoming.read_events(wait_sec=10)
        if len(actions) == 0:
            raise Exception('Unable to connect with remote compute resource.')
        for action in actions:
            self._process_incoming_action(action)
        if self._kachery_config is None:
            raise Exception('Did not get a kachery config from the remote compute resource')
            
        self._report_action()

    def handle_job(self, job):
        super(RemoteJobHandler, self).handle_job(job)

        for f in _flatten_nested_collection(job._wrapped_function_arguments, _type=File):
            self._send_file_as_needed(f)

        job_serialized = job._serialize(generate_code=True)
        # the CODE member is a big block of code text.
        # Replace with a hash ref and send to kachery of compute resource
        job_serialized[JobKeys.CODE] = ka.store_object(job_serialized[JobKeys.CODE], to=self._kachery_config)
        self._event_stream_outgoing.write_event(dict(
            type=ComputeResourceActionTypes.ADD_JOB,
            job_id=job._job_id,
            job_serialized=job_serialized
        ))
        self._jobs[job._job_id] = job

        self._report_action()
    
    def cancel_job(self, job_id):
        if job_id not in self._jobs:
            print(f'Warning: RemoteJobHandler -- cannot cancel job {job_id}. Job with this id not found.')
            return
        self._event_stream_outgoing.write_event(dict(
            type=ComputeResourceActionTypes.CANCEL_JOB,
            job_id=job_id
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
        elif _type == ComputeResourceActionTypes.SET_KACHERY_CONFIG:
            self._kachery_config = action['kachery_config']
    
    def iterate(self) -> None:
        elapsed_event_poll = time.time() - self._timestamp_event_poll
        if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
            self._timestamp_event_poll = time.time()    
            self._report_alive()
            # TODO: avaid polling by wait_sec=10, or something
            actions = self._event_stream_incoming.read_events(0)
            for action in actions:
                self._process_incoming_action(action)

    def _load_file(self, sha1_path):
        if self._kachery_config is not None:
            return ka.load_file(sha1_path, fr=self._kachery_config)
        else:
            return None

    def _send_file_as_needed(self, x:File) -> None:
        if self._kachery_config is None: return # We have no file store; nothing we can do.

        remote_handler = getattr(x, '_remote_job_handler', None)
        if remote_handler is None:
            if self._compute_resource_id is None: return
            ka.store_file(x.path, to=self._kachery_config)
        else: 
            #  If we're the remote handler, we don't need to do anything.
            if remote_handler._compute_resource_id == self._compute_resource_id:
                return
            raise Exception('This case not yet supported (we need to transfer data from one compute resource to another)')
        
    def _report_alive(self):
        self._event_stream_outgoing.write_event(dict(
            type=ComputeResourceActionTypes.REPORT_ALIVE
        ))
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

    def cleanup(self):
        pass