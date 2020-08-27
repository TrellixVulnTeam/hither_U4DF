from typing import Union
import time
import kachery_p2p as kp
from ._basejobhandler import BaseJobHandler
from ._workerprocess import WorkerProcess
from ._serialize_job import _serialize_job
from ._exceptions import JobCancelledException
from .job  import Job
from ._enums import JobStatus
from ._util import _deserialize_item
from .computeresource._compute_resource_enums import MessageTypes, MessageKeys, SubfeedNames


class RemoteJobHandler(BaseJobHandler):
    def __init__(self, compute_resource_uri):
        super().__init__()
        self.is_remote = True

        self._compute_resource_uri = compute_resource_uri

        # only create these if needed
        self._job_handler_uri = None
        self._job_handler_feed = None
        self._worker_process = None

        self._jobs = {}

        self._last_outgoing_keepalive_timestamp = time.time()

    def cleanup(self):
        if self._worker_process is not None:
            # maybe later we will want to clean up the job handler feed:
            # self._job_handler_feed.delete()
            self._worker_process.stop()
            if self._job_handler_uri is not None:
                registry_subfeed = kp.load_feed(self._compute_resource_uri).get_subfeed(SubfeedNames.JOB_HANDLER_REGISTRY)
                registry_subfeed.submit_message({
                    MessageKeys.TYPE: MessageTypes.REMOVE_JOB_HANDLER,
                    MessageKeys.JOB_HANDLER_URI: self._job_handler_uri
                })

    def handle_job(self, job: Job):
        super(RemoteJobHandler, self).handle_job(job)
        if not job.get_container():
            exception = Exception('Cannot run uncontainerized job on remote compute resource. Use @hi.container() on the function and hi.Config(container=True) for the job.')
            job._set_error_status(exception=exception, runtime_info=dict())
            return
        self._jobs[job.get_job_id()] = job
        job_serialized = _serialize_job(job=job, generate_code=True)
        if self._worker_process is None:
            registry_subfeed = kp.load_feed(self._compute_resource_uri).get_subfeed(SubfeedNames.JOB_HANDLER_REGISTRY)
            self._job_handler_feed = kp.create_feed()
            self._outgoing_subfeed = self._job_handler_feed.get_subfeed(SubfeedNames.MAIN)
            self._job_handler_uri = self._job_handler_feed.get_uri()
            registry_subfeed.submit_message({
                MessageKeys.TYPE: MessageTypes.ADD_JOB_HANDLER,
                MessageKeys.JOB_HANDLER_URI: self._job_handler_uri
            })
            self._start_worker_process()
        
        self._outgoing_subfeed.append_message({
            MessageKeys.TYPE: MessageTypes.ADD_JOB,
            MessageKeys.JOB_ID: job.get_job_id(),
            MessageKeys.JOB_SERIALIZED: job_serialized
        })

    def cancel_job(self, job_id):
        assert job_id in self._jobs
        assert self._worker_process is not None
        self._outgoing_subfeed.append_message({
            MessageKeys.TYPE: MessageTypes.CANCEL_JOB,
                MessageKeys.JOB_ID: job_id
        })

    def iterate(self):
        if self._worker_process is not None:
            self._worker_process.iterate()
            elapsed = time.time() - self._last_outgoing_keepalive_timestamp
            if elapsed > 20:
                self._last_outgoing_keepalive_timestamp = time.time()
                self._outgoing_subfeed.append_message({
                    MessageKeys.TYPE: MessageTypes.KEEP_ALIVE
                })
            

    def _handle_message_from_worker(self, message):
        if message['type'] == 'message_from_compute_resource':
            msg = message['message']
            t = msg[MessageKeys.TYPE]
            if t == MessageTypes.JOB_QUEUED:
                self._handle_job_queued_message(msg)
            elif t == MessageTypes.JOB_STARTED:
                self._handle_job_started_message(msg)
            elif t == MessageTypes.JOB_FINISHED:
                self._handle_job_finished_message(msg)
            elif t == MessageTypes.JOB_ERROR:
                self._handle_job_error_message(msg)
            elif t == MessageTypes.KEEP_ALIVE:
                self._handle_keep_alive_message(msg)
            else:
                print(f'WARNING: Unexpected message type from compute resource: {t}')

    def _handle_job_queued_message(self, message):
        job_id = message[MessageKeys.JOB_ID]
        if job_id not in self._jobs:
            print('WARNING: RemoteJobHandler - unrecognized job id for queued job')
            return
        job = self._jobs[job_id]
        # in future we should do something with the info (the compute resource acklowledges the job)

    def _handle_job_started_message(self, message):
        job_id = message[MessageKeys.JOB_ID]
        if job_id not in self._jobs:
            print('WARNING: RemoteJobHandler - unrecognized job id for started job')
            return
        job = self._jobs[job_id]
        job._set_status(JobStatus.RUNNING)

    def _handle_job_finished_message(self, message):
        job_id = message[MessageKeys.JOB_ID]
        if job_id not in self._jobs:
            print('WARNING: RemoteJobHandler - unrecognized job id for finished job')
            return
        job = self._jobs[job_id]
        if MessageKeys.RESULT in message:
            result = _deserialize_item(message[MessageKeys.RESULT])
        elif MessageKeys.RESULT_URI:
            result_uri = message[MessageKeys.RESULT_URI]
            x = kp.load_object(result_uri)
            result = _deserialize_item(x['result'])
        else:
            print('WARNING: RemoteJobHandler - no result or result_uri for finished job')
            result = None
            return
        job._set_finished_status(result=result, runtime_info=message[MessageKeys.RUNTIME_INFO])

    def _handle_job_error_message(self, message):
        job_id = message[MessageKeys.JOB_ID]
        if job_id not in self._jobs:
            print('WARNING: RemoteJobHandler - unrecognized job id for errored job')
            return
        job = self._jobs[job_id]
        runtime_info = message[MessageKeys.RUNTIME_INFO]
        if job._runtime_info is not None and job._runtime_info.get('cancelled'):
            exception = JobCancelledException(message[MessageKeys.EXCEPTION])
        else:
            exception = Exception(message[MessageKeys.EXCEPTION])
        job._set_error_status(exception=exception, runtime_info=runtime_info)

    def _handle_keep_alive_message(self, message):
        pass # todo

    def _start_worker_process(self):
        self._worker_process = WorkerProcess(
            RemoteJobHandlerWorker,
            (self._compute_resource_uri, self._job_handler_uri)
        )
        self._worker_process.on_message_from_process(self._handle_message_from_worker)
        self._worker_process.start()

class RemoteJobHandlerWorker:
    def __init__(self, compute_resource_uri, job_handler_uri):
        self._compute_resource_uri = compute_resource_uri
        self._job_handler_uri = job_handler_uri
        self._job_handler_feed = kp.load_feed(self._job_handler_uri)
        self._incoming_subfeed = kp.load_feed(self._compute_resource_uri).get_subfeed(self._job_handler_uri)
        
    def handle_message_from_parent(self, message):
        pass
    def iterate(self):
        try:
            messages = self._incoming_subfeed.get_next_messages(wait_msec=3000)
        except:
            # perhaps the daemon is down
            messages = None
        if messages is not None:
            for m in messages:
                self.send_message_to_parent({
                    'type': 'message_from_compute_resource',
                    'message': m
                })
    # The following methods will be overwritten by the framework
    # They are just placeholders to keep linters happy
    def send_message_to_parent(self, message): # overwritten by framework
        pass
    def exit(self): # overwritten by framework
        pass
