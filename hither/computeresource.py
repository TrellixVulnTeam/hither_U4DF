import time
import traceback
import os
from typing import Optional, Dict, Any
import multiprocessing
from multiprocessing.connection import Connection
import numbers

from ._containermanager import ContainerManager
from ._util import _serialize_item, _get_poll_interval
from ._enums import JobStatus, JobKeys
from ._exceptions import DeserializationException
from .job import Job
import kachery_p2p as kp

class ComputeResourceActionTypes:
    # sent by compute resource at startup
    COMPUTE_RESOURCE_STARTED = 'COMPUTE_RESOURCE_STARTED'

    # sent from job handler to compute resource
    REGISTER_JOB_HANDLER = 'REGISTER_JOB_HANDLER'
    ADD_JOB = 'ADD_JOB'
    CANCEL_JOB = 'CANCEL_JOB'
    REPORT_ALIVE = 'REPORT_ALIVE'
    JOB_HANDLER_FINISHED = 'JOB_HANDLER_FINISHED'

    # sent from compute resource to job handler
    JOB_HANDLER_REGISTERED = 'JOB_HANDLER_REGISTERED'
    JOB_FINISHED = 'JOB_FINISHED'
    JOB_ERROR = 'JOB_ERROR'

    # sent either direction (information only)
    LOG = 'LOG'

HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE = 'HITHER_REMOTE_JOB_HANDLER_TO_COMPUTE_RESOURCE'
HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER = 'HITHER_COMPUTE_RESOURCE_TO_REMOTE_JOB_HANDLER'

class ConnectedClient:
    def __init__(self, *, compute_resource, handler_uri, incoming_feed, outgoing_feed):
        # pointer to parent compute resource -- and get some useful variables
        self._handler_uri = handler_uri
        self._compute_resource = compute_resource
        self._incoming_feed = incoming_feed
        self._outgoing_feed = outgoing_feed

        self._job_handler = compute_resource._job_handler
        self._job_cache = compute_resource._job_cache

        self._finished = False

        # for polling
        self._timestamp_event_poll = 0
        self._timestamp_last_action = time.time()

        # list of jobs for this particular connected client
        self._jobs: Dict[str, Job] = dict()

        # timestamp for when the client last reported being alive
        self._timestamp_client_report_alive = time.time()

        self._outgoing_feed.append_message(dict(
            type=ComputeResourceActionTypes.JOB_HANDLER_REGISTERED,
            compute_resource_node_id=kp.get_node_id(),
            timestamp=time.time() - 0
        ))

        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        self._pipe_to_worker_process = pipe_to_child
        self._worker_process = multiprocessing.Process(
            target=_cc_worker,
            args=(pipe_to_parent, self._handler_uri)
        )
        self._worker_process.start()
    
    def halt(self):
        self._pipe_to_worker_process.send('exit')
        self._finished = True

    def iterate(self):
        if self._finished:
            return
        if not self._worker_process.is_alive():
            self._finished = True
            return
        
        # # read and process all incoming messages (polling)
        # elapsed_event_poll = time.time() - self._timestamp_event_poll
        # if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
        #     self._timestamp_event_poll = time.time()
        #     try:
        #         actions = self._incoming_feed.get_next_messages(wait_msec=10)
        #     except:
        #         traceback.print_exc()
        #         print('Error reading from incoming feed. Stopping job handler.')
        #         self._finished = True
        #         return
        #     if actions is not None:
        #         for action in actions:
        #             try:
        #                 self._process_action(action)
        #             except:
        #                 traceback.print_exc()
        #                 print('Error processing action from incoming feed. Stopping job handler')
        #                 self._finished = True
        #                 return
        while self._pipe_to_worker_process.poll():
            action = self._pipe_to_worker_process.recv()
            try:
                self._process_action(action)
            except:
                traceback.print_exc()
                print('Error processing action from incoming feed. Stopping job handler')
                self.halt()
                return
        
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
                self._mark_job_as_error(job_id=job_id, label=job._label, runtime_info=job._runtime_info, exception=job._exception)
                del self._jobs[job._job_id]
                self._report_action
            elif job._status == JobStatus.PENDING:
                pass # Local status will remain PENDING until changed by remote. This is expected.
            elif job._status == JobStatus.QUEUED:
                pass
            else:
                raise Exception(f"Job {job_id} has unexpected status in compute resource: {job._status} {type(job._status)} {JobStatus.ERROR}") # pragma: no cover

    def cancel_all_jobs(self):
        for job in self._jobs.values():
            if job.get_status() in [JobStatus.RUNNING, JobStatus.QUEUED]:
                self._job_handler.cancel_job(job._job_id)

    def _process_action(self, action):
        # process an incoming message/action
        _type = action['type']
        if _type == ComputeResourceActionTypes.CANCEL_JOB:
            # cancel a job... get the job id and forward the request to the job handler on this end
            self._outgoing_feed.append_message(dict(
                type=ComputeResourceActionTypes.LOG,
                timestamp=time.time() - 0,
                action=action
            ))
            job_id = action['job_id']
            self._job_handler.cancel_job(job_id)
            self._report_action()
        elif _type == ComputeResourceActionTypes.REPORT_ALIVE:
            # the client has reported they are alive
            self._timestamp_client_report_alive = time.time()
        elif _type == ComputeResourceActionTypes.ADD_JOB:
            # Add a job
            self._outgoing_feed.append_message(dict(
                type=ComputeResourceActionTypes.LOG,
                timestamp=time.time() - 0,
                action=action
            ))
            action['handler_uri'] = self._handler_uri
            self._add_job(action)
            self._report_action()
        elif _type == ComputeResourceActionTypes.JOB_HANDLER_FINISHED:
            self._outgoing_feed.append_message(dict(
                type=ComputeResourceActionTypes.LOG,
                timestamp=time.time() - 0,
                action=action
            ))
            self.halt()
            
        elif type == ComputeResourceActionTypes.LOG:
            pass
        else:
            raise Exception(f'Unexpected action type: {_type}') # pragma: no cover
    
    def _add_job(self, action):
        # Add a job that was sent from the client
        job_id, handler_uri, job_serialized = JobKeys._unpack_serialized_job(action)
        label = job_serialized[JobKeys.LABEL]
        if not (self._hydrate_code_for_serialized_job(job_id, job_serialized)
                and self._hydrate_container_for_serialized_job(job_id, job_serialized)):
            return
        print(f'Queuing job: {label}') # TODO: Convert to log statement
        
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
                self._mark_job_as_error(job_id=job_id, label=label, exception=job._exception, runtime_info=job._runtime_info)
                return
        # No finished or errored version of the Job was found in the cache. Thus, queue it.
        self._queue_job(job, handler_uri)
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
        assert code is not None, 'Code is None in serialized job.'
        try:
            code_obj = kp.load_object(code)
            if code_obj is None:
                raise DeserializationException("Kachery returned no serialized code for function.")
            serialized_job[JobKeys.CODE] = code_obj
        except Exception as e:
            exc = f'Error loading code for function {label}: {code} ({str(e)})'
            print(exc)
            self._mark_job_as_error(job_id=job_id, label=label, exception=Exception(exc), runtime_info=None)
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
                raise Exception("Cannot run serialized job outside of container. Use container=True.")
            ContainerManager.prepare_container(container)
        except Exception as e:
            print(f"Error preparing container for pending job: {label}\n{e}") # TODO: log
            self._mark_job_as_error(job_id=job_id, label=label, exception=e, runtime_info=None)
            return False
        return True
    def _handle_finished_job(self, job):
        print(f'Job finished: {job._job_id}') # TODO: Change to formal log statement?
        self._mark_job_as_finished(job=job)
        if self._job_cache is not None:
            self._job_cache.cache_job_result(job)
    def _queue_job(self, job:Job, handler_uri:str) -> None:
        try:
            job.download_parameter_files_if_needed()
        except Exception as e:
            print(f"Error downloading input files for job: {job._label}\n{e}")
            self._mark_job_as_error(job_id=job._job_id, label=job._label, exception=e, runtime_info=None)
            return
        self._jobs[job._job_id] = job
        self._job_handler.handle_job(job)
        job._handler_uri = handler_uri
        # self._database._mark_job_as_queued(job._job_id, self._compute_resource_id)

    def _mark_job_as_finished(self, *, job: Job) -> None:
        serialized_result = _serialize_item(job._result)
        action = {
            "type": ComputeResourceActionTypes.JOB_FINISHED,
            "timestamp": time.time() - 0,
            "job_id": job._job_id, # change this to JobKeys.JOB_ID if safe
            "label": job._label,
            JobKeys.RUNTIME_INFO: job.get_runtime_info()
        }
        # decide whether to store the result or a result_uri
        if _result_small_enough_to_store_directly(serialized_result):
            action[JobKeys.RESULT] = serialized_result
        else:
            action[JobKeys.RESULT_URI] = kp.store_object(dict(result=serialized_result))
            
        self._outgoing_feed.append_message(action)
        # self._database._mark_job_as_finished(job._job_id, self._compute_resource_id,
        #     runtime_info=job._runtime_info, result=serialized_result)
        
    def _mark_job_as_error(self, *,
            job_id: str, label: str, runtime_info: Optional[dict], exception: Optional[Exception]) -> None:
        print(f"Job error: {job_id} {label}\n{exception}") # TODO: Change to formal log statement?
        # fix the following to conform to above method:
        action = dict([
            ("type", ComputeResourceActionTypes.JOB_ERROR),
            ("timestamp", time.time() - 0),
            ("job_id", job_id),
            ("label", label),
            (JobKeys.RUNTIME_INFO, runtime_info),
            (JobKeys.EXCEPTION, str(exception))
        ])
        self._outgoing_feed.append_message(action)
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

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

class ComputeResource:
    def __init__(self, *, compute_resource_uri, job_handler, job_cache=None, compute_resource_dir):
        self._compute_resource_uri = compute_resource_uri
        self._compute_resource_dir = compute_resource_dir
        self._timestamp_event_poll = 0
        self._timestamp_last_action = time.time()
        self._job_handler = job_handler
        self._job_cache = job_cache
        self._connected_clients = dict()

        self._compute_resource_feed = kp.load_feed(self._compute_resource_uri)
        assert self._compute_resource_feed.is_writeable()
        self._registry_feed = self._compute_resource_feed.get_subfeed('job_handler_registry')
        self._registry_feed.set_position(self._registry_feed.get_num_messages())

    def cleanup(self):
        self._pipe_to_worker_process.send('exit')

    def run(self):
        self._registry_feed.append_message(dict(
            type=ComputeResourceActionTypes.COMPUTE_RESOURCE_STARTED,
            timestamp=time.time() - 0
        ))

        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        self._pipe_to_worker_process = pipe_to_child
        self._worker_process = multiprocessing.Process(
            target=_cr_worker,
            args=(pipe_to_parent, self._compute_resource_uri, self._compute_resource_dir),
        )
        self._worker_process.start()

        try:
            while True:
                self._iterate()
                time.sleep(0.02) # TODO: alternative to busy-wait?
        except:
            self.cleanup()
            raise

    def set_access_rules(self, access_rules):
        self._registry_feed.set_access_rules(dict(
            rules = [
                dict(
                    nodeId=r["node_id"],
                    write=True
                )
                for r in access_rules
            ]
        ))

    def _process_action(self, action):
        _type = action['type']
        if _type == ComputeResourceActionTypes.REGISTER_JOB_HANDLER:
            handler_uri = action['uri']
            print(f'Registering job handler: {handler_uri}')
            incoming_feed = kp.load_feed(handler_uri).get_subfeed('main')
            outgoing_feed = self._compute_resource_feed.get_subfeed(handler_uri)
            try:
                self._connected_clients[handler_uri] = ConnectedClient(compute_resource=self, handler_uri=handler_uri, incoming_feed=incoming_feed, outgoing_feed=outgoing_feed)
            except:
                traceback.print_exc()
                print('Warning: exception while creating connected client')
            self._report_action()
        elif _type == ComputeResourceActionTypes.COMPUTE_RESOURCE_STARTED:
            pass
        else:
            raise Exception(f'Unexpected action type: {_type}') # pragma: no cover

    def _iterate(self):
        # self._iterate_timer = time.time() # Never actually used

        elapsed_event_poll = time.time() - self._timestamp_event_poll
        if elapsed_event_poll > _get_poll_interval(self._timestamp_last_action):
            self._timestamp_event_poll = time.time()

            while self._pipe_to_worker_process.poll():
                action = self._pipe_to_worker_process.recv()
                self._process_action(action)

            # try:
            #     actions = self._registry_feed.get_next_messages(wait_msec=500)
            # except:
            #     traceback.print_exc()
            #     print('Error reading messages from registry feed. Perhaps daemon in down. Pausing.')
            #     time.sleep(10)
            #     actions = []
            # for action in actions:
            #     self._process_action(action)
        
            # need to create a copy of the list of clients, because they might get deleted from the dict
            clients = [c for c in self._connected_clients.values()]
            for client in clients:
                if client._finished:
                    client.cancel_all_jobs()
                    client.halt()
                    del self._connected_clients[client._handler_uri]
                else:
                    client.iterate()
                    elapsed_since_client_alive = client._timestamp_client_report_alive - time.time()
                    if elapsed_since_client_alive > 60:
                        print(f'Closing job handler: {client._handler_uri}')
                        client.cancel_all_jobs()
                        client.halt()
                        del self._connected_clients[client._handler_uri]
        
        self._job_handler.iterate()
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

def _cr_worker(pipe_to_parent: Connection, compute_resource_uri: str, compute_resource_dir: str) -> None:
    feed = kp.load_feed(compute_resource_uri)
    subfeed = feed.get_subfeed('job_handler_registry')
    subfeed.set_position(subfeed.get_num_messages())
    while True:
        if not os.path.exists(compute_resource_dir):
            print(f'Stopping compute resource working because directory does not exist: {compute_resource_dir}')
            return
        
        if pipe_to_parent.poll():
            pipe_to_parent.recv()
            return
        
        try:
            actions = subfeed.get_next_messages(wait_msec=6000)
        except:
            print('Warning: unable to get messages from job handler registry. Perhaps daemon is down...')
            time.sleep(10)
            actions = []
        if actions is not None:
            for action in actions:
                pipe_to_parent.send(action)
        time.sleep(0.3)

def _cc_worker(pipe_to_parent: Connection, job_handler_feed_uri: str) -> None:
    feed = kp.load_feed(job_handler_feed_uri)
    subfeed = feed.get_subfeed('main')
    while True:
        if pipe_to_parent.poll():
            pipe_to_parent.recv()    
            return
        try:
            actions = subfeed.get_next_messages(wait_msec=6000)
        except:
            print('Warning: unable to get messages from job handler. Perhaps daemon is down.')
            return
        if actions is not None:
            for action in actions:
                pipe_to_parent.send(action)
        time.sleep(0.3)