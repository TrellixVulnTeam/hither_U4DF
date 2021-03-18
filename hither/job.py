import hashlib
import json
from typing import Callable, Union, TYPE_CHECKING, Any, List, Dict
import time
from copy import deepcopy
import kachery_p2p as kp
from ._enums import JobStatus
from ._execute_job import _execute_job
from ._util import _random_string
from ._util import _serialize_item
from ._util import _flatten_nested_collection, _copy_structure_with_changes
from ._containermanager import ContainerManager
if TYPE_CHECKING:
    from ._basejobhandler import BaseJobHandler
    from .jobcache import JobCache
    from ._jobmanager import _JobManager

class Job:
    def __init__(
        self, *,
        job_id: Union[None, str],
        f: Union[None, Callable],
        code_uri: Union[None, str],
        wrapped_function_arguments,
        job_handler: 'BaseJobHandler',
        job_cache: 'JobCache',
        job_manager: '_JobManager',
        container: Union[None, str],
        label: str,
        force_run: bool,
        rerun_failing: bool,
        cache_failing: bool,
        required_files: Union[None, str, List, Dict],
        jhparams: Dict,
        function_name: str,
        function_version: str,
        job_timeout: Union[None, float]
    ):
        if job_id is None:
            job_id = _random_string(15)
        self._job_id = str(job_id)
        self._f = f
        self._code_uri = code_uri
        self._wrapped_function_arguments = wrapped_function_arguments
        self._job_handler = job_handler
        self._job_cache = job_cache
        self._job_manager = job_manager
        self._container = container
        self._label = label
        self._force_run = force_run
        self._rerun_failing = rerun_failing
        self._cache_failing = cache_failing
        self._required_files = required_files
        self._jhparams = jhparams
        self._function_name = function_name
        self._function_version = function_version
        self._job_timeout = job_timeout

        self._status: JobStatus = JobStatus.PENDING
        self._result = None
        self._runtime_info = None
        self._exception = Exception()

        self._on_status_changed_callbacks = []

    def get_job_id(self):
        return self._job_id

    def wait(self, timeout: Union[float, None]=None) -> Union[None, Any]:
        timer = time.time()
        while True:
            self._job_manager.iterate()
            if self._status == JobStatus.FINISHED:
                return self._result
            elif self._status == JobStatus.ERROR:
                raise self._exception
            elif self._status == JobStatus.QUEUED:
                pass
            elif self._status == JobStatus.RUNNING:
                pass
            else:
                raise Exception(f'Unexpected job status: {self._status}')
            if timeout == 0:
                return None
            time.sleep(0.02)
            elapsed = time.time() - timer
            # Not the same as the job timeout... this is the wait timeout
            if timeout is not None and elapsed > timeout:
                return None
    
    def get_container(self) -> Union[None, str]:
        return self._container
    
    def get_status(self) -> JobStatus:
        return self._status
    
    def get_label(self) -> str:
        return self._label
    
    def get_function_name(self) -> str:
        return self._function_name
    
    def get_function_version(self) -> str:
        return self._function_version
    
    def get_result(self) -> Any:
        if self._status is JobStatus.ERROR:
            raise self._exception
        if self._status is not JobStatus.FINISHED:
            raise Exception('Cannot get result of job that is not finished.')
        return self._result
    
    def get_exception(self) -> Exception:
        if self._status is not JobStatus.ERROR:
            raise Exception('Cannot get exception of job that does not have error status')
        return self._exception
    
    def get_jhparams(self) -> dict:
        return self._jhparams
    
    def set_label(self, label) -> 'Job':
        self._label = label
        return self

    def get_runtime_info(self) -> Union[None, dict]:
        if self._runtime_info is None:
            return None
        return deepcopy(self._runtime_info)
    
    def print_console_out(self) -> None:
        if self._status not in JobStatus.complete_statuses():
            # don't print anything if the job is not complete
            return
        runtime_info = self.get_runtime_info()
        if runtime_info is None:
            return
        if 'console_out' not in runtime_info:
            return
        _print_console_out(runtime_info['console_out'])
    
    def cancel(self):
        assert self._job_handler is not None, 'Cannot cancel a job that does not have a job handler'
        self._job_handler.cancel_job(job_id=self._job_id)
    
    def compute_hash(self):
        return _compute_job_hash(
            function_name=self._function_name,
            function_version=self._function_version,
            serialized_args=_serialize_item(self._wrapped_function_arguments)
        )
    
    def prepare_container_if_needed(self) -> None:
        """Calls global container manager to ensure container images are downloaded, if a container is
        required for the Job. On container fetch error, set error status and record the exception in the Job.
        """
        # No need to prepare a container if none was specified
        if self._container is None: return
        # If we are attached to a remote job handler, the container is actually needed on the
        # remote resource, not the machine where we're currently executing. Don't prepare anything.
        if self._job_handler is not None and self._job_handler.is_remote: return
        try:
            ContainerManager.prepare_container(self._container)
        except:
            exception = Exception(f"Unable to prepare container for Job {self._label}: {self._container}")
            self._set_error_status(exception=exception, runtime_info=dict())
    
    def load_required_files_if_needed(self) -> None:
        if self._required_files is None: return
        if self._job_handler is not None and self._job_handler.is_remote: return
        uri_list: List[str] = _flatten_nested_collection(self._required_files, _type=str)
        for uri in uri_list:
            if uri.startswith('sha1://') or uri.startswith('sha1dir://'):
                try:
                    x = kp.load_file(uri)
                except:
                    exception = Exception(f"Unable to load file for Job {self._label}: {uri}")
                    self._set_error_status(exception=exception, runtime_info=dict())
                    return
                if x is None:
                    exception = Exception(f"Unable to load file for Job (*) {self._label}: {uri}")
                    self._set_error_status(exception=exception, runtime_info=dict())
    
    def is_ready_to_run(self) -> bool:
        """Checks current status and status of Jobs this Job depends on, to determine whether this
        Job can be run. If the job depends on errored jobs, then the status is switched
        to error.

        Raises:
            NotImplementedError: For _same_hash_as functionality.

        Returns:
            bool -- True if this Job can be run (does not depend on any jobs that are not finished)
        """
        if self._status is not JobStatus.QUEUED: return False
        wrapped_jobs: List[Job] = _flatten_nested_collection(self._wrapped_function_arguments, _type=Job)
        # Check if we depend on any Job that's in error status. If we do, we are in error status,
        # since that dependency is now unresolvable
        errored_jobs: List[Job] = [e for e in wrapped_jobs if e._status == JobStatus.ERROR]
        if errored_jobs:
            self._status = JobStatus.ERROR
            first_exception = errored_jobs[0].get_exception()
            self._exception = Exception(f'Error in dependent job: {str(first_exception)}')
            return False

        # If any job we depend on is still not finished, we are not ready to run
        nonfinished_jobs: List[Job] = [j for j in wrapped_jobs if j._status is not JobStatus.FINISHED]
        if nonfinished_jobs:
            return False

        # in the absence of any Job dependency issues, assume we are ready to run
        return True

    def resolve_dependent_job_values(self) -> None:
        self._wrapped_function_arguments = \
            _copy_structure_with_changes(self._wrapped_function_arguments,
                lambda arg: arg.get_result(), _type = Job, _as_side_effect = False)

    def on_status_changed(self, cb): 
        self._on_status_changed_callbacks.append(cb)
    
    def _set_finished_status(self, result, runtime_info):
        self._result = result
        self._runtime_info = runtime_info
        self._set_status(JobStatus.FINISHED)
    
    def _set_error_status(self, exception, runtime_info):
        self._exception = exception
        self._runtime_info = runtime_info
        self._set_status(JobStatus.ERROR)

    def _set_status(self, status: JobStatus):
        if self._status == status:
            return
        self._status = status
        for cb in self._on_status_changed_callbacks:
            cb()
    
    def _execute(self, cancel_filepath: Union[None, str]=None) -> None:
        return _execute_job(self, cancel_filepath=cancel_filepath)

def _print_console_out(x):
    for a in x['lines']:
        t = _fmt_time(a['timestamp'])
        txt = a['text']
        print(f'{t}: {txt}')

def _compute_job_hash(*, function_name, function_version, serialized_args):
    hash_object = {
        'function_name': function_name,
        'function_Version': function_version,
        'args': serialized_args
    }
    return _get_object_hash(hash_object)

def _get_object_hash(hash_object: dict):
    return _sha1_of_object(hash_object)

def _sha1_of_string(txt: str) -> str:
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret

def _sha1_of_object(obj: object) -> str:
    txt = json.dumps(obj, sort_keys=True, separators=(',', ':'))
    return _sha1_of_string(txt)

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()