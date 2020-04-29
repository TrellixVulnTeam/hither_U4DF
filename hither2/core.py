from copy import deepcopy
import inspect
import sys
import time
from typing import Union, Any
import os

from ._config import config
from ._enums import JobStatus
from .file import File
from .job import Job
from .jobcache import JobCache
import kachery as ka
from ._shellscript import ShellScript
from ._util import _random_string, _docker_form_of_container_string, _deserialize_item, _serialize_item

_default_global_config = dict(
    container=None,
    job_handler=None,
    job_cache=None,
    download_results=None,
    job_timeout=None
)

config.set_default_config(_default_global_config)


# TODO: QUERY: Is this functionality needed, can we remove it entirely? Or do we need to support it
# in the rewrite of configuration
def set_config(
        container: Union[str, bool, None]=None,
        job_handler: Any=None,
        job_cache: Union[JobCache, None]=None,
        download_results: Union[bool, None]=None,
        job_timeout: Union[float, None]=None
) -> None:
    """Set hither2 configuration parameters.
    
    Usually you will want to instead use the context manager
    form of this function:
    ```
    with hi.config(...):
        ...
    ```
    See help for the `config` function.
    Any parameter that is left as the default None
    will not be modified.
    """
    # _global_config.set_config(
    #     container=container,
    #     job_handler=job_handler,
    #     job_cache=job_cache,
    #     download_results=download_results,
    #     job_timeout=job_timeout
    # )
    pass


class _JobManager:
    def __init__(self) -> None:
        self._queued_jobs = dict()
        self._running_jobs = dict()
    def queue_job(self, job):
        job._status = JobStatus.QUEUED
        self._queued_jobs[job._job_id] = job
    def iterate(self):
        # Called periodically during wait()
    
        # Check which queued jobs are ready to run
        queued_job_ids = list(self._queued_jobs.keys())

        # Check which containers need to be prepared (pulled or built)
        for id in queued_job_ids:
            job: Job = self._queued_jobs[id]
            if job._container is not None:
                if not job._job_handler.is_remote:
                    try:
                        _prepare_container(job._container)
                    except:
                        job._status = JobStatus.ERROR
                        job._exception = Exception(f'Unable to prepare container for job {job._label}: {job._container}')

        # Check which queued jobs are ready to run (and remove jobs where status!='queued')
        for id in queued_job_ids:
            job: Job = self._queued_jobs[id]
            if job._status != JobStatus.QUEUED:
                del self._queued_jobs[id]
            elif not hasattr(job, '_same_hash_as'):
                if  self._job_is_ready_to_run(job):
                    del self._queued_jobs[id]
                    if _some_jobs_have_status(job._wrapped_function_arguments, [JobStatus.ERROR]):
                        exc = _get_first_job_exception_in_item(job._wrapped_function_arguments)
                        job._status = JobStatus.ERROR
                        job._exception = Exception(f'Exception in argument. {str(exc)}')
                    else:
                        self._running_jobs[id] = job
                        job._wrapped_function_arguments = _resolve_job_values(job._wrapped_function_arguments)
                        if job._job_cache is not None:
                            if not job._job_handler.is_remote:
                                job._job_cache.check_job(job)
                        if job._status == JobStatus.QUEUED:
                            # still queued even after checking the cache
                            print(f'')
                            print(f'Handling job: {job._label}')
                            job._status = JobStatus.RUNNING
                            job._job_handler.handle_job(job)

        # Check which running jobs are finished and iterate job handlers of running or preparing jobs
        running_job_ids = list(self._running_jobs.keys())
        for id in running_job_ids:
            job: Job = self._running_jobs[id]
            if job._status == JobStatus.RUNNING:
                # Note: we effectively iterate the same job handler potentially many times here -- I think that's okay but not 100% sure.
                job._job_handler.iterate()
            if job._status in JobStatus.get_complete_statuses():
                if job._download_results:
                    _download_files_as_needed_in_item(job._result)
                if job._job_cache is not None:
                    if not job._job_handler.is_remote:
                        job._job_cache.cache_job_result(job)
                del self._running_jobs[id]
    
    def reset(self):
        self._queued_jobs = dict()
        self._running_jobs = dict()
    
    def wait(self, timeout: Union[float, None]=None):
        timer = time.time()
        while True:
            self.iterate()
            if self._queued_jobs == {} and self._running_jobs == {}:
                return
            if timeout == 0:
                return
            time.sleep(0.02)
            elapsed = time.time() - timer
            if timeout is not None and elapsed > timeout:
                return
    
    def _job_is_ready_to_run(self, job):
        assert job._status == JobStatus.QUEUED
        if _some_jobs_have_status(job._wrapped_function_arguments, [JobStatus.ERROR]):
            # In this case the job will error due to an error input
            return True
        if _some_jobs_have_status(job._wrapped_function_arguments, JobStatus.get_incomplete_statuses()):
            return False
        return True
    
_global_job_manager = _JobManager()

def reset():
    _global_job_manager.reset()
    config.set_default_config(_default_global_config)

def container(container):
    assert container.startswith('docker://'), f"Container string {container} must begin with docker://"
    def wrap(f):
        setattr(f, '_hither_container', container)
        return f
    return wrap

def opts(no_resolve_input_files=None):
    def wrap(f):
        if no_resolve_input_files is not None:
            setattr(f, '_no_resolve_input_files', no_resolve_input_files)
        return f
    return wrap


def additional_files(additional_files):
    def wrap(f):
        setattr(f, '_hither_additional_files', additional_files)
        return f
    return wrap

def local_modules(local_modules):
    def wrap(f):
        setattr(f, '_hither_local_modules', local_modules)
        return f
    return wrap

def wait(timeout: Union[float, None]=None):
    _global_job_manager.wait(timeout)

_global_registered_functions_by_name = dict()

# run a registered function by name
def run(function_name, **kwargs):
    assert function_name in _global_registered_functions_by_name, f'Hither function {function_name} not registered'
    f = _global_registered_functions_by_name[function_name]
    return f.run(**kwargs)

############################################################
def function(name, version):
    def wrap(f):
        # register the function
        assert f.__name__ == name, f"Name does not match function name: {name} <> {f.__name__}"
        if name in _global_registered_functions_by_name:
            path1 = _function_path(f)
            path2 = _function_path(_global_registered_functions_by_name[name])
            if path1 != path2:
                print(f"Warning: Hither function with name {name} is registered in two different files: {path1} {path2}")
        else:
            _global_registered_functions_by_name[name] = f
        
        def run(**arguments_for_wrapped_function):
            configured_container = config.get_current_config_value('container')
            if configured_container is True:
                container = getattr(f, '_hither_container', None)
            elif configured_container is not None and configured_container is not False:
                container = configured_container
            else:
                container=None
            job_handler = config.get_current_config_value('job_handler')
            job_cache = config.get_current_config_value('job_cache')
            if job_handler is None:
                job_handler = _global_job_handler
            download_results = config.get_current_config_value('download_results')
            if download_results is None:
                download_results = False
            job_timeout = config.get_current_config_value('job_timeout')
            label = name
            if hasattr(f, '_no_resolve_input_files'):
                no_resolve_input_files = f._no_resolve_input_files
            else:
                no_resolve_input_files = False
            job = Job(f=f, wrapped_function_arguments=arguments_for_wrapped_function,
                      job_manager=_global_job_manager, job_handler=job_handler, job_cache=job_cache,
                      container=container, label=label, download_results=download_results,
                      function_name=name, function_version=version,
                      job_timeout=job_timeout, no_resolve_input_files=no_resolve_input_files)
            _global_job_manager.queue_job(job)
            return job
        setattr(f, 'run', run)
        return f
    return wrap

# TODO: replace all this recursive dict-traversing functionality with a utility, perhaps of this form:
# def traverse_dict(x, func):
#    .... execute the function on every item, recursively
# We'll also need something like
# def filt_dict(x, func):
#    .... same as above except returns a new dict
# This will greatly improve code coverage
def _download_files_as_needed_in_item(x):
    if isinstance(x, File):
        info0 = ka.get_file_info(x._sha1_path, fr=None)
        if info0 is None:
            remote_handler = getattr(x, '_remote_job_handler')
            assert remote_handler is not None, f'Unable to find file: {x._sha1_path}'
            a = remote_handler._load_file(x._sha1_path)
            assert a is not None, f'Unable to load file {x._sha1_path} from remote compute resource: {remote_handler._compute_resource_id}'
        else:
            pass
    elif type(x) == dict:
        for val in x.values():
            _download_files_as_needed_in_item(val)
    elif type(x) == list:
        for val in x:
            _download_files_as_needed_in_item(val)
    elif type(x) == tuple:
        for val in x:
            _download_files_as_needed_in_item(val)
    else:
        pass

class DefaultJobHandler:
    def __init__(self):
        self.is_remote = False
    def handle_job(self, job):
        job._execute()
    def cancel_job(self, job_id):
        print('Warning: not yet able to cancel job of defaultjobhandler')
    def iterate(self):
        pass

_global_job_handler = DefaultJobHandler()


def _deserialize_job(serialized_job):
    return Job._deserialize(serialized_job)

def _some_jobs_have_status(x, status_list):
    if isinstance(x, Job):
        if x._status in status_list:
            return True
    elif type(x) == dict:
        for v in x.values():
            if _some_jobs_have_status(v, status_list):
                return True
    elif type(x) == list:
        for v in x:
            if _some_jobs_have_status(v, status_list):
                return True
    elif type(x) == tuple:
        for v in x:
            if _some_jobs_have_status(v, status_list):
                return True
    return False

def _get_first_job_exception_in_item(x):
    if isinstance(x, Job):
        if x._status == JobStatus.ERROR:
            return f'{x._label}: {str(x._exception)}'
    elif type(x) == dict:
        for v in x.values():
            exc = _get_first_job_exception_in_item(v)
            if exc is not None:
                return exc
    elif type(x) == list:
        for v in x:
            exc = _get_first_job_exception_in_item(v)
            if exc is not None:
                return exc
    elif type(x) == tuple:
        for v in x:
            exc = _get_first_job_exception_in_item(v)
            if exc is not None:
                return exc
    return None

def _resolve_job_values(x):
    if isinstance(x, Job):
        return x._result
    elif type(x) == dict:
        ret = dict()
        for k, v in x.items():
            ret[k] = _resolve_job_values(v)
        return ret
    elif type(x) == list:
        return [_resolve_job_values(v) for v in x]
    elif type(x) == tuple:
        return tuple([_resolve_job_values(v) for v in x])
    else:
        return x

_prepared_singularity_containers = dict()
_prepared_docker_images = dict()

def _prepare_container(container):
    if os.getenv('HITHER_USE_SINGULARITY', None) == 'TRUE':
        if container not in _prepared_singularity_containers:
            _do_prepare_singularity_container(container)
            _prepared_singularity_containers[container] = True
    else:
        if os.getenv('HITHER_DO_NOT_PULL_DOCKER_IMAGES', None) != 'TRUE':
            if container not in _prepared_docker_images:
                _do_pull_docker_image(container)
                _prepared_docker_images[container] = True


def _do_prepare_singularity_container(container):
    print(f'Building singularity container: {container}')
    ss = ShellScript(f'''
        #!/bin/bash

        exec singularity run {container} echo "built {container}"
    ''')
    ss.start()
    retcode = ss.wait()
    if retcode != 0:
        raise Exception(f'Problem building container {container}')

def _do_pull_docker_image(container):
    print(f'Pulling docker container: {container}')
    container = _docker_form_of_container_string(container)
    if (sys.platform == "win32"):
        if 1: # pragma: no cover
            ss = ShellScript(f'''
                docker pull {container}
            ''')
    else:
        ss = ShellScript(f'''
            #!/bin/bash
            set -ex
            
            exec docker pull {container}
        ''')
    ss.start()
    retcode = ss.wait()
    if retcode != 0:
        raise Exception(f'Problem pulling container {container}')

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))