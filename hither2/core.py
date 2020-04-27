import time
from typing import Union, Any
import os
import sys
import inspect
import kachery as ka
from copy import deepcopy
from ._etconf import ETConf
from ._shellscript import ShellScript
from ._generate_source_code_for_function import _generate_source_code_for_function
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._util import _random_string, _docker_form_of_container_string, _deserialize_item, _serialize_item
from .jobcache import JobCache
from .file import File
from ._resolve_files_in_item import _resolve_files_in_item, _deresolve_files_in_item
from ._enums import JobStatus

# TODO: think about splitting this file into pieces

_default_global_config = dict(
    container=None,
    job_handler=None,
    job_cache=None,
    download_results=None,
    job_timeout=None
)

_global_config = ETConf(
    defaults=_default_global_config
)

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
    _global_config.set_config(
        container=container,
        job_handler=job_handler,
        job_cache=job_cache,
        download_results=download_results,
        job_timeout=job_timeout
    )

class config:
    def __init__(self,
        container: Union[str, bool, None]=None,
        job_handler: Any=None,
        job_cache: Union[JobCache, None]=None,
        download_results: Union[bool, None]=None,
        job_timeout: Union[float, None]=None
    ):
        """Set hither2 config parameters in a context manager.

        Example usage:
        ```
        import hither2 as hi
        with hi.config(container=True):
            # code goes here
        ```
        Parameters set to None are left unchanged
        
        Parameters
        ----------
        container : Union[str, bool, None], optional
            If bool, controls whether to use the default docker container specified for each function job
            If str, use the docker container given by the string, by default None
        job_handler : Any, optional
            The job handler to use for each function job, by default None
        job_cache : Union[JobCache, None], optional
            The job cache to use for each function job, by default None
        download_results : Union[bool, None], optional
            Whether to download results after the function job runs (applied to remote job handler), by default None
        job_timeout : Union[float, None], optional
            A timeout time (in seconds) for each function job, by default None
        """
        self._config = dict(
            container=container,
            job_handler=job_handler,
            job_cache=job_cache,
            download_results=download_results,
            job_timeout=job_timeout
        )
        self._old_config = None
    def __enter__(self):
        self._old_config = _global_config.get_config()
        set_config(**self._config)
    def __exit__(self, exc_type, exc_val, exc_tb):
        _global_config.set_full_config(self._old_config)

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
                    if _some_jobs_have_status(job._kwargs, [JobStatus.ERROR]):
                        exc = _get_first_job_exception_in_item(job._kwargs)
                        job._status = JobStatus.ERROR
                        job._exception = Exception(f'Exception in argument. {str(exc)}')
                    else:
                        self._running_jobs[id] = job
                        job._kwargs = _resolve_job_values(job._kwargs)
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
        if _some_jobs_have_status(job._kwargs, [JobStatus.ERROR]):
            # In this case the job will error due to an error input
            return True
        if _some_jobs_have_status(job._kwargs, JobStatus.get_incomplete_statuses()):
            return False
        return True
    
def _files_are_available_locally_in_item(x):
    if isinstance(x, File):
        info0 = ka.get_file_info(x._sha1_path, fr=None)
        if info0 is None:
            return False
        else:
            pass
    elif type(x) == dict:
        for val in x.values():
            if not _files_are_available_locally_in_item(val):
                return False
    elif type(x) == list:
        for val in x:
            if not _files_are_available_locally_in_item(val):
                return False
    elif type(x) == tuple:
        for val in x:
            if not _files_are_available_locally_in_item(val):
                return False
    else:
        pass
    return True

_global_job_manager = _JobManager()

def reset():
    _global_job_manager.reset()
    set_config(**_default_global_config)

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
        
        def run(**kwargs):
            if _global_config.get('container') is True:
                container = getattr(f, '_hither_container', None)
            elif _global_config.get('container') is not None and _global_config.get('container') is not False:
                container = _global_config.get('container')
            else:
                container=None
            job_handler = _global_config.get('job_handler')
            job_cache = _global_config.get('job_cache')
            if job_handler is None:
                job_handler = _global_job_handler
            download_results = _global_config.get('download_results')
            if download_results is None:
                download_results = False
            job_timeout = _global_config.get('job_timeout')
            label = name
            if hasattr(f, '_no_resolve_input_files'):
                no_resolve_input_files = f._no_resolve_input_files
            else:
                no_resolve_input_files = False
            job = Job(f=f, kwargs=kwargs, job_manager=_global_job_manager, job_handler=job_handler, job_cache=job_cache, container=container, label=label, download_results=download_results, job_timeout=job_timeout, no_resolve_input_files=no_resolve_input_files)
            _global_job_manager.queue_job(job)
            return job
        setattr(f, 'run', run)
        # These attributes should definitely not be set, and instead values should be passed to Job() above.
        setattr(f, '_hither_name', name)
        setattr(f, '_hither_version', version)
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

# TODO: put this class in a separate .py file
class Job:
    def __init__(self, *, f, kwargs, job_manager, job_handler, job_cache, container, label, download_results, job_timeout: Union[float, None], code=None, function_name=None, function_version=None, job_id=None, no_resolve_input_files=False):
        self._f = f
        self._code = code
        self._function_name = function_name
        self._function_version = function_version
        self._no_resolve_input_files = no_resolve_input_files
        self._label = label
        self._kwargs = _deresolve_files_in_item(kwargs)
        # self._kwargs = _deserialize_item(_serialize_item(self._kwargs))
        self._job_id = job_id
        if self._job_id is None:
            self._job_id = _random_string(15)
        self._container = container
        self._download_results = download_results
        self._job_timeout = None

        self._status = JobStatus.PENDING
        self._result = None
        self._runtime_info = None
        self._exception: Union[Exception, None] = None

        self._job_handler = job_handler
        self._job_manager = job_manager
        self._job_cache = job_cache
        
        # this is used by wait() in the case where results were not downloaded
        # and the job has already been sent to the remote compute resource
        self._substitute_job_for_wait: Union[None, Job] = None

        # This code will go away
        if self._function_name is None:
            self._function_name = getattr(self._f, '_hither_name')
        if self._function_version is None:
            self._function_version = getattr(self._f, '_hither_version')
        
        # Not used for now
        self._efficiency_job_hash_ = None

        # If the job handler is local, then we need to be sure to mark remote jobs with _download_results=True
        # Note, though, that this will only work if the remote jobs have not yet been sent to the compute resource
        # In that case, we need to insert another 'identity' job
        if self._job_handler is None or (not self._job_handler.is_remote):
            _mark_download_results_for_remote_jobs_in_item(self._kwargs)

    def wait(self, timeout: Union[float, None]=None, resolve_files=True):
        if resolve_files and self._substitute_job_for_wait is not None:
            assert self._substitute_job_for_wait is not None # for pyright
            return self._substitute_job_for_wait.wait(timeout=timeout, resolve_files=resolve_files)

        timer = time.time()

        if resolve_files and self._job_handler.is_remote:
            # in this case, we need to make sure that files are downloaded from the remote resource
            if not self._download_results:
                if self._status in JobStatus.get_prerun_statuses():
                    # it's not too late. Let's just request download now
                    self._download_results = True
                else:
                    # here is where we make a substitute job
                    # let's wait until finished, and then we'll see what we need to do
                    result = self.wait(timeout=timeout, resolve_files=False)
                    if result is None:
                        return None
                    if not _files_are_available_locally_in_item(result):
                        from ._identity import identity
                        assert self._substitute_job_for_wait is None, 'Unexpected at this point in the code: self._substitute_job_for_wait is not None'
                        with config(job_handler=self._job_handler, download_results=True):
                            self._substitute_job_for_wait = identity.run(x=result)
                        # compute the remainder timeout for this call to wait()
                        timeout2 = timeout
                        if timeout2 is not None:
                            elapsed = time.time() - timer
                            timeout2 = max(0, timeout2 - elapsed)
                        return self._substitute_job_for_wait.wait(timeout=timeout2, resolve_files=resolve_files)
                    else:
                        return _resolve_files_in_item(self._result)
        while True:
            self._job_manager.iterate()
            if self._status == JobStatus.FINISHED:
                if resolve_files:
                    return _resolve_files_in_item(self._result)
                else:
                    return self._result
            elif self._status == JobStatus.ERROR:
                assert self._exception is not None
                raise self._exception
            elif self._status == JobStatus.QUEUED:
                pass
            elif self._status == JobStatus.RUNNING:
                pass
            else:
                raise Exception(f'Unexpected status: {self._status}') # pragma: no cover
            if timeout == 0:
                return None
            time.sleep(0.02)
            elapsed = time.time() - timer
            # Not the same as the job timeout... this is the wait timeout
            if timeout is not None and elapsed > timeout:
                return None
    def status(self):
        return self._status
    def result(self):
        if self._status == JobStatus.FINISHED:
            return self._result
        raise Exception('Cannot get result of job that is not yet finished.')
    def exception(self):
        if self._status == JobStatus.ERROR:
            assert self._exception is not None
        return self._exception
    def set(self, *, label=None):
        if label is not None:
            self._label = label
        return self
    def runtime_info(self):
        return deepcopy(self._runtime_info)
    def _execute(self):
        if self._container is not None:
            job_serialized = self._serialize(generate_code=True)
            success, result, runtime_info, error = _run_serialized_job_in_container(job_serialized)
            self._runtime_info = runtime_info
            if success:
                self._result = result
                self._status = JobStatus.FINISHED
            else:
                assert error is not None
                assert error != 'None'
                self._exception = Exception(error)
                self._status = JobStatus.ERROR
        else:
            assert self._f is not None, 'Cannot execute job outside of container when function is not available'
            try:
                if not self._no_resolve_input_files:
                    kwargs = _resolve_files_in_item(self._kwargs)
                else:
                    kwargs = self._kwargs
                ret = self._f(**kwargs)
                self._result = _deresolve_files_in_item(ret)
                # self._result = _deserialize_item(_serialize_item(ret))
                self._status = JobStatus.FINISHED
            except Exception as e:
                self._status = JobStatus.ERROR
                self._exception = e
    def _serialize(self, generate_code):
        function_name = self._function_name
        function_version = self._function_version
        if generate_code:
            if self._code is not None:
                code = self._code
            else:
                assert self._f is not None, 'Cannot serialize function with generate_code=True when function and code are both not available'
                additional_files = getattr(self._f, '_hither_additional_files', [])
                local_modules = getattr(self._f, '_hither_local_modules', [])
                code = _generate_source_code_for_function(self._f, name=function_name, additional_files=additional_files, local_modules=local_modules)
            function = None
        else:
            assert self._f is not None, 'Cannot serialize function with generate_code=False when function is not available'
            code = None
            function = self._f
        x = dict(
            job_id=self._job_id,
            function=function,
            code=code,
            function_name=function_name,
            function_version=function_version,
            label=self._label,
            kwargs=_serialize_item(self._kwargs),
            container=self._container,
            download_results=self._download_results,
            job_timeout=self._job_timeout,
            no_resolve_input_files=self._no_resolve_input_files
        )
        x = _serialize_item(x)
        return x
    
    def _efficiency_job_hash(self):
        # For purpose of efficiently handling the exact same job queued multiple times simultaneously
        # Important: this is NOT the hash used to lookup previously-run jobs in the cache
        # NOTE: this is not used for now
        if self._efficiency_job_hash_ is not None:
            return self._efficiency_job_hash_
        efficiency_job_hash_obj = dict(
            function_name=self._function_name,
            function_version=self._function_version,
            kwargs=_serialize_item(self._kwargs),
            container=self._container,
            download_results=self._download_results,
            job_timeout=self._job_timeout,
            no_resolve_input_files=self._no_resolve_input_files
        )
        self._efficiency_job_hash_ = ka.get_object_hash(efficiency_job_hash_obj)
        return self._efficiency_job_hash_
    
    @staticmethod
    def _deserialize(serialized_job, job_manager=None):
        j = serialized_job
        return Job(
            f=j['function'],
            code=j['code'],
            function_name=j['function_name'],
            function_version=j['function_version'],
            label=j['label'],
            kwargs=_deserialize_item(j['kwargs']),
            container=j['container'],
            download_results=j.get('download_results', False),
            job_timeout=j.get('job_timeout', None),
            job_manager=job_manager,
            job_handler=None,
            job_cache=None,
            job_id=j['job_id'],
            no_resolve_input_files=j['no_resolve_input_files']
        )

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

def _mark_download_results_for_remote_jobs_in_item(x):
    # If the job handler is local, then we need to be sure to mark remote jobs with _download_results=True
    # Note, though, that this will only work if the remote jobs have not yet been sent to the compute resource
    # In that case, we need to insert another 'identity' job
    if isinstance(x, Job):
        if x._job_handler.is_remote:
            if x._status in JobStatus.get_prerun_statuses():
                x._download_results = True
                return x
            else:
                from ._identity import identity
                with config(job_handler=x._job_handler, download_results=True, container=True):
                    return identity.run(x=x)
    elif type(x) == dict:
        ret = dict()
        for k, v in x.items():
            ret[k] = _mark_download_results_for_remote_jobs_in_item(v)
        return ret
    elif type(x) == list:
        return [_mark_download_results_for_remote_jobs_in_item(v) for v in x]
    elif type(x) == tuple:
        return tuple([_mark_download_results_for_remote_jobs_in_item(v) for v in x])
    else:
        return x

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