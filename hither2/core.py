from typing import Dict, Union
import os
import sys
import random
import time
import numpy as np
import kachery as ka
from ._etconf import ETConf
from ._shellscript import ShellScript
from ._temporarydirectory import TemporaryDirectory
from ._generate_source_code_for_function import _generate_source_code_for_function
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._util import _random_string, _docker_form_of_container_string

_global_config = ETConf(
    defaults=dict(
        container=None,
        # cache=None,
        # cache_failing=None,
        # rerun_failing=None,
        # force_run=None,
        # gpu=None,
        # exception_on_fail=None, # None means True
        # job_handler=None,
        # show_console=None, # None means True
        # show_cached_console=None, # None means False
        # job_timeout=None,
        # log_path=None
    )
)

def set_config(
        container: Union[str, None]=None,
        # cache: Union[str, dict, None]=None,
        # cache_failing: Union[bool, None]=None,
        # rerun_failing: Union[bool, None]=None,
        # force_run: Union[bool, None]=None,
        # gpu: Union[bool, None]=None,
        # exception_on_fail: Union[bool, None]=None,
        # job_handler: Union[Any, None]=None,
        # show_console: Union[bool, None]=None,
        # show_cached_console: Union[bool, None]=None,
        # job_timeout: Union[float, None]=None,
        # log_path: Union[str, None]=None
) -> None:
    _global_config.set_config(
        container=container,
        # cache=cache,
        # force_run=force_run,
        # cache_failing=cache_failing,
        # rerun_failing=rerun_failing,
        # gpu=gpu,
        # exception_on_fail=exception_on_fail,
        # job_handler=job_handler,
        # show_console=show_console,
        # show_cached_console=show_cached_console,
        # job_timeout=job_timeout,
        # log_path=log_path
    )

class config:
    def __init__(self,
        container: Union[str, None]=None,
        # cache: Union[str, dict, None]=None,
        # cache_failing: Union[bool, None]=None,
        # rerun_failing: Union[bool, None]=None,
        # force_run: Union[bool, None]=None,
        # gpu: Union[bool, None]=None,
        # exception_on_fail: Union[bool, None]=None,
        # job_handler: Union[Any, None]=None,
        # show_console: Union[bool, None]=None,
        # show_cached_console: Union[bool, None]=None,
        # job_timeout: Union[float, None]=None,
        # log_path: Union[str, None]=None
    ):
        self._config = dict(
            container=container,
            # cache=cache,
            # cache_failing=cache_failing,
            # rerun_failing=rerun_failing,
            # force_run=force_run,
            # gpu=gpu,
            # exception_on_fail=exception_on_fail,
            # job_handler=job_handler,
            # show_console=show_console,
            # show_cached_console=show_cached_console,
            # job_timeout=job_timeout,
            # log_path=log_path
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
        job._status = 'queued'
        self._queued_jobs[job._job_id] = job
    def iterate(self):
        # Called periodically during wait()
    
        # Check which queued jobs are ready to run
        queued_job_ids = list(self._queued_jobs.keys())

        # Check which containers need to be prepared (pulled or built)
        for id in queued_job_ids:
            job: Job = self._queued_jobs[id]
            if job._container is not None:
                # TODO: check whether we are even going to run this locally
                _prepare_container(job._container)

        # Check which queued jobs are ready to run
        for id in queued_job_ids:
            job: Job = self._queued_jobs[id]
            if self._job_is_ready_to_run(job):
                del self._queued_jobs[id]
                if _some_jobs_have_errors(job._kwargs):
                    job._status = 'error'
                    job._exception = Exception('Exception in argument.')
                    return
                self._running_jobs[id] = job
                job._kwargs = _resolve_job_values(job._kwargs)
                job._status = 'running'
                job._job_handler.handle_job(job)

        # Check which running jobs are finished
        running_job_ids = list(self._running_jobs.keys())
        for id in running_job_ids:
            job: Job = self._running_jobs[id]
            if job._status != 'running':
                del self._running_jobs[id]
    
    def _job_is_ready_to_run(self, job):
        assert job._status == 'queued'
        if _some_jobs_are_still_queued(job._kwargs):
            return False
        return True

_global_job_manager = _JobManager()

def container(container):
    def wrap(f):
        setattr(f, '_hither_container', container)
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

def function(name, version):
    def wrap(f):
        def run(**kwargs):
            if '_label' in kwargs:
                label = kwargs['_label']
                del kwargs['_label']
            else:
                label = None
            if _global_config.get('container') is True:
                container = getattr(f, '_hither_container', None)
            elif _global_config.get('container') is not None and _global_config.get('container') is not False:
                container = _global_config.get('container')
            else:
                container=None
            job = Job(f=f, kwargs=kwargs, job_manager=_global_job_manager, job_handler=_global_job_handler, container=container, label=label)
            _global_job_manager.queue_job(job)
            return job
        setattr(f, 'run', run)
        setattr(f, '_hither_name', name)
        setattr(f, '_hither_version', version)
        return f
    return wrap

class DefaultJobHandler:
    def __init__(self):
        pass
    def handle_job(self, job):
        job._execute()

_global_job_handler = DefaultJobHandler()

class Job:
    def __init__(self, *, f, kwargs, job_manager, job_handler, container, label):
        self._f = f
        self._label = label
        self._kwargs = kwargs
        self._job_id = _random_string(5)
        self._status = 'pending'
        self._result = None
        self._exception = Exception()
        self._job_handler = job_handler
        self._job_manager = job_manager
        self._container = container
    def wait(self):
        while True:
            if self._status == 'finished':
                return self._result
            elif self._status == 'error':
                raise self._exception
            elif self._status == 'queued':
                pass
            else:
                raise Exception(f'Unexpected status: {self._status}')
            self._job_manager.iterate()
            time.sleep(1)
    def result(self):
        if self._status == 'finished':
            return self._result
    def _execute(self):
        if self._container is not None:
            job_serialized = self._serialize()
            success, result, runtime_info = _run_serialized_job_in_container(job_serialized)
            if success:
                self._result = result
                self._status = 'finished'
            else:
                self._exception = Exception('Problem running function in container.')
                self._status = 'error'
        else:
            try:
                ret = self._f(**self._kwargs)
                self._result = ret
                self._status = 'finished'
            except Exception as e:
                self._status = 'error'
                self._exception = e
    def _serialize(self):
        function_name = getattr(self._f, '_hither_name')
        function_version = getattr(self._f, '_hither_version')
        additional_files = getattr(self._f, '_hither_additional_files', [])
        local_modules = getattr(self._f, '_hither_local_modules', [])
        x = dict(
            function_name=function_name,
            function_version=function_version,
            label=self._label,
            code=_generate_source_code_for_function(self._f, name=function_name, additional_files=additional_files, local_modules=local_modules),
            kwargs=self._kwargs,
            container=self._container
        )
        x = _serialize_item(x)
        return x

def _serialize_item(x):
    if isinstance(x, np.ndarray):
        sha1_path = ka.store_npy(array=x, basename='array.npy')
        with ka.config(algorithm='sha1'):
            sha1 = ka.get_file_hash(sha1_path)
        return dict(
            _type='npy',
            sha1=sha1
        )
    elif isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _serialize_item(val)
        return ret
    elif type(x) == list:
        ret = []
        for i, val in enumerate(x):
            ret.append(_serialize_item(val))
        return ret
    else:
        return x

def _deserialize_item(x):
    if type(x) == dict:
        if '_type' in x and x['_type'] == 'npy' and 'sha1' in x:
            sha1 = x['sha1']
            return ka.load_npy(f'sha1://{sha1}/file.npy')
        ret = dict()
        for key, val in x.items():
            ret[key] = _deserialize_item(val)
        return ret
    elif type(x) == list:
        ret = []
        for i, val in enumerate(x):
            ret.append(_deserialize_item(val))
        return ret
    else:
        return x

def _some_jobs_are_still_queued(x):
    if isinstance(x, Job):
        if x._status == 'queued':
            return True
    elif type(x) == dict:
        for v in x.values():
            if _some_jobs_are_still_queued(v):
                return True
    elif type(x) == list:
        for v in x:
            if _some_jobs_are_still_queued(v):
                return True
    return False

def _some_jobs_have_errors(x):
    if isinstance(x, Job):
        if x._status == 'error':
            return True
    elif type(x) == dict:
        for v in x.values():
            if _some_jobs_are_still_queued(v):
                return True
    elif type(x) == list:
        for v in x:
            if _some_jobs_are_still_queued(v):
                return True
    return False

def _resolve_job_values(x):
    if isinstance(x, Job):
        return x._result
    elif type(x) == dict:
        ret = dict()
        for k, v in x.items():
            ret[k] = _resolve_job_values(v)
        return ret
    elif type(x) == list:
        ret = []
        for v in x:
            ret.append(_resolve_job_values(v))
        return ret
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
        ss = ShellScript(f'''
            docker pull {container}
        ''')
    else:
        ss = ShellScript(f'''
            #!/bin/bash

            exec docker pull {container}
        ''')
    ss.start()
    retcode = ss.wait()
    if retcode != 0:
        raise Exception(f'Problem pulling container {container}')

