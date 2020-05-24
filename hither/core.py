import inspect
from types import SimpleNamespace
from typing import Optional
import os

from ._Config import Config
from .defaultjobhandler import DefaultJobHandler
from ._enums import ConfigKeys, JobKeys
from .job import Job
from ._jobmanager import _JobManager

_default_global_config = dict(
    container=None,
    job_handler=None,
    job_cache=None,
    download_results=None,
    job_timeout=None
)

Config.set_default_config(_default_global_config)
_global_job_manager = _JobManager()

def reset():
    _global_job_manager.reset()
    Config.set_default_config(_default_global_config)

def container(container):
    assert container.startswith('docker://'), f"Container string {container} must begin with docker://"
    def wrap(f):
        setattr(f, '_hither_container', container)
        return f
    return wrap

def opts(no_resolve_input_files=None):
    def wrap(f):
        if no_resolve_input_files is not None:
            setattr(f, JobKeys.NO_RESOLVE_INPUT_FILES , no_resolve_input_files)
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

def wait(timeout: Optional[float]=None):
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
            configured_container = Config.get_current_config_value(ConfigKeys.CONTAINER)
            if configured_container is True:
                container = getattr(f, JobKeys.HITHER_CONTAINER, None)
            elif configured_container is not None and configured_container is not False:
                container = configured_container
            else:
                container=None
            job_handler = Config.get_current_config_value(ConfigKeys.JOB_HANDLER)
            job_cache = Config.get_current_config_value(ConfigKeys.JOB_CACHE)
            if job_handler is None:
                job_handler = _global_job_handler
            download_results = Config.get_current_config_value(ConfigKeys.DOWNLOAD_RESULTS)
            if download_results is None:
                download_results = False
            job_timeout = Config.get_current_config_value(ConfigKeys.TIMEOUT)
            label = name
            no_resolve_input_files = getattr(f, JobKeys.NO_RESOLVE_INPUT_FILES, False)
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
    

_global_job_handler = DefaultJobHandler()
# TODO: the following code is duplicated -- see config.py
setattr(_global_job_handler, '_internal_counts', SimpleNamespace(
    num_jobs=0,
    num_run_jobs=0,
    num_finished_jobs=0,
    num_errored_jobs=0,
    num_skipped_jobs=0
))


# TODO: Would be nice to avoid needing this
def _deserialize_job(serialized_job):
    return Job._deserialize(serialized_job)

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))
