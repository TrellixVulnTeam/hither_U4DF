import inspect
from types import SimpleNamespace
from typing import Optional
import os

from ._Config import Config
from .defaultjobhandler import DefaultJobHandler
from ._enums import ConfigKeys, InternalFunctionAttributeKeys
from .job import Job
from ._jobmanager import _JobManager
from ._exceptions import DuplicateFunctionException

_default_global_config = dict(
    container=None,
    job_handler=None,
    job_cache=None,
    job_timeout=None,
    force_run=False,
    rerun_failing=False,
    cache_failing=False,
    required_files=None,
    jhparams={}
)

Config.set_default_config(_default_global_config)
_global_job_manager = _JobManager()

def reset():
    _global_job_manager.reset()
    Config.set_default_config(_default_global_config)

def _apply_container_to_hither_function(f, container):
    assert container.startswith('docker://'), f"Container string {container} must begin with docker://"
    setattr(f, InternalFunctionAttributeKeys.HITHER_CONTAINER, container)

def container(container):
    def wrap(f):
        _apply_container_to_hither_function(f, container)
        return f
    return wrap

# a placeholder for now
def opts(example_opt=None):
    def wrap(f):
        if example_opt is not None:
            setattr(f, '_example_opt', example_opt)
        return f
    return wrap


def additional_files(additional_files):
    def wrap(f):
        setattr(f, InternalFunctionAttributeKeys.HITHER_ADDITIONAL_FILES, additional_files)
        return f
    return wrap

def local_modules(local_modules):
    assert isinstance(local_modules, list), 'local_modules is not a list'
    def wrap(f):
        setattr(f, InternalFunctionAttributeKeys.HITHER_LOCAL_MODULES, local_modules)
        return f
    return wrap

def wait(timeout: Optional[float]=None):
    _global_job_manager.wait(timeout)

_global_registered_functions_by_name = dict()

# run a registered function by name
def run(function_name, **kwargs):
    f = get_function(function_name)
    assert f is not None, f'Hither function {function_name} not registered'
    return f.run(**kwargs)

def get_function(function_name):
    if (function_name in _global_registered_functions_by_name):
        return _global_registered_functions_by_name[function_name]['function']

############################################################
def function(name, version, container=None):
    def wrap(f):
        # register the function
        assert f.__name__ == name, f"Name does not match function name: {name} <> {f.__name__}"
        if name in _global_registered_functions_by_name:
            path1 = _function_path(f)
            path2 = _function_path(_global_registered_functions_by_name[name]['function'])
            if path1 != path2:
                if version != _global_registered_functions_by_name[name]['version']:
                    raise DuplicateFunctionException(f'Hither function {name} is registered in two different files with different versions: {path1} {path2}')
                print(f"Warning: Hither function with name {name} is registered in two different files: {path1} {path2}") # pragma: no cover
        else:
            _global_registered_functions_by_name[name] = dict(
                function=f,
                version=version
            )
        
        def run(**arguments_for_wrapped_function):
            configured_container = Config.get_current_config_value(ConfigKeys.CONTAINER)
            if configured_container is True:
                container = getattr(f, InternalFunctionAttributeKeys.HITHER_CONTAINER, None)
            elif configured_container is not None and configured_container is not False:
                container = configured_container
            else:
                container=None
            job_handler = Config.get_current_config_value(ConfigKeys.JOB_HANDLER)
            job_cache = Config.get_current_config_value(ConfigKeys.JOB_CACHE)
            if job_handler is None:
                job_handler = _global_job_handler
            job_timeout = Config.get_current_config_value(ConfigKeys.TIMEOUT)
            force_run = Config.get_current_config_value(ConfigKeys.FORCE_RUN)
            rerun_failing = Config.get_current_config_value(ConfigKeys.RERUN_FAILING)
            cache_failing = Config.get_current_config_value(ConfigKeys.CACHE_FAILING)
            required_files = Config.get_current_config_value(ConfigKeys.REQUIRED_FILES)
            jhparams = Config.get_current_config_value(ConfigKeys.JHPARAMS)
            label = name
            job = Job(
                job_id=None, # will be generated
                f=f,
                code_uri=None,
                wrapped_function_arguments=arguments_for_wrapped_function,
                job_handler=job_handler,
                job_cache=job_cache,
                job_manager=_global_job_manager,
                container=container,
                label=label,
                force_run=force_run,
                rerun_failing=rerun_failing,
                cache_failing=cache_failing,
                required_files=required_files,
                jhparams=jhparams,
                function_name=name,
                function_version=version,
                job_timeout=job_timeout
            )
            _global_job_manager.queue_job(job)
            return job
        setattr(f, 'run', run)
        if container is not None:
            _apply_container_to_hither_function(f, container)
        return f
    return wrap
    

_global_job_handler = DefaultJobHandler()

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))
