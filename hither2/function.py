from .run_function_in_container import run_function_in_container
import os
import inspect
from hither2.run_script_in_container import DockerImage
from typing import List, Union
from ._config import Config
from ._job import JobResult
from ._job_cache import JobCache

_global_registered_functions_by_name = dict()

# run a registered function by name
def run(function_name, **kwargs):
    f = get_function(function_name)
    return f.run(**kwargs)

def get_function(function_name):
    assert function_name in _global_registered_functions_by_name, f'Hither function {function_name} not registered'
    return _global_registered_functions_by_name[function_name]['function']

class DuplicateFunctionException(Exception):
    pass

def function(
    name: str,
    version: str, *,
    image: Union[DockerImage, None]=None,
    modules: List[str]=[],
    kachery_support: bool=False,
    register_globally=False
):
    def wrap(f):
        # register the function
        assert f.__name__ == name, f"Name does not match function name: {name} <> {f.__name__}"
        if register_globally:
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
        
        def _run_function_with_config(*,
            kwargs: dict,
            job_cache: Union[JobCache, None],
            use_container: bool
        ):
            if job_cache is not None:
                cache_result = _check_job_cache(kwargs, job_cache)
                if cache_result is not None:
                    if cache_result.get_status() == 'finished':
                        print(f'Using cached result for {name} ({version})')
                        return cache_result.get_return_value()

            if use_container and (image is not None):
                return run_function_in_container(
                    function=f,
                    image=image,
                    kwargs=kwargs,
                    modules=modules,
                    environment={},
                    bind_mounts=[],
                    kachery_support=kachery_support
                )
            else:
                return f(**kwargs)
        
        def _check_job_cache(kwargs: dict, job_cache: JobCache) -> Union[JobResult, None]:
            job_hash: Union[str, None] = _compute_job_hash(function_name=name, function_version=version, kwargs=kwargs)
            if job_hash is not None:
                job_result = job_cache._fetch_cached_job_result(job_hash)
                if job_result is not None:
                    if job_result.get_status() == 'finished':
                        return job_result.get_return_value()
        
        def new_f(**arguments_for_wrapped_function):
            config0 = Config.get_current_config()
            return _run_function_with_config(
                kwargs=arguments_for_wrapped_function,
                job_cache=config0.job_cache,
                use_container=config0.use_container
            )
        setattr(new_f, '_hither_run_function_with_config', _run_function_with_config)
        setattr(new_f, '_hither_check_job_cache', _check_job_cache)
        setattr(new_f, '_hither_image', image)
        setattr(new_f, '_hither_function_name', name)
        setattr(new_f, '_hither_function_version', version)
        return new_f
    return wrap

def _compute_job_hash(
    function_name: str,
    function_version: str,
    kwargs: dict
):
    # todo: finish
    return None

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))