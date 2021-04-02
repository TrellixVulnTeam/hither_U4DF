from .function import FunctionWrapper
from .run_scriptdir_in_container import DockerImage
from typing import Callable, Union
from ._job_cache import JobCache
from ._check_job_cache import _check_job_cache
from .run_function_in_container import run_function_in_container


def _run_function(*,
    function_wrapper: FunctionWrapper,
    kwargs: dict,
    use_container: bool
):
    fw = function_wrapper
    # if job_cache is not None:
    #     cache_result = _check_job_cache(function_name=fw.name, function_version=fw.version, kwargs=kwargs, job_cache=job_cache)
    #     if cache_result is not None:
    #         if cache_result.status == 'finished':
    #             print(f'Using cached result for {fw.name} ({fw.version})')
    #             return cache_result.return_value

    image = fw.image
    if use_container and (image is not None):
        return run_function_in_container(
            function=fw.f,
            image=image,
            kwargs=kwargs,
            modules=fw.modules,
            environment={},
            bind_mounts=[],
            kachery_support=fw.kachery_support
        )
    else:
        return fw.f(**kwargs)