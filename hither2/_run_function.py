from .function import FunctionWrapper
from .run_scriptdir_in_container import DockerImage
from typing import Any, Callable, List, Tuple, Union
from ._job_cache import JobCache
from ._check_job_cache import _check_job_cache
from .run_function_in_container import run_function_in_container
from .consolecapture import ConsoleCapture


def _run_function(*,
    function_wrapper: FunctionWrapper,
    image: Union[DockerImage, None],
    kwargs: dict,
    show_console: bool
) -> Tuple[Any, Union[None, Exception], Union[None, List[dict]]]:
    # fw = function_wrapper
    # if job_cache is not None:
    #     cache_result = _check_job_cache(function_name=fw.name, function_version=fw.version, kwargs=kwargs, job_cache=job_cache)
    #     if cache_result is not None:
    #         if cache_result.status == 'finished':
    #             print(f'Using cached result for {fw.name} ({fw.version})')
    #             return cache_result.return_value

    if image is not None:
        return run_function_in_container(
            function_wrapper=function_wrapper,
            image=image,
            kwargs=kwargs,
            show_console=show_console,
            _environment={},
            _bind_mounts=[],
            _kachery_support=function_wrapper.kachery_support,
            _nvidia_support=function_wrapper.nvidia_support
        )
    else:
        with ConsoleCapture(show_console=show_console) as cc:
            try:
                return_value = function_wrapper.f(**kwargs)
                error = None
            except Exception as e:
                return_value = None
                error = e
            return return_value, error, cc.lines