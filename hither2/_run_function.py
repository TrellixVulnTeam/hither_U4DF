from .function import FunctionWrapper
from .run_scriptdir_in_container import DockerImage
from typing import Any, Callable, List, Tuple, Union
from ._job_cache import JobCache
from ._check_job_cache import _check_job_cache
from .run_function_in_container import run_function_in_container
from .consolecapture import ConsoleCapture
from .runtimehook import PostContainerContext, PreContainerContext, RuntimeHook, PreRunContext, PostRunContext


def _run_function(*,
    function_wrapper: FunctionWrapper,
    image: Union[DockerImage, bool, None],
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

    if image:
        # run
        return_value, exc, console_lines = run_function_in_container(
            function_wrapper=function_wrapper,
            image=image,
            kwargs=kwargs,
            show_console=show_console,
            _environment={},
            _bind_mounts=[],
            _kachery_support=function_wrapper.kachery_support,
            _nvidia_support=function_wrapper.nvidia_support
        )

        return return_value, exc, console_lines
    else:
        with ConsoleCapture(show_console=show_console) as cc:
            try:
                # prerun
                prerun_context = PreRunContext(kwargs=kwargs)
                for h in function_wrapper._runtime_hooks:
                    h.prerun(prerun_context)
                new_kwargs = prerun_context.kwargs

                # run
                return_value = function_wrapper.f(**new_kwargs)
                
                # postrun
                postrun_context = PostRunContext(kwargs=kwargs, return_value=return_value)
                for h in function_wrapper._runtime_hooks:
                    h.postrun(postrun_context)
                new_return_value = postrun_context.return_value
                error = None
            except Exception as e:
                new_return_value = None
                error = e
            return new_return_value, error, cc.lines