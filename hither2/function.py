import os
import inspect
from .run_scriptdir_in_container import DockerImage
from typing import Any, Callable, Dict, List, Type, Union
from ._config import Config
from ._job import Job
from ._job import JobResult
from ._job_cache import JobCache
from .runtimehook import RuntimeHook, PreContainerContext

_global_registered_functions_by_name: Dict[str, Callable] = {}

def get_function(function_name) -> Union[Callable, None]:
    if function_name in _global_registered_functions_by_name:
        return _global_registered_functions_by_name[function_name]
    else:
        return None

class DuplicateFunctionException(Exception):
    pass

class FunctionWrapper:
    def __init__(self, *,
        f: Callable[..., Any],
        name: str,
        version: str,
        image: Union[DockerImage, bool, None],
        modules: List[str],
        kachery_support: bool,
        nvidia_support: bool,
        runtime_hooks: List[RuntimeHook]
    ) -> None:
        self._f = f
        self._name = name
        self._version = version
        self._image = image
        self._modules = modules
        self._kachery_support = kachery_support
        self._nvidia_support = nvidia_support
        self._runtime_hooks = runtime_hooks

        function_name = self._name
        try:
            function_source_path = inspect.getsourcefile(_unwrap_function(f)) # important to unwrap the function so we don't get the source file name of the wrapped function (if there are decorators)
            if function_source_path is None:
                raise Exception(f'Unable to get source file for function {function_name} (*). Cannot run in a container or remotely.')
            function_source_path = os.path.abspath(function_source_path)
        except:
            raise Exception(f'Unable to get source file for function {function_name}. Cannot run in a container or remotely.')
        self._function_source_path = function_source_path

    @property
    def f(self) -> Callable:
        return self._f
    @property
    def name(self) -> str:
        return self._name
    @property
    def version(self) -> str:
        return self._version
    @property
    def image(self) -> Union[DockerImage, bool, None]:
        return self._image
    @property
    def modules(self) -> List[str]:
        return self._modules
    @property
    def kachery_support(self) -> bool:
        return self._kachery_support
    @property
    def nvidia_support(self) -> bool:
        return self._nvidia_support
    @property
    def function_source_path(self) -> str:
        return self._function_source_path

def function(
    name: str,
    version: str, *,
    image: Union[DockerImage, bool, None]=None,
    modules: List[str]=[],
    kachery_support: bool=False,
    nvidia_support: bool=False,
    register_globally=False,
    runtime_hooks: List[RuntimeHook]=[]
):
    def wrap(f: Callable[..., Any]):
        assert f.__name__ == name, f"Name does not match function name: {name} <> {f.__name__}"
        _function_wrapper = FunctionWrapper(
            f=f,
            name=name,
            version=version,
            image=image,
            modules=modules,
            kachery_support=kachery_support,
            nvidia_support=nvidia_support,
            runtime_hooks=runtime_hooks
        )
        setattr(f, '_hither_function_wrapper', _function_wrapper)
        # register the function
        if register_globally:    
            if name in _global_registered_functions_by_name:
                f2 = _global_registered_functions_by_name[name]
                path1 = _function_path(f)
                path2 = _function_path(f2)
                if path1 != path2:
                    w1 = _get_hither_function_wrapper(f)
                    w2 = _get_hither_function_wrapper(f2)
                    if w1.version != w2.version:
                        raise DuplicateFunctionException(f'Hither function {name} is registered in two different files with different versions: {path1} {path2}')
                    print(f"Warning: Hither function with name {name} is registered in two different files: {path1} {path2}") # pragma: no cover
            else:
                _global_registered_functions_by_name[name] = f
        
        def run(**kwargs):
            return Job(f, kwargs)
        setattr(f, 'run', run)

        return f
    return wrap

def _get_hither_function_wrapper(f: Callable) -> FunctionWrapper:
    return getattr(f, '_hither_function_wrapper', None)

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))

# strip away the decorators
def _unwrap_function(f):
    if hasattr(f, '__wrapped__'):
        return _unwrap_function(f.__wrapped__)
    else:
        return f