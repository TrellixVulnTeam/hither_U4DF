import os
import inspect
from .run_scriptdir_in_container import DockerImage
from typing import Callable, Dict, List, Union
from ._config import Config
from ._job import Job
from ._job import JobResult
from ._job_cache import JobCache

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
        f: Callable,
        name: str,
        version: str,
        image: Union[DockerImage, None],
        modules: List[str],
        kachery_support: bool
    ) -> None:
        self._f = f
        self._name = name
        self._version = version
        self._image = image
        self._modules = modules
        self._kachery_support = kachery_support
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
    def image(self) -> Union[DockerImage, None]:
        return self._image
    @property
    def modules(self) -> List[str]:
        return self._modules
    @property
    def kachery_support(self) -> bool:
        return self._kachery_support

def function(
    name: str,
    version: str, *,
    image: Union[DockerImage, None]=None,
    modules: List[str]=[],
    kachery_support: bool=False,
    register_globally=False
):
    def wrap(f):
        assert f.__name__ == name, f"Name does not match function name: {name} <> {f.__name__}"
        _function_wrapper = FunctionWrapper(
            f=f,
            name=name,
            version=version,
            image=image,
            modules=modules,
            kachery_support=kachery_support
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