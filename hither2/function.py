from .run_function_in_container import run_function_in_container
import os
import inspect
from hither2.run_script_in_container import DockerImage
from typing import List, Union
from ._config import Config

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

def function(name, version, image: Union[str, DockerImage, None]=None, modules: List[str]=[], kachery_support: bool=False):
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
        
        def new_f(**arguments_for_wrapped_function):
            conf = Config.get_current_config()
            if conf.use_container and (image is not None):
                return run_function_in_container(
                    function=f,
                    image=image,
                    kwargs=arguments_for_wrapped_function,
                    modules=modules,
                    environment={},
                    bind_mounts=[],
                    kachery_support=kachery_support
                )
            else:
                return f(**arguments_for_wrapped_function)
        return new_f
    return wrap

def _function_path(f):
    return os.path.abspath(inspect.getfile(f))