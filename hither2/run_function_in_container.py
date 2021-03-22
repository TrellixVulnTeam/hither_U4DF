import os
import inspect
import shutil
import importlib
from typing import Callable, Dict, List
from .run_script_in_container import DockerImage, run_script_in_container
from ._temporarydirectory import TemporaryDirectory
from .run_script_in_container import BindMount
from ._safe_pickle import _safe_pickle, _safe_unpickle

def run_function_in_container(
    function: Callable, *,
    image: DockerImage,
    kwargs: dict,
    modules: List[str] = [],
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = [],
    kachery_support = False
):
    if kachery_support:
        from .run_function_in_container_with_kachery_support import run_function_in_container_with_kachery_support
        return run_function_in_container_with_kachery_support(
            function=function,
            image=image,
            kwargs=kwargs,
            modules=modules,
            environment=environment,
            bind_mounts=bind_mounts
        )
    with TemporaryDirectory() as tmpdir:
        input_dir = tmpdir + '/input'
        output_dir = tmpdir + '/output'
        os.mkdir(input_dir)
        os.mkdir(output_dir)
        modules_dir = tmpdir + '/input/modules'
        os.mkdir(modules_dir)
        src_dir = tmpdir + '/input/modules/f_src'

        function_name: str = function.__name__
        try:
            function_source_fname = inspect.getsourcefile(_unwrap_function(function)) # important to unwrap the function so we don't get the source file name of the wrapped function (if there are decorators)
            if function_source_fname is None:
                raise Exception('Unable to get source file for function {function_name} (*). Cannot run in a container or remotely.')
            function_source_fname = os.path.abspath(function_source_fname)
        except:
            raise Exception('Unable to get source file for function {function_name}. Cannot run in a container or remotely.'.format(function_name))
        
        function_source_basename = os.path.basename(function_source_fname)
        function_source_basename_noext = os.path.splitext(function_source_basename)[0]
        shutil.copytree(os.path.dirname(function_source_fname), src_dir)
        with open(src_dir + '/__init__.py', 'w') as f:
            pass

        _safe_pickle(input_dir + '/kwargs.pkl', kwargs)

        modules2 = modules + ['hither2']
        for module in modules2:
            module_path = os.path.dirname(importlib.import_module(module).__file__)
            shutil.copytree(module_path, modules_dir + '/' + module)

        script = f'''
        #!/usr/bin/env python3

        import sys
        sys.path.append('/input/modules')

        import hither2 as hi2

        from f_src.{function_source_basename_noext} import {function_name}

        def main(): 
            kwargs = hi2._safe_unpickle('/input/kwargs.pkl')
            return_value = {function_name}(**kwargs)
            hi2._safe_pickle('/output/return_value.pkl', return_value)

        if __name__ == '__main__':
            main()
        '''
        run_script_in_container(
            image=image,
            script=script,
            input_dir=input_dir,
            output_dir=output_dir,
            environment=environment,
            bind_mounts=bind_mounts
        )

        return_value = _safe_unpickle(output_dir + '/return_value.pkl')
        return return_value

# strip away the decorators
def _unwrap_function(f):
    if hasattr(f, '__wrapped__'):
        return _unwrap_function(f.__wrapped__)
    else:
        return f