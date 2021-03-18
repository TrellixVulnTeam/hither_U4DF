import os
import json
import inspect
import shutil
import importlib
from typing import Callable, Dict, List
from .run_script_in_container import run_script_in_container
from ._temporarydirectory import TemporaryDirectory
from .run_script_in_container import BindMount


def run_function_in_container(
    function: Callable, *,
    image: str,
    kwargs: dict,
    modules: List[str] = [],
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = []
):
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
        with open(input_dir + '/kwargs.json', 'w') as f:
            json.dump(kwargs, f)

        modules2 = modules + ['hither2']
        for module in modules2:
            module_path = os.path.dirname(importlib.import_module(module).__file__)
            shutil.copytree(module_path, modules_dir + '/' + module)

        script = f'''
        #!/usr/bin/env python3

        import sys
        import json

        sys.path.append('/input/modules')
        from f_src.{function_source_basename_noext} import {function_name}

        def main(): 
            with open('/input/kwargs.json', 'r') as f:
                kwargs = json.load(f)
            return_value = {function_name}(**kwargs)
            with open('/output/return_value.json', 'w') as f:
                json.dump(return_value, f)

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

        with open(output_dir + '/return_value.json', 'r') as f:
            return_value = json.load(f)
        return return_value

# strip away the decorators
def _unwrap_function(f):
    if hasattr(f, '__wrapped__'):
        return _unwrap_function(f.__wrapped__)
    else:
        return f