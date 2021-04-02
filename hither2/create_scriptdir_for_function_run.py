import fnmatch
from .dockerimage import DockerImage
import importlib
import inspect
import os
import shutil
import json
from typing import Callable, Dict, List, Union
from ._safe_pickle import _safe_pickle
from .run_scriptdir_in_container import BindMount


def create_scriptdir_for_function_run(
    *,
    directory: str,
    function: Callable,
    kwargs: dict,
    modules: List[str] = [],
    image: Union[DockerImage, None] = None,
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = []
):
    import kachery_p2p as kp
    
    if image is not None:
        if not image.is_prepared():
            raise Exception(f'Image must be prepared prior to running in container: {image.get_name()}:{image.get_tag()}')
        incontainer_scriptdir_path = f'{directory}/incontainer_scriptdir'
        create_scriptdir_for_function_run(directory=incontainer_scriptdir_path, function=function, kwargs=kwargs, modules=modules, environment=environment)
        bind_mounts_path = f'{directory}/bind_mounts.json'
        with open(bind_mounts_path, 'w') as f:
            json.dump([x.serialize() for x in bind_mounts], f)
        output_path = f'{directory}/output'
        os.mkdir(output_path)
        run_script = kp.ShellScript(f'''
        #!/bin/bash

        set -e

        export PYTHONUNBUFFERED=1

        exec hither-scriptdir-runner run-scriptdir-in-container --scriptdir {incontainer_scriptdir_path} --bind-mounts {bind_mounts_path} --output-dir {output_path} --image {image.get_name()}:{image.get_tag()}
        ''')
        run_script.write(f'{directory}/run')
        return
    
    if len(bind_mounts) > 0:
        raise Exception('Cannot use bind mounts without image')
    
    if not os.path.isdir(directory):
        os.mkdir(directory)

    input_dir =  f'{directory}/input'
    output_dir = f'{directory}/output'
    os.mkdir(input_dir)
    os.mkdir(output_dir)
    modules_dir = f'{directory}/input/modules'
    os.mkdir(modules_dir)
    src_dir = f'{directory}/input/modules/f_src'

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

    _copy_py_module_dir(os.path.dirname(function_source_fname), src_dir)
    # shutil.copytree(os.path.dirname(function_source_fname), src_dir)
    with open(f'{src_dir}/__init__.py', 'w') as f:
        pass

    _safe_pickle(f'{input_dir}/kwargs.pkl', kwargs)

    modules2 = modules + ['hither2', 'kachery_p2p']
    for module in modules2:
        module_path = os.path.dirname(importlib.import_module(module).__file__)
        _copy_py_module_dir(module_path, f'{modules_dir}/{module}')

    script = f'''
    #!/usr/bin/env python3

    import os
    thisdir = os.path.dirname(os.path.realpath(__file__))
    input_dir = f'{{thisdir}}/input'
    output_dir = f'{{thisdir}}/output'

    import sys
    sys.path.append(f'{{input_dir}}/modules')

    import hither2 as hi

    from f_src.{function_source_basename_noext} import {function_name}

    def main(): 
        kwargs = hi._safe_unpickle(f'{{input_dir}}/kwargs.pkl')
        try:
            return_value = {function_name}(**kwargs)
        except Exception as e:
            hi._safe_pickle(f'{{output_dir}}/error.pkl', str(e))
            raise e
        hi._safe_pickle(f'{{output_dir}}/return_value.pkl', return_value)

    if __name__ == '__main__':
        main()
    '''

    env_path = f'{directory}/env'
    env_lines: List[str] = []
    for k, v in environment.items():
        env_lines.append(f'export {k}="{v}"')
    env_text = '\n'.join(env_lines)
    with open(env_path, 'w') as f:
        f.write(env_text)

    # make sure we do this at the very end in an atomic operation
    run_path = f'{directory}/run'
    kp.ShellScript(script=script).write(run_path + '.tmp')
    shutil.move(run_path + '.tmp', run_path)

def _copy_py_module_dir(src_path: str, dst_path: str):
    patterns = ['*.py']
    if not os.path.isdir(dst_path):
        os.mkdir(dst_path)
    for fname in os.listdir(src_path):
        src_fpath = f'{src_path}/{fname}'
        dst_fpath = f'{dst_path}/{fname}'
        if os.path.isfile(src_fpath):
            matches = False
            for pattern in patterns:
                if fnmatch.fnmatch(fname, pattern):
                    matches = True
            if matches:
                shutil.copyfile(src_fpath, dst_fpath)
        elif os.path.isdir(src_fpath):
            if (not fname.startswith('__')) and (not fname.startswith('.')):
                _copy_py_module_dir(src_fpath, dst_fpath)

# strip away the decorators
def _unwrap_function(f):
    if hasattr(f, '__wrapped__'):
        return _unwrap_function(f.__wrapped__)
    else:
        return f