import fnmatch
from .function import FunctionWrapper
from .dockerimage import DockerImage
import importlib
import os
import shutil
import json
from copy import deepcopy
from typing import Dict, List, Union
import kachery_p2p as kp
from ._safe_pickle import _safe_pickle
from .run_scriptdir_in_container import BindMount

def _update_bind_mounts_and_environment_for_kachery_support(
    bind_mounts: List[BindMount] = [],
    environment: Dict[str, str] = dict()
):
    bind_mounts2 = deepcopy(bind_mounts)
    environment2 = deepcopy(environment)
    
    kachery_storage_dir = kp._kachery_storage_dir()
    if kachery_storage_dir is None:
        raise Exception('Unable to determine kachery storage directory.')
    kachery_temp_dir = kp._kachery_temp_dir()
    bind_mounts2.append(
        BindMount(source=kachery_storage_dir, target=kachery_storage_dir, read_only=True)
    )
    bind_mounts2.append(
        BindMount(source=kachery_temp_dir, target=kachery_temp_dir, read_only=False)
    )
    environment2['KACHERY_TEMP_DIR'] = kachery_temp_dir
    kachery_p2p_api_port = os.getenv('KACHERY_P2P_API_PORT', None)
    if kachery_p2p_api_port is not None:
        environment2['KACHERY_P2P_API_PORT'] = kachery_p2p_api_port
    kachery_p2p_api_host = os.getenv('KACHERY_P2P_API_HOST', None)
    if kachery_p2p_api_host is not None:
        environment2['KACHERY_P2P_API_HOST'] = kachery_p2p_api_host
    return bind_mounts2, environment2


def create_scriptdir_for_function_run(
    *,
    directory: str,
    function_wrapper: FunctionWrapper,
    kwargs: dict,
    use_container: bool,
    show_console: bool,
    _bind_mounts: List[BindMount] = [],
    _environment: Dict[str, str] = {},
    _kachery_support: Union[None, bool] = None
):
    import kachery_p2p as kp

    if not os.path.isdir(directory):
        os.mkdir(directory)
    
    if _kachery_support is None:
        _kachery_support = function_wrapper.kachery_support
    if _kachery_support:
        _bind_mounts, _environment = _update_bind_mounts_and_environment_for_kachery_support(_bind_mounts, _environment)

    image = function_wrapper.image
    modules = function_wrapper.modules
    
    if (image is not None) and use_container:
        if not image.is_prepared():
            raise Exception(f'Image must be prepared prior to running in container: {image.get_name()}:{image.get_tag()}')
        incontainer_scriptdir_path = f'{directory}/incontainer_scriptdir'
        create_scriptdir_for_function_run(
            directory=incontainer_scriptdir_path,
            function_wrapper=function_wrapper,
            kwargs=kwargs,
            use_container=False,
            show_console=show_console,
            _kachery_support = False,
            _environment=_environment
        )
        bind_mounts_path = f'{directory}/bind_mounts.json'
        with open(bind_mounts_path, 'w') as f:
            json.dump([x.serialize() for x in _bind_mounts], f)
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
    
    if len(_bind_mounts) > 0:
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

    function_name: str = function_wrapper.name
    function_source_path = function_wrapper.function_source_path
    
    function_source_basename = os.path.basename(function_source_path)
    function_source_basename_noext = os.path.splitext(function_source_basename)[0]

    _copy_py_module_dir(os.path.dirname(function_source_path), src_dir)
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

    import traceback
    import hither2 as hi

    from f_src.{function_source_basename_noext} import {function_name}

    def main(): 
        kwargs = hi._safe_unpickle(f'{{input_dir}}/kwargs.pkl')
        with hi.ConsoleCapture(show_console={show_console}) as cc:
            try:
                return_value = {function_name}(**kwargs)
                error = None
            except Exception as e:
                return_value = None
                error = e
                print(traceback.format_exc())
            if return_value is not None:
                hi._safe_pickle(f'{{output_dir}}/return_value.pkl', return_value)
            if error is not None:
                hi._safe_pickle(f'{{output_dir}}/error_message.pkl', str(error))
            hi._safe_pickle(f'{{output_dir}}/console_lines.pkl', cc.lines)

    if __name__ == '__main__':
        main()
    '''

    env_path = f'{directory}/env'
    env_lines: List[str] = []
    for k, v in _environment.items():
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