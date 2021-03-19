from typing import List, Callable
import os
import inspect
import fnmatch
import kachery_p2p as kp
import simplejson
from ._enums import InternalFunctionAttributeKeys

def _generate_source_code_for_function(function: Callable) -> dict:
    name = function.__name__
    try:
        function_source_fname = os.path.abspath(inspect.getsourcefile(_unwrap_function(function))) # important to unwrap the function so we don't get the source file name of the wrapped function (if there are decorators)
    except:
        raise Exception('Unable to get source file for function {}. Cannot run in a container or remotely.'.format(name))
    additional_files = getattr(function, InternalFunctionAttributeKeys.HITHER_ADDITIONAL_FILES, [])
    local_modules = getattr(function, InternalFunctionAttributeKeys.HITHER_LOCAL_MODULES, [])
    function_source_dirname = os.path.dirname(function_source_fname)
    function_source_basename = os.path.basename(function_source_fname)
    function_source_basename_noext = os.path.splitext(function_source_basename)[0]
    assert isinstance(function_source_dirname, str)
    code = _read_python_code_of_directory(
        function_source_dirname,
        additional_files=additional_files,
        exclude_init=True
    )
    code['files'].append(dict(
        name='__init__.py',
        content='from .{} import {}'.format(
            function_source_basename_noext, name)
    ))
    hither_dir = os.path.dirname(os.path.realpath(__file__))
    kachery_p2p_dir = os.path.dirname(os.path.realpath(kp.__file__))
    simplejson_dir = os.path.dirname(os.path.realpath(simplejson.__file__))
    local_module_paths: List[str] = []
    for lm in local_modules:
        if os.path.isabs(lm):
            local_module_paths.append(lm)
        else:
            local_module_paths.append(os.path.join(function_source_dirname, lm))
    code['dirs'].append(dict(
        name='_local_modules',
        content=dict(
            files=[],
            dirs=[
                dict(
                    name=os.path.basename(local_module_path),
                    content=_read_python_code_of_directory(os.path.join(function_source_dirname, local_module_path), exclude_init=False)
                )
                for local_module_path in local_module_paths + [hither_dir, kachery_p2p_dir, simplejson_dir]
            ]
        )
    ))
    return code

def _read_python_code_of_directory(dirname, exclude_init, additional_files=[]):
    patterns = ['*.py'] + additional_files
    files = []
    dirs = []
    for fname in os.listdir(dirname):
        if os.path.isfile(dirname + '/' + fname):
            matches = False
            for pattern in patterns:
                if fnmatch.fnmatch(fname, pattern):
                    matches = True
            if exclude_init and (fname == '__init__.py'):
                matches = False
            if matches:
                with open(dirname + '/' + fname, 'rb') as f:
                    try:
                        txt = f.read().decode('utf-8')
                    except:
                        print('WARNING: Problem decoding text file: {}'.format(dirname + '/' + fname))
                        txt = f.read().decode('utf-8', 'ignore')
                files.append(dict(
                    name=fname,
                    content=txt
                ))
        elif os.path.isdir(dirname + '/' + fname):
            if (not fname.startswith('__')) and (not fname.startswith('.')):
                content = _read_python_code_of_directory(
                    dirname + '/' + fname, additional_files=additional_files, exclude_init=False)
                if len(content['files']) + len(content['dirs']) > 0:
                    dirs.append(dict(
                        name=fname,
                        content=content
                    ))
    return dict(
        files=files,
        dirs=dirs
    )

# strip away the decorators
def _unwrap_function(f):
    if hasattr(f, '__wrapped__'):
        return _unwrap_function(f.__wrapped__)
    else:
        return f