from .function import FunctionWrapper
from .run_scriptdir import run_scriptdir
from .create_scriptdir_for_function_run import create_scriptdir_for_function_run
import os
import shutil
import fnmatch
from typing import Callable, Dict, List, Union
from .run_scriptdir_in_container import DockerImage, BindMount, run_scriptdir_in_container
from ._safe_pickle import _safe_unpickle
from .create_scriptdir_for_function_run import _update_bind_mounts_and_environment_for_kachery_support

def run_function_in_container(
    function_wrapper: FunctionWrapper, *,
    kwargs: dict,
    _environment: Dict[str, str] = dict(),
    _bind_mounts: List[BindMount] = [],
    _kachery_support: Union[None, bool] = None
):
    import kachery_p2p as kp
    if _kachery_support is None:
        _kachery_support = function_wrapper.kachery_support
    if _kachery_support:
        _bind_mounts, _environment = _update_bind_mounts_and_environment_for_kachery_support(_bind_mounts, _environment)
    with kp.TemporaryDirectory(remove=True) as tmpdir:
        create_scriptdir_for_function_run(
            directory=tmpdir,
            function_wrapper=function_wrapper,
            kwargs=kwargs,
            use_container=True,
            _environment=_environment,
            _bind_mounts=_bind_mounts
        )
        output_dir = f'{tmpdir}/output'
        run_scriptdir(scriptdir=tmpdir)

        return_value = _safe_unpickle(output_dir + '/return_value.pkl')
        return return_value

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