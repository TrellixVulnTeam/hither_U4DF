from .run_scriptdir import run_scriptdir
from .create_scriptdir_for_function_run import create_scriptdir_for_function_run
import os
import shutil
import fnmatch
from typing import Callable, Dict, List
from .run_scriptdir_in_container import DockerImage, BindMount, run_scriptdir_in_container
from ._safe_pickle import _safe_unpickle

def run_function_in_container(
    function: Callable, *,
    image: DockerImage,
    kwargs: dict,
    modules: List[str] = [],
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = [],
    kachery_support = False
):
    import kachery_p2p as kp
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
    with kp.TemporaryDirectory(remove=True) as tmpdir:
        create_scriptdir_for_function_run(
            directory=tmpdir,
            function=function,
            kwargs=kwargs,
            modules=modules,
            image=image,
            environment=environment,
            bind_mounts=bind_mounts
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