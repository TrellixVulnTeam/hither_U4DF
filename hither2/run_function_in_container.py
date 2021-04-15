from .runtimehook import PreContainerContext, PostContainerContext
from .function import FunctionWrapper
from .run_scriptdir import run_scriptdir
from .create_scriptdir_for_function_run import create_scriptdir_for_function_run
import os
import shutil
import fnmatch
from typing import Any, Callable, Dict, List, Tuple, Union
from .run_scriptdir_in_container import DockerImage, BindMount, run_scriptdir_in_container
from ._safe_pickle import _safe_unpickle
from .create_scriptdir_for_function_run import _update_bind_mounts_and_environment_for_kachery_support

def run_function_in_container(
    function_wrapper: FunctionWrapper, *,
    image: Union[DockerImage, bool],
    kwargs: dict,
    show_console: bool,
    _environment: Dict[str, str] = dict(),
    _bind_mounts: List[BindMount] = [],
    _kachery_support: Union[None, bool] = None,
    _nvidia_support: Union[None, bool] = None
) -> Tuple[Any, Union[None, Exception], Union[None, List[dict]]]:
    import kachery_p2p as kp
    if _kachery_support is None:
        _kachery_support = function_wrapper.kachery_support
    if _kachery_support:
        _bind_mounts, _environment = _update_bind_mounts_and_environment_for_kachery_support(_bind_mounts, _environment)
    if _nvidia_support is None:
        _nvidia_support = function_wrapper.nvidia_support

    with kp.TemporaryDirectory(remove=True) as tmpdir:
        create_scriptdir_for_function_run(
            directory=tmpdir,
            function_wrapper=function_wrapper,
            image=image,
            kwargs=kwargs,
            show_console=show_console,
            _environment=_environment,
            _bind_mounts=_bind_mounts,
            _kachery_support=False,
            _nvidia_support=_nvidia_support
        )
        output_dir = f'{tmpdir}/output'
        j = run_scriptdir(scriptdir=tmpdir)

        if j.status == 'complete':
            return_value_path = output_dir + '/return_value.pkl'
            error_message_path = output_dir + '/error_message.pkl'
            if os.path.isfile(return_value_path):
                return_value = _safe_unpickle(return_value_path)
                error = None
            elif os.path.isfile(error_message_path):
                return_value = None
                error_message = _safe_unpickle(error_message_path)
                error = Exception(error_message)
            else:
                return_value = None
                error_message = 'Not found: error_message.pkl'
                error = Exception(error_message)
        else:
            raise Exception(f'Unexpected status for scriptdir job: {j.status}')
        console_lines_path = output_dir + '/console_lines.pkl'
        if os.path.isfile(console_lines_path):
            console_lines = _safe_unpickle(console_lines_path)
        else:
            console_lines = None
        
        # postcontainer
        if error is None:
            postcontainer_context = PostContainerContext(kwargs=kwargs, image=image, return_value=return_value)
            for h in function_wrapper._runtime_hooks:
                h.postcontainer(postcontainer_context)
            new_return_value = postcontainer_context.return_value
        else:
            new_return_value = return_value

        return new_return_value, error, console_lines

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