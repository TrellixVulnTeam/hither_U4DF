import kachery as ka
import numpy as np
from .file import File

def _deresolve_files_in_item(x):
    if isinstance(x, np.ndarray):
        path = ka.store_npy(x)
        return File(path, item_type = 'ndarray')
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _deresolve_files_in_item(val)
        return ret
    elif type(x) == list:
        return [_deresolve_files_in_item(val) for val in x]
    elif type(x) == tuple:
        return tuple([_deresolve_files_in_item(val) for val in x])
    else:
        return x

def _resolve_files_in_item(x):
    if isinstance(x, File):
        if x._item_type == 'file':
            path = ka.load_file(x._sha1_path)
            assert path is not None, f'Unable to load file: {x._sha1_path}'
            return path
        elif x._item_type == 'ndarray':
            return x.array()
        else:
            raise Exception(f'Unexpected item type: {x._item_type}')
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _resolve_files_in_item(val)
        return ret
    elif type(x) == list:
        return [_resolve_files_in_item(val) for val in x]
    elif type(x) == tuple:
        return tuple([_resolve_files_in_item(val) for val in x])
    else:
        return x