import random
import numpy as np
import base64
import io
import kachery as ka
from .file import File

def _serialize_item(x, return_files_for_ndarrays=False):
    kwargs = dict(
        return_files_for_ndarrays=return_files_for_ndarrays
    )
    if isinstance(x, np.ndarray):
        if return_files_for_ndarrays:
            path0 = ka.store_npy(x, basename='ndarray.npy')
            ff = File(path0, item_type='ndarray')
            return _serialize_item(ff, return_files_for_ndarrays=return_files_for_ndarrays)
        else:
            return dict(
                _type='npy',
                data_b64=_npy_to_b64(x)
            )
    elif isinstance(x, File):
        return x.serialize()
    elif isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _serialize_item(val, **kwargs)
        return ret
    elif type(x) == list:
        return [_serialize_item(val, **kwargs) for val in x]
    elif type(x) == tuple:
        # we need to distinguish between a tuple and list for json serialization
        return dict(
            _type='tuple',
            data=_serialize_item(list(x))
        )
    else:
        return x

def _deserialize_item(x, return_ndarrays_for_npy_files=False):
    if type(x) == dict:
        if '_type' in x and x['_type'] == 'npy' and 'sha1' in x:
            sha1 = x['sha1']
            return ka.load_npy(f'sha1://{sha1}/file.npy')
        elif '_type' in x and x['_type'] == 'npy' and 'data_b64' in x:
            data_b64 = x['data_b64']
            return _b64_to_npy(data_b64)
        elif '_type' in x and x['_type'] == 'tuple':
            return _deserialize_item(tuple(x['data']))
        if File.can_deserialize(x):
            return File.deserialize(x)
        ret = dict()
        for key, val in x.items():
            ret[key] = _deserialize_item(val, return_ndarrays_for_npy_files=return_ndarrays_for_npy_files)
        return ret
    elif type(x) == list:
        return [_deserialize_item(val, return_ndarrays_for_npy_files=return_ndarrays_for_npy_files) for val in x]
    elif type(x) == tuple:
        return tuple([_deserialize_item(val, return_ndarrays_for_npy_files=return_ndarrays_for_npy_files) for val in x])
    elif type(x) == File:
        if x.path.endswith('.npy'):
            return np.load(ka.load_file(x.path))
        else:
            return x
    else:
        return x

def _npy_to_b64(x):
    f = io.BytesIO()
    np.save(f, x)
    return base64.b64encode(f.getvalue()).decode('utf-8')

def _b64_to_npy(x):
    bytes0 = base64.b64decode(x.encode())
    f = io.BytesIO(bytes0)
    return np.load(f)

def _utctime():
    from datetime import datetime, timezone
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()

def _docker_form_of_container_string(container):
    if container.startswith('docker://'):
        return container[len('docker://'):]
    else:
        return container

def _random_string(num: int):
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))