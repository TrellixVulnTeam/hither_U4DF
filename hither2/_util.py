import random
import numpy as np
import base64
import io
import kachery as ka
from .file import File

def _serialize_item(x):
    if isinstance(x, np.ndarray):
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
            ret[key] = _serialize_item(val)
        return ret
    elif type(x) == list:
        return [_serialize_item(val) for val in x]
    elif type(x) == tuple:
        return tuple([_serialize_item(val) for val in x])
    else:
        return x

def _deserialize_item(x):
    if type(x) == dict:
        if '_type' in x and x['_type'] == 'npy' and 'sha1' in x:
            sha1 = x['sha1']
            return ka.load_npy(f'sha1://{sha1}/file.npy')
        elif '_type' in x and x['_type'] == 'npy' and 'data_b64' in x:
            data_b64 = x['data_b64']
            return _b64_to_npy(data_b64)
        if File.can_deserialize(x):
            return File.deserialize(x)
        ret = dict()
        for key, val in x.items():
            ret[key] = _deserialize_item(val)
        return ret
    elif type(x) == list:
        return [_deserialize_item(val) for val in x]
    elif type(x) == tuple:
        return tuple([_deserialize_item(val) for val in x])
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


def _docker_form_of_container_string(container):
    if container.startswith('docker://'):
        return container[len('docker://'):]
    else:
        return container

def _random_string(num: int):
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))