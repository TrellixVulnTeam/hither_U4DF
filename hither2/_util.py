import random
from .file import File

def _serialize_item(x):
    if isinstance(x, File):
        return x.serialize()
    # TODO: move these cases where they belong
    # elif isinstance(x, np.integer):
    #     return int(x)
    # elif isinstance(x, np.floating):
    #     return float(x)
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _serialize_item(val)
        return ret
    elif type(x) == list:
        return [_serialize_item(val) for val in x]
    elif type(x) == tuple:
        # we need to distinguish between a tuple and list for json serialization
        return dict(
            _type='tuple',
            data=_serialize_item(list(x))
        )
    else:
        if _is_jsonable(x):
            # this will capture int, float, str, bool
            return x
        raise Exception(f'Unable to serialize item of type: {type(x)}')

def _is_jsonable(x):
    import json
    try:
        json.dumps(x)
        return True
    except:
        return False

def _deserialize_item(x):
    if type(x) == dict:
        if '_type' in x and x['_type'] == 'tuple':
            return _deserialize_item(tuple(x['data']))
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
        if _is_jsonable(x):
            # this will capture int, float, str, bool
            return x
        raise Exception(f'Unable to deserialize item of type: {type(x)}')

# Might be useful to keep these around even though we don't use them any more
# def _npy_to_b64(x):
#     f = io.BytesIO()
#     np.save(f, x)
#     return base64.b64encode(f.getvalue()).decode('utf-8')

# def _b64_to_npy(x):
#     bytes0 = base64.b64decode(x.encode())
#     f = io.BytesIO(bytes0)
#     return np.load(f)

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