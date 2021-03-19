from typing import Any

def _serialize_input(x: Any):
    if isinstance(x, int) or isinstance(x, float) or isinstance(x, str) or isinstance(x, bool) or (x is None):
        return x
    elif isinstance(x, dict):
        y = {}
        for k, v in x.items():
            y[k] = _serialize_input(v)
        return y
    elif isinstance(x, list):
        return [_serialize_input(a) for a in x]
    elif isinstance(x, tuple):
        return {
            '_hither2_type': 'tuple',
            'value': [_serialize_input(a) for a in x]
        }
    elif _is_numpy_array(x):
        return _serialize_numpy_array(x)
    elif _is_numpy_number(x):
        return _serialize_numpy_number(x)    
    else:
        raise Exception('Input is not serializable')

def _serialize_output(x: Any):
    try:
        return _serialize_input(x)
    except:
        raise Exception('Output is not serializable')

def _is_numpy_array(x):
    try:
        import numpy as np
    except:
        return False
    return isinstance(x, np.ndarray)

def _serialize_numpy_array(x):
    import kachery_p2p as kp
    return {
        '_hither2_type': 'numpy.ndarray',
        'value': kp.store_npy(x)
    }

def _deserialize_numpy_array(x: dict):
    import kachery_p2p as kp
    v = x['value']
    return kp.load_npy(v)

def _is_numpy_number(x):
    try:
        import numpy as np
    except:
        return False
    return isinstance(x, np.integer) or isinstance(x, np.floating) or isinstance(x, np.complexfloating)

def _serialize_numpy_number(x):
    import numpy as np
    if isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif isinstance(x, np.complexfloating):
        raise Exception('Complex scalars not supported for serialization. Use np.ndarray instead.')
    else:
        raise Exception('Unexpected numpy number')

def _deserialize_input(x: Any):
    if isinstance(x, int) or isinstance(x, float) or isinstance(x, str) or isinstance(x, bool) or (x is None):
        return x
    elif isinstance(x, dict):
        ht = x.get('_hither2_type', None)
        if ht == 'tuple':
            return tuple([_deserialize_input(a) for a in x['value']])
        elif ht == 'numpy.ndarray':
            return _deserialize_numpy_array(x)
        else:
            y = {}
            for k, v in x.items():
                y[k] = _deserialize_input(v)
            return y
    elif isinstance(x, list):
        return [_deserialize_input(a) for a in x]
    else:
        raise Exception('Input is not deserializable')

def _deserialize_output(x: Any):
    try:
        return _deserialize_input(x)
    except:
        raise Exception('Output is not deserializable')