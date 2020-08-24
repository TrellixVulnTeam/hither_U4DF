import time
import os
import io
import base64
import numpy as np
from typing import Union, List, Any, Callable
import random

def _serialize_item(x:Any, require_jsonable:bool=True) -> Any:
    if isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = _serialize_item(val, require_jsonable=require_jsonable)
        return ret
    elif type(x) == list:
        return [_serialize_item(val, require_jsonable=require_jsonable) for val in x]
    elif type(x) == tuple:
        # we need to distinguish between a tuple and list for json serialization
        return dict(
            _type='tuple',
            data=_serialize_item(list(x), require_jsonable=require_jsonable)
        )
    elif isinstance(x, np.ndarray):
        return dict(
            _type='npy',
            data_b64=_npy_to_b64(x)
        )
    else:
        if _is_jsonable(x):
            # this will capture int, float, str, bool
            return x
    if require_jsonable:
        # Did not return on any previous statement
        raise Exception(f'Unable to serialize item of type: {type(x)}')
    else:
        return x

def _is_jsonable(x:Any) -> bool:
    import json
    try:
        json.dumps(x)
        return True
    except:
        return False

def _deserialize_item(x:Any) -> Any:
    if isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif type(x) == dict:
        if '_type' in x and x['_type'] == 'tuple':
            return _deserialize_item(tuple(x['data']))
        elif '_type' in x and x['_type'] == 'npy':
            return _b64_to_npy(x['data_b64'])
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

def _npy_to_b64(x):
    f = io.BytesIO()
    np.save(f, x)
    return base64.b64encode(f.getvalue()).decode('utf-8')

def _b64_to_npy(x):
    bytes0 = base64.b64decode(x.encode())
    f = io.BytesIO(bytes0)
    return np.load(f)

def _random_string(num: int) -> str:
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))

def _flatten_nested_collection(item: Any, _type: Union[Any, None] = None) -> List[Any]:
    """Flattens the input data structure, returning a list of only the content (non-dict,
    list, or tuple) elements.

    Arguments:
        item {Any} -- A data structure consisting of [0..n) elements, each of which may be
        a list, dict, or tuple ("collection elements"), or a "content element." Collection
        elements may be nested to any depth, and may contain both collection elements and
        content elements at the same level.

    Keyword Arguments:
        _type {Union[Any, None]} -- If set, filter the content elements so that only those
        matching the input type are returned. (default: {None})

    Returns:
        List[Any] -- Every content (leaf) item of the input structure.
    """
    itemtype = type(item)
    if itemtype not in [dict, list, tuple]: 
        if _type is not None and not isinstance(item, _type): return []
        return [item]

    elements = []
    if itemtype == dict:
        for value in item.values():
            elements.extend(_flatten_nested_collection(value, _type))
    else: # item is a list or tuple
        for value in item:
            elements.extend(_flatten_nested_collection(value, _type))
        # equivalent to the briefer, but more confusing, version:
        # elements.extend(value for i in item for value in _flatten_nested_collection(i))
        # (which is read as "return `value`, for i in item: for value in _fnc(i):")
    return elements

def _copy_structure_with_changes(structure: Any, replacement_function: Callable[..., Any],
        _as_side_effect:bool = False,
        _type: Union[Any, None] = None) -> Any:
    """Create a copy of the input data structure, with certain values modified by <replacement_function>.
    Note that care must be taken to keep a separate reference to the original data structure if you
    wish to keep it as well as the replacement. Additionally, objects in <structure> are not
    deep-copied, so if <replacement_function> makes a modification to an object in <structure>,
    the original reference will still point to the modified object.

    Arguments:
        structure {Any} -- Structure to be modified (dict, list, nested dicts...)
        replacement_function {Callable[..., Any]} -- Function which applies mutations to
        the elements of <structure> and returns a new value. If the <_type> parameter is not
        set, this function must perform whatever type inspection is necessary to ensure
        it only operates on specific types and return without modification any
        values of types it does not operate on.

    Keyword Arguments:
        _as_side_effect {bool} -- If True, we assume that <replacement_function> will be called
        for its side effects (perhaps by calling a method on an object that does not have any
        return of its own), and so will return the original value of its target even if the
        target is of the selected type. For instance, in a replacement function of the form
        'lambda x: x.copy_to_server()' (where the method `copy_to_server(self)` causes self
        to take some action but does not return self), setting <_as_side_effect> ensures that
        the value of `x` appears in the returned data structure; if <_as_side_effect> were
        set to False, then the return of `x.copy_to_server()` would be used, resulting in
        the object being replaced with None in the copied structure. (default: {False})

        _type {Union[Any, None]} -- If set, only elements of the matching type will be passed
        to <replacement_function>; others will be put into place unmodified. (default: {None})

    Returns:
        Any -- A copy of the original data structure, with modifications.
    """
    entrytype = type(structure)
    copy = None
    # ignore cases where there is no data structure to modify
    if entrytype not in [dict, list, tuple]:
        if _type is not None and not isinstance(structure, _type):
            return structure
        if _as_side_effect:
            replacement_function(structure)
            return structure
        return replacement_function(structure)
    if entrytype == dict:
        copy = {}
        for k, v in structure.items():
            copy[k] = _copy_structure_with_changes(v, replacement_function, _type=_type)
        return copy
    elif entrytype == list:
        return [_copy_structure_with_changes(v, replacement_function, _type=_type) for v in structure]
    elif entrytype == tuple:
        return tuple([_copy_structure_with_changes(v, replacement_function, _type=_type) for v in structure])

def _docker_inject_user_dir():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    return f'{thisdir}/docker_inject_user'
