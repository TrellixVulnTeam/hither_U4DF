from typing import Union, List, Any, Callable
import random
from ._enums import HitherFileType
from .file import File

def _serialize_item(x, require_jsonable=True):
    if isinstance(x, File):
        return x.serialize()
    # TODO: move these cases where they belong
    # elif isinstance(x, np.integer):
    #     return int(x)
    # elif isinstance(x, np.floating):
    #     return float(x)
    # TODO: This will be required when file enums are working
    # elif isinstance(x, HitherFileType):
    #     return x.value
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
    else:
        if _is_jsonable(x):
            # this will capture int, float, str, bool
            return x
    if require_jsonable:
        # Did not return on any previous statement
        raise Exception(f'Unable to serialize item of type: {type(x)}')
    else:
        return x

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

def _flatten_nested_collection(item: Any, _type: Union[Any, None] = None) -> List[Any]:
    """Return a single list of the base items in a nested data structure consisting of an
    arbitrary combination of dicts, lists, and tuples.s

    Arguments:
        item {Any} -- (Potentially arbitrarily) nested data structure to flatten. May contain
        base items and sub-collections at the same level.

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

def _replace_values_in_structure(structure: Any, replacement_function: Callable[..., Any]) -> Any:
    """Modify values of input data structure in place, according to function.

    Arguments:
        structure {Any} -- Structure to be modified (dict, list, nested dicts...)
        replacement_function {Callable[..., Any]} -- Function which applies mutations to
        the elements of <structure> and returns a new value to be stored in the original
        data structure. This function should perform whatever type inspection is necessary
        to ensure it only operates on specific types, and should return, unmodified, any
        values which it does not operate on.

    Returns:
        Any -- The original structure, as modified in-place.
    """
    entrytype = type(structure)
    # ignore cases where there is no data structure to modify
    if entrytype not in [dict, list, tuple]:
        return replacement_function(structure)
    if entrytype == dict:
        for k, v in structure.items():
            structure[k] = _replace_values_in_structure(v, replacement_function)
    elif entrytype == list:
        structure = [_replace_values_in_structure(v, replacement_function) for v in structure]
    elif entrytype == tuple:
        structure = tuple([_replace_values_in_structure(v, replacement_function) for v in structure])
    return structure


