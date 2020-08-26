import numbers

def _result_small_enough_to_store_directly(x, allow_small_dicts=True, allow_small_lists=True):
    if isinstance(x, numbers.Number):
        return True
    if isinstance(x, str):
        if len(x) <= 1000:
            return True
    if allow_small_dicts and isinstance(x, dict):
        if len(x.keys()) <= 3:
            for k, v in x.items():
                if not _result_small_enough_to_store_directly(k, allow_small_dicts=False, allow_small_lists=False):
                    return False
                if not _result_small_enough_to_store_directly(v, allow_small_dicts=False, allow_small_lists=False):
                    return False
            return True
    if allow_small_lists and (isinstance(x, tuple) or isinstance(x, list)):
        if len(x) <= 3:
            for v in x:
                if not _result_small_enough_to_store_directly(v, allow_small_dicts=False, allow_small_lists=False):
                    return False
            return True
    return False