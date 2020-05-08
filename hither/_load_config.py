from typing import Any, Dict, Optional, List, Union
import os
import kachery as ka
import tempfile
import json
import stat
import time
from urllib import request
from ._util import _random_string

def _load_preset_config_from_github(*, url, name):
    config = _load_config_from_github(url)
    config = config['configurations']
    if name not in config:
        raise Exception(f'Preset configuration not found: {name}')
    config = config[name]
    config = _resolve_env(config)
    return config

def _resolve_env(x: Any) -> Any:
    if type(x) == dict and 'env' in x:
        env0 = x['env']
        if env0 in os.environ:
            return os.environ[env0]
        else:
            raise Exception('You need to set the {} environment variable'.format(env0))
    elif type(x) == dict:
        ret = dict()
        for k, v in x.items():
            ret[k] = _resolve_env(v)
        return ret
    elif type(x) == list:
        return [_resolve_env(a) for a in x]
    elif type(x) == tuple:
        return tuple([_resolve_env(a) for a in x])
    else:
        return x

_global_config_cache = dict()
def _load_config_from_github(url) -> Dict:
    if url in _global_config_cache:
        return dict(_global_config_cache[url])
    hash0 = ka.get_object_hash(dict(url=url))
    config_path = f'{tempfile.gettempdir()}/hither_config_{hash0}.json'
    try_download = True
    obj = None
    if os.path.exists(config_path) and (os.getenv('RUNNING_PYTEST', None) != 'TRUE'): # pragma: no cover
        try:
            with open(config_path, 'r') as f:
                obj0 = json.load(f)
            if obj0 and obj0.get('configurations'):
                obj = obj0
        except:
            pass
        if obj is not None and _file_age_sec(config_path) <= 60:
            try_download = False
    if try_download:
        try:
            obj0 = _http_get_json(f'{url}?{_random_string(10)}') # use random string to prevent using cache for http request
            if obj0 is not None and obj0.get('configurations', None):
                obj = obj0
                try:
                    with open(config_path, 'w') as f:
                        json.dump(obj, f)
                except: # pragma: no cover
                    print(f'Warning: Problem writing preset configurations to: {config_path}')
            else: # pragma: no cover
                print('Warning: Problem loading preset configurations from: {}'.format(url))
        except: # pragma: no cover
            print('Warning: unable to load preset configurations from: {}'.format(url))
    if obj is None:
        raise Exception('Unable to load preset configurations')
    _global_config_cache[url] = obj
    return obj

def _file_age_sec(pathname):
    return time.time() - os.stat(pathname)[stat.ST_MTIME]

def _http_get_json(url: str, use_cache_on_success: bool=False, verbose: Optional[bool]=False, retry_delays: Optional[List[float]]=None) -> Union[dict, None]:
    timer = time.time()
    if retry_delays is None:
        retry_delays = [0.2, 0.5]
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_get_json::: ' + url)
    try:
        req = request.urlopen(url)
    except: # pragma: no cover
        if len(retry_delays) > 0:
            print('Retrying http request in {} sec: {}'.format(
                retry_delays[0], url))
            time.sleep(retry_delays[0])
            return _http_get_json(url, verbose=verbose, retry_delays=retry_delays[1:])
        else:
            return None
    try:
        ret = json.load(req)
    except:
        print(f'Unable to parse json from url: {url}')
        return None
    if verbose:
        print('Elapsed time for _http_get_json: {} {}'.format(time.time() - timer, url))
    return ret