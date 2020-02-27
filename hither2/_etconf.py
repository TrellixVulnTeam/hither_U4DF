from copy import deepcopy
import time
import os
import stat
import json
from typing import Optional, List, Union
import urllib.request as request
from ._filelock import FileLock

class ETConf:
    def __init__(self, *, defaults, config_dir=None):
        self._defaults = defaults
        self._config_dir = config_dir
        self._config = deepcopy(self._defaults)
    def set_config(self, **kwargs):
        for k, v in kwargs.items():
            if v is not None:
                if isinstance(v, dict):
                    self._config[k] = deepcopy(v)
                else:
                    self._config[k] = v
    def set_full_config(self, config):
        for k, v in config.items():
            if isinstance(v, dict):
                self._config[k] = deepcopy(v)
            else:
                self._config[k] = v
    def get_config(self):
        ret = dict()
        for k, v in self._config.items():
            if isinstance(v, dict):
                ret[k] = deepcopy(v)
            else:
                ret[k] = v
        return ret
    def get(self, k):
        c = self.get_config()
        return c[k]
