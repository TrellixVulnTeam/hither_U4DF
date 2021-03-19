import time
import uuid
from typing import Any, Callable, Union

class JobResult:
    def __init__(self, *, return_value: Any=None, error: Union[Exception, None]=None, status: str):
        self._return_value = return_value
        self._error = error
        self._status = status
    def get_return_value(self):
        return self._return_value
    def get_error(self):
        return self._error
    def get_status(self):
        return self._status

class Job:
    def __init__(self, function: Callable, kwargs: dict):
        from ._config import Config
        from ._job_manager import global_job_manager
        self._job_manager = global_job_manager
        self._config = Config.get_current_config()
        self._function = function
        self._kwargs = kwargs
        self._job_id = 'j-' + str(uuid.uuid4())[-12:]
        self._timestamp_created = time.time()
        self._status = 'pending'
        self._result: Union[JobResult, None] = None
        self._error: Union[Exception, None] = None

        self._job_manager._add_job(self)
    def get_job_id(self):
        return self._job_id
    def get_status(self):
        return self._status
    def get_function(self):
        return self._function
    def get_kwargs(self):
        x = _resolve_kwargs(self._kwargs)
        assert isinstance(x, dict)
        return x
    def get_config(self):
        return self._config
    def get_result(self):
        return self._result
    def _set_queued(self):
        self._status = 'queued'
    def _set_finished(self, return_value: Any):
        self._status = 'finished'
        self._result = JobResult(return_value=return_value, status='finished')
    def _set_error(self, error: Exception):
        self._status = 'error'
        self._result = JobResult(error=error, status='error')
    def wait(self, timeout_sec: Union[float, None]=None):
        timer = time.time()
        while True:
            self._job_manager._iterate()
            if self._status == 'finished':
                r = self._result
                assert r is not None
                return r
            elif self._status == 'error':
                e = self._error
                assert e is not None
                raise e
            else:
                time.sleep(0.05)
            if timeout_sec is not None:
                elaped = time.time() - timer
                if elaped > timeout_sec:
                    return None

def _resolve_kwargs(x: Any):
    if isinstance(x, Job):
        if x.get_status() == 'finished':
            return x.get_result().get_return_value()
        else:
            return x
    elif isinstance(x, dict):
        y = {}
        for k, v in x.items():
            y[k] = _resolve_kwargs(v)
        return y
    elif isinstance(x, list):
        return [_resolve_kwargs(a) for a in x]
    elif isinstance(x, tuple):
        return tuple([_resolve_kwargs(a) for a in x])
    else:
        return x