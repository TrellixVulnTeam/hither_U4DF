from hither2.run_script_in_container import DockerImage
import time
import uuid
from typing import Any, Callable, Union

class JobResult:
    def __init__(self, *, return_value: Any=None, error: Union[Exception, None]=None, status: str):
        if status == 'finished':
            assert error == None, 'Error must be None if status is finished'
        elif status == 'error':
            assert return_value == None, 'Return value must be None if status is error'
        else:
            raise Exception(f'Unexpected status for job result: {status}')
        self._return_value = return_value
        self._error = error
        self._status = status
    @property
    def return_value(self):
        return self._return_value
    @property
    def error(self):
        return self._error
    @property
    def status(self):
        return self._status
    def to_cache_dict(self):
        import kachery_p2p as kp
        return {
            'returnValueUri': kp.store_pkl(self._return_value) if self._return_value is not None else None,
            'errorMessage': str(self._error) if self._error is not None else None,
            'status': self._status
        }
    @staticmethod
    def from_cache_dict(x: dict):
        rv = x.get('returnValue', None)
        e = x.get('errorMessage', None)
        s = x.get('status', '')
        return JobResult(
            return_value=rv,
            error=Exception(e) if e is not None else None,
            status=s
        )

class Job:
    def __init__(self, function: Callable, kwargs: dict):
        from ._config import Config
        from ._job_manager import global_job_manager
        from .function import _get_hither_function_wrapper
        self._job_manager = global_job_manager
        self._config = Config.get_current_config()
        self._function = function
        fw = _get_hither_function_wrapper(function)
        if fw is None:
            raise Exception('This function is not a hither function. You must use the @hither.function decorator.')
        self._function_wrapper = fw
        self._kwargs = kwargs
        self._job_id = 'j-' + str(uuid.uuid4())[-12:]
        self._timestamp_created = time.time()
        self._status = 'pending'
        self._result: Union[JobResult, None] = None
        if self._config.use_container:
            if self._function_wrapper.image is not None:
                self._function_wrapper.image.prepare()

        self._job_manager._add_job(self)
    @property
    def job_id(self):
        return self._job_id
    @property
    def status(self):
        return self._status
    @property
    def function(self):
        return self._function
    @property
    def function_wrapper(self):
        return self._function_wrapper
    @property
    def function_name(self):
        return self._function_wrapper.name
    @property
    def function_version(self):
        return self._function_wrapper.version
    @property
    def image(self) -> Union[DockerImage, None]:
        return self._function_wrapper.image
    def get_resolved_kwargs(self):
        x = _resolve_kwargs(self._kwargs)
        assert isinstance(x, dict)
        return x
    @property
    def config(self):
        return self._config
    @property
    def result(self):
        return self._result
    def _set_queued(self):
        self._status = 'queued'
    def _set_running(self):
        self._status = 'running'
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
                e = self._result._error
                assert e is not None
                raise Exception(f'Error in {self.function_name} ({self.function_version}): {str(e)}')
            else:
                time.sleep(0.05)
            if timeout_sec is not None:
                elaped = time.time() - timer
                if elaped > timeout_sec:
                    return None

def _resolve_kwargs(x: Any):
    if isinstance(x, Job):
        if x.status == 'finished':
            return x.result.return_value
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