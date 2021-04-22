from hither2.runtimehook import PreContainerContext
from .run_scriptdir_in_container import DockerImage
import time
import uuid
from typing import Any, Callable, Dict, List, Union, cast

class JobResult:
    def __init__(self, *, return_value: Any=None, error: Union[Exception, None]=None, console_lines: List[dict], status: str):
        if status == 'finished':
            assert error == None, 'Error must be None if status is finished'
        elif status == 'error':
            assert return_value == None, 'Return value must be None if status is error'
        else:
            raise Exception(f'Unexpected status for job result: {status}')
        self._return_value = return_value
        self._error = error
        self._console_lines = console_lines
        self._status = status
    @property
    def return_value(self):
        return self._return_value
    @property
    def error(self):
        return self._error
    @property
    def console_lines(self):
        return self._console_lines
    @property
    def status(self):
        return self._status
    def to_cache_dict(self):
        import kachery_p2p as kp
        return {
            'returnValueUri': kp.store_pkl(self._return_value) if self._return_value is not None else None,
            'errorMessage': str(self._error) if self._error is not None else None,
            'consoleLinesUri': kp.store_json(self._console_lines),
            'status': self._status
        }
    @staticmethod
    def from_cache_dict(x: dict):
        import kachery_p2p as kp
        rv_uri = x.get('returnValueUri', None)
        e = x.get('errorMessage', None)
        cl_uri = x.get('consoleLinesUri', None)
        s = x.get('status', '')
        if rv_uri is None:
            raise Exception('No returnValueUri')
        if kp.load_file(rv_uri) is None:
            raise Exception('Unable to load cached return value')
        return_value = kp.load_pkl(rv_uri)
        if cl_uri is not None:
            cl = cast(List[dict], kp.load_json(cl_uri))
        else:
            cl = None
        return JobResult(
            return_value=return_value,
            error=Exception(e) if e is not None else None,
            console_lines=cl if cl is not None else [],
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
        self._timestamp_created: float = time.time() - 0
        self._timestamp_started: Union[float, None] = None
        self._timestamp_completed: Union[float, None] = None
        self._status = 'pending'
        self._cancel_pending = False
        self._result: Union[JobResult, None] = None
        self._result_is_from_cache: bool = False
        self._console_lines: Union[None, List[dict]] = None

        self._job_manager._add_job(self)
        if self._config.log:
            self._config.log._report_job_created(self)
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
    def image(self) -> Union[DockerImage, bool, None]:
        return self._function_wrapper.image
    @property
    def timestamp_started(self):
        return self._timestamp_started
    @property
    def timestamp_completed(self):
        return self._timestamp_completed
    def _prepare(self, kwargs: Dict[str, Any]):
        image = self.get_image(kwargs)
        if image is not None:
            image.prepare()
    def get_image(self, kwargs: Dict[str, Any]) -> Union[DockerImage, None]:
        if not self.config.use_container:
            return None
        image = self._function_wrapper.image
        if isinstance(image, bool):
            if image:
                ctx = PreContainerContext(kwargs=kwargs, image=image)
                for h in self._function_wrapper._runtime_hooks:
                    h.precontainer(ctx)
                image = ctx.image
                if not isinstance(image, DockerImage):
                    raise Exception('Precontainer hooks did not resolve image')
                return image
            else:
                return None
        else:
            return image
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
    @property
    def result_is_from_cache(self):
        return self._result_is_from_cache
    def cancel(self):
        self._cancel_pending = True
    @property
    def cancel_pending(self):
        return self._cancel_pending
    @property
    def log(self):
        return self.config.log
    def _set_queued(self):
        self._status = 'queued'
        if self.log:
            self.log._report_job_queued(self)
    def _set_running(self):
        self._timestamp_started = time.time() - 0
        self._status = 'running'
        if self.log:
            self.log._report_job_running(self)
    def _set_finished(self, return_value: Any, result_is_from_cache: bool=False):
        self._timestamp_completed = time.time() - 0
        self._status = 'finished'
        self._result = JobResult(return_value=return_value, status='finished', console_lines=self._console_lines if self._console_lines is not None else [])
        self._result_is_from_cache = result_is_from_cache
        if self.log:
            self.log._report_job_finished(self)
    def _set_error(self, error: Exception):
        self._timestamp_completed = time.time() - 0
        self._status = 'error'
        self._result = JobResult(error=error, status='error', console_lines=self._console_lines if self._console_lines is not None else [])
        if self.log:
            self.log._report_job_error(self)
    def _set_console_lines(self, lines: List[dict]=[]):
        self._console_lines = lines
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
    def print_console(self, label: Union[None, str]=None):
        if label is None:
            label = self.function_name
        lines = self._console_lines
        if lines is not None:
            _print_console_lines(lines, label=label)

def _print_console_lines(lines: List[dict], *, label: str=''):
    if lines is None:
        return
    for line in lines:
        ts = line["timestamp"]
        txt = line["text"]
        label_str = f' {label}' if label else ''
        print(f'{_fmt_time(ts)}{label_str}: {txt}')

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

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()