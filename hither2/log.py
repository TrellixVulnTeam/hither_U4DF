from typing import Dict, List, Union, cast
import uuid
import kachery_p2p as kp
import time
from ._job import Job, _print_console_lines


class Log:
    def __init__(self):
        self._log_id = 'log-' + str(uuid.uuid4())[-12:]
        feed = kp.load_feed('hither-logs', create=True)
        logs_subfeed = feed.get_subfeed('logs')
        logs_subfeed.append_message({'log_id': self._log_id, 'timestamp': time.time() - 0})
        self._subfeed = feed.get_subfeed({'log_id': self._log_id})
    @property
    def log_id(self):
        return self._log_id
    def _report_job_created(self, job: Job):
        self._subfeed.append_message({
            'type': 'jobCreated',
            'timestamp': time.time() - 0,
            'job_id': job.job_id,
            'function_name': job.function_name,
            'function_version': job.function_version,
            'use_container': job.config.use_container
        })
    def _report_job_queued(self, job: Job):
        self._subfeed.append_message({
            'type': 'jobQueued',
            'timestamp': time.time() - 0,
            'job_id': job.job_id
        })
    def _report_job_running(self, job: Job):
        self._subfeed.append_message({
            'type': 'jobRunning',
            'timestamp': time.time() - 0,
            'job_id': job.job_id
        })
    def _report_job_finished(self, job: Job):
        self._subfeed.append_message({
            'type': 'jobFinished',
            'timestamp': time.time() - 0,
            'job_id': job.job_id,
            'console_lines_uri': kp.store_json(job.result.console_lines) if job.result.console_lines is not None else None
        })
    def _report_job_error(self, job: Job):
        self._subfeed.append_message({
            'type': 'jobError',
            'timestamp': time.time() - 0,
            'job_id': job.job_id,
            'error_message': str(job.result.error),
            'console_lines_uri': kp.store_json(job.result.console_lines) if job.result.console_lines is not None else None
        })

class LogReader:
    def __init__(self, log_id: str) -> None:
        self._log_id = log_id
        feed = kp.load_feed('hither-logs', create=True)
        self._subfeed = feed.get_subfeed({'log_id': self._log_id})
        self._jobs: Dict[str, LogReaderJob] = {}
    def print(self, *, print_console: bool=False, job_id: Union[None, str]):
        messages = self._subfeed.get_next_messages()
        for m in messages:
            self._process_message(m)
            t = m.get('type', None)
            _job_id = m.get('job_id', '')
            if (job_id is None) or (_job_id == '') or (job_id == _job_id):
                j = self._jobs.get(_job_id, None)
                ts = m.get('timestamp', None)
                if j is None:
                    continue
                if t == 'jobCreated':
                    print(f'{_fmt_time(ts)} JOB-CREATED   {_job_id} {j.function_name} ({j.function_version})')
                elif t == 'jobQueued':
                    print(f'{_fmt_time(ts)} JOB-QUEUED    {_job_id} {j.function_name} ({j.function_version})')
                elif t == 'jobRunning':
                    print(f'{_fmt_time(ts)} JOB-RUNNING   {_job_id} {j.function_name} ({j.function_version})')
                elif t == 'jobFinished':
                    print(f'{_fmt_time(ts)} JOB-FINISHED  {_job_id} {j.function_name} ({j.function_version})')
                elif t == 'jobError':
                    print(f'{_fmt_time(ts)} JOB-ERROR     {_job_id} {j.function_name} ({j.function_version}) - {j.error_message}')
                if t in ['jobFinished', 'jobError']:
                    if print_console:
                        cl = j.console_lines
                        if cl is not None:
                            _print_console_lines(cl, label=f'[{_job_id}]')
    @property
    def log_id(self):
        return self._log_id
    
    def _process_message(self, m: dict):
        t = m.get('type', None)
        if t == 'jobCreated':
            job_id = m.get('job_id', '')
            j = LogReaderJob(created_message=m)
            self._jobs[job_id] = j
        elif t in ['jobQueued', 'jobRunning', 'jobFinished', 'jobError']:
            job_id = m.get('job_id', '')
            j = self._jobs.get(job_id, None)
            if j is not None:
                j._process_message(m)

class LogReaderJob:
    def __init__(self, created_message: dict):
        m = created_message
        self._timestamps = {
            'created': None,
            'queued': None,
            'running': None,
            'finished': None,
            'error': None
        }
        self._job_id: str = m.get('job_id', '')
        self._function_name: str = m.get('function_name', '')
        self._function_version: str = m.get('function_version', '')
        self._use_container: bool = m.get('use_container', False)
        self._timestamps['created'] = m.get('timestamp', None)
        self._status = 'pending'
        self._error_message: Union[None, str] = None
        self._console_lines_uri: Union[None, str] = None
    def _process_message(self, m: dict):
        t = m.get('type', None)
        if t == 'jobQueued':
            timestamp = m.get('timestamp', None)
            self._timestamps['queued'] = timestamp
            self._status = 'queued'
        elif t == 'jobRunning':
            timestamp = m.get('timestamp', None)
            self._timestamps['running'] = timestamp
            self._status = 'running'
        elif t == 'jobFinished':
            timestamp = m.get('timestamp', None)
            self._timestamps['finished'] = timestamp
            self._status = 'finished'
            self._console_lines_uri = m.get('console_lines_uri', None)
        elif t == 'jobError':
            timestamp = m.get('timestamp', None)
            self._timestamps['error'] = timestamp
            self._status = 'error'
            self._error_message = m.get('error_message', '')
            self._console_lines_uri = m.get('console_lines_uri', None)
    @property
    def status(self) -> Union[str, None]:
        return self._status
    @property
    def error_message(self) -> Union[str, None]:
        return self._error_message
    @property
    def function_name(self):
        return self._function_name
    @property
    def function_version(self):
        return self._function_version
    @property
    def console_lines(self) -> Union[None, List[dict]]:
        if self._console_lines_uri is not None:
            return cast(Union[None, List[dict]], kp.load_json(self._console_lines_uri))
        else:
            return None
    @property
    def timestamp_created(self) -> Union[float, None]:
        return self._timestamps['created']
    @property
    def timestamp_queued(self) -> Union[float, None]:
        return self._timestamps['queued']
    @property
    def timestamp_running(self) -> Union[float, None]:
        return self._timestamps['running']
    @property
    def timestamp_finished(self) -> Union[float, None]:
        return self._timestamps['finished']
    @property
    def timestamp_error(self) -> Union[float, None]:
        return self._timestamps['error']

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()
