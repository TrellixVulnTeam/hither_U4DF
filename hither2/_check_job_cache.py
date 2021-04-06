from typing import Any, Callable, Dict, List, Union
from .function import FunctionWrapper
from ._job_cache import JobCache, _compute_job_hash, job_cache_version
from ._job import JobResult, Job

def _batch_check_job_cache(jobs: List[Job]):
    # max number to check at a time
    batch_size = 50
    if len(jobs) > batch_size:
        for i in range(0, len(jobs), batch_size):
            _batch_check_job_cache(jobs[i:i+batch_size])
        return
    import kachery_p2p as kp
    watches = {}
    jobs_by_id: Dict[str, Job] = {}
    for job in jobs:
        jc = job.config.job_cache
        if jc is not None:
            jobs_by_id[job.job_id] = job
            job_hash = _compute_job_hash(function_name=job.function_name, function_version=job.function_version, kwargs=job.get_resolved_kwargs())
            jc._feed
            watches[job.job_id] = {
                'feedId': jc._feed.get_feed_id(),
                'subfeedName': {'jobHash': job_hash},
                'position': 0
            }
    while True:
        got_something = False
        if len(watches.keys()) > 0:
            new_messages = kp.watch_for_new_messages(watches, wait_msec=0)
        else:
            new_messages = {}
        for job_id, messages in new_messages.items():
            job = jobs_by_id[job_id]
            if len(messages) > 0:
                got_something = True
                del watches[job_id]
                obj = messages[-1] # last message
                if obj.get('jobCacheVersion', None) == job_cache_version:
                    try:
                        job_result = JobResult.from_cache_dict(
                            obj['jobResult']
                        )
                    except Exception as e:
                        print('Warning: problem retrieving cached result:', e)
                        job_result = None
                    if job_result is not None:
                        if job_result.status == 'finished':
                            print(f'Using cached result for {job.function_name} ({job.function_version})')
                            if job_result.console_lines is not None:
                                job._set_console_lines(job_result.console_lines)
                            job._set_finished(job_result.return_value, result_is_from_cache=True)
                else:
                    print('Warning: incorrect job cache version')
        if not got_something:
            break

def _check_job_cache(function_name: str, function_version: str, kwargs: Dict[str, Any], job_cache: JobCache):
    job_hash: Union[str, None] = _compute_job_hash(function_name=function_name, function_version=function_version, kwargs=kwargs)
    if job_hash is not None:
        job_result = job_cache._fetch_cached_job_result(job_hash)
        if job_result is not None:
            if job_result.status == 'finished':
                return job_result

def _write_result_to_job_cache(job_result: JobResult, function_name: str, function_version: str, kwargs: Dict[str, Any], job_cache: JobCache):
    job_hash: Union[str, None] = _compute_job_hash(function_name=function_name, function_version=function_version, kwargs=kwargs)
    if job_hash is not None:
        job_cache._cache_job_result(job_hash, job_result)