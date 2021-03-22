from typing import Any, Callable, Dict, Union
from .function import FunctionWrapper
from ._job_cache import JobCache, _compute_job_hash


def _check_job_cache(function_name: str, function_version: str, kwargs: Dict[str, Any], job_cache: JobCache):
    job_hash: Union[str, None] = _compute_job_hash(function_name=function_name, function_version=function_version, kwargs=kwargs)
    if job_hash is not None:
        job_result = job_cache._fetch_cached_job_result(job_hash)
        if job_result is not None:
            if job_result.status == 'finished':
                return job_result