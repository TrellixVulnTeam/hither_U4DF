from typing import TYPE_CHECKING
from ._enums import InternalFunctionAttributeKeys, SerializedJobKeys
from ._generate_source_code_for_function import _generate_source_code_for_function
from ._util import _serialize_item, _deserialize_item

if TYPE_CHECKING:
    from .job import Job

def _serialize_job(job: 'Job', generate_code:bool):
    function_name = job.get_function_name()
    function_version = job.get_function_version()
    if generate_code:
        if job._code is not None:
            code = job._code
        else:
            assert job._f is not None, 'Cannot serialize function with generate_code=True when function and code are both not available'
            # only generate code once per function
            if not hasattr(job._f, '_hither_generated_code'):
                code0 = _generate_source_code_for_function(job._f)
                setattr(job._f, InternalFunctionAttributeKeys.HITHER_GENERATED_CODE, code0)
            code = getattr(job._f, InternalFunctionAttributeKeys.HITHER_GENERATED_CODE)
        function = None
    else:
        assert job._f is not None, 'Cannot serialize function with generate_code=False when function is not available'
        code = None
        function = job._f
    x = {
        SerializedJobKeys.JOB_ID: job._job_id,
        SerializedJobKeys.FUNCTION: function,
        SerializedJobKeys.CODE: code,
        SerializedJobKeys.FUNCTION_NAME: function_name,
        SerializedJobKeys.FUNCTION_VERSION: function_version,
        SerializedJobKeys.LABEL: job.get_label(),
        SerializedJobKeys.WRAPPED_ARGS: _serialize_item(job._wrapped_function_arguments),
        SerializedJobKeys.CONTAINER: job._container,
        SerializedJobKeys.JOB_TIMEOUT: job._job_timeout,
        SerializedJobKeys.FORCE_RUN: job._force_run,
        SerializedJobKeys.RERUN_FAILING: job._rerun_failing,
        SerializedJobKeys.CACHE_FAILING: job._cache_failing
    }
    return x

def _deserialize_job(
    *,
    serialized_job,
    job_handler,
    job_cache,
    job_manager
) -> 'Job':
    from .job import Job
    j = serialized_job
    return Job(
        job_id=j[SerializedJobKeys.JOB_ID],
        f=None,
        code=j[SerializedJobKeys.CODE],
        wrapped_function_arguments=_deserialize_item(j[SerializedJobKeys.WRAPPED_ARGS]),
        job_handler=job_handler,
        job_cache=job_cache,
        job_manager=job_manager,
        container=j[SerializedJobKeys.CONTAINER],
        label=j[SerializedJobKeys.LABEL],
        force_run=j[SerializedJobKeys.FORCE_RUN],
        rerun_failing=j[SerializedJobKeys.RERUN_FAILING],
        cache_failing=j[SerializedJobKeys.CACHE_FAILING],
        function_name=j[SerializedJobKeys.FUNCTION_NAME],
        function_version=j[SerializedJobKeys.FUNCTION_VERSION],
        job_timeout=j[SerializedJobKeys.JOB_TIMEOUT]
    )
