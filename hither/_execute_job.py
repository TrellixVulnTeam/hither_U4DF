from typing import TYPE_CHECKING
from ._enums import JobStatus
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._consolecapture import ConsoleCapture
from ._serialize_job import _serialize_job
from ._exceptions import JobCancelledException

if TYPE_CHECKING:
    from .job import Job

def _execute_job(job: 'Job', cancel_filepath=None) -> None:
    # Note that cancel_filepath will only have an effect if we are running this in a container
    container = job.get_container()
    if container is not None:
        job_serialized = _serialize_job(job=job, generate_code=True)
        success, result, runtime_info, error = _run_serialized_job_in_container(job_serialized, cancel_filepath=cancel_filepath)
        if success:
            job._set_finished_status(
                result=result,
                runtime_info=runtime_info
            )
        else:
            assert error is not None
            assert error != 'None'
            if runtime_info.get('cancelled'):
                exception = JobCancelledException(error)
            else:
                exception = Exception(error)
            job._set_error_status(
                runtime_info=runtime_info,
                exception=exception
            )
    else:
        assert job._f is not None, 'Cannot execute job outside of container when function is not available'
        try:
            args0 = job._wrapped_function_arguments
            with ConsoleCapture(label=job.get_label(), show_console=True) as cc:
                ret = job._f(**args0)
            job._set_finished_status(
                result=ret,
                runtime_info=cc.runtime_info()
            )
        except Exception as e:
            job._set_error_status(
                runtime_info=dict(),
                exception=e
            )
