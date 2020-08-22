from typing import TYPE_CHECKING
from ._enums import JobStatus
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._consolecapture import ConsoleCapture
from ._serialize_job import _serialize_job

if TYPE_CHECKING:
    from .job import Job

def _execute_job(job: 'Job', cancel_filepath=None) -> None:
    # Note that cancel_filepath will only have an effect if we are running this in a container
    container = job.get_container()
    if container is not None:
        job_serialized = _serialize_job(job=job, generate_code=True)
        success, result, runtime_info, error = _run_serialized_job_in_container(job_serialized, cancel_filepath=cancel_filepath)
        job._runtime_info = runtime_info
        if success:
            job._result = result
            job._set_status(JobStatus.FINISHED)
        else:
            assert error is not None
            assert error != 'None'
            job._exception = Exception(error)
            job._set_status(JobStatus.ERROR)
    else:
        assert job._f is not None, 'Cannot execute job outside of container when function is not available'
        try:
            args0 = job._wrapped_function_arguments
            with ConsoleCapture(label=job.get_label(), show_console=True) as cc:
                ret = job._f(**args0)
            job._runtime_info = cc.runtime_info()
            job._result = ret
            job._set_status(JobStatus.FINISHED)
        except Exception as e:
            job._set_status(JobStatus.ERROR)
            job._exception = e
