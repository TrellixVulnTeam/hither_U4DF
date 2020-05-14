from enum import Enum
from typing import Any, Union, Callable, List, Type, Dict, Tuple

# TODO: NOTE: We should be targeting Python 3.7+ for performance reasons when using typing
# NOTE: This will also allow self-referential class type annotations without the ''s, IF
# we do `from __future__import annotations` at the top
# See https://stackoverflow.com/questions/33533148/how-do-i-specify-that-the-return-type-of-a-method-is-the-same-as-the-class-itsel
# and https://stackoverflow.com/questions/41135033/type-hinting-within-a-class

class JobStatus(Enum):
    ERROR = 'error'
    PENDING = 'pending' # remote-only status
    WAITING = 'waiting' # remote-only status
    QUEUED = 'queued'
    RUNNING = 'running'
    FINISHED = 'finished'
    CANCELED = 'canceled' # remote-only status (for compute resource/Slurm/etc)

    @classmethod
    def complete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.ERROR, JobStatus.FINISHED, JobStatus.CANCELED]

    @classmethod
    def incomplete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.QUEUED, JobStatus.RUNNING]

    @classmethod
    def prerun_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.PENDING, JobStatus.QUEUED]

    @classmethod
    def local_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.FINISHED, JobStatus.ERROR]

class HitherFileType(Enum):
    FILE = 'file'
    NUMPY = 'ndarray'
    SERIALIZED_FILE = 'hither_file'
    
class JobKeys:
    CLIENT_CODE = 'client_code'
    CODE = 'code'
    COMPUTE_RESOURCE = 'compute_resource_id'
    COMPUTE_RESOURCE_STATUS = 'compute_resource_status' # the Job's status on the remote resource.
    CONTAINER = 'container'
    DOWNLOAD_RESULTS = 'download_results'
    EXCEPTION = 'exception'
    FUNCTION = 'function'
    FUNCTION_NAME = 'function_name'
    FUNCTION_VERSION = 'function_version'
    JOB_HASH = 'hash'
    JOB_ID = 'job_id'
    JOB_TIMEOUT = 'job_timeout'
    LABEL = 'label'
    # represents whether the job state was last changed by the compute resource (as opposed to
    # by the local job handler)
    LAST_MODIFIED_BY_COMPUTE_RESOURCE = 'last_modified_by_compute_resource'
    NO_RESOLVE_INPUT_FILES = 'no_resolve_input_files'
    RESULT = 'result'
    RUNTIME_INFO = 'runtime_info'
    SERIALIZATION = 'job_serialized'
    STATUS = 'status'
    WRAPPED_ARGS = 'kwargs' # TODO CHANGE ME ONCE ALL REFERENCES ARE CENTRALIZED

    @staticmethod
    def _verify_serialized_job(doc:Dict[str, any]) -> bool:
        """Checks an input dictionary (expected to correspond to a Job serialized to MongoDB job
        monitor bus) for a bare minimum of required keys to establish it is actually a Job object.

        Arguments:
            doc {Dict[str, any]} -- Dictionary of fields corresponding to a Hither Job.

        Returns:
            bool -- True if required fields are present; else False.
        """
        required_keys = [
            JobKeys.LABEL,
            JobKeys.CODE,
            JobKeys.CONTAINER
        ]
        for x in required_keys:
            if not x in doc: return False
        return True

    @staticmethod
    def _unpack_serialized_job(doc:Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """Fetches the Job Id, Handler Id, and serialized job representation from a Mongo
        serialized Job record.

        Arguments:
            doc {Dict[str, any]} -- A JSON document serialized for the Job-bus database.

        Raises:
            Exception: Raised if the input document is not in the right format, as evidenced by
            missing required keys.

        Returns:
            Tuple[str, str, Dict[str, Any]] -- Tuple of (Job_Id, handler_id, serialized_job)
        """
        required_keys = [JobKeys.SERIALIZATION, JobKeys.JOB_ID, JobHandlerKeys.HANDLER_ID]
        for x in required_keys:
            if x in doc: continue
            raise Exception(f"Input document {doc} is missing required key {x}; is it really a serialized Job?")
        job_id = doc[JobKeys.JOB_ID]
        handler = doc[JobHandlerKeys.HANDLER_ID]
        serialized_job = doc[JobKeys.SERIALIZATION]
        return (job_id, handler, serialized_job)

class JobHandlerKeys:
    UTCTIME = 'utctime'
    HANDLER_ID = 'handler_id'

class ComputeResourceKeys:
    COMPUTE_RESOURCE = JobKeys.COMPUTE_RESOURCE # alias, since these should have same value
    KACHERY = 'kachery'
    UTCTIME = 'utctime'