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
    QUEUED = 'queued'
    RUNNING = 'running'
    FINISHED = 'finished'

    @classmethod
    def complete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.ERROR, JobStatus.FINISHED]

    @classmethod
    def incomplete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.QUEUED, JobStatus.RUNNING]

    @classmethod
    def prerun_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.PENDING, JobStatus.QUEUED]

    @classmethod
    def local_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.FINISHED, JobStatus.ERROR]

class SerializedJobKeys:
    CACHE_FAILING = 'cache_failing'
    CODE_URI = 'code_uri'
    CONTAINER = 'container'
    FORCE_RUN = 'force_run'
    FUNCTION = 'function'
    FUNCTION_NAME = 'function_name'
    FUNCTION_VERSION = 'function_version'
    JOB_ID = 'job_id'
    JOB_TIMEOUT = 'job_timeout'
    LABEL = 'label'
    RERUN_FAILING = 'rerun_failing'
    REQUIRED_FILES = 'required_files'
    JHPARAMS = 'jhparams'
    WRAPPED_ARGS = 'kwargs' # TODO CHANGE ME ONCE ALL REFERENCES ARE CENTRALIZED

class InternalFunctionAttributeKeys:
    HITHER_ADDITIONAL_FILES = '_hither_additional_files'
    HITHER_CONTAINER = '_hither_container'
    HITHER_LOCAL_MODULES = '_hither_local_modules'
    HITHER_GENERATED_CODE_URI = '_hither_generated_code_uri'

class JobKeys:
    RESULT = 'result'
    RESULT_URI = 'result_uri'
    RUNTIME_INFO = 'runtime_info'
    SERIALIZATION = 'job_serialized'
    STATUS = 'status'
    TIMED_OUT = 'Timed out'

class EnvironmentKeys:
    ## Environment variables
    HITHER_DEBUG_ENV = 'HITHER_DEBUG'
    HITHER_USE_SINGULARITY_ENV = 'HITHER_USE_SINGULARITY'
    HITHER_NO_DOCKER_PULL_ENV = 'HITHER_DO_NOT_PULL_DOCKER_IMAGES'

class ConfigKeys:
    ## Keys for the configuration dictionary
    CONTAINER = 'container'
    JOB_HANDLER = 'job_handler'
    JOB_CACHE = 'job_cache'
    TIMEOUT = 'job_timeout'
    FORCE_RUN = 'force_run'
    RERUN_FAILING = 'rerun_failing'
    CACHE_FAILING = 'cache_failing'
    REQUIRED_FILES = 'required_files'
    JHPARAMS = 'jhparams'

    @staticmethod
    def known_configuration_keys() -> List[str]:
        return [ConfigKeys.CONTAINER, ConfigKeys.JOB_HANDLER, ConfigKeys.JOB_CACHE,
                ConfigKeys.TIMEOUT, ConfigKeys.FORCE_RUN,
                ConfigKeys.RERUN_FAILING, ConfigKeys.CACHE_FAILING, ConfigKeys.REQUIRED_FILES]

class CachedJobResultKeys:
    JOB_HASH = 'job_hash'
    RESULT = 'result'
    RESULT_URI = 'result_uri'
    EXCEPTION = 'exception'
    RUNTIME_INFO = 'runtime_info'
    STATUS = 'status'