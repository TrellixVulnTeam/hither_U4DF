from enum import Enum
from typing import Any, Union, Callable, List, Type

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
    def get_complete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.ERROR, JobStatus.FINISHED, JobStatus.CANCELED]

    @classmethod
    def get_incomplete_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.QUEUED, JobStatus.RUNNING]

    @classmethod
    def get_prerun_statuses(cls: Type['JobStatus']) -> List['JobStatus']:
        return [JobStatus.PENDING, JobStatus.QUEUED]
    
    