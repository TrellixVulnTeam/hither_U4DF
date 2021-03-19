from copy import deepcopy
from enum import Enum
from typing import Union, cast
from collections import deque
from typing import Any, Deque, Dict, List, Union, TYPE_CHECKING
from ._job_handler import JobHandler, DefaultJobHandler

class Inherit(Enum):
    INHERIT = ''

class ConfigEntry:
    def __init__(self, use_container: bool, job_handler: JobHandler):
        self.use_container = use_container
        self.job_handler = job_handler

class Config:
    config_stack: Deque[ConfigEntry] = deque()

    def __init__(self,
        use_container: Union[bool, Inherit]=Inherit.INHERIT,
        job_handler: Union[JobHandler, Inherit]=Inherit.INHERIT
    ):
        old_config = Config.config_stack[-1] # throws if no default set
        self.new_config = ConfigEntry(
            use_container=use_container if not isinstance(use_container, Inherit) else old_config.use_container,
            job_handler=job_handler if not isinstance(job_handler, Inherit) else old_config.job_handler
        )

    @staticmethod
    def get_current_config() -> ConfigEntry:
        return Config.config_stack[-1]

    def __enter__(self):
        Config.config_stack.append(self.new_config)
    def __exit__(self, exc_type, exc_val, exc_tb):
        Config.config_stack.pop()

default_job_handler = DefaultJobHandler()

default_config = ConfigEntry(
    use_container=False,
    job_handler=default_job_handler
)
Config.config_stack.append(default_config)