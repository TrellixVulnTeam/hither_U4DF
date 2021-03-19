from copy import deepcopy
from enum import Enum
from typing import Union, cast
from collections import deque
from typing import Any, Deque, Dict, List, Union, TYPE_CHECKING

class Inherit(Enum):
    INHERIT = ''

class ConfigEntry:
    def __init__(self, use_container: bool):
        self.use_container = use_container

class Config:
    config_stack: Deque[ConfigEntry] = deque()

    def __init__(self,
        use_container: Union[bool, Inherit]=Inherit.INHERIT
    ):
        old_config = Config.config_stack[-1] # throws if no default set
        self.new_config = ConfigEntry(
            use_container=use_container if use_container is not Inherit.INHERIT else old_config.use_container
        )

    @staticmethod
    def get_current_config() -> ConfigEntry:
        return Config.config_stack[-1]

    def __enter__(self):
        Config.config_stack.append(self.new_config)
    def __exit__(self, exc_type, exc_val, exc_tb):
        Config.config_stack.pop()

default_config = ConfigEntry(
    use_container=False
)
Config.config_stack.append(default_config)