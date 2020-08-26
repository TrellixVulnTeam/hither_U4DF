from types import SimpleNamespace
from collections import deque
from copy import deepcopy
from enum import Enum
from typing import Any, Deque, Dict, List, Union, TYPE_CHECKING
from ._basejobhandler import BaseJobHandler
from ._enums import ConfigKeys

## Construction to avoid circular imports at runtime when we just need to fix a type reference
if TYPE_CHECKING:
    from .jobcache import JobCache

class Inherit(Enum):
    INHERIT = ''

class Config:
    config_stack: Deque[Dict[str, Any]] = deque()

    def __init__(self,
        container: Union[str, bool, Inherit, None]=Inherit.INHERIT,
        job_handler: Union[BaseJobHandler, Inherit]=Inherit.INHERIT,
        job_cache: Union['JobCache', Inherit, None]=Inherit.INHERIT,
        job_timeout: Union[float, Inherit, None]=Inherit.INHERIT,
        force_run: Union[bool, Inherit]=Inherit.INHERIT,
        rerun_failing: Union[bool, Inherit]=Inherit.INHERIT,
        cache_failing: Union[bool, Inherit]=Inherit.INHERIT,
        required_files: Union[str, Dict, List, None, Inherit]=Inherit.INHERIT,
        jhparams: Union[Dict, Inherit]=Inherit.INHERIT
    ):
        """Set hither config parameters in a context manager, inheriting unchanged parameters
        from the default config.

        Example usage:
        ```
        import hither as hi
        with hi.Config(container=True):
            # code goes here
        ```

        Three options relate to the job cache if it is provided:
            force_run=True will cause the job to rerun even if it is found in the cache.
            cache_failing=True will cause errored jobs to be cached (as errored)
            rerun_failing=True will cause the job to rerun even if a matching errored job is found in the cache.

        Note that cache_failing=False and rerun_failing=True are similar but not
        exactly the same. Indeed, if rerun_failing=True and cache_failing=True,
        then the job will run even if a matching errored job is found in the
        cache. But then at the end of the job, it will be cached, even if it
        fails. This distinction is relevant when the cache is persistent across
        multiple accesses which might have different configurations.
        
        Parameters
        ----------
        container : Union[str, bool, None], optional
            If bool, controls whether to use the default docker container specified for each function job
            If str, use the docker container given by the string, by default None
        job_handler : Any, optional
            The job handler to use for each function job, by default None
        job_cache : Union[JobCache, None], optional
            The job cache to use for each function job, by default None
        job_timeout : Union[float, None], optional
            A timeout time (in seconds) for each function job, by default None
        force_run : Union[bool], optional
            If True, run the job even if the result was found in the job cache (but still cache the job if a job cache has been set)
        rerun_failing: Union[bool], optional
            If True, run the job even if a failing result was found in the job cache (but still cache the job if a job cache has been set, and cache a failing job if cache_failing=True)
        cache_failing: Union[bool], optional
            If True, cache the job even if it fails, and use cached jobs even if they are failing
        required_files: Union[str, Dict, List, None], optional
            Indicates that certain files, specified by kachery URIs, are required at runtime for the job
        jhparams: dict, optional
            Parameters for the job handler
        """
        old_config = Config.config_stack[-1] # throws if no default set
        self.new_config = dict()
        for k, v in old_config.items():
            # NOTE: per our typing, none of the objects are actually supposed to be dicts
            # but we had this in ETConf, so I'm keeping it around to be careful
            self.new_config[k] = deepcopy(v) if isinstance(v, dict) else v

        # TODO: find a neater way to do this (kwargs?)
        self.coalesce(ConfigKeys.CONTAINER, container)
        self.coalesce(ConfigKeys.JOB_HANDLER, job_handler)
        self.coalesce(ConfigKeys.JOB_CACHE, job_cache)
        self.coalesce(ConfigKeys.TIMEOUT, job_timeout)
        self.coalesce(ConfigKeys.FORCE_RUN, force_run)
        self.coalesce(ConfigKeys.RERUN_FAILING, rerun_failing)
        self.coalesce(ConfigKeys.CACHE_FAILING, cache_failing)
        self.coalesce(ConfigKeys.REQUIRED_FILES, required_files)
        self.coalesce(ConfigKeys.JHPARAMS, jhparams)

    @staticmethod
    # TODO: python 3.8 gives better tools for typehinting dicts, revise this eventually
    def set_default_config(cfg: Dict[Any, Any]) -> None:
        # TODO: Add a guard against resetting default config when one already exists?
        for k in ConfigKeys.known_configuration_keys():
            if k not in cfg:
                raise Exception(f"Proposed default configuration is missing a value for {k}") # pragma: no cover
            if cfg[k] == Inherit.INHERIT:
                raise Exception(f"Default configuration has no way to inherit the value of {k}.") # pragma: no cover
        Config.config_stack.clear()
        Config.config_stack.append(cfg)

    @staticmethod
    def get_current_config_value(key: str) -> Any:
        d = Config.config_stack[-1]
        return d[key]

    def __enter__(self):
        Config.config_stack.append(self.new_config)
    def __exit__(self, exc_type, exc_val, exc_tb):
        Config.config_stack.pop()

    def coalesce(self, config_key: str, val: Any) -> None:
        if val == Inherit.INHERIT:
            # On INHERIT, we return without making changes, keeping the value from
            # the parent config. Then "None" can be used as an actual value.
            return
        self.new_config[config_key] = val
