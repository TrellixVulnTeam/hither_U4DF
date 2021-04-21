from typing import Any, Dict, List, Union
from .dockerimage import DockerImage
from ._bindmount import BindMount

class PreContainerContext:
    def __init__(self, *, kwargs: Dict[str, Any], image: Union[DockerImage, bool]):
        self._kwargs = {**kwargs}
        self._image = image
        self._bind_mounts: List[BindMount] = []
        self._environment: Dict[str, str] = {}
    @property
    def kwargs(self):
        return self._kwargs
    @property
    def image(self):
        return self._image
    @image.setter
    def image(self, val: DockerImage):
        self._image = val
    def add_bind_mount(self, bind_mount: BindMount):
        self._bind_mounts.append(bind_mount)
    def set_env(self, key: str, value: str):
        self._environment[key] = value

class PostContainerContext:
    def __init__(self, *, kwargs: Dict[str, Any], image: Union[DockerImage, bool], return_value: Any):
        self._kwargs = {**kwargs}
        self._image = image
        self._return_value = return_value
    @property
    def kwargs(self):
        return self._kwargs
    @property
    def image(self):
        return self._image
    @property
    def return_value(self):
        return self._return_value
    @return_value.setter
    def return_value(self, val: Any):
        self._return_value = val

class PreRunContext:
    def __init__(self, *, kwargs: Dict[str, Any]):
        self._kwargs = {**kwargs}
    @property
    def kwargs(self):
        return self._kwargs

class PostRunContext:
    def __init__(self, *, kwargs: Dict[str, Any], return_value: Any):
        self._kwargs = {**kwargs}
        self._return_value = return_value
    @property
    def kwargs(self):
        return self._kwargs
    @property
    def return_value(self):
        return self._return_value
    @return_value.setter
    def return_value(self, val: Any):
        self._return_value = val

class RuntimeHook:
    def __init__(self):
        pass
    def precontainer(self, context: PreContainerContext):
        pass
    def postcontainer(self, context: PostContainerContext):
        pass
    def prerun(self, context: PreRunContext):
        pass
    def postrun(self, context: PostRunContext):
        pass

