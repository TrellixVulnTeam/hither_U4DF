from abc import abstractmethod
import os
from typing import Union

class DockerImage:
    def __init__(self):
        pass
    @abstractmethod
    def prepare(self) -> None:
        pass
    @abstractmethod
    def is_prepared(self) -> bool:
        pass
    @abstractmethod
    def get_name(self) -> str:
        pass
    @abstractmethod
    def get_tag(self) -> str:
        pass

def _use_singularity():
    return os.getenv('HITHER_USE_SINGULARITY', None) in ['1', 'TRUE']

class LocalDockerImage(DockerImage):
    def __init__(self, name: str, *, tag: Union[str, None]=None):
        if tag is not None:
            if ':' in name: raise Exception('Cannot provide tag argument if name already contains a tag')
            self._name = name
            self._tag = tag
        else:
            v = name.split(':')
            if len(v) == 1:
                self._name = name
                self._tag = 'latest'
            elif len(v) == 2:
                self._name = v[0]
                self._tag = v[1]
            else:
                raise Exception(f'Invalid docker image name: {name}')
        self._prepared = False
    def prepare(self):
        import kachery_p2p as kp
        if not self._prepared:
            if _use_singularity():
                raise Exception('Cannot use LocalDockerImage in singularity mode')
            else:
                ss = kp.ShellScript(f'''
                #!/bin/bash

                result=$( docker images -q {self._name} )

                if [[ -n "$result" ]]; then
                exit 0
                else
                exit 1
                fi
                ''')
                ss.start()
                ss.wait()
                self._prepared = True
    def is_prepared(self) -> bool:
        return self._prepared
    def get_name(self) -> str:
        return self._name
    def get_tag(self) -> str:
        return self._tag

class RemoteDockerImage(DockerImage):
    def __init__(self, name: str, *, tag: Union[str, None]=None):
        if name.startswith('docker://'):
            name = name[len('docker://'):]
        if tag is not None:
            if ':' in name: raise Exception('Cannot provide tag argument if name already contains a tag')
            self._name = name
            self._tag = tag
        else:
            v = name.split(':')
            if len(v) == 1:
                self._name = name
                self._tag = 'latest'
            elif len(v) == 2:
                self._name = v[0]
                self._tag = v[1]
            else:
                raise Exception(f'Invalid docker image name: {name}')
        self._prepared = False
    def prepare(self):
        import kachery_p2p as kp
        if not self._prepared:
            if _use_singularity():
                ss = kp.ShellScript(f'''
                #!/bin/bash

                singularity pull docker://{self._name}
                ''')
                ss.start()
                ss.wait()
                self._prepared = True
            else:
                ss = kp.ShellScript(f'''
                #!/bin/bash

                docker pull {self._name}
                ''')
                ss.start()
                ss.wait()
                self._prepared = True
    def is_prepared(self) -> bool:
        return self._prepared
    def get_name(self) -> str:
        return self._name
    def get_tag(self) -> str:
        return self._tag