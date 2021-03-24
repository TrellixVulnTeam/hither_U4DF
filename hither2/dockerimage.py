from abc import abstractmethod
import os
from ._shellscript import ShellScript

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

def _use_singularity():
    return os.getenv('HITHER_USE_SINGULARITY', None) in ['1', 'TRUE']

class DockerImageFromScript(DockerImage):
    def __init__(self, *, name: str, dockerfile: str):
        self._name = name
        self._dockerfile = dockerfile
        self._prepared = False
    def prepare(self):
        if not self._prepared:
            if _use_singularity():
                raise Exception('Cannot use DockerImageFromScript in singularity mode')
            else:
                # import docker

                dockerfile_dir = os.path.dirname(self._dockerfile)
                dockerfile_basename = os.path.basename(self._dockerfile)

                # client = docker.from_env()
                # image = client.images.build(tag=self._name, path=dockerfile_dir, dockerfile=dockerfile_basename)
                
                ss = ShellScript(f'''
                #!/bin/bash

                cd {dockerfile_dir}
                docker build -t {self._name} -f {dockerfile_basename} .
                ''')
                ss.start()
                ss.wait()
                self._prepared = True
    def is_prepared(self) -> bool:
        return self._prepared
    def get_name(self) -> str:
        return self._name

class LocalDockerImage(DockerImage):
    def __init__(self, name: str):
        self._name = name
        self._prepared = False
    def prepare(self):
        if not self._prepared:
            if _use_singularity():
                raise Exception('Cannot use LocalDockerImage in singularity mode')
            else:
                ss = ShellScript(f'''
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

class RemoteDockerImage(DockerImage):
    def __init__(self, name: str):
        self._name = name
        self._prepared = False
    def prepare(self):
        if not self._prepared:
            if _use_singularity():
                ss = ShellScript(f'''
                #!/bin/bash

                singularity pull docker://{self._name}
                ''')
                ss.start()
                ss.wait()
                self._prepared = True
            else:
                ss = ShellScript(f'''
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