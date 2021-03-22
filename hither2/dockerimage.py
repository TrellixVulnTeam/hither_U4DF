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
    def get_name(self) -> str:
        pass

class DockerImageFromScript(DockerImage):
    def __init__(self, *, name: str, dockerfile: str):
        self._name = name
        self._dockerfile = dockerfile
        self._prepared = False
    def prepare(self):
        if not self._prepared:
            dockerfile_dir = os.path.dirname(self._dockerfile)
            dockerfile_basename = os.path.basename(self._dockerfile)
            ss = ShellScript(f'''
            #!/bin/bash

            cd {dockerfile_dir}
            docker build -t {self._name} -f {dockerfile_basename} .
            ''')
            ss.start()
            ss.wait()
            self._prepared = True
    def get_name(self) -> str:
        return self._name

class LocalDockerImage(DockerImage):
    def __init__(self, name: str):
        self._name = name
        self._prepared = False
    def prepare(self):
        if not self._prepared:
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
    def get_name(self) -> str:
        return self._name

class RemoteDockerImage(DockerImage):
    def __init__(self, name: str):
        self._name = name
        self._prepared = False
    def prepare(self):
        if not self._prepared:
            ss = ShellScript(f'''
            #!/bin/bash

            docker pull {self._name}
            ''')
            ss.start()
            ss.wait()
            self._prepared = True
    def get_name(self) -> str:
        return self._name