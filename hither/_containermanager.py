import os
import sys

from ._enums import ConfigKeys
from ._shellscript import ShellScript

class ContainerManager:
    """What 'container preparation' actually does is make sure that whatever container
    configuration information has been attached to a Job's function `f` has been pulled
    from a global container store (like dockerhub) and is available whenever the Job
    wants to run, before it runs.
    The value of f._hither_container is added in core.py if the `container` param is
    passed, and should be something like a docker:// URL.
    """
    _prepared_docker_images = dict()
    _prepared_singularity_containers = dict()

    @staticmethod
    def prepare_container(container:str) -> None:
        """Add the specified container string to the relevant list of already-prepared
        containers, and fetch the right kind of container image (docker or singularity)
        based on environment variable configuration.

        Arguments:
            container {str} -- URL to container image (like 'docker://...') or other
            string value that can be passed to 'docker pull' and 'docker run' (or
            'singularity run ...')
        """
        if os.getenv(ConfigKeys.HITHER_USE_SINGULARITY_ENV, None) == 'TRUE':
            if container not in ContainerManager._prepared_singularity_containers:
                ContainerManager._do_prepare_singularity_container(container)
                ContainerManager._prepared_singularity_containers[container] = True
        else:
            if os.getenv(ConfigKeys.HITHER_NO_DOCKER_PULL_ENV, None) != 'TRUE':
                if container not in ContainerManager._prepared_docker_images:
                    ContainerManager._do_pull_docker_image(container)
                    ContainerManager._prepared_docker_images[container] = True

    @staticmethod
    def _do_prepare_singularity_container(container:str) -> None:
        """Executes script to prepare container by calling 'singularity run ...'

        Arguments:
            container {str} -- string of arguments to be passed to 'singularity run'.

        Raises:
            Exception: Thrown if the shell script calling the singularity command
            returns a non-zero (error) status code.
        """
        print(f'Building singularity container: {container}')
        ss = ShellScript(f'''
            #!/bin/bash

            exec singularity run {container} echo "built {container}"
        ''')
        ss.start()
        retcode = ss.wait()
        if retcode != 0:
            raise Exception(f'Problem building container {container}')

    @staticmethod
    def _do_pull_docker_image(container:str) -> None:
        """Fetches Docker container images from the store per the URL in the
        parameter.

        Arguments:
            container {str} -- URL for Docker image, optionally starting with
            'docker://'. Passed to 'docker pull'.

        Raises:
            Exception: Raised if shell script calling 'docker pull' returns non-zero
            (error) status code.
        """
        print(f'Pulling docker container: {container}')
        container = ContainerManager._docker_form_of_container_string(container)
        if (sys.platform == "win32"):
            if 1: # pragma: no cover
                ss = ShellScript(f'''
                    docker pull {container}
                ''')
        else:
            ss = ShellScript(f'''
                #!/bin/bash
                set -ex
                
                exec docker pull {container}
            ''')
        ss.start()
        retcode = ss.wait()
        if retcode != 0:
            raise Exception(f'Problem pulling container {container}')

    @staticmethod
    def _docker_form_of_container_string(container:str) -> str:
        """Strips leading 'docker://' from string.

        Arguments:
            container {str} -- URL for Docker image.

        Returns:
            str -- input, without leading 'docker://'
        """
        if container.startswith('docker://'):
            return container[len('docker://'):]
        else:
            return container



