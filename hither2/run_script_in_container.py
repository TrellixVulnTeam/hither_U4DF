from abc import abstractmethod
import os
import shutil
import stat
from typing import Dict, List, Union, cast
import tarfile
from ._temporarydirectory import TemporaryDirectory
from ._shellscript import ShellScript
from .dockerimage import DockerImage

class BindMount:
    def __init__(self, source: str, target: str, read_only: bool):
        self.source = source
        self.target = target
        self.read_only = read_only

def run_script_in_container(*,
    image: DockerImage,
    script: str,
    input_dir: Union[str, None]=None, # corresponds to /input in the container
    output_dir: Union[str, None]=None, # corresponds to /output in the container
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = []
):
    import docker
    from docker.types import Mount
    from docker.models.containers import Container

    if isinstance(image, DockerImage):
        image.prepare()
        image_name = image.get_name()

    client = docker.from_env()
    with TemporaryDirectory() as tmpdir:
        script_path = tmpdir + '/script'
        ShellScript(script).write(script_path)
        os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) # executable

        environment_strings: List[str] = []
        for k, v in environment.items():
            environment_strings.append(f'export {k}="{v}"')
        env_path = tmpdir + '/env'
        with open(env_path, 'w') as f:
            f.write('\n'.join(environment_strings))

        # entrypoint script to run inside the container
        run_script = f'''
        #!/bin/bash

        set -e
        
        source /hither-env

        # do not buffer the stdout
        export PYTHONUNBUFFERED=1

        mkdir /output
        cd /working
        exec ./script
        '''
        run_path = tmpdir + '/run'
        ShellScript(run_script).write(run_path)
        os.chmod(run_path, os.stat(script_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) # executable

        mounts=[
            Mount(target='/hither-env', source=env_path, type='bind', read_only=True),
            Mount(target='/hither-run', source=run_path, type='bind', read_only=True),
            Mount(target='/working/script', source=script_path, type='bind', read_only=True),
        ]
        for bm in bind_mounts:
            mounts.append(
                Mount(target=bm.target, source=bm.source, type='bind', read_only=bm.read_only)
            )

        container = cast(Container, client.containers.create(
            image_name,
            ['/hither-run'],
            mounts=mounts,
            network_mode='host'
        ))

        # copy input directory to /input
        if input_dir:
            input_tar_path = tmpdir + '/input.tar.gz'
            with tarfile.open(input_tar_path, 'w:gz') as tar:
                tar.add(input_dir, arcname='input')
            with open(input_tar_path, 'rb') as tarf:
                container.put_archive('/', tarf)

        container.start()
        logs = container.logs(stream=True)
        for a in logs:
            for b in a.split(b'\n'):
                if b:
                    print(b.decode())
        
        if output_dir:
            strm, st = container.get_archive(path='/output/')
            output_tar_path = tmpdir + '/output.tar.gz'
            with open(output_tar_path, 'wb') as f:
                for d in strm:
                    f.write(d)
            with tarfile.open(output_tar_path) as tar:
                tar.extractall(tmpdir)
            for fname in os.listdir(tmpdir + '/output'):
                shutil.move(tmpdir + '/output/' + fname, output_dir + '/' + fname)