from abc import abstractmethod
import os
import shutil
import stat
from typing import Dict, List, Union, cast
import tarfile

from numpy import source
from .dockerimage import DockerImage, RemoteDockerImage

class BindMount:
    def __init__(self, source: str, target: str, read_only: bool):
        self.source = source
        self.target = target
        self.read_only = read_only

def run_scriptdir_in_container(*,
    image: DockerImage,
    scriptdir: str,
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = []
):
    import kachery_p2p as kp

    if not image.is_prepared():
        raise Exception(f'Image must be prepared prior to running in container: {image.get_name()}:{image.get_tag()}')

    run_path = f'{scriptdir}/run'
    input_dir = f'{scriptdir}/input'
    output_dir = f'{scriptdir}/output'

    with kp.TemporaryDirectory() as tmpdir:
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

        mkdir -p /working/output
        cd /working
        exec ./run
        '''
        entry_path = tmpdir + '/entry'
        kp.ShellScript(run_script).write(entry_path)
        os.chmod(entry_path, os.stat(entry_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) # executable


        all_bind_mounts: List[BindMount] = [
            BindMount(target='/hither-env', source=env_path, read_only=True),
            BindMount(target='/hither-entry', source=entry_path, read_only=True),
            BindMount(target='/working/run', source=run_path, read_only=True)
        ]
        for bm in bind_mounts:
            all_bind_mounts.append(bm)
        
        use_singularity = os.getenv('HITHER_USE_SINGULARITY', None)
        if use_singularity in [None, 'FALSE', '0']:
            _run_script_in_container_docker(
                all_bind_mounts=all_bind_mounts,
                image=image,
                input_dir=input_dir,
                output_dir=output_dir,
                tmpdir=tmpdir,
                script_path='/hither-entry'
            )
        elif use_singularity in ['TRUE', '1']:
            _run_script_in_container_singularity(
                all_bind_mounts=all_bind_mounts,
                image=image,
                input_dir=input_dir,
                output_dir=output_dir,
                tmpdir=tmpdir,
                script_path='/hither-entry'
            )
        else:
            raise Exception('Unexpected value of HITHER_USE_SINGULARITY environment variable')

def _run_script_in_container_docker(*,
    all_bind_mounts: List[BindMount],
    image: DockerImage,
    input_dir: Union[str, None], # corresponds to /input in the container
    output_dir: Union[str, None], # corresponds to /output in the container
    tmpdir: str,
    script_path: str # path of script inside the container
):
    import docker
    from docker.types import Mount
    from docker.models.containers import Container

    image_name = image.get_name()
    image_tag = image.get_tag()

    client = docker.from_env()

    # create the mounts
    mounts = [
        Mount(target=x.target, source=x.source, type='bind', read_only=x.read_only)
        for x in all_bind_mounts
    ]

    # create the container
    container = cast(Container, client.containers.create(
        image_name + ':' + image_tag,
        [script_path],
        mounts=mounts,
        network_mode='host'
    ))

    # copy input directory to /input
    if input_dir:
        input_tar_path = tmpdir + '/input.tar.gz'
        with tarfile.open(input_tar_path, 'w:gz') as tar:
            tar.add(input_dir, arcname='input')
        with open(input_tar_path, 'rb') as tarf:
            container.put_archive('/working/', tarf)

    # run the container
    container.start()
    logs = container.logs(stream=True)
    for a in logs:
        for b in a.split(b'\n'):
            if b:
                print(b.decode())
    
    # copy output from /working/output
    if output_dir:
        strm, st = container.get_archive(path='/working/output/')
        output_tar_path = tmpdir + '/output.tar.gz'
        with open(output_tar_path, 'wb') as f:
            for d in strm:
                f.write(d)
        with tarfile.open(output_tar_path) as tar:
            tar.extractall(tmpdir)
        for fname in os.listdir(tmpdir + '/output'):
            shutil.move(tmpdir + '/output/' + fname, output_dir + '/' + fname)

def _run_script_in_container_singularity(*,
    all_bind_mounts: List[BindMount],
    image: DockerImage,
    input_dir: Union[str, None], # corresponds to /input in the container
    output_dir: Union[str, None], # corresponds to /output in the container
    tmpdir: str,
    script_path: str # path of script inside the container
):
    import kachery_p2p as kp
    image_name = image.get_name()
    image_tag = image.get_tag()

    bind_opts = ' '.join([
        f'--bind {bm.source}:{bm.target}'
        for bm in all_bind_mounts
    ])

    ss = kp.ShellScript(f'''
    #!/bin/bash

    singularity exec \\
        {bind_opts} \\
        --bind {input_dir}:/working/input \\
        --bind {output_dir}:/working/output \\
        docker://{image_name}:{image_tag} \\
        {script_path}
    ''')
    print(ss._script)
    ss.start()
    ss.wait()