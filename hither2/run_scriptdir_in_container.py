from abc import abstractmethod
import json
import os
import shutil
from typing import List, Union, cast
import tarfile

from numpy import source
from .dockerimage import DockerImage, RemoteDockerImage

class BindMount:
    def __init__(self, source: str, target: str, read_only: bool):
        self.source = source
        self.target = target
        self.read_only = read_only
    def serialize(self):
        return {
            'source': self.source,
            'target': self.target,
            'read_only': self.read_only
        }
    @staticmethod
    def deserialize(x: dict):
        return BindMount(**x)

def run_scriptdir_in_container(*,
    scriptdir: str,
    image_name: str,
    bind_mounts_path: str,
    output_dir: str
):
    with open(bind_mounts_path, 'r') as f:
        x = json.load(f)
        bind_mounts = [BindMount.deserialize(a) for a in x]
    
    run_scriptdir_in_container_2(scriptdir=scriptdir, image_name=image_name, bind_mounts=bind_mounts, output_dir=output_dir)

def run_scriptdir_in_container_2(*,
    scriptdir: str,
    image_name: str,
    bind_mounts: List[BindMount],
    output_dir: str
):
    import kachery_p2p as kp

    run_path = f'{scriptdir}/run'
    env_path = f'{scriptdir}/env'
    input_dir = f'{scriptdir}/input'

    with kp.TemporaryDirectory() as tmpdir:
        # entrypoint script to run inside the container
        entry_sh_script = f'''
        #!/bin/bash

        set -e

        # do not buffer the stdout
        export PYTHONUNBUFFERED=1

        mkdir -p /working/output
        cd /working
        exec ./run
        '''
        entry_sh_path = tmpdir + '/entry.sh'
        kp.ShellScript(entry_sh_script).write(entry_sh_path)

        ##############################################
        all_bind_mounts: List[BindMount] = [
            BindMount(target='/hither-entry.sh', source=entry_sh_path, read_only=True),
            BindMount(target='/working/run', source=run_path, read_only=True),
            BindMount(target='/working/env', source=env_path, read_only=True)
        ]
        for bm in bind_mounts:
            all_bind_mounts.append(bm)
        
        use_singularity = os.getenv('HITHER_USE_SINGULARITY', None)
        if use_singularity in [None, 'FALSE', '0']:
            _run_script_in_container_docker(
                all_bind_mounts=all_bind_mounts,
                image_name=image_name,
                input_dir=input_dir,
                output_dir=output_dir,
                tmpdir=tmpdir,
                script_path='/hither-entry.sh'
            )
        elif use_singularity in ['TRUE', '1']:
            _run_script_in_container_singularity(
                all_bind_mounts=all_bind_mounts,
                image_name=image_name,
                input_dir=input_dir,
                output_dir=output_dir,
                tmpdir=tmpdir,
                script_path='/hither-entry.sh'
            )
        else:
            raise Exception('Unexpected value of HITHER_USE_SINGULARITY environment variable')

def _run_script_in_container_docker(*,
    all_bind_mounts: List[BindMount],
    image_name: str,
    input_dir: Union[str, None], # corresponds to /input in the container
    output_dir: Union[str, None], # corresponds to /output in the container
    tmpdir: str,
    script_path: str # path of script inside the container
):
    import docker
    from docker.types import Mount
    from docker.models.containers import Container

    client = docker.from_env()

    # create the mounts
    mounts = [
        Mount(target=x.target, source=x.source, type='bind', read_only=x.read_only)
        for x in all_bind_mounts
    ]

    # create the container
    container = cast(Container, client.containers.create(
        image_name,
        [script_path],
        mounts=mounts,
        network_mode='host'
    ))

    # copy input directory to /working/input
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
    image_name: str,
    input_dir: Union[str, None], # corresponds to /input in the container
    output_dir: Union[str, None], # corresponds to /output in the container
    tmpdir: str,
    script_path: str # path of script inside the container
):
    import kachery_p2p as kp

    bind_opts = ' '.join([
        f'--bind {bm.source}:{bm.target}'
        for bm in all_bind_mounts
    ])

    ss = kp.ShellScript(f'''
    #!/bin/bash

    singularity exec \\
        {bind_opts} \\
        -C \\
        --bind {input_dir}:/working/input \\
        --bind {output_dir}:/working/output \\
        docker://{image_name} \\
        {script_path}
    ''')
    print(ss._script)
    ss.start()
    ss.wait()