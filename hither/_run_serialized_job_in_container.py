import os
import sys
from typing import Any, List, Tuple, Union, Dict
import json
import kachery_p2p as kp

from ._containermanager import ContainerManager
from ._enums import SerializedJobKeys, EnvironmentKeys
from ._temporarydirectory import TemporaryDirectory
from ._shellscript import ShellScript
from ._util import _random_string, _deserialize_item


def _run_serialized_job_in_container(job_serialized, cancel_filepath: Union[str, None]=None):
    name = job_serialized[SerializedJobKeys.FUNCTION_NAME]
    version = job_serialized[SerializedJobKeys.FUNCTION_VERSION]
    label = job_serialized[SerializedJobKeys.LABEL]
    if label is None:
        label = name
    show_console = True
    gpu = False
    # This variable is reserved for future use
    # timeout: Union[None, float] = None
    
    code_uri = job_serialized[SerializedJobKeys.CODE_URI]
    assert code_uri is not None, '_run_serialized_job_in_container: code_uri is None'
    code = kp.load_object(code_uri)
    assert code is not None, f'_run_serialized_job_in_container: unable to load code from kachery storage: {code_uri}'
    container = job_serialized[SerializedJobKeys.CONTAINER]

    kwargs = job_serialized[SerializedJobKeys.WRAPPED_ARGS]

    remove = True
    if os.getenv(EnvironmentKeys.HITHER_DEBUG_ENV, None) == 'TRUE':
        remove = False
    with TemporaryDirectory(prefix='tmp_hither_run_in_container_' + name + '_', remove=remove) as temp_path:
        _write_python_code_to_directory(os.path.join(temp_path, 'function_src'), code)
        
        if container is not None:
            run_in_container_path = '/run_in_container'
            env_vars_inside_container = dict(
                PYTHONPATH=f'/working/function_src/_local_modules',
                HOME='$HOME'
            )
            if os.getenv('KACHERY_P2P_API_PORT', None) is not None:
                env_vars_inside_container['KACHERY_P2P_API_PORT'] = os.getenv('KACHERY_P2P_API_PORT', '')
        else:
            raise Exception('Unexpected: container is None') # pragma: no cover
            # run_in_container_path = temp_path
            # env_vars_inside_container = dict(
            #     PYTHONPATH=f'{run_in_container_path}/function_src/_local_modules'
            # )

        run_py_script = """
            #!/usr/bin/env python

            import sys
            import json
            import traceback

            def main():
                try:
                    import hither
                    ok_import_hither = True
                except Exception as e:
                    traceback.print_exc()
                    retval = None
                    success = False
                    error = str(e)
                    runtime_info = dict()
                    ok_import_hither = False

                # important to do this so that
                # we don't have the program attempting
                # to access the daemon, which won't be
                # running in the container
                # import kachery_p2p as kp
                # kp._experimental_config(nop2p=True)

                if ok_import_hither:
                    from hither import ConsoleCapture
                    from hither import _deserialize_item, _serialize_item

                    kwargs = json.loads('{kwargs_json}')
                    kwargs = _deserialize_item(kwargs)
                    with ConsoleCapture(label='{label}', show_console={show_console_str}) as cc:
                        print('###### RUNNING: {label}')
                        try:
                            from function_src import {function_name}
                            retval = {function_name}(**kwargs)
                            success = True
                            error = None
                        except Exception as e:
                            traceback.print_exc()
                            retval = None
                            success = False
                            error = str(e)
                    
                    retval = _serialize_item(retval)
                    
                    runtime_info = cc.runtime_info()
                result = dict(
                    retval=retval,
                    success=success,
                    runtime_info=runtime_info,
                    error=error
                )
                with open('{run_in_container_path}/result.json', 'w') as f:
                    json.dump(result, f)

            if __name__ == "__main__":
                try:
                    main()
                except:
                    sys.stdout.flush()
                    sys.stderr.flush()
                    raise
        """.format(
            kwargs_json=json.dumps(kwargs),
            function_name=name,
            label=label,
            show_console_str='True' if show_console else 'False',
            run_in_container_path=run_in_container_path
        )

        # For unindenting
        ShellScript(run_py_script).write(os.path.join(temp_path, 'run.py'))

        # See: https://wiki.bash-hackers.org/commands/builtin/exec
        run_inside_container_script = """
            #!/bin/bash
            set -e

            # export NUM_WORKERS={num_workers_env}
            # export MKL_NUM_THREADS=$NUM_WORKERS
            # export NUMEXPR_NUM_THREADS=$NUM_WORKERS
            # export OMP_NUM_THREADS=$NUM_WORKERS

            export {env_vars_inside_container}
            mkdir /working
            cp -r /run_in_container/* working/
            exec python3 /working/run.py
        """.format(
            env_vars_inside_container=' '.join(['{}={}'.format(k, v) for k, v in env_vars_inside_container.items()]),
            num_workers_env=os.getenv('NUM_WORKERS', ''),
            run_in_container_path=run_in_container_path
        )

        ShellScript(run_inside_container_script).write(os.path.join(temp_path, 'run.sh'))

        docker_container_name = None
        kachery_storage_dir = kp._kachery_storage_dir()
        if kachery_storage_dir is None:
            raise Exception('No kachery storage directory found.')

        # fancy_command = 'bash -c "((bash /run_in_container/run.sh | tee /run_in_container/stdout.txt) 3>&1 1>&2 2>&3 | tee /run_in_container/stderr.txt) 3>&1 1>&2 1>&3 | tee /run_in_container/console_out.txt"'
        if container is None:
            raise Exception('Unexpected: container is None') # pragma: no cover
            # run_outside_container_script = """
            #     #!/bin/bash

            #     exec {run_in_container_path}/run.sh
            # """.format(
            #     run_in_container_path=run_in_container_path
            # )
        elif os.getenv(EnvironmentKeys.HITHER_USE_SINGULARITY_ENV, None) == 'TRUE':
            if gpu:
                gpu_opt = '--nv'
            else:
                gpu_opt = ''
            run_outside_container_script = f"""
                #!/bin/bash

                # {label} ({name} {version})

                exec singularity exec -e {gpu_opt} \\
                    -B {kachery_storage_dir}:{kachery_storage_dir} \\
                    -B {temp_path}:/run_in_container \\
                    {container} \\
                    bash /run_in_container/run.sh
            """
        else:
            if gpu:
                gpu_opt = '--gpus all'
            else:
                gpu_opt = ''
            docker_container_name = _random_string(8) + '_' + name
            # May not want to use -t below as it has the potential to mess up line feeds in the parent process!
            container2 = ContainerManager._docker_form_of_container_string(container)
            run_outside_container_script = f"""
            #!/bin/bash

            # {label} ({name} {version})

            exec docker run --name {docker_container_name} -i {gpu_opt} \\
                -v /etc/localtime:/etc/localtime:ro \\
                --net host \\
                -v {kachery_storage_dir}:{kachery_storage_dir} \\
                -v {temp_path}:/run_in_container \\
                {container2} \\
                bash /run_in_container/run.sh
            """
        print('#############################################################')
        print(run_outside_container_script)
        print('#############################################################')

        try:
            ss = ShellScript(run_outside_container_script, keep_temp_files=False, label='run_outside_container', docker_container_name=docker_container_name)
            ss.start()
            # timer = time.time()
            did_timeout = False
            did_cancel = False
            while True:
                retcode = ss.wait(0.2)
                if retcode is not None:
                    break
                if cancel_filepath is not None:
                    if os.path.exists(cancel_filepath):
                        print('Stopping job because cancel file was found.')
                        did_cancel = True
                        ss.stop()
                        os.unlink(cancel_filepath)
                # elapsed = time.time() - timer
                # TODO: this code is currently unreachable but should be uncommented when `timeout` is used.
                # if timeout is not None:
                #     if elapsed > timeout:
                #         print(f'Stopping job due to timeout {elapsed} > {timeout}')
                #         did_timeout = True
                #         ss.stop()
        finally:
            if docker_container_name is not None:
                ss_cleanup = ShellScript(f"""
                #!/bin/bash

                docker stop {docker_container_name} || true
                docker kill {docker_container_name} 2>&1 | grep -v 'is not running' || true
                docker rm {docker_container_name} || true
                """)
                ss_cleanup.start()
                ss_cleanup.wait()

        if did_timeout:
            runtime_info = dict(
                timed_out=True
            )
            success=False,
            error='Job timed out'
            retval = -1
        elif did_cancel:
            runtime_info = dict(
                cancelled=True
            )
            success=False
            error='Job cancelled'
            retval = -1
        else:
            if (retcode != 0):
                # This is a genuine framework exception because if it were a function exception, we'd get that reported in the runtime_info
                raise Exception('Unexpected non-zero exit code ({}) running [{}] in container {}'.format(retcode, label, container))
            with open(os.path.join(temp_path, 'result.json')) as f:
                obj = json.load(f)
            retval = _deserialize_item(obj['retval'])
            runtime_info = obj['runtime_info']
            success = obj['success']
            error = obj['error']
            if not success:
                assert error is not None
                assert error != 'None'
        
        return success, retval, runtime_info, error

def _write_python_code_to_directory(dirname: str, code: dict) -> None:
    if os.path.exists(dirname):
        raise Exception('Unexpected: Cannot write code to already existing directory: {}'.format(dirname)) # pragma: no cover
    os.mkdir(dirname)
    for item in code['files']:
        fname0 = dirname + '/' + item['name']
        with open(fname0, 'w', newline='\n') as f:
            f.write(item['content'])
    for item in code['dirs']:
        _write_python_code_to_directory(
            dirname + '/' + item['name'], item['content'])
