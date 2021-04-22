import shutil
from hither2.runtimehook import PostContainerContext
import os
import json
import time
from typing import Dict, Union
from ._job import Job
from .create_scriptdir_for_function_run import create_scriptdir_for_function_run
from ._safe_pickle import _safe_unpickle

class SlurmAllocation:
    def __init__(self, *, directory: str, srun_command: str, allocation_id: str):
        import kachery_p2p as kp
        self._directory = directory
        self._srun_command = srun_command
        self._allocation_id = allocation_id
        self._jobs: Dict[str, Job] = {}
        self._jobs_dir = f'{self._directory}/jobs'
        self._script: Union[kp.ShellScript, None] = None
        self._status: str = 'pending'
        self._timestamp_created = time.time()
        if not os.path.isdir(self._jobs_dir):
            os.mkdir(self._jobs_dir)
    def start(self):
        import kachery_p2p as kp
        import yaml
        self._timestamp_started = time.time()
        if not os.path.exists(self._directory):
            os.mkdir(self._directory)
        config_path = f'{self._directory}/config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump({}, f)
        running_path = f'{self._directory}/running'
        with open(running_path, 'w') as f:
            f.write('Slurm allocation will stop if this file is deleted.')
        start_scriptdir_runner_script = kp.ShellScript(f'''
        #!/bin/bash

        set -e

        cd {self._directory}
        hither-scriptdir-runner start
        ''')
        start_scriptdir_runner_script.write(f'{self._directory}/start.sh')
        self._script = kp.ShellScript(f'''
        #!/bin/bash

        set -e

        exec bash -c "{self._srun_command} {self._directory}/start.sh"
        ''')
        self._status = 'starting'
        self._script.start()
    def stop(self):
        print('Stopping allocation')
        self._status = 'stopping'
        running_path = self._directory + '/running'
        if os.path.isfile(running_path):
            os.unlink(running_path)
        self._script.wait()
        self._status = 'stopped'
    def add_job(self, job: Job):
        self._jobs[job.job_id] = job
        function_wrapper = job.function_wrapper
        kwargs=job.get_resolved_kwargs()
        image = job.image if job.config.use_container else None
        create_scriptdir_for_function_run(
            directory=f'{self._jobs_dir}/{job.job_id}',
            function_wrapper=function_wrapper,
            image=image,
            kwargs=kwargs,
            show_console=job.config.show_console
        )
    def has_job(self, job_id: str):
        return job_id in self._jobs
    def cancel_job(self, job_id: str, reason: str):
        if job_id not in self._jobs: return
        j = self._jobs[job_id]
        j._set_error(Exception(f'Job cancelled (slurmallocation): {reason}'))
        directory = f'{self._jobs_dir}/{job_id}'
        shutil.rmtree(directory)
        del self._jobs[job_id]
    @property
    def is_running(self):
        return (self._status == 'running')
    @property
    def status(self):
        return self._status
    @property
    def timestamp_created(self):
        return self._timestamp_created
    @property
    def timestamp_started(self):
        return self._timestamp_started
    @property
    def allocation_id(self):
        return self._allocation_id
    @property
    def num_queued_jobs(self):
        return len([j for j in self._jobs.values() if j.status == 'queued'])
    @property
    def num_running_jobs(self):
        return len([j for j in self._jobs.values() if j.status == 'running'])
    @property
    def num_finished_jobs(self):
        return len([j for j in self._jobs.values() if j.status == 'finished'])
    @property
    def num_errored_jobs(self):
        return len([j for j in self._jobs.values() if j.status == 'error'])
    def iterate(self):
        state_path = f'{self._directory}/state.json'
        if not os.path.exists(state_path):
            return
        if self._status == 'starting':
            # now it's running because the state.json file exists
            self._status = 'running'
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
        except Exception as e:
            print('WARNING: problem reading state.json', str(e))
            return
        for job_id, job_state in state['jobs'].items():
            s = job_state['status']
            if job_id in self._jobs:
                j = self._jobs[job_id]
                if s == 'pending':
                    raise Exception('Unexpected pending status for job in slurm allocation')
                elif s == 'queued':
                    pass
                elif s == 'running':
                    if j.status != 'running':
                        j._set_running()
                elif s == 'complete':
                    if j.status not in ['finished', 'error']:
                        console_lines_path = f'{self._jobs_dir}/{job_id}/output/console_lines.pkl'
                        if os.path.isfile(console_lines_path):
                            console_lines = _safe_unpickle(console_lines_path)
                            j._set_console_lines(console_lines)
                        return_value_path = f'{self._jobs_dir}/{job_id}/output/return_value.pkl'
                        error_message_path = f'{self._jobs_dir}/{job_id}/output/error_message.pkl'
                        if os.path.isfile(return_value_path):
                            return_value = _safe_unpickle(return_value_path)

                            # postcontainer
                            kwargs=j.get_resolved_kwargs()
                            image = j.image if j.config.use_container else None
                            if image:
                                postcontainer_context = PostContainerContext(kwargs=kwargs, image=image, return_value=return_value)
                                for h in j.function_wrapper._runtime_hooks:
                                    h.postcontainer(postcontainer_context)
                                new_return_value = postcontainer_context.return_value
                            else:
                                new_return_value = return_value
                    
                            j._set_finished(new_return_value)
                        elif os.path.isfile(error_message_path):
                            error_message = _safe_unpickle(error_message_path)
                            j._set_error(Exception(error_message))
                        else:
                            error_message = 'No error_message.pkl found'
                            j._set_error(Exception(error_message))
    def get_num_incomplete_jobs(self):
        ret = 0
        for job_id, job in self._jobs.items():
            if job.status not in ['finished', 'error']:
                ret += 1
        return ret

