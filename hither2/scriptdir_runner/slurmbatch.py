import os
import json
import shutil
from typing import Dict, Union
from .._job import Job
from ..create_scriptdir_for_function import create_scriptdir_for_function
from .._safe_pickle import _safe_unpickle

class SlurmBatch:
    def __init__(self, *, directory: str, srun_cmd: str):
        import kachery_p2p as kp
        self._directory = directory
        self._srun_cmd = srun_cmd
        self._jobs: Dict[str, Job] = {}
        self._jobs_dir = f'{self._directory}/jobs'
        self._script: Union[kp.ShellScript, None] = None
        self._is_running: bool = False
        if not os.path.isdir(self._jobs_dir):
            os.mkdir(self._jobs_dir)
    def start(self):
        import kachery_p2p as kp
        import yaml
        if not os.path.exists(self._directory):
            os.mkdir(self._directory)
        config_path = f'{self._directory}/config.yaml'
        with open(config_path, 'w') as f:
            yaml.dump({}, f)
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

        exec {self._srun_cmd} {self._directory}/start.sh
        ''')
        self._script.start()
    def stop(self):
        print('Stopping batch')
        with open(self._directory + '/stop', 'w') as f:
            f.write('Please stop.')
        self._script.wait()
    def add_job(self, job: Job):
        self._jobs[job.job_id] = job
        create_scriptdir_for_function(
            directory=f'{self._jobs_dir}/{job.job_id}',
            function=job.function,
            kwargs=job.get_resolved_kwargs(),
            modules=[] # wait until containerization is implemented
        )
    def is_running(self):
        return self._is_running
    def iterate(self):
        state_path = f'{self._directory}/state.json'
        if not os.path.exists(state_path):
            return
        self._is_running = True
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
        except Exception as e:
            print('WARNING: problem reading state.json', str(e))
            return
        for job_id, job_state in state['jobs'].items():
            s = job_state['status']
            j = self._jobs[job_id]
            if s == 'pending':
                pass
            elif s == 'running':
                j._set_running()
            elif s == 'finished':
                return_value = _safe_unpickle(f'{self._jobs_dir}/{job_id}/output/return_value.pkl')
                j._set_finished(return_value)
            elif s == 'error':
                error_path = f'{self._jobs_dir}/{job_id}/output/error.pkl'
                if os.path.isfile(error_path):
                    error_message = _safe_unpickle(error_path)
                else:
                    error_message = 'Error running job - no error.pkl found'
                j._set_error(Exception(error_message))
    def get_num_incomplete_jobs(self):
        ret = 0
        for job_id, job in self._jobs.items():
            if job.status not in ['finished', 'error']:
                ret += 1
        return ret

