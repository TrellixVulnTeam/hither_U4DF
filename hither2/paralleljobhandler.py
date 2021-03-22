from hither2.function import FunctionWrapper
import os
import signal
import tempfile
from typing import List, Dict, Any
import time
import multiprocessing
from multiprocessing.connection import Connection
import time
from ._config import ConfigEntry
from ._job_handler import JobHandler
from ._job import Job
from ._run_function import _run_function

class ParallelJobHandler(JobHandler):
    def __init__(self, num_workers):
        super().__init__()
        self._num_workers = num_workers
        self._processes: List[dict] = []
        self._halted = False

    def cleanup(self):
        self._halted = True

    def queue_job(self, job: Job):
        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()

        process = multiprocessing.Process(target=_pjh_run_job, args=(pipe_to_parent, job.function_wrapper, job.get_resolved_kwargs(), job.config))
        self._processes.append(dict(
            job=job,
            process=process,
            pipe_to_child=pipe_to_child,
            pjh_status='pending'
        ))
    
    def cancel_job(self, job_id):
        for p in self._processes:
            if p['job']._job_id == job_id:
                if p['pjh_status'] == 'running':
                    print(f'ParallelJobHandler: Terminating job.')
                    pp = p['process']
                    pp.terminate()
                    pp.join()
                    j: Job = p['job']
                    j._set_error(Exception('job cancelled'))
                    p['pjh_status'] = 'error'
                else:
                    # TODO: Consider if existing ERROR or FINISHED status should change this behavior
                    j: Job = p['job']
                    j._set_error(Exception('Job cancelled *'))
                    p['pjh_status'] = 'error'
    
    def iterate(self):
        if self._halted:
            return

        for p in self._processes:
            if p['pjh_status'] == 'running':
                if p['pipe_to_child'].poll():
                    try:
                        ret = p['pipe_to_child'].recv()
                    except:
                        ret = None
                    if ret is not None:
                        p['pipe_to_child'].send('okay!')
                        j: Job = p['job']
                        rv = ret['return_value']
                        e: str = ret['error']
                        if e is None:
                            j._set_finished(rv)
                            p['pjh_status'] = 'finished'
                        else:
                            j._set_error(Exception(f'Error running job (pjh): {e}'))
                            p['pjh_status'] = 'error'
        
        num_running = 0
        for p in self._processes:
            if p['pjh_status'] == 'running':
                num_running = num_running + 1

        for p in self._processes:
            if p['pjh_status'] == 'pending':
                if num_running < self._num_workers:
                    p['pjh_status'] = 'running'
                    j: Job = p['job']
                    j._set_running()
                    p['process'].start()
                    num_running = num_running + 1
        
        time.sleep(0.02)

def _pjh_run_job(pipe_to_parent: Connection, function_wrapper: FunctionWrapper, kwargs: Dict[str, Any], config: ConfigEntry) -> None:
    try:
        return_value = _run_function(
            function_wrapper=function_wrapper,
            kwargs=kwargs,
            job_cache=None, # already tried elsewhere
            use_container=config.use_container
        )
        error = None
    except Exception as e:
        error = e
        return_value = None

    ret = dict(
        return_value=return_value,
        error=str(error) if error is not None else None
    )

    pipe_to_parent.send(ret)
    # wait for message to return
    while True:
        if pipe_to_parent.poll():
            pipe_to_parent.recv()
            return
        time.sleep(0.02)