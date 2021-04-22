from hither2.dockerimage import DockerImage
from .function import FunctionWrapper
from typing import List, Dict, Any, Union
import time
import multiprocessing
from multiprocessing.connection import Connection
import time
import atexit
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
        _all_parallel_job_handlers.append(self)

    def cleanup(self):
        self._halted = True
        for p in self._processes:
            pp = p['process']
            if pp is not None:
                pp.terminate()
                try:
                    pp.join()
                except:
                    print('WARNING: unable to join process in cleanup')
    
    def is_remote(self) -> bool:
        return False

    def queue_job(self, job: Job):
        self._processes.append(dict(
            job=job,
            process=None,
            pipe_to_child=None,
            pjh_status='pending'
        ))
    
    def cancel_job(self, job_id: str, reason: str):
        for p in self._processes:
            if p['job']._job_id == job_id:
                if p['pjh_status'] == 'running':
                    print(f'ParallelJobHandler: Terminating job.')
                    pp = p['process']
                    pp.terminate()
                    try:
                        pp.join()
                    except:
                        print('WARNING: unable to join process for job being cancelled')
                    j: Job = p['job']
                    j._set_error(Exception(f'job cancelled: {reason}'))
                    p['pjh_status'] = 'error'
                else:
                    # TODO: Consider if existing ERROR or FINISHED status should change this behavior
                    j: Job = p['job']
                    j._set_error(Exception(f'Job cancelled prior to running: {reason}'))
                    p['pjh_status'] = 'error'
    
    def iterate(self):
        if self._halted:
            return

        for p in self._processes:
            if p['pjh_status'] == 'running':
                pp: multiprocessing.Process = p['process']
                j: Job = p['job']
                if pp.is_alive():
                    if p['pipe_to_child'].poll():
                        try:
                            ret = p['pipe_to_child'].recv()
                        except:
                            ret = None
                        if ret is not None:
                            p['pipe_to_child'].send('okay!')
                            rv = ret['return_value']
                            e: Union[None, str] = ret['error']
                            console_lines: Union[None, List[dict]] = ret['console_lines']
                            if console_lines is not None:
                                j._set_console_lines(console_lines)
                            if e is None:
                                j._set_finished(rv)
                                p['pjh_status'] = 'finished'
                                try:
                                    p['process'].join()
                                except:
                                    raise Exception('pjh: Problem joining finished job process')
                                try:
                                    p['process'].close()
                                except:
                                    raise Exception('pjh: Problem closing finished job process')
                            else:
                                j._set_error(Exception(f'Error running job (pjh): {e}'))
                                p['pjh_status'] = 'error'
                                try:
                                    p['process'].join()
                                except:
                                    print('WARNING: problem joining errored job process')
                                try:
                                    p['process'].close()
                                except:
                                    print('WARNING: problem closing errored job process')
                else:
                    j._set_error(Exception(f'Job process is not alive'))
                    p['pjh_status'] = 'error'
                    try:
                        p['process'].close()
                    except:
                        print('WARNING: problem closing job process that is no longer alive (probably crashed)')
        
        num_running = 0
        for p in self._processes:
            if p['pjh_status'] == 'running':
                num_running = num_running + 1

        for p in self._processes:
            if p['pjh_status'] == 'pending':
                if num_running < self._num_workers:
                    job: Job = p['job']
                    pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
                    kwargs = job.get_resolved_kwargs()
                    image = job.get_image(kwargs) if job.config.use_container else None
                    process = multiprocessing.Process(target=_pjh_run_job, args=(pipe_to_parent, job.function_wrapper, kwargs, image, job.config))
                    p['process'] = process
                    p['pipe_to_child'] = pipe_to_child

                    p['pjh_status'] = 'running'
                    j: Job = p['job']
                    j._set_running()
                    p['process'].start()
                    num_running = num_running + 1

def _pjh_run_job(pipe_to_parent: Connection, function_wrapper: FunctionWrapper, kwargs: Dict[str, Any], image: Union[DockerImage, None], config: ConfigEntry) -> None:
    return_value, error, console_lines = _run_function(
        function_wrapper=function_wrapper,
        image=image,
        kwargs=kwargs,
        show_console=config.show_console
    )

    ret = dict(
        return_value=return_value,
        error=str(error) if error is not None else None,
        console_lines=console_lines
    )

    pipe_to_parent.send(ret)
    # wait for message to return
    while True:
        if pipe_to_parent.poll():
            pipe_to_parent.recv()
            return
        time.sleep(0.02)

_all_parallel_job_handlers: List[ParallelJobHandler] = []
def cleanup_all():
    for pjh in _all_parallel_job_handlers:
        if not pjh._halted:
            print(f'Cleaning up parallel job handler')
            pjh.cleanup()

atexit.register(cleanup_all)