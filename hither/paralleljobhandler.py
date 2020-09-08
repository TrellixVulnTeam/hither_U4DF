import os
import signal
import tempfile
from typing import List, Dict, Any
import time
import multiprocessing
from multiprocessing.connection import Connection
import time

from .job import Job
from ._serialize_job import _serialize_job, _deserialize_job
from ._basejobhandler import BaseJobHandler
from ._enums import JobStatus
from ._exceptions import JobCancelledException

class ParallelJobHandler(BaseJobHandler):
    def __init__(self, num_workers):
        super().__init__()
        self._num_workers = num_workers
        self._processes: List[dict] = []
        self._halted = False

    def cleanup(self):
        self._halted = True

    def handle_job(self, job: Job):
        super(ParallelJobHandler, self).handle_job(job)
        import kachery as ka
        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        serialized_job = _serialize_job(job=job, generate_code=(job._container is not None))

        # Note that cancel_filepath will only have an effect if we are running this in a container
        if job._container is not None:
            cancel_filepath = f'{tempfile.gettempdir()}/pjh_cancel_job_{job._job_id}.txt'
        else:
            cancel_filepath = None
        process = multiprocessing.Process(target=_pjh_run_job, args=(pipe_to_parent, cancel_filepath, job))
        self._processes.append(dict(
            job=job,
            process=process,
            pipe_to_child=pipe_to_child,
            pjh_status=JobStatus.PENDING
        ))
    
    def cancel_job(self, job_id):
        for p in self._processes:
            if p['job']._job_id == job_id:
                if p['pjh_status'] == JobStatus.RUNNING:
                    # Note that cancel_filepath will only have an effect if we are running this in a container
                    if p['job']._container is not None:
                        print(f'ParallelJobHandler: Stopping process by writing cancel file.')
                        cancel_filepath = f'{tempfile.gettempdir()}/pjh_cancel_job_{job_id}.txt'
                        with open(cancel_filepath + '.tmp', 'w') as f:
                            f.write('cancel.')
                        os.rename(cancel_filepath+'.tmp', cancel_filepath)
                    else:
                        print(f'ParallelJobHandler: Terminating job.')
                        pp = p['process']
                        pp.terminate()
                        pp.join()
                        p['job']._result = None
                        p['job']._set_error_status(exception=JobCancelledException('Job cancelled'), runtime_info=dict(cancelled=True))
                        p['pjh_status'] = JobStatus.ERROR
                    # if pp.is_alive():
                    #     pp.join(timeout=2)
                    # print(f'ParallelJobHandler: Process stopped.')
                else:
                    # TODO: Consider if existing ERROR or FINISHED status should change this behavior 
                    p['job']._result = None
                    p['job']._set_error_status(exception=JobCancelledException('Job cancelled'), runtime_info=dict(cancelled=True))
                    p['pjh_status'] = JobStatus.ERROR
    
    def iterate(self):
        if self._halted:
            return

        for p in self._processes:
            if p['pjh_status'] == JobStatus.RUNNING:
                if p['pipe_to_child'].poll():
                    try:
                        ret = p['pipe_to_child'].recv()
                    except:
                        ret = None
                    if ret is not None:
                        p['pipe_to_child'].send('okay!')
                        p['job']._result = ret['result']
                        p['job']._set_status(ret['status'])
                        p['job']._exception = ret['exception']
                        p['job']._runtime_info = ret['runtime_info']
                        p['pjh_status'] = JobStatus.FINISHED
        
        num_running = 0
        for p in self._processes:
            if p['pjh_status'] == JobStatus.RUNNING:
                num_running = num_running + 1

        for p in self._processes:
            if p['pjh_status'] == JobStatus.PENDING:
                if num_running < self._num_workers:
                    p['pjh_status'] = JobStatus.RUNNING
                    p['job']._set_status(JobStatus.RUNNING)
                    p['process'].start()
                    num_running = num_running + 1
        
        time.sleep(0.02)

def _pjh_run_job(pipe_to_parent: Connection, cancel_filepath: str, job: Job) -> None:
    import kachery as ka

    # Note that cancel_filepath will only have an effect if we are running this in a container
    job._execute(cancel_filepath=cancel_filepath)

    ret = dict(
        result=job._result,
        status=job._status,
        exception=job._exception,
        runtime_info=job._runtime_info
    )
    pipe_to_parent.send(ret)
    # wait for message to return
    while True:
        if pipe_to_parent.poll():
            pipe_to_parent.recv()
            return
        time.sleep(0.02)