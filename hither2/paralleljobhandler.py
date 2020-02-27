from typing import List, Dict, Any
import time
import multiprocessing
from multiprocessing.connection import Connection
import time
import hither2 as hi

class ParallelJobHandler:
    def __init__(self, num_workers):
        self.is_remote = False
        self._num_workers = num_workers
        self._processes: List[dict] = []
        self._halted = False

    def handle_job(self, job):
        import kachery as ka
        pipe_to_parent, pipe_to_child = multiprocessing.Pipe()
        serialized_job = job._serialize(generate_code=(job._container is not None))
        process = multiprocessing.Process(target=_pjh_run_job, args=(pipe_to_parent, serialized_job, ka.get_config()))
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
                    pp = p['process']
                    print(f'Terminating process.')
                    pp.terminate()
                    pp.join()
                p['pjh_status'] = 'canceled'
    
    def iterate(self):
        if self._halted:
            return

        for p in self._processes:
            if p['pjh_status'] == 'running':
                if p['pipe_to_child'].poll():
                    ret = p['pipe_to_child'].recv()
                    p['pipe_to_child'].send('okay!')
                    p['job']._result=ret['result']
                    p['job']._status=ret['status']
                    p['job']._exception=ret['exception']
                    p['pjh_status'] = 'finished'
        
        num_running = 0
        for p in self._processes:
            if p['pjh_status'] == 'running':
                num_running = num_running + 1

        for p in self._processes:
            if p['pjh_status'] == 'pending':
                if num_running < self._num_workers:
                    p['pjh_status'] = 'running'
                    p['process'].start()
                    num_running = num_running + 1
        
        time.sleep(0.02)

    def cleanup(self):
        pass

def _pjh_run_job(pipe_to_parent: Connection, serialized_job: Any, kachery_config: dict) -> None:
    import kachery as ka
    ka.set_config(**kachery_config)
    job = hi._deserialize_job(serialized_job)
    job._execute()
    ret = dict(
        result=job._result,
        status=job._status,
        exception=job._exception
    )
    pipe_to_parent.send(ret)
    # wait for message to return
    while True:
        if pipe_to_parent.poll():
            pipe_to_parent.recv()
            return
        time.sleep(0.02)