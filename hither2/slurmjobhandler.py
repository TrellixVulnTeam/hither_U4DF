import shutil
import time
from typing import Dict, List, Union
import uuid
import os
from ._job_handler import JobHandler
from ._job import Job
from .slurmallocation import SlurmAllocation

class SlurmJobHandler(JobHandler):
    def __init__(self, *, num_jobs_per_allocation: int, max_simultaneous_allocations: Union[int, None], srun_command: str):
        import kachery_p2p as kp
        super().__init__()
        self._num_jobs_per_allocation = num_jobs_per_allocation
        self._max_num_allocations = max_simultaneous_allocations
        self._srun_command = srun_command
        self._pending_jobs: Dict[str, Job] = {}
        with kp.TemporaryDirectory(remove=False) as tmpdir:
            self._directory = tmpdir
        self._allocations: List[SlurmAllocation] = []
        self._allocations_marked_for_stopping: Dict[str, Union[None, float]] = {}
        self._last_print_status_timestamp = 0

    def cleanup(self):
        for b in self._allocations:
            if b.status == 'running':
                b.stop()
        shutil.rmtree(self._directory)
        self._halted = True
    
    def is_remote(self) -> bool:
        return False

    def queue_job(self, job: Job):
        self._pending_jobs[job.job_id] = job
    
    def _find_running_allocation_with_empty_slot(self):
        num_running_allocations = 0
        num_pending_allocations = 0
        num_starting_allocations = 0
        for b in self._allocations:
            if b.status == 'running':
                num_running_allocations += 1
                n = b.num_queued_jobs + b.num_running_jobs
                if n < self._num_jobs_per_allocation:
                    return b
            elif b.status == 'pending':
                num_pending_allocations += 1
            elif b.status == 'starting':
                num_starting_allocations += 1
        if (self._max_num_allocations is None) or (num_running_allocations < self._max_num_allocations):
            if num_pending_allocations + num_starting_allocations == 0:
                self._start_new_allocation()
        return None
    
    def _start_new_allocation(self):
        print('Starting allocation')
        allocation_id = 'a-' + str(uuid.uuid4())[-8:]
        allocationdir = f'{self._directory}/{allocation_id}'
        os.mkdir(allocationdir)
        b = SlurmAllocation(directory=allocationdir, srun_command=self._srun_command, allocation_id=allocation_id)
        self._allocations.append(b)
        b.start()
    
    def cancel_job(self, job_id: str):
        pass
        # todo
    
    def iterate(self):
        self._print_status()

        pending_job_ids = list(self._pending_jobs.keys())
        for job_id in pending_job_ids:
            job = self._pending_jobs[job_id]
            b = self._find_running_allocation_with_empty_slot()
            if b is not None:
                job._set_queued()
                b.add_job(job)
                del self._pending_jobs[job_id]

        for b in self._allocations:
            bi = b.allocation_id
            if b.status != 'stopped':
                b.iterate()
                if (b.num_queued_jobs + b.num_running_jobs == 0) and (len(self._pending_jobs.values()) == 0):
                    x = self._allocations_marked_for_stopping.get(bi)
                    elapsed = time.time() - x if x is not None else -1
                    if elapsed > 2:
                        b.stop()
                    else:
                        self._allocations_marked_for_stopping[bi] = time.time()
    
    def _print_status(self):
        elapsed = time.time() - self._last_print_status_timestamp
        if elapsed < 3: # don't report more often than this
            return
        lines: List[str] = []
        lines.append('*******************************************************************')
        for b in self._allocations:
            lines.append(f'ALLOC {b.allocation_id} {b.status} - {b.num_queued_jobs} queued; {b.num_running_jobs} running; {b.num_finished_jobs} finished; {b.num_errored_jobs} errored;')
        lines.append('*******************************************************************')
        lines.append('')
        txt = '\n'.join(lines)
        if (elapsed > 20) or (txt != self._last_print_status_text):
            self._last_print_status_text = txt
            self._last_print_status_timestamp = time.time()
            print(txt)
        

