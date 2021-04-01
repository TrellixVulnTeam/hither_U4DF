import shutil
import time
from typing import Dict, List, Union
import uuid
import os
from ._job_handler import JobHandler
from ._job import Job
from .scriptdir_runner.slurmbatch import SlurmBatch

class SlurmJobHandler(JobHandler):
    def __init__(self, *, num_jobs_per_batch: int, max_num_batches: Union[int, None], srun_command: str):
        import kachery_p2p as kp
        super().__init__()
        self._num_jobs_per_batch = num_jobs_per_batch
        self._max_num_batches = max_num_batches
        self._srun_command = srun_command
        self._pending_jobs: Dict[str, Job] = {}
        with kp.TemporaryDirectory(remove=False) as tmpdir:
            self._directory = tmpdir
        self._pending_batches: Dict[str, SlurmBatch] = {}
        self._running_batches: Dict[str, SlurmBatch] = {}
        self._batches_marked_for_stopping: Dict[str, Union[None, float]] = {}

    def cleanup(self):
        for b in self._running_batches.values():
            b.stop()
        shutil.rmtree(self._directory)
        self._halted = True
    
    def is_remote(self) -> bool:
        return False

    def queue_job(self, job: Job):
        self._pending_jobs[job.job_id] = job
    
    def _find_batch_with_empty_slot(self):
        for b in self._running_batches.values():
            n = b.get_num_incomplete_jobs()
            if n < self._num_jobs_per_batch:
                return b
        if (self._max_num_batches is None) or (len(self._running_batches.values()) < self._max_num_batches):
            if len(self._pending_batches.values()) == 0:
                self._start_new_batch()
        return None
    
    def _start_new_batch(self):
        print('Starting batch')
        batch_id = 'batch-' + str(uuid.uuid4())[-12:]
        batchdir = f'{self._directory}/{batch_id}'
        os.mkdir(batchdir)
        b = SlurmBatch(directory=batchdir, srun_cmd=self._srun_command)
        b.start()
        self._pending_batches[batch_id] = b
    
    def cancel_job(self, job_id: str):
        pass
        # todo
    
    def iterate(self):
        pending_job_ids = list(self._pending_jobs.keys())
        for job_id in pending_job_ids:
            job = self._pending_jobs[job_id]
            b = self._find_batch_with_empty_slot()
            if b is not None:
                job._set_queued()
                b.add_job(job)
                del self._pending_jobs[job_id]

        running_batch_ids = list(self._running_batches.keys())
        for bi in running_batch_ids:
            b = self._running_batches[bi]
            b.iterate()
            if (b.get_num_incomplete_jobs() == 0) and (len(self._pending_jobs.values()) == 0):
                x = self._batches_marked_for_stopping.get(bi)
                elapsed = time.time() - x if x is not None else -1
                if elapsed > 2:
                    b.stop()
                    del self._running_batches[bi]
                else:
                    self._batches_marked_for_stopping[bi] = time.time()
        pending_batch_ids = list(self._pending_batches.keys())
        for bi in pending_batch_ids:
            b = self._pending_batches[bi]
            b.iterate()
            if b.is_running():
                self._running_batches[bi] = b
                del self._pending_batches[bi]
