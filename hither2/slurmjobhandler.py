import shutil
from typing import List
import uuid
import os
from ._job_handler import JobHandler
from ._job import Job
from .scriptdir_runner.slurmbatch import SlurmBatch

class SlurmJobHandler(JobHandler):
    def __init__(self, *, num_jobs_per_batch: int, srun_command: str):
        import kachery_p2p as kp
        super().__init__()
        self._num_jobs_per_batch = num_jobs_per_batch
        self._srun_command = srun_command
        with kp.TemporaryDirectory(remove=False) as tmpdir:
            self._directory = tmpdir
        self._batches: List[SlurmBatch] = []

    def cleanup(self):
        for b in self._batches:
            b.stop()
        shutil.rmtree(self._directory)
        self._halted = True
    
    def is_remote(self) -> bool:
        return False

    def queue_job(self, job: Job):
        b = self._find_job_with_empty_slot()
        b.add_job(job)
    
    def _find_job_with_empty_slot(self):
        for b in self._batches:
            n = b.get_num_incomplete_jobs()
            if n < self._num_jobs_per_batch:
                return b
        return self._add_batch()
    
    def _add_batch(self):
        batch_id = 'batch-' + str(uuid.uuid4())[-12:]
        batchdir = f'{self._directory}/{batch_id}'
        os.mkdir(batchdir)
        b = SlurmBatch(directory=batchdir, srun_cmd=self._srun_command)
        b.start()
        self._batches.append(b)
        return b
    
    def cancel_job(self, job_id: str):
        pass
        # todo
    
    def iterate(self):
        new_batch_list: List[SlurmBatch] = []
        for b in self._batches:
            b.iterate()
            if b.get_num_incomplete_jobs() == 0:
                b.stop()
            else:
                new_batch_list.append(b)
        self._batches = new_batch_list
