from typing import Dict
import hither as hi
from ..job import Job, JobStatus
from .._serialize_job import _deserialize_job

class ComputeResourceJobManager:
    def __init__(self):
        # jobs by job_hash
        self._jobs_by_job_hash: Dict[str, Job] = {}
        self._cr_job_handler = hi.ParallelJobHandler(num_workers=4)
    def add_job(self, job_hash, job_serialized) -> Job:
        job = None
        if job_hash in self._jobs_by_job_hash:
            job = self._jobs_by_job_hash[job_hash]
            if job._status == JobStatus.ERROR:
                create_new_job = True
            else:
                create_new_job = False
        else:
            create_new_job = True
        if create_new_job:
            job = _deserialize_job(
                serialized_job=job_serialized,
                job_handler=None,
                job_cache=None,
                job_manager=None
            )
            self._jobs_by_job_hash[job_hash] = job
            self._cr_job_handler.handle_job(job)
        assert job is not None
        return job
    def iterate(self):
        self._cr_job_handler.iterate()
        # todo: periodic cleanup of jobs_by_job_hash