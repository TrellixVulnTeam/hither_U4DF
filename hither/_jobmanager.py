import os
import sys
import time
from typing import Any, Union, Dict, List

from ._enums import JobStatus
from .job import Job
from ._shellscript import ShellScript
from ._util import _docker_form_of_container_string

class _JobManager:
    def __init__(self) -> None:
        self._queued_jobs = dict()
        self._running_jobs = dict()

    def queue_job(self, job):
        job._status = JobStatus.QUEUED
        self._queued_jobs[job._job_id] = job

    def process_job_queues(self):
        # Called periodically during wait()
        self.prune_job_queue()
        self.prepare_containers_for_queued_jobs()
        self.run_queued_jobs()
        self.review_running_jobs()

    def prune_job_queue(self):
        for _id, job in list(self._queued_jobs.items()):
            if job._status != JobStatus.QUEUED:
                del self._queued_jobs[_id]

    def prepare_containers_for_queued_jobs(self):
        for job in self._queued_jobs.values():
            if not job.container_may_be_needed(): continue
            # TODO: Push this back to the Job
            # TODO: This would require a container collection that lives independently,
            # like the Configs, rather than as a property of a particular JobManager.
            # We'll explore this later.
            try:
                self.prepare_container(job._container)
            except:
                job._status = JobStatus.ERROR
                job._exception = Exception(f'Unable to prepare container for job {job._label}: {job._container}')

    def run_queued_jobs(self):
        queued_job_ids = list(self._queued_jobs.keys())
        for _id in queued_job_ids:
            job: Job = self._queued_jobs[_id]
            if not job.is_ready_to_run(): continue

            del self._queued_jobs[_id]
            if job._status == JobStatus.ERROR: continue

            self._running_jobs[_id] = job
            job.resolve_wrapped_job_values()
            if job._job_cache is not None:
                if not job._job_handler.is_remote:
                    job._job_cache.check_job(job)
            # TODO: Do we actually do anything with the results of that check?

            job._job_handler.handle_job(job)

    def review_running_jobs(self):
        # Check which running jobs are finished and iterate job handlers of running or preparing jobs
        running_job_ids = list(self._running_jobs.keys())
        for _id in running_job_ids:
            job: Job = self._running_jobs[_id]
            if job._status == JobStatus.RUNNING:
                # Note: we effectively iterate the same job handler potentially many times here -- I think that's okay but not 100% sure.
                # NOTE: I think it's okay, but any reason not to just move on to the next Job?
                job._job_handler.iterate()
            if job._status in JobStatus.complete_statuses():
                self.finish_completed_job(job)

    def finish_completed_job(self, job:Job) -> None:
        del self._running_jobs[job._job_id]
        if job._download_results:
            job.download_results_if_needed()
        if job._job_cache is None or not job._job_handler.is_remote():
            return
        job._job_cache.cache_job_result(job)

    def reset(self):
        self._queued_jobs = dict()
        self._running_jobs = dict()
    
    def wait(self, timeout: Union[float, None]=None):
        timer = time.time()
        while True:
            self.process_job_queues()
            if self._queued_jobs == {} and self._running_jobs == {}:
                return
            if timeout == 0:
                return
            time.sleep(0.02)
            elapsed = time.time() - timer
            if timeout is not None and elapsed > timeout:
                return

    _prepared_singularity_containers = dict()
    _prepared_docker_images = dict()
    
    # NOTE: What these 'container preparation' methods actually do is make sure that
    # whatever container configuration has been attached to a Job's function `f`
    # has been pulled from a global container store and is available wherever the
    # job wants to run, before it runs.
    # The value of f._hither_container is added in core.py if the `container` param
    # is passed, and should be a docker:// URL.

    def prepare_container(self, container):
        if os.getenv('HITHER_USE_SINGULARITY', None) == 'TRUE':
            if container not in self._prepared_singularity_containers:
                self._do_prepare_singularity_container(container)
                self._prepared_singularity_containers[container] = True
        else:
            if os.getenv('HITHER_DO_NOT_PULL_DOCKER_IMAGES', None) != 'TRUE':
                if container not in self._prepared_docker_images:
                    self._do_pull_docker_image(container)
                    self._prepared_docker_images[container] = True


    def _do_prepare_singularity_container(self, container):
        print(f'Building singularity container: {container}')
        ss = ShellScript(f'''
            #!/bin/bash

            exec singularity run {container} echo "built {container}"
        ''')
        ss.start()
        retcode = ss.wait()
        if retcode != 0:
            raise Exception(f'Problem building container {container}')

    def _do_pull_docker_image(self, container):
        print(f'Pulling docker container: {container}')
        container = _docker_form_of_container_string(container)
        if (sys.platform == "win32"):
            if 1: # pragma: no cover
                ss = ShellScript(f'''
                    docker pull {container}
                ''')
        else:
            ss = ShellScript(f'''
                #!/bin/bash
                set -ex
                
                exec docker pull {container}
            ''')
        ss.start()
        retcode = ss.wait()
        if retcode != 0:
            raise Exception(f'Problem pulling container {container}')



