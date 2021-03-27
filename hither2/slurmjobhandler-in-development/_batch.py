import os
from enum import Enum
import time
from typing import Optional, List, Union

DEFAULT_JOB_TIMEOUT = 1200

class BatchStatus(Enum):
    ERROR = 'error'
    PENDING = 'pending'
    WAITING = 'waiting'
    RUNNING = 'running'
    FINISHED = 'finished'

class _Batch():
    def __init__(self, *,
        working_dir: str,
        batch_label: str,
        num_workers: int,
        num_cores_per_job: int,
        use_slurm: bool,
        time_limit: Union[float, None],
        additional_srun_opts: List[str]
    ):
        """Constructor for _Batch class internal to SlurmJobHandler

        Parameters
        ----------
        working_dir : str
            The working directory within the slurm job handler working directory
        batch_label : str
            A label for display purposes
        """
        os.mkdir(working_dir)
        self._status = BatchStatus.PENDING
        self._time_started: Optional[float] = None
        self._working_dir = working_dir
        self._batch_label = batch_label
        self._num_workers = num_workers
        self._num_cores_per_job = num_cores_per_job
        self._use_slurm = use_slurm
        self._time_limit = time_limit
        self._additional_srun_opts = additional_srun_opts
        self._workers: List[_Worker] = []
        self._had_a_job = False
        self._timestamp_slurm_process_started = None

        # Create the workers
        for i in range(self._num_workers):
            self._workers.append(_Worker(base_path=self._working_dir + '/worker_{}'.format(i)))

        self._slurm_process = _SlurmProcess(
            working_dir=self._working_dir,
            num_workers=self._num_workers,
            num_cores_per_job=self._num_cores_per_job,
            additional_srun_opts=self._additional_srun_opts,
            use_slurm=self._use_slurm,
            time_limit=self._time_limit
        )

    def isPending(self) -> bool:
        return self._status == BatchStatus.PENDING

    def isWaitingToStart(self) -> bool:
        return self._status == BatchStatus.WAITING

    def isRunning(self) -> bool:
        return self._status == BatchStatus.RUNNING

    def isFinished(self) -> bool:
        return self._status == BatchStatus.FINISHED

    def timeStarted(self) -> Optional[float]:
        return self._time_started

    def elapsedSinceStarted(self) -> float:
        if not self._time_started:
            return 0
        return time.time() - self._time_started

    def iterate(self) -> None:
        """Periodically take care of business

        Returns
        -------
        None
            [description]
        """
        if self.isPending():
            pass
        elif self.isWaitingToStart():
            # first iterate all the workers so they can do what they need to do
            for w in self._workers:
                w.iterate()
            if os.path.exists(self._working_dir):
                if os.path.exists(os.path.join(self._working_dir, 'slurm_started.txt')):
                    self._status = BatchStatus.RUNNING
                    self._time_started = time.time()
                # else:
                #     # the following is probably not needed
                #     # but I suspected some trouble with our ceph
                #     # file system where the expected file
                #     # was not being detected until I added this
                #     # line. hmmmmm.
                #     x = os.listdir(self._working_dir)
                #     if len(x) == 0:
                #         assert('Unexpected problem. We should at least have a running.txt and a *.py file here.')
                #     elapsed = time.time() - self._timestamp_slurm_process_started
                #     if elapsed > 60:
                #         raise Exception(f'Unable to start batch after {elapsed} sec.')
        elif self.isRunning():
            # first iterate all the workers so they can do what they need to do
            for w in self._workers:
                w.iterate()

            # If we had a job in the past and we
            # haven't had anything to do for last 30 seconds, then
            # let's just end.
            if self._had_a_job:
                still_doing_stuff = False
                for w in self._workers:
                    if w.hasJob():
                        still_doing_stuff = True
                    else:
                        if w.everHadJob():
                            elapsed = w.elapsedTimeSinceLastJob()
                            assert elapsed is not None, "Unexpected elapsed is None"
                            if elapsed <= 30:
                                still_doing_stuff = True
                if not still_doing_stuff:
                    self.halt()
        elif self.isFinished():
            # We are finished so there's nothing to do
            pass

    def canAddJob(self, job: Job) -> bool:
        """Return True if we are able to add job, based on timing info, etc.

        Parameters
        ----------
        job : hither job
            Job to potentially add

        Returns
        -------
        bool
            Whether the job can be added
        """
        if self.isFinished():
            # We are finished, so we can't add any jobs
            return False
        # Determine the specified timeout of the job
        job_timeout = job._job_timeout
        if job_timeout is None:
            # if job doesn't have timeout, we use the default
            job_timeout = DEFAULT_JOB_TIMEOUT
        # See if adding this job would exceed the time limit
        if self._time_limit is not None:
            if job_timeout + self.elapsedSinceStarted() > self._time_limit + 5:
                # We would exceed the time limit. Can't add the job
                return False
        # If some worker has a vacancy then we can add the job
        for w in self._workers:
            if not w.hasJob():
                return True
        # Otherwise, we have no vacancy for a new job
        return False

    def hasJob(self) -> bool:
        """Return True if some worker has a job
        """
        for w in self._workers:
            if w.hasJob():
                return True
        return False

    def addJob(self, job: Job) -> None:
        """Add a job to the batch. Presumably it was already checked with canAddJob()

        Parameters
        ----------
        job : hither job
            The job to add

        Returns
        -------
        None
        """
        if self._status != BatchStatus.RUNNING:
            raise Exception('Cannot add job to batch that is not running.')

        # Determine number running, for display information
        num_running = 0
        for w in self._workers:
            if w.hasJob():
                num_running = num_running + 1

        # The job object
        print('Adding job to batch {} ({}/{}): [{}]'.format(self._batch_label, num_running + 1, self._num_workers, job._label))

        # Since we are adding a job, we declare that we have had a job
        self._had_a_job = True

        # Add the job to a vacant worker
        for w in self._workers:
            if not w.hasJob():
                w.setJob(job)
                return

        # This would be unexpected because we should have already checked with canAddJob()
        raise Exception('Unexpected: Unable to add job to batch. Unexpected -- no vacancies.')

    def start(self) -> None:
        """Start the batch

        Returns
        -------
        None
        """
        assert self._status == BatchStatus.PENDING, "Unexpected... cannot start a batch that is not pending."

        # Write the running.txt file
        running_fname = self._working_dir + '/running.txt'
        with FileLock(running_fname + '.lock', exclusive=True):
            with open(running_fname, 'w') as f:
                f.write('batch.')

        # Start the slurm process
        self._slurm_process.start()
        self._timestamp_slurm_process_started = time.time()

        self._status = BatchStatus.WAITING
        # self._time_started = time.time()  # instead of doing it here, let's wait until a worker has actually started.

    def halt(self) -> None:
        """Halt the batch
        """
        # Remove the running.txt file which should trigger the workers to end
        running_fname = self._working_dir + '/running.txt'
        if os.path.exists(running_fname):
            with FileLock(running_fname + '.lock', exclusive=True):
                os.remove(self._working_dir + '/running.txt')
        self._status = BatchStatus.FINISHED
        # wait a bit for it to resolve on its own (because we removed the running.txt)
        if not self._slurm_process.wait(5):
            print('Waiting for slurm process to end.')
            self._slurm_process.wait(5)
        # now force the halt
        self._slurm_process.halt()