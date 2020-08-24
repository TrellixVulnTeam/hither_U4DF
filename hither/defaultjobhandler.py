from ._basejobhandler import BaseJobHandler
from ._enums import JobStatus

class DefaultJobHandler(BaseJobHandler):
    def __init__(self):
        super().__init__()

    def cleanup(self):
        pass

    def handle_job(self, job):
        # superclass implementation does standard logging and universal job status update
        super(DefaultJobHandler, self).handle_job(job)
        job._set_status(JobStatus.RUNNING)
        job._execute()

    def cancel_job(self, job_id):
        print('Warning: not able to cancel job of defaultjobhandler')

    def iterate(self):
        pass
