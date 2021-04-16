import tarfile
from hither2.dockerimage import DockerImage
from .function import FunctionWrapper
from typing import List, Dict, Any, Union
import time
import multiprocessing
from multiprocessing.connection import Connection
import time
from ._config import ConfigEntry
from ._job_handler import JobHandler
from ._job import Job
from ._run_function import _run_function
from .create_scriptdir_for_function_run import create_scriptdir_for_function_run

class FeedJobHandler(JobHandler):
    def __init__(self, *, feed_name: Union[str, None]=None, feed_uri: Union[str, None]=None):
        super().__init__()
        
        import kachery_p2p as kp
        if (feed_name is not None) and (feed_uri is not None):
            raise Exception('You cannot specify both feed_name and feed_id')
        if feed_name is not None:
            feed = kp.load_feed(feed_name, create=True)
        elif feed_uri is not None:
            feed = kp.load_feed(feed_uri)
        else:
            raise Exception('You must specify a feed_name or a feed_uri')
        self._feed = feed
        self._job_status_subfeed = feed.get_subfeed('job-statuses')
        self._job_status_subfeed.set_position(self._job_status_subfeed.get_num_local_messages())

        self._job_records: Dict[str, dict] = {}

        self._halted = False

    def cleanup(self):
        for k, v in self._job_records.items():
            job: Job = v['job']
            self.cancel_job(job.job_id)
        self._halted = True
    
    def is_remote(self) -> bool:
        return False

    def queue_job(self, job: Job):
        import kachery_p2p as kp
        if job.config.use_container and job.image:
            image = job.get_image(job.get_resolved_kwargs())
        else:
            image = None
        if not image:
            job._set_error(Exception('Cannot queue uncontainerized job to feed job handler'))
            return
        job_uri = job.store()
        self._job_records[job.job_id] = {
            'job': job,
            'job_uri': job_uri
        }

        subfeed = self._feed.get_subfeed('job-definitions')
        subfeed.append_message({
            'type': 'add_job',
            'job_id': job.job_id,
            'job_uri': job_uri
        })

    
    def cancel_job(self, job_id: str):
        job = self._job_records.get(job_id, None)
        if job is None:
            return
        subfeed = self._feed.get_subfeed('job-definitions')
        subfeed.append_message({
            'type': 'cancel_job',
            'job_id': job_id
        })
    
    def iterate(self):
        if self._halted:
            return

        messages = self._job_status_subfeed.get_next_messages(wait_msec=100)
        

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