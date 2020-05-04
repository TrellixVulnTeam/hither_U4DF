from types import SimpleNamespace
import time
from typing import Dict
import kachery as ka
#from hither2 import _deserialize_item
from ._basejobhandler import BaseJobHandler
from .database import Database
from ._enums import JobStatus
from .file import File
from ._load_config import _load_preset_config_from_github
from ._util import _random_string, _utctime, _deserialize_item, _flatten_nested_collection

class RemoteJobHandler(BaseJobHandler):
    def __init__(self, *, database: Database, compute_resource_id):
        self.is_remote = True
        
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._handler_id = _random_string(15)
        self._jobs: Dict = {}
        self._kachery = None

        self._timestamp_database_poll = 0
        self._timestamp_last_action = time.time()

        self._internal_counts = SimpleNamespace(
            num_jobs=0,
            num_finished_jobs=0,
            num_errored_jobs=0
        )

        db1 = self._get_db(collection='active_compute_resources')
        t0 = _utctime() - 20
        query = dict(
            compute_resource_id=compute_resource_id,
            utctime={'$gt': t0}
        )
        doc = None
        for _ in range(5):
            doc = db1.find_one(query)
            if doc is not None:
                break
            time.sleep(0.5)
        if doc is None:
            raise Exception(f'No active compute resource found: {compute_resource_id}')
        self._kachery = doc['kachery']
    
    @staticmethod
    def preset(name):
        db = Database.preset(name)
        config = _load_preset_config_from_github(url='https://raw.githubusercontent.com/laboratorybox/hither2/config/config/2020a.json', name=name)
        return RemoteJobHandler(database=db, compute_resource_id=config['compute_resource_id'])

    def handle_job(self, job):
        super(RemoteJobHandler, self).handle_job(job)
        self._internal_counts.num_jobs += 1
        self._report_active()

        for f in _flatten_nested_collection(job._wrapped_function_arguments, _type=File):
            self._send_file_as_needed(f)

        job_serialized = job._serialize(generate_code=True)
        # send the code to the kachery
        job_serialized['code'] = ka.store_object(job_serialized['code'], to=self._kachery)

        db = self._get_db()
        doc = dict(
            compute_resource_id=self._compute_resource_id,
            handler_id=self._handler_id,
            job_id=job._job_id,
            job_serialized=job_serialized,
            status=JobStatus.QUEUED.value,
            compute_resource_status=JobStatus.PENDING.value,
            runtime_info=None,
            result=None,
            last_modified_by_compute_resource=False,
            client_code=None
        )
        db.insert_one(doc)
        self._jobs[job._job_id] = job

        self._report_action()
    
    def cancel_job(self, job_id):
        print('Warning: not yet able to cancel job of remotejobhandler')
    
    def iterate(self):
        elapsed_database_poll = time.time() - self._timestamp_database_poll
        if elapsed_database_poll > self._poll_interval():
            self._timestamp_database_poll = time.time()
            self._report_active()

            self._iterate_timer = time.time()
            db = self._get_db()
            client_code = _random_string(15)
            query = dict(
                compute_resource_id=self._compute_resource_id,
                handler_id=self._handler_id,
                last_modified_by_compute_resource=True
            )
            update = {
                '$set': dict(
                    last_modified_by_compute_resource=False,
                    client_code=client_code
                )
            }
            db.update_many(query, update=update)
            for doc in db.find(dict(client_code=client_code)):
                self._report_action()
                job_id = doc['job_id']
                if job_id in self._jobs:
                    j = self._jobs[job_id]
                    compute_resource_status = JobStatus(doc['compute_resource_status'])
                    if compute_resource_status == JobStatus.QUEUED:
                        print(f'Job queued: {job_id}')
                    elif compute_resource_status == JobStatus.RUNNING:
                        print(f'Job running: {job_id}')
                    elif compute_resource_status == JobStatus.FINISHED:
                        print(f'Job finished: {job_id}')
                        self._internal_counts.num_finished_jobs += 1
                        j._runtime_info = doc['runtime_info']
                        j._status = JobStatus.FINISHED
                        j._result = _deserialize_item(doc['result'])
                        for f in _flatten_nested_collection(j._result, _type=File):
                            setattr(f, '_remote_job_handler', self)
                        del self._jobs[job_id]
                    elif compute_resource_status == JobStatus.ERROR:
                        print(f'Job error: {job_id}')
                        self._internal_counts.num_errored_jobs += 1
                        j._runtime_info = doc['runtime_info']
                        j._status = JobStatus.ERROR
                        j._exception = Exception(doc['exception'])
                        del self._jobs[job_id]
                    else:
                        raise Exception(f'Unexpected compute resource status: {compute_resource_status}')
    
    def _load_file(self, sha1_path):
        return ka.load_file(sha1_path, fr=self._kachery)

    def _send_file_as_needed(self, x:File) -> None:
        if self._kachery is None: return # No file store; nothing we can do.
        # TODO: Should this case raise an exception?

        remote_handler = getattr(x, '_remote_job_handler', None)
        if remote_handler is None:
            if self._compute_resource_id is None: return
            ka.store_file(x.path, to=self._kachery)

        # A remote handler *is* configured.
        x_compute_resource_id = remote_handler._compute_resource_id
        #  If we *are* the remote handler, we don't need to do anything.
        if x_compute_resource_id == self._compute_resource_id: return

        raise Exception('This case not yet supported (we need to transfer data from one compute resource to another)')
        
    def _report_active(self):
        db = self._get_db(collection='active_job_handlers')
        filter = dict(
            handler_id=self._handler_id
        )
        update = {
            '$set': dict(
                handler_id=self._handler_id,
                utctime=_utctime()
            )
        }
        db.update_one(filter, update=update, upsert=True)
    
    def _report_action(self):
        self._timestamp_last_action = time.time()

    def _poll_interval(self):
        elapsed_since_last_action = time.time() - self._timestamp_last_action
        if elapsed_since_last_action < 3:
            return 0.1
        elif elapsed_since_last_action < 20:
            return 1
        elif elapsed_since_last_action < 60:
            return 3
        else:
            return 6

    def cleanup(self):
        pass

    def _get_db(self, collection='hither2_jobs'):
        return self._database.collection(collection)
