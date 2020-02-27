import time
from typing import Dict
from multiprocessing.connection import Connection
import kachery as ka
from hither2 import _deserialize_item
from ._util import _random_string
from .database import Database
from .file import File

class RemoteJobHandler:
    def __init__(self, *, database: Database, compute_resource_id):
        self.is_remote = True
        
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._handler_id = _random_string(15)
        self._jobs: Dict = {}
        self._kachery = None

        self._timestamp_database_poll = 0
        self._timestamp_last_action = time.time()

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

    def handle_job(self, job):
        self._report_active()

        self._send_files_as_needed_in_item(job._kwargs)

        job_serialized = job._serialize(generate_code=True)
        # send the code to the kachery
        job_serialized['code'] = ka.store_object(job_serialized['code'], to=self._kachery)

        db = self._get_db()
        doc = dict(
            compute_resource_id=self._compute_resource_id,
            handler_id=self._handler_id,
            job_id=job._job_id,
            job_serialized=job_serialized,
            status='queued',
            compute_resource_status='pending',
            runtime_info=None,
            result=None,
            last_modified_by_compute_resource=False,
            client_code=None
        )
        db.insert_one(doc)
        self._jobs[job._job_id] = job

        self._report_action()
    
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
                    compute_resource_status = doc['compute_resource_status']
                    if compute_resource_status == 'queued':
                        print(f'Job queued: {job_id}')
                    elif compute_resource_status == 'running':
                        print(f'Job queued: {job_id}')
                    elif compute_resource_status == 'finished':
                        print(f'Job finished: {job_id}')
                        j._runtime_info = doc['runtime_info']
                        j._status = 'finished'
                        j._result = _deserialize_item(doc['result'])
                        self._attach_compute_resource_id_to_files_in_item(j._result)
                        del self._jobs[job_id]
                    elif compute_resource_status == 'error':
                        print(f'Job error: {job_id}')
                        j._runtime_info = doc['runtime_info']
                        j._status = 'error'
                        j._exception = Exception(doc['exception'])
                        del self._jobs[job_id]
                    else:
                        raise Exception(f'Unexpected compute resource status: {compute_resource_status}')
    
    def _load_file(self, sha1_path):
        return ka.load_file(sha1_path, fr=self._kachery)

    def _send_files_as_needed_in_item(self, x):
        if isinstance(x, File):
            remote_handler = getattr(x, '_remote_job_handler', None)
            if remote_handler is not None:
                x_compute_resource_id = remote_handler._compute_resource_id
            else:
                x_compute_resource_id = None
            if self._kachery is None:
                pass
            elif x_compute_resource_id == self._compute_resource_id:
                pass
            else:
                if x_compute_resource_id is None:
                    ka.store_file(x.path, to=self._kachery)
                else:
                    raise Exception('This case not yet supported (we need to transfer data from one compute resource to another)')
        elif type(x) == dict:
            for val in x.values():
                self._send_files_as_needed_in_item(val)
        elif type(x) == list:
            for val in x:
                self._send_files_as_needed_in_item(val)
        elif type(x) == tuple:
            for val in x:
                self._send_files_as_needed_in_item(val)
        else:
            pass
    
    def _attach_compute_resource_id_to_files_in_item(self, x):
        if isinstance(x, File):
            setattr(x, '_remote_job_handler', self)
        elif type(x) == dict:
            for val in x.values():
                self._attach_compute_resource_id_to_files_in_item(val)
        elif type(x) == list:
            for val in x:
                self._attach_compute_resource_id_to_files_in_item(val)
        elif type(x) == tuple:
            for val in x:
                self._attach_compute_resource_id_to_files_in_item(val)
        else:
            pass

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

def _utctime():
    from datetime import datetime, timezone
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()