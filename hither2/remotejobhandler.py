import time
from typing import Dict
from multiprocessing.connection import Connection
import kachery as ka
from hither2 import _deserialize_item
from ._util import _random_string

class RemoteJobHandler:
    def __init__(self, *, mongo_url, database, compute_resource_id):
        self._mongo_url = mongo_url
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._handler_id = _random_string(15)
        self._jobs: Dict = {}
        self._client = None
        self._client_db_url = None
        self._iterate_timer = time.time()

    def handle_job(self, job):
        job_serialized = job._serialize(generate_code=True)
        job_serialized['code'] = ka.store_object(job_serialized['code'])
        db = self._get_db()
        doc = dict(
            compute_resource_id=self._compute_resource_id,
            handler_id=self._handler_id,
            job_id=job._job_id,
            job_serialized=job_serialized,
            status='queued',
            runtime_info=None,
            result=None,
            last_modified_by_compute_resource=False,
            client_code=None
        )
        db.insert(doc)
        self._jobs[job._job_id] = job
    
    def iterate(self):
        elapsed = time.time() - self._iterate_timer
        if elapsed < 3:
            return
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
            job_id = doc['job_id']
            if job_id in self._jobs:
                j = self._jobs[job_id]
                j._runtime_info = doc['runtime_info']
                j._status = doc['status']
                if doc['status'] == 'finished':
                    j._result = _deserialize_item(doc['result'])
                    del self._jobs[job_id]
                elif doc['status'] == 'error':
                    j._exception = _deserialize_item(doc['exception'])
                    del self._jobs[job_id]

    def cleanup(self):
        pass

    def _get_db(self):
        import pymongo
        url = self._mongo_url
        if url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(url, retryWrites=False)
            self._url = url
        return self._client[self._database]['hither2_jobs']
