import traceback
import time
import kachery as ka
from .core import config, _run_serialized_job_in_container, _serialize_item

class ComputeResource:
    def __init__(self, mongo_url, database, compute_resource_id):
        self._mongo_url = mongo_url
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._jobs = dict()
        self._client = None
        self._client_db_url = None
        self._iterate_timer = time.time()
    def clear(self):
        db = self._get_db()
        db.remove(dict(
            compute_resource_id=self._compute_resource_id
        ))
    def run(self):
        while True:
            self._iterate()
            time.sleep(0.02)
    def _iterate(self):
        elapsed = time.time() - self._iterate_timer
        if elapsed < 3:
            return
        self._iterate_timer = time.time()
        db = self._get_db()
        query = dict(
            compute_resource_id=self._compute_resource_id,
            last_modified_by_compute_resource=False,
            status='queued'
        )
        for doc in db.find(query):
            try:
                job_serialized = doc['job_serialized']
                job_serialized['code'] = ka.load_object(job_serialized['code'])
                success, result, runtime_info = _run_serialized_job_in_container(job_serialized)
                if success:
                    status = 'finished'
                    exception = None
                else:
                    status = 'error'
                    exception = 'test-exception'
            except:
                traceback.print_exc()
                status = 'error'
                success = False
                result = None
                runtime_info = None
                exception = 'unexpected problem'
            filter0 = dict(
                compute_resource_id=self._compute_resource_id,
                job_id=doc['job_id']
            )
            update = {
                '$set': dict(
                    status=status,
                    result=_serialize_item(result),
                    runtime_info=runtime_info,
                    exception=exception,
                    last_modified_by_compute_resource=True
                )
            }
            db.update_one(filter0, update=update)
    
    def _get_db(self):
        import pymongo
        url = self._mongo_url
        if url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(url, retryWrites=False)
            self._url = url
        return self._client[self._database]['hither2_jobs']

    