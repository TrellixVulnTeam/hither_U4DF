import time
import kachery as ka
from .core import _serialize_item, _deserialize_job, _prepare_container
from ._util import _random_string

class ComputeResource:
    def __init__(self, mongo_url, database, compute_resource_id, job_handler):
        self._mongo_url = mongo_url
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._instance_id = _random_string(15)
        self._client = None
        self._client_db_url = None
        self._iterate_timer = time.time()
        self._job_handler = job_handler
        self._jobs = dict()
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

        self._report_active()
        active_job_handler_ids = self._get_active_job_handler_ids()

        self._iterate_timer = time.time()
        db = self._get_db()

        # Handle pending jobs
        query = dict(
            compute_resource_id=self._compute_resource_id,
            last_modified_by_compute_resource=False,
            status='queued',
            compute_resource_status='pending'
        )
        for doc in db.find(query):
            self._handle_pending_job(doc)
        
        # Handle jobs
        job_ids = list(self._jobs.keys())
        for job_id in job_ids:
            job = self._jobs[job_id]
            reported_status = getattr(job, '_reported_status')
            filter0 = dict(
                compute_resource_id=self._compute_resource_id,
                job_id=job_id
            )
            if job._status == 'running':
                if reported_status != 'running':
                    print(f'Job running: {job_id}')
                    update = {
                        '$set': dict(
                            status='running',
                            compute_resource_status='running',
                            last_modified_by_compute_resource=True
                        )
                    }
                    db.update_one(filter0, update=update)
                    setattr(job, '_reported_status', 'running')
            elif job._status == 'finished':
                print(f'Job finished: {job_id}')
                update = {
                    '$set': dict(
                        status='finished',
                        compute_resource_status='finished',
                        result=_serialize_item(job._result),
                        runtime_info=job._runtime_info,
                        exception=None,
                        last_modified_by_compute_resource=True
                    )
                }
                db.update_one(filter0, update=update)
                del self._jobs[job_id]
            elif job._status == 'error':
                print(f'Job error: {job_id}')
                update = {
                    '$set': dict(
                        status='error',
                        compute_resource_status='error',
                        result=None,
                        runtime_info=job._runtime_info,
                        exception='test-exception',
                        last_modified_by_compute_resource=True
                    )
                }
                db.update_one(filter0, update=update)
                del self._jobs[job_id]
            
            # check if handler is still active
            if job_id in self._jobs:
                handler_id = getattr(job, '_handler_id')
                if handler_id not in active_job_handler_ids:
                    print(f'Removing job because client handler is no longer active: {job_id}')
                    self._job_handler.cancel_job(job_id)
                    db.remove(filter0)
                    del self._jobs[job_id]
        
        self._job_handler.iterate()
    
    def _get_active_job_handler_ids(self):
        db = self._get_db(collection='active_job_handlers')

        # remove the expired
        t0 = _utctime() - 10
        query = dict(
            utctime={'$lt': t0}
        )
        db.remove(query)

        # return handler ids for those that were not removed
        return [doc['handler_id'] for doc in db.find({})]
    
    def _handle_pending_job(self, doc):
        job_id = doc["job_id"]
        print(f'Queuing job: {job_id}')
        
        try:
            job_serialized = doc['job_serialized']
            job_serialized['code'] = ka.load_object(job_serialized['code'])
            container = job_serialized['container']
            if container is None:
                raise Exception('Cannot run serialized job outside of container.')
            _prepare_container(container)
        except Exception as e:
            print(f'Error handing pending job: {job_id}')
            print(e)
            self._mark_job_as_error(doc, exception=e, runtime_info=None)
            return
        
        job = _deserialize_job(job_serialized)
        self._jobs[job_id] = job
        self._job_handler.handle_job(job)
        db = self._get_db()
        filter0 = dict(
            compute_resource_id=self._compute_resource_id,
            job_id=doc['job_id']
        )
        update = {
            '$set': dict(
                compute_resource_status='queued',
                last_modified_by_compute_resource=True
            )
        }
        db.update_one(filter0, update=update)
        setattr(job, '_reported_status', 'queued')
        setattr(job, '_handler_id', doc['handler_id'])
    
    def _mark_job_as_error(self, doc, exception, runtime_info):
        job_id = doc['job_id']
        print(f'Job error: {job_id}')
        db = self._get_db()
        filter0 = dict(
            compute_resource_id=self._compute_resource_id,
            job_id=doc['job_id']
        )
        update = {
            '$set': dict(
                status='error',
                compute_resource_status='error',
                result=None,
                runtime_info=runtime_info,
                exception='{}'.format(exception),
                last_modified_by_compute_resource=True
            )
        }
        db.update_one(filter0, update=update)
    
    def _report_active(self):
        db = self._get_db(collection='active_compute_resources')
        filter = dict(
            compute_resource_id=self._compute_resource_id
        )
        update = {
            '$set': dict(
                compute_resource_id=self._compute_resource_id,
                utctime=_utctime()
            )
        }
        db.update_one(filter, update=update, upsert=True)

    def _get_db(self, collection='hither2_jobs'):
        import pymongo
        url = self._mongo_url
        if url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(url, retryWrites=False)
            self._url = url
        return self._client[self._database][collection]

    
def _utctime():
    from datetime import datetime, timezone
    return datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()