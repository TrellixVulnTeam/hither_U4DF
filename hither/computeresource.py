import time
import kachery as ka
from .core import _serialize_item, _deserialize_job, _prepare_container
from ._util import _random_string, _utctime
from .database import Database
from ._enums import JobStatus
from .file import File

# TODO: Functionalize, tighten.
# TODO: Consider filtering db query/filter/update statements through an interface function.
# TODO: Inject a JobManager into this instead of relying on redirection through core._prepare_container?

class ComputeResource:
    def __init__(self, *, database: Database, compute_resource_id, kachery, job_handler, job_cache=None):
        self._database = database
        self._compute_resource_id = compute_resource_id
        self._kachery = kachery
        self._instance_id = _random_string(15)
        self._database = database
        self._timestamp_database_poll = 0
        self._timestamp_last_action = time.time()
        self._job_handler = job_handler
        self._job_cache = job_cache
        self._jobs = dict()
    def clear(self):
        db = self._get_db()
        db.delete_many(dict(
            compute_resource_id=self._compute_resource_id
        ))
    def run(self):
        while True:
            self._iterate()
            time.sleep(0.02)
    def _iterate(self):
        active_job_handler_ids = self._get_active_job_handler_ids()

        self._iterate_timer = time.time()
        db = self._get_db()

        elapsed_database_poll = time.time() - self._timestamp_database_poll
        if elapsed_database_poll > self._poll_interval():
            self._timestamp_database_poll = time.time()

            self._report_active()

            # Handle pending jobs
            query = dict(
                compute_resource_id=self._compute_resource_id,
                last_modified_by_compute_resource=False,
                status=JobStatus.QUEUED.value,
                compute_resource_status=JobStatus.PENDING.value  # status on the compute resource
            )
            for doc in db.find(query):
                self._report_action()
                self._handle_pending_job(doc)
        
        # Handle jobs
        job_ids = list(self._jobs.keys())
        for job_id in job_ids:
            job = self._jobs[job_id]
            reported_status = getattr(job, '_reported_status')
            if job._status == JobStatus.RUNNING:
                if reported_status != JobStatus.RUNNING:
                    print(f'Job running: {job_id}')
                    filter0 = dict(
                        compute_resource_id=self._compute_resource_id,
                        job_id=job_id
                    )
                    update = {
                        '$set': dict(
                            status=JobStatus.RUNNING.value,
                            compute_resource_status=JobStatus.RUNNING.value,
                            last_modified_by_compute_resource=True
                        )
                    }
                    db.update_one(filter0, update=update)
                    self._report_action()
                    setattr(job, '_reported_status', JobStatus.RUNNING)
            elif job._status == JobStatus.FINISHED:
                print(f'Job finished: {job_id}')
                self._handle_finished_job(job)
                if self._job_cache is not None:
                    self._job_cache.cache_job_result(job)
                del self._jobs[job_id]
            elif job._status == JobStatus.ERROR:
                # _print_console_out(job._runtime_info['console_out'])
                print(job._exception)
                print(f'Job error: {job_id}')
                self._mark_job_as_error(job_id=job_id, runtime_info=job._runtime_info, exception=job._exception)
                del self._jobs[job_id]
            
            # check if handler is still active
            if job_id in self._jobs:
                handler_id = getattr(job, '_handler_id')
                if handler_id not in active_job_handler_ids:
                    print(f'Removing job because client handler is no longer active: {job_id}')
                    self._job_handler.cancel_job(job_id)
                    filter0 = dict(
                        compute_resource_id=self._compute_resource_id,
                        job_id=job_id
                    )
                    db.delete_many(filter0)
                    del self._jobs[job_id]
        
        self._job_handler.iterate()
    
    def _get_active_job_handler_ids(self):
        db = self._get_db(collection='active_job_handlers')
        db_jobs = self._get_db()

        # remove the expired job handlers
        t0 = _utctime() - 10
        query = dict(
            utctime={'$lt': t0}
        )
        for doc in db.find(query):
            handler_id = doc['handler_id']
            print(f'Removing job handler: {handler_id}')
            db.delete_many(dict(handler_id=handler_id))
            db_jobs.delete_many(dict(handler_id=handler_id))

        # return handler ids for those that were not removed
        return [doc['handler_id'] for doc in db.find({})]
    
    def _handle_pending_job(self, doc):
        job_id = doc["job_id"]
        label = doc['job_serialized']['label']
        print(f'Queuing job: {label}')
        
        job_serialized = doc['job_serialized']
        if job_serialized['code'] is not None:
            try:
                code_obj = ka.load_object(job_serialized['code'], fr=self._kachery)
                if code_obj is None:
                    raise Exception("Kachery returned no serialized code for function.")
                job_serialized['code'] = code_obj
            except Exception as e:
                exc = f'Error loading code for function {label}: {job_serialized["code"]} ({str(e)})'
                print(exc)
                self._mark_job_as_error(job_id=job_id, exception=Exception(exc), runtime_info=None)
                return
        container = job_serialized['container']
        if container is None:
            exc = f'Cannot run serialized job outside of container: {label}'
            print(exc)
            self._mark_job_as_error(job_id=job_id, exception=Exception(exc), runtime_info=None)
            return
        try:
            _prepare_container(container)
        except Exception as e:
            print(f'Error preparing container for pending job: {label}')
            print(e)
            self._mark_job_as_error(job_id=job_id, exception=e, runtime_info=None)
            return
        
        job = _deserialize_job(job_serialized)
        if self._job_cache:
            self._job_cache.check_job(job)
        db = self._get_db()
        filter0 = dict(
            compute_resource_id=self._compute_resource_id,
            job_id=doc['job_id']
        )
        if job._status == JobStatus.FINISHED:
            print(f'Found job in cache: {label}')
            self._handle_finished_job(job)
        elif job._status == JobStatus.ERROR:
            print(f'Found error job in cache: {label}')
            self._mark_job_as_error(job_id=job_id, exception=job._exception, runtime_info=job._runtime_info)
        else:
            try:
                job.download_parameter_files_if_needed(kachery=self._kachery)
            except Exception as e:
                print(f'Error downloading input files for job: {label}')
                print(e)
                self._mark_job_as_error(job_id=job_id, exception=e, runtime_info=None)
                return
            self._jobs[job_id] = job
            self._job_handler.handle_job(job)
            update = {
                '$set': dict(
                    compute_resource_status=JobStatus.QUEUED.value,
                    last_modified_by_compute_resource=True
                )
            }
            db.update_one(filter0, update=update)
            setattr(job, '_reported_status', JobStatus.QUEUED)
            setattr(job, '_handler_id', doc['handler_id'])
    
    def _handle_finished_job(self, job):
        job.kache_results_if_needed(kachery=self._kachery)
        self._mark_job_as_finished(job_id=job._job_id, runtime_info=job._runtime_info, result=job._result)
    
    def _mark_job_as_error(self, *, job_id, runtime_info, exception):
        print(f'Job error: {job_id}')
        db = self._get_db()
        filter0 = dict(
            compute_resource_id=self._compute_resource_id,
            job_id=job_id
        )
        update = {
            '$set': dict(
                status=JobStatus.ERROR.value,
                compute_resource_status=JobStatus.ERROR.value,
                result=None,
                runtime_info=runtime_info,
                exception='{}'.format(exception),
                last_modified_by_compute_resource=True
            )
        }
        db.update_one(filter0, update=update)
        self._report_action()
    
    def _mark_job_as_finished(self, *, job_id, runtime_info, result):
        db = self._get_db()
        filter0 = dict(
            compute_resource_id=self._compute_resource_id,
            job_id=job_id
        )
        update = {
            '$set': dict(
                status=JobStatus.FINISHED.value,
                compute_resource_status=JobStatus.FINISHED.value,
                result=_serialize_item(result),
                runtime_info=runtime_info,
                exception=None,
                last_modified_by_compute_resource=True
            )
        }
        db.update_one(filter0, update=update)
        self._report_action()
    
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
    def _report_active(self):
        db = self._get_db(collection='active_compute_resources')
        filter = dict(
            compute_resource_id=self._compute_resource_id
        )
        update = {
            '$set': dict(
                compute_resource_id=self._compute_resource_id,
                kachery=self._kachery,
                utctime=_utctime()
            )
        }
        db.update_one(filter, update=update, upsert=True)

    def _get_db(self, collection='hither_jobs'):
        return self._database.collection(collection)

def _print_console_out(x):
    for a in x['lines']:
        t = _fmt_time(a['timestamp'])
        txt = a['text']
        print(f'{t}: {txt}')

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()