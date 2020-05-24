#from pymongo import MongoClient, collection, cursor (NOTE: This will hopefully be usable once TypeAlias
# is part of the language, see PEP-613. TODO)

import time
from typing import Optional, Any, Dict, List

from ._load_config import _load_preset_config_from_github
from ._enums import JobStatus, JobKeys, JobHandlerKeys, ComputeResourceKeys
from ._util import _utctime, _random_string

class Database:
    # Might do away with these
    ActiveJobHandlers = 'active_job_handlers'
    ActiveComputeResources = 'active_compute_resources'
    CachedJobResults = 'cached_job_results'
    HitherJobCollection = 'hither_jobs'

    def __init__(self, *, mongo_url: str, database: str):
        """Wraps a connection to a Mongo database instance used to store jobs, and other Hither
        job management data.

        Arguments:
            mongo_url {str} -- URL of the MongoDB instance, including password.
            database {str} -- Name of the specific database storing Hither job information.
        """
        self._mongo_url: str = mongo_url
        self._database: str = database
        # self._client: Optional[MongoClient] = None
        self._client: Optional[Any] = None

    @staticmethod
    def load_preset_config(name: str) -> 'Database':
        config: dict = _load_preset_config_from_github(url='https://raw.githubusercontent.com/flatironinstitute/hither/config/config/2020a.json', name=name)
        mongo_url: str = config['mongo_url']
        if 'password' in config:
            mongo_url = mongo_url.replace('${password}', config['password'])
        db: 'Database' = Database(mongo_url=mongo_url, database=config['database'])
        return db

    # This actually returns a collection but there are issues with importing pymongo at file level
    def _collection(self, collection_name: str) -> Any:
        import pymongo
        # NOTE: pymongo is imported here instead of at the top of the file because when a hither
        # function is containerized and shipped to a compute resource, the entire hither module
        # goes along with it. If the minimal container does not contain a pymongo instance, then
        # trying to do the import at the top of this file--even when its contents aren't used--
        # would raise an error.
        # TODO: Minimize the parts of the hither module that are wrapped with function calls - OR -
        # TODO: Use an environment variable to conditionally evaluate the import at the file level
        if self._client is None:
            self._client = pymongo.MongoClient(self._mongo_url, retryWrites=False)
        self._client.server_info() # will throw a timeout error on bad connection
        return self._client[self._database][collection_name]

    def _job_handlers(self) -> Any:
        return self._collection(Database.ActiveJobHandlers)

    def _compute_resources(self) -> Any:
        return self._collection(Database.ActiveComputeResources)

    def _cached_job_results(self) -> Any:
        return self._collection(Database.CachedJobResults)

    def _hither_jobs(self) -> Any:
        return self._collection(Database.HitherJobCollection)

    def _make_update(self, update:Dict[str, Any]) -> Dict[str, Any]:
        return { '$set': update }


  ##### Job Handler interface ###############

    def _get_active_job_handler_ids(self) -> List[str]:
        self._clear_expired_job_handlers()
        return [ x[JobHandlerKeys.HANDLER_ID] for x in self._job_handlers().find() ]

    def _report_job_handler_active(self, handler_id:str) -> None:
        _filter = { JobHandlerKeys.HANDLER_ID: handler_id }
        update_query = self._make_update({
            JobHandlerKeys.HANDLER_ID: handler_id,
            JobHandlerKeys.UTCTIME: _utctime()
        })
        self._job_handlers().update_one(_filter, update=update_query, upsert=True)

    def _clear_expired_job_handlers(self) -> None:
        timeout_cutoff = _utctime() - 10 # TODO: IS THIS THE RIGHT INTERPRETATION?
        query = { JobHandlerKeys.UTCTIME: { '$lt': timeout_cutoff } }
        handler_ids:List[str] = [ x[JobHandlerKeys.HANDLER_ID]
                                  for x in self._job_handlers().find(query) ]
        self._job_handlers().delete_many({ JobHandlerKeys.HANDLER_ID: { '$in': handler_ids } })
        self._hither_jobs().delete_many({ JobHandlerKeys.HANDLER_ID: { '$in': handler_ids } })
        for id in handler_ids:
            print(f'Removed job handler: {id}') # TODO: Make log

  ##### Compute Resource interface ###############

    def _report_compute_resource_active(self, resource_id:str, kachery:str) -> None:
        _filter = { JobKeys.COMPUTE_RESOURCE: resource_id }
        update_query = self._make_update({
            JobKeys.COMPUTE_RESOURCE: resource_id,
            ComputeResourceKeys.KACHERY: kachery,
            ComputeResourceKeys.UTCTIME: _utctime()
        })
        self._compute_resources().update_one(_filter, update=update_query, upsert=True)

    def _get_active_compute_resource_kachery_handle(self, resource_id:str, seconds_ago:int = 20) -> str:
        earliest_registry_date = _utctime() - seconds_ago
        query = {
            ComputeResourceKeys.COMPUTE_RESOURCE: resource_id,
            ComputeResourceKeys.UTCTIME: { '$gt': earliest_registry_date }
        }
        for _ in range(5):
            doc = self._compute_resources().find_one(query)
            if doc is not None:
                return doc[ComputeResourceKeys.KACHERY]
            time.sleep(0.5)
        raise Exception(f"No compute resource with id {resource_id} active since {earliest_registry_date} found in five attempts.")

  ##### Job cache interface ###############

    def _fetch_cached_job_result(self, hash:str) -> Optional[Dict[str, Any]]:
        job = self._cached_job_results().find_one({ JobKeys.JOB_HASH: hash })
        if job is None: return None
        if JobKeys.STATUS not in job: return None # TODO: throw error? If this key is missing it's probably not a Job
        return job

    # TODO: Job is, of course, obviously a Job, but typing it right now would lead to circular imports
    def _cache_job_result(self, job_hash: str, job:Any) -> None:
        query = { JobKeys.JOB_HASH: job_hash }
        update_query = self._make_update({
            JobKeys.JOB_HASH: job_hash,
            JobKeys.STATUS: job._status.value,
            JobKeys.RESULT: job._serialized_result(),
            JobKeys.RUNTIME_INFO: job._runtime_info,
            JobKeys.EXCEPTION: '{}'.format(job._exception)
        })
        self._cached_job_results().update_one(query, update_query, upsert=True)

  ##### Job processing interface ###############
    def add_pending_job(self, *,
                compute_resource_id:str, handler_id:str, job_id:str, job_serialized:str)-> None:
        new_job = {
            JobKeys.COMPUTE_RESOURCE: compute_resource_id,
            JobHandlerKeys.HANDLER_ID: handler_id,
            JobKeys.JOB_ID: job_id,
            JobKeys.SERIALIZATION: job_serialized,
            JobKeys.STATUS: JobStatus.QUEUED.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.PENDING.value,
            JobKeys.RUNTIME_INFO: None,
            JobKeys.RESULT: None,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: False,
            JobKeys.CLIENT_CODE: None
        }
        self._hither_jobs().insert_one(new_job)
    
    def cancel_job(self, *,
                compute_resource_id:str, handler_id:str, job_id:str)-> None:
        query = {
            JobKeys.COMPUTE_RESOURCE: compute_resource_id,
            JobHandlerKeys.HANDLER_ID: handler_id,
            JobKeys.JOB_ID: job_id
        }
        update_query = self._make_update({
            JobKeys.CANCEL_REQUESTED: True
        })
        self._hither_jobs().update_one(query, update_query, upsert=True)

    # This actually returns a cursor but there are issues with importing pymongo at file level
    def _fetch_pending_jobs(self, *, _compute_resource_id: str) -> List[Any]:
        query = {
            JobKeys.COMPUTE_RESOURCE:_compute_resource_id,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: False,
            JobKeys.STATUS: JobStatus.QUEUED.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.PENDING.value  # status on the compute resource
        }
        return self._hither_jobs().find(query)
    
    def _fetch_job_ids_for_cancel_requests(self, *, _compute_resource_id: str) -> List[str]:
        query = {
            JobKeys.COMPUTE_RESOURCE:_compute_resource_id,
            JobKeys.CANCEL_REQUESTED: True
        }
        return [job[JobKeys.JOB_ID] for job in self._hither_jobs().find(query)]

    # NOTE: Actually should return a Cursor, as above
    def _fetch_remote_modified_jobs(self, *, compute_resource_id:str, handler_id: str) -> List[Any]:
        # Fake a transaction by arbitrarily tagging the updated items & then retrieving tag
        client_code = _random_string(15)
        query = {
            JobKeys.COMPUTE_RESOURCE: compute_resource_id,
            JobHandlerKeys.HANDLER_ID: handler_id,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        }
        update_query = self._make_update({
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: False,
            JobKeys.CLIENT_CODE: client_code
        })
        self._hither_jobs().update_many(query, update=update_query)
        modified_objects_query = {
            JobKeys.CLIENT_CODE: client_code
        }
        return self._hither_jobs().find(modified_objects_query)

    def _clear_jobs_for_compute_resource(self, compute_resource_id:str) -> None:
        _filter = { JobKeys.COMPUTE_RESOURCE: compute_resource_id }
        self._hither_jobs().delete_many(_filter)

    def _delete_job(self, job_id:str, compute_resource:str) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        self._hither_jobs().delete_many(_filter)

    def _mark_job_as_error(self, job_id:str, compute_resource:str, *,
                            runtime_info: Optional[dict],
                            exception: Optional[Exception]) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        update_query = self._make_update({
            JobKeys.STATUS: JobStatus.ERROR.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.ERROR.value,
            JobKeys.RESULT: None,
            JobKeys.RUNTIME_INFO: runtime_info,
            JobKeys.EXCEPTION: f"{exception}",
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        })
        self._hither_jobs().update_one(_filter, update=update_query)

    def _mark_job_as_finished(self, job_id:str, compute_resource:str, *,
                                runtime_info: Optional[dict],
                                result: Optional[Any]) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        update_query = self._make_update({
            JobKeys.STATUS: JobStatus.FINISHED.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.FINISHED.value,
            JobKeys.RESULT: result,
            JobKeys.RUNTIME_INFO: runtime_info,
            JobKeys.EXCEPTION: None,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        })
        self._hither_jobs().update_one(_filter, update=update_query)

    def _mark_job_as_queued(self, job_id:str, compute_resource:str) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        update_query = self._make_update({
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.QUEUED.value,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        })
        self._hither_jobs().update_one(_filter, update=update_query)

    def _mark_job_as_running(self, job_id:str, compute_resource:str) -> None:
        _filter = {
            JobKeys.COMPUTE_RESOURCE: compute_resource,
            JobKeys.JOB_ID: job_id
        }
        update_query = self._make_update({
            JobKeys.STATUS: JobStatus.RUNNING.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.RUNNING.value,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        })
        self._hither_jobs().update_one(_filter, update=update_query)