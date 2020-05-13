#from pymongo import MongoClient, collection, cursor (NOTE: This will hopefully be usable once TypeAlias
# is part of the language, see PEP-613. TODO)

from typing import Optional, Union, Any, Dict, List, Tuple

from ._load_config import _load_preset_config_from_github
from ._enums import JobStatus
from .file import File
from ._util import _utctime


class Database:
    HitherJobCollection = 'hither_jobs'
    ActiveJobHandlers = 'active_job_handlers'
    ActiveComputeResources = 'active_compute_resources'

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
        self._client_db_url: Optional[str] = None

    # TODO NOTE: method only appears in remotejobhandler.
    @staticmethod
    def preset(name: str) -> 'Database':
        config: dict = _load_preset_config_from_github(url='https://raw.githubusercontent.com/flatironinstitute/hither/config/config/2020a.json', name=name)
        mongo_url: str = config['mongo_url']
        if 'password' in config:
            mongo_url = mongo_url.replace('${password}', config['password'])
        db: 'Database' = Database(mongo_url=mongo_url, database=config['database'])
        return db

# This actually returns a collection but there are issues with importing pymongo at file level
    def collection(self, collection_name: str) -> Any:
        import pymongo
        # NOTE: Neither of these values are changed in this codebase, nor should they be changed elsewhere?
        if self._mongo_url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(self._mongo_url, retryWrites=False)
            self._client_db_url = self._mongo_url
        return self._client[self._database][collection_name]

    def _make_update(self, update:Dict[str, Any]) -> Dict[str, Any]:
        return { '$set': update }

    def _get_active_job_handler_ids(self) -> List[str]:
        self._clear_expired_job_handlers()
        return [ x[JobHandlerKeys.HANDLER_ID] for x in self.collection(Database.ActiveJobHandlers).find() ]

    def _clear_expired_job_handlers(self) -> None:
        timeout_cutoff = _utctime() - 10 # TODO: IS THIS THE RIGHT INTERPRETATION?
        query = { JobHandlerKeys.UTCTIME: { '$lt': timeout_cutoff } }
        handler_ids:List[str] = [ x[JobHandlerKeys.HANDLER_ID]
                                  for x in self.collection(Database.ActiveJobHandlers).find(query) ]
        self.collection(Database.ActiveJobHandlers)\
            .delete_many({ JobHandlerKeys.HANDLER_ID: { '$in': handler_ids } })
        self.collection(Database.HitherJobCollection)\
            .delete_many({ JobHandlerKeys.HANDLER_ID: { '$in': handler_ids } })
        for id in handler_ids:
            print(f'Removed job handler: {id}') # TODO: Make log

    # This actually returns a cursor but there are issues with importing pymongo at file level
    def _fetch_pending_jobs(self, *, _compute_resource_id: str) -> List[Any]:
        query = {
            JobKeys.COMPUTE_RESOURCE:_compute_resource_id,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: False,
            JobKeys.STATUS: JobStatus.QUEUED.value,
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.PENDING.value  # status on the compute resource
        }
        return self.collection(Database.HitherJobCollection).find(query)

    def _clear_jobs_for_compute_resource(self, compute_resource_id:str) -> None:
        _filter = { JobKeys.COMPUTE_RESOURCE: compute_resource_id }
        self.collection(Database.HitherJobCollection).delete_many(_filter)

    def _delete_job(self, job_id:str, compute_resource:str) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        self.collection(Database.HitherJobCollection)\
            .delete_many(_filter)

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
        self.collection(Database.HitherJobCollection)\
            .update_one(_filter, update=update_query)

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
        self.collection(Database.HitherJobCollection)\
            .update_one(_filter, update=update_query)

    def _mark_job_as_queued(self, job_id:str, compute_resource:str) -> None:
        _filter = {
            JobKeys.JOB_ID: job_id,
            JobKeys.COMPUTE_RESOURCE: compute_resource
        }
        update_query = self._make_update({
            JobKeys.COMPUTE_RESOURCE_STATUS: JobStatus.QUEUED.value,
            JobKeys.LAST_MODIFIED_BY_COMPUTE_RESOURCE: True
        })
        self.collection(Database.HitherJobCollection)\
            .update_one(_filter, update=update_query)

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
        self.collection(Database.HitherJobCollection)\
            .update_one(_filter, update=update_query)

    def _report_compute_resource_active(self, resource_id:str, kachery:str) -> None:
        _filter = { JobKeys.COMPUTE_RESOURCE: resource_id }
        update_query = self._make_update({
            JobKeys.COMPUTE_RESOURCE: resource_id,
            ComputeResourceKeys.KACHERY: kachery,
            ComputeResourceKeys.UTCTIME: _utctime()
        })
        self.collection(Database.ActiveComputeResources)\
            .update_one(_filter, update=update_query, upsert=True)

# TODO: Put this somewhere else, in a Constants file or something
# (or else make it part of the Job class)
# NOTE: It probably belongs somewhere closer to serialization/deserialization code too...
class JobKeys:
    CODE = 'code'
    COMPUTE_RESOURCE = 'compute_resource_id'
    COMPUTE_RESOURCE_STATUS = 'compute_resource_status' # the Job's status on the remote resource.
    CONTAINER = 'container'
    DOWNLOAD_RESULTS = 'download_results'
    EXCEPTION = 'exception'
    FUNCTION = 'function'
    FUNCTION_NAME = 'function_name'
    FUNCTION_VERSION = 'function_version'
    JOB_ID = 'job_id'
    JOB_TIMEOUT = 'job_timeout'
    LABEL = 'label'
    # represents whether the job state was last changed by the compute resource (as opposed to
    # by the local job handler)
    LAST_MODIFIED_BY_COMPUTE_RESOURCE = 'last_modified_by_compute_resource'
    NO_RESOLVE_INPUT_FILES = 'no_resolve_input_files'
    RESULT = 'result'
    RUNTIME_INFO = 'runtime_info'
    SERIALIZATION_LABEL = 'job_serialized'
    STATUS = 'status'
    WRAPPED_ARGS = 'kwargs' # TODO CHANGE ME ONCE ALL REFERENCES ARE CENTRALIZED

    @staticmethod
    def _verify_serialized_job(doc:Dict[str, any]) -> bool:
        """Checks an input dictionary (expected to correspond to a Job serialized to MongoDB job
        monitor bus) for a bare minimum of required keys to establish it is actually a Job object.

        Arguments:
            doc {Dict[str, any]} -- Dictionary of fields corresponding to a Hither Job.

        Returns:
            bool -- True if required fields are present; else False.
        """
        required_keys = [
            JobKeys.LABEL,
            JobKeys.CODE,
            JobKeys.CONTAINER
        ]
        for x in required_keys:
            if not x in doc: return False
        return True

    @staticmethod
    def _unpack_serialized_job(doc:Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """Fetches the Job Id, Handler Id, and serialized job representation from a Mongo
        serialized Job record.

        Arguments:
            doc {Dict[str, any]} -- A JSON document serialized for the Job-bus database.

        Raises:
            Exception: Raised if the input document is not in the right format, as evidenced by
            missing required keys.

        Returns:
            Tuple[str, str, Dict[str, Any]] -- Tuple of (Job_Id, handler_id, serialized_job)
        """
        required_keys = [JobKeys.SERIALIZATION_LABEL, JobKeys.JOB_ID, JobHandlerKeys.HANDLER_ID]
        for x in required_keys:
            if x in doc: continue
            raise Exception(f"Input document {doc} is missing required key {x}; is it really a serialized Job?")
        job_id = doc[JobKeys.JOB_ID]
        handler = doc[JobHandlerKeys.HANDLER_ID]
        serialized_job = doc[JobKeys.SERIALIZATION_LABEL]
        return (job_id, handler, serialized_job)

    # @staticmethod
    # def _get_serialized_job_data(doc:Dict[str, Any]) -> Tuple[str, Optional[str], Any]:
    #     """Pulls label, container, and code from a serialized job record for deserialization preprocessing.

    #     Arguments:
    #         doc {Dict[str, Any]} -- JSON document representing a serialized Hither Job.

    #     Raises:
    #         Exception: Raised if the input document is not in the right format, as evidenced by
    #         missing required keys.

    #     Returns:
    #         Tuple[str, Optional[str], Any] -- Tuple of (label, container, code) for serialized Job.
    #     """
    #     if not JobKeys._verify_serialized_job(doc):
    #         raise Exception(f"Input document {doc} lacks keys required for a serialized Job.")
    #     label = doc[JobKeys.LABEL]
    #     container = doc[JobKeys.CONTAINER]
    #     code = doc[JobKeys.CODE]
    #     return (label, container, code)

class JobHandlerKeys:
    UTCTIME = 'utctime'
    HANDLER_ID = 'handler_id'

class ComputeResourceKeys:
    COMPUTE_RESOURCE = JobKeys.COMPUTE_RESOURCE # alias, since these should have same value
    KACHERY = 'kachery'
    UTCTIME = 'utctime'
