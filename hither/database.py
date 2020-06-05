#from pymongo import MongoClient, collection, cursor (NOTE: This will hopefully be usable once TypeAlias
# is part of the language, see PEP-613. TODO)

import time
from typing import Optional, Any, Dict, List

from ._enums import JobKeys

class Database:
    CachedJobResults = 'cached_job_results'

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

    def _cached_job_results(self) -> Any:
        return self._collection(Database.CachedJobResults)

    def _make_update(self, update:Dict[str, Any]) -> Dict[str, Any]:
        return { '$set': update }

  ##### Job cache interface ###############

    def _fetch_cached_job_result(self, hash:str) -> Optional[Dict[str, Any]]:
        job = self._cached_job_results().find_one({ JobKeys.JOB_HASH: hash })
        if job is None: return None
        if JobKeys.STATUS not in job: return None # TODO: throw error? If this key is missing it's probably not a Job
        return job

    # TODO: Job is, of course, obviously a Job, but typing it right now would lead to circular imports
    def _cache_job_result(self, job_hash: str, job:Any) -> None:
        query = { JobKeys.JOB_HASH: job_hash }
        update_query = self._make_update(job._as_cached_result())
        self._cached_job_results().update_one(query, update_query, upsert=True)