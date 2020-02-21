class Database:
    def __init__(self, *, mongo_url, database):
        self._mongo_url = mongo_url
        self._database = database
        self._client = None
        self._client_db_url = None
    def collection(self, collection_name):
        import pymongo
        url = self._mongo_url
        if url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(url, retryWrites=False)
            self._client_db_url = url
        return self._client[self._database][collection_name]