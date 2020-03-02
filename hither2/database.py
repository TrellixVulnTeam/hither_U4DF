from ._load_config import _load_preset_config_from_github

class Database:
    def __init__(self, *, mongo_url, database):
        self._mongo_url = mongo_url
        self._database = database
        self._client = None
        self._client_db_url = None
    @staticmethod
    def preset(name):
        config = _load_preset_config_from_github(url='https://raw.githubusercontent.com/laboratorybox/hither2/config/config/2020a.json', name=name)
        mongo_url = config['mongo_url']
        if 'password' in config:
            mongo_url = mongo_url.replace('${password}', config['password'])
        db = Database(mongo_url=mongo_url, database=config['database'])
        return db
    def collection(self, collection_name):
        import pymongo
        url = self._mongo_url
        if url != self._client_db_url:
            if self._client is not None:
                self._client.close()
            self._client = pymongo.MongoClient(url, retryWrites=False)
            self._client_db_url = url
        return self._client[self._database][collection_name]