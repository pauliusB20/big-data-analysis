from pymongo.collection import Collection
from pymongo import MongoClient, errors
import config

class MongoHelper:
    
    _worker_client: MongoClient
    
    def __init__(self):        
        self._worker_client = MongoClient(
            config.MONGO_URI,
            # Limit the per-process pool so N workers don't overwhelm Mongo.
            maxPoolSize=config.MONGO_CONNECTIONS,
            # Surface connection problems quickly rather than hanging.
            serverSelectionTimeoutMS=5,
            socketTimeoutMS=30_000,
            compressors="snappy,zstd,zlib",
        ) 
        self.collection = self._worker_client[config.MONGO_DB][config.MONGO_COLLECTION]
       
    def add_to_db(self, chunk: list[object]) -> None:
        self.collection.insert_many(chunk, ordered=False)

    def _is_db_full(self) -> bool:
        if config.MONGO_COLLECTION not in self._worker_client[config.MONGO_DB].list_collection_names():
            raise Exception("Data table not present")
        
        return self.collection.count_documents({}) > 0       
 