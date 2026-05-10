from pymongo.collection import Collection
from pymongo import MongoClient, errors
import config

class MongoDB:
    
    _worker_client: MongoClient
    
    def __init__(self, max_pool_size: int = config.MONGO_CONNECTIONS):        
        self._worker_client = MongoClient(
            config.MONGO_URI,
            # Limit the per-process pool so N workers don't overwhelm Mongo.
            maxPoolSize=max_pool_size,
            # Surface connection problems quickly rather than hanging.
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=30_000,
            compressors="snappy,zstd,zlib",
        ) 
        self.db = self._worker_client[config.MONGO_DB]
        self.collection_filtered = self.db[config.MONGO_COLLECTION_FILTERED]
        self.collection = self.db[config.MONGO_COLLECTION]
       
    def add_to_db(self, chunk: list[object]) -> None:
        self.collection.insert_many(chunk, ordered=False)

    def _is_db_full(self) -> bool:
        if config.MONGO_COLLECTION not in self.db.list_collection_names():
            raise Exception("Data table not present")
        
        return self.collection.count_documents({}) > 0       
 