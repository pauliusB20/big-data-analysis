from pymongo import MongoClient
from multiprocessing import Pool
from helper import FileReader
from datetime import datetime
from models import ShipRow
import config
import os


def process_file_chunk(chunk: list[ShipRow]) -> tuple:
    pid = os.getpid()
    
    # TODO
    # Add insert to db
    # EXAMPLE
    # client = MongoClient("mongodb://localhost:30000")
    
    # db = client["AIS_04_18"]
    # collection = db["data"]
    
    # example:
    # docs = []
    # for i in range(10000):
    #     docs.append({
    #         "name": f"user_{i}",
    #         "timestamp": datetime.utcnow(),  # important: shard key field
    #         "value": i
    #     })

    # # Insert many documents
    # collection.insert_many(docs)
    
    
    return pid

def run_ais_db_insert() -> None:
    
    file_reader = FileReader(
        file_path=config.CSV_FILE,
        chunk_size=config.CHUNK_SIZE
    )
    task_counts = 0
    # Reading in parallel
    with Pool(processes=config.WORKERS) as parser_pool:
        for pid in parser_pool.imap_unordered(
            func = process_file_chunk,
            iterable = file_reader._read_csv,
            chunksize=config.TAKS_PER_WORKER
        ):
            task_counts += 1 
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if task_counts % config.LOG_EVERY == 0:
                print(f"Worker ID = {pid} finished at {timestamp}")

if __name__ == "__main__":
    
    run_ais_db_insert()
    
    
    # TODO:    
    # Add filtering mongo db and delta t calculations