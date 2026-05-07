from pymongo.collection import Collection
from pymongo import MongoClient, errors
from multiprocessing import Pool
from helper import FileReader
from datetime import datetime
import os, sys
import config

_worker_client: MongoClient = None
_worker_collection: Collection = None

def _get_mongo_collection():
    """Return a cached collection handle for this worker process."""
    global _worker_client, _worker_collection
    if not _worker_client and not _worker_collection:
        _worker_client = MongoClient(
            config.MONGO_URI,
            # Limit the per-process pool so N workers don't overwhelm Mongo.
            maxPoolSize=config.MONGO_CONNECTIONS,
            # Surface connection problems quickly rather than hanging.
            serverSelectionTimeoutMS=10_000,
            socketTimeoutMS=30_000,
            compressors="snappy,zstd,zlib",
        )
        _worker_collection = _worker_client[config.MONGO_DB][config.MONGO_COLLECTION]

    return _worker_collection

def insert_chunk_to_mongo(chunk: list[dict]) -> None:
    if not chunk:
        return
    try:
        collection = _get_mongo_collection()
        collection.insert_many(chunk, ordered=False)
    except errors.BulkWriteError as bwe:
        n_errors = len(bwe.details.get("writeErrors", []))
        print(f"[PID {os.getpid()}] BulkWriteError: {n_errors} doc(s) failed.")
    except Exception as exc:
        # Catch unexpected errors so the worker reports them instead of dying silently.
        print(f"[PID {os.getpid()}] Unexpected error: {exc!r}")
        raise Exception("Something went wrong")  


def process_file_chunk(chunk: list[dict]) -> tuple[int, int]:
    """Returns (pid, rows_processed) for progress tracking."""
    pid = os.getpid()
    insert_chunk_to_mongo(chunk)

    return pid, len(chunk)

def _test_mongo_connection() -> None:
    print("INFO: Testing mongodb cluster connection before insert")
    try:
        MongoClient(
            config.MONGO_URI,
        )
        print(f"SUCCESS: Conneced to {config.MONGO_URI}")
        
    except Exception as error:
        sys.exit(f"ERROR: Received error while testing: {str(error)}")
        

def _is_db_full() -> bool:
    return _worker_collection.count_documents({}) > 0       
    
def run_ais_db_insert() -> None:
    task_counts = 0
    total_rows = 0

    with Pool(processes=config.WORKERS,  maxtasksperchild=10) as parser_pool:
        file_reader = FileReader(
            file_path=config.CSV_FILE,
            chunk_size=config.CHUNK_SIZE,
        )

        try:
            for pid, rows_processed in parser_pool.imap_unordered(
                func=process_file_chunk,
                iterable=file_reader._read_csv(),
                chunksize=config.TAKS_PER_WORKER,               
            ):
                task_counts += 1
                total_rows += rows_processed

                if task_counts % config.LOG_EVERY == 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(
                        f"[{timestamp}] Worker {pid} | "
                        f"chunks done: {task_counts} | "
                        f"rows inserted: {total_rows:,}"
                    )

        except Exception as exc:
            # Any worker exception surfaces here — log it before the pool tears down.
            print(f"[MAIN] Pipeline failed: {exc!r}")
            raise

    print(f"[MAIN] Done. Total chunks: {task_counts}, total rows: {total_rows:,}")

def _show_execution_statistics(start_time: datetime, end_time: datetime) -> None:
    total_execution_seconds = (end_time - start_time).seconds
    total_execution_minutes_raw = total_execution_seconds / 60
    seconds_part = total_execution_minutes_raw % 1
    total_exec_sec = 60 * seconds_part
    total_exec_min = int(total_execution_minutes_raw)
    
    print(
        f"Total execution: {total_exec_min} minutes"
        f"{total_exec_sec} seconds"
    )

if __name__ == "__main__":
    
    print("Starting AIS_DATA analysis tool...")
    start_time = datetime.now()
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Program start time: {start_time_str}")
    
    _test_mongo_connection()
    
    if not _is_db_full():
        print("Loading data from dataset file and inserting into mongodb...")
        run_ais_db_insert()
        
    
    # Data analysis part for analyzing records
    # code for task 3.3 and task 3.4 goes here
    
    
    # ------------------
    
    end_time = datetime.now()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Program start time: {start_time_str}")
    print(f"Program end time: {end_time_str}")
    
    _show_execution_statistics(start_time, end_time)
    
    print("DONE")