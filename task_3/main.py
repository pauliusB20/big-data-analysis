from pymongo.collection import Collection
from pymongo import MongoClient, errors
from multiprocessing import Pool
from helper import FileReader
from datetime import datetime
from models import ShipRow
import os, sys
import config

_worker_client: MongoClient = None
_worker_collection: Collection = None

def _get_collection():
    """Return a cached collection handle for this worker process."""
    global _worker_client, _worker_collection
    if not _worker_client and not _worker_collection:
        _worker_client = MongoClient(
            config.MONGO_URI,
            # Limit the per-process pool so N workers don't overwhelm Mongo.
            maxPoolSize=2,
            # Surface connection problems quickly rather than hanging.
            serverSelectionTimeoutMS=10_000,
            socketTimeoutMS=30_000,
            compressors="snappy,zstd,zlib",
        )
        _worker_collection = _worker_client[config.MONGO_DB][config.MONGO_COLLECTION]

    return _worker_collection

def insert_chunk_to_mongo(chunk: list[dict]) -> None:
    # docs = [row._get_doc() for row in chunk]
    if not chunk:
        return
    try:
        collection = _get_collection()
        collection.insert_many(chunk, ordered=False)
    except errors.BulkWriteError as bwe:
        n_errors = len(bwe.details.get("writeErrors", []))
        print(f"[PID {os.getpid()}] BulkWriteError: {n_errors} doc(s) failed.")
    except Exception as exc:
        # Catch unexpected errors so the worker reports them instead of dying silently.
        print(f"[PID {os.getpid()}] Unexpected error: {exc!r}")
        raise  # Re-raise so imap_unordered surfaces it in the main process.


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


if __name__ == "__main__":
    
    _test_mongo_connection()
    
    run_ais_db_insert()