from filter import run_task_3_filtering
from time_analysis import calculate_delta_t
from pymongo.collection import Collection
from pymongo import MongoClient, errors
from multiprocessing import Pool
from helper import FileReader
from datetime import datetime
from db import MongoDB
import os, sys
import config


mongo: MongoDB = None

def init_worker() -> None:
    global mongo
    mongo = MongoDB()

def process_file_chunk(chunk: list[dict]) -> tuple[int, int]:
    """Returns (pid, rows_processed) for progress tracking."""
    pid = os.getpid()
    
    # performing data insert
    try:
        mongo.add_to_db(chunk)
        
    except errors.BulkWriteError as bwe:
        n_errors = len(bwe.details.get("writeErrors", []))
        print(f"[PID {os.getpid()}] BulkWriteError: {n_errors} doc(s) failed.")
    
    except Exception as exc:
        # Catch unexpected errors so the worker reports them instead of dying silently.
        print(f"[PID {os.getpid()}] Unexpected error: {exc!r}")
        raise Exception("Something went wrong")

    return pid, len(chunk)

def _test_mongo_connection() -> MongoDB:
    print("INFO: Testing mongodb cluster connection before insert")
    try:
        mongo_helper = MongoDB()
        mongo_helper._worker_client.admin.command("ping")
        print(f"SUCCESS: Connected to {config.MONGO_URI}")
    
    except Exception as error:
        sys.exit(f"ERROR: Received error while testing: {str(error)}")
        
    
def run_ais_db_insert() -> None:
    task_counts = 0
    total_rows = 0

    with Pool(
        processes=config.WORKERS,  
        maxtasksperchild=10,
        initializer=init_worker
    ) as parser_pool:
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

    print(f"[MAIN] Done. Total chunks: {task_counts}, total rows: {total_rows:,}")

def show_execution_statistics(start_time: datetime) -> None:
    end_time = datetime.now()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    total_execution_seconds = (end_time - start_time).seconds
    total_execution_minutes_raw = total_execution_seconds / 60
    seconds_part = total_execution_minutes_raw % 1
    total_exec_sec = round(60 * seconds_part, 2)
    total_exec_min = int(total_execution_minutes_raw)
    
    print(f"Program start time: {start_time_str}")
    print(f"Program end time: {end_time_str}")
    print(
        f"Total execution: {total_exec_min} minutes "
        f"{total_exec_sec} seconds"
    )

if __name__ == "__main__":
    
    print("Starting AIS_DATA analysis tool...")
    start_time = datetime.now()
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Program start time: {start_time_str}")
    
    _test_mongo_connection()
    
    mongo = MongoDB()
    if not mongo._is_db_full():
        print("Loading data from dataset file and inserting into mongodb...")
        run_ais_db_insert()

    run_task_3_filtering()
    calculate_delta_t()    
    show_execution_statistics(start_time)
    
    
    print("DONE")
