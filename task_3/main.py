from pymongo import MongoClient, errors
from multiprocessing import Pool
from helper import FileReader
from datetime import datetime
from models import ShipRow
import config
import os


def ship_row_to_doc(row: ShipRow) -> dict:
    return {
        "mmsi":        row.mmsi,
        "timestamp":   row.timestamp,
        "longitude":   float(row.longitude),
        "latitude":    float(row.latitude),
        "sog":         float(row.sog),
        "draught":     float(row.draught),
        "cargo_type":  row.cargo_type,
        "ship_type":   row.ship_type,
        "nav_status":  row.nav_status,
        "vessel_type": row.vessel_type,
    }



_worker_client: MongoClient | None = None


def _get_collection():
    """Return a cached collection handle for this worker process."""
    global _worker_client
    if _worker_client is None:
        _worker_client = MongoClient(
            config.MONGO_URI,
            # Limit the per-process pool so N workers don't overwhelm Mongo.
            maxPoolSize=2,
            # Surface connection problems quickly rather than hanging.
            serverSelectionTimeoutMS=10_000,
            socketTimeoutMS=30_000,
        )
    return _worker_client[config.MONGO_DB][config.MONGO_COLLECTION]


def insert_chunk_to_mongo(chunk: list[ShipRow]) -> None:
    docs = [ship_row_to_doc(row) for row in chunk]
    if not docs:
        return
    try:
        _get_collection().insert_many(docs, ordered=False)
    except errors.BulkWriteError as bwe:
        n_errors = len(bwe.details.get("writeErrors", []))
        print(f"[PID {os.getpid()}] BulkWriteError: {n_errors} doc(s) failed.")
    except Exception as exc:
        # Catch unexpected errors so the worker reports them instead of dying silently.
        print(f"[PID {os.getpid()}] Unexpected error: {exc!r}")
        raise  # Re-raise so imap_unordered surfaces it in the main process.


def process_file_chunk(chunk: dict) -> tuple[int, int]:
    """Returns (pid, rows_processed) for progress tracking."""
    pid = os.getpid()
    rows_processed = 0

    for file_path, rows in chunk.items():
        insert_chunk_to_mongo(rows)
        rows_processed += len(rows)

    return pid, rows_processed


def run_ais_db_insert() -> None:
    # Construct FileReader AFTER the pool is created to avoid
    # leaking file handles into forked workers.
    task_counts = 0
    total_rows = 0

    with Pool(processes=config.WORKERS) as parser_pool:
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
    run_ais_db_insert()