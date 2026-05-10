from pymongo import MongoClient, errors
from multiprocessing import Pool
import os
import time
import config

def _is_mmsi_valid(mmsi: str) -> bool:
    """Validates real ship MMSI codes"""
    if mmsi is None:
        return False
    return (
        mmsi.isdigit() 
        and 2 <= int(mmsi[0]) <= 7 
        and len(mmsi) == 9
        and len(set(mmsi)) > 1
    )

def init_worker():
    """Gives to each parallel worker their own database connection."""
    global client, db
    client = MongoClient(config.MONGO_URI, maxPoolSize=10, serverSelectionTimeoutMS=5000)
    db = client[config.MONGO_DB]

def process_mmsi_chunk(mmsi_chunk: list[str]) -> tuple[int, int]:
    """
    Worker function. Takes a list of valid MMSIs, streams their valid data, 
    and inserts it into the collection.
    """
    pid = os.getpid()
    rows_inserted = 0
    
    # Filtering
    query = {
    "mmsi": {"$in": mmsi_chunk},
    "vessel_type": {"$in": ["Class A", "Class B"]},
    "latitude": {"$exists": True, "$ne": None,
                 "$gte": 53.0, "$lte": 66.0},
    "longitude": {"$exists": True, "$ne": None,
                  "$gte": 10.0, "$lte": 30.0},
    "sog": {"$exists": True,
            "$gte": 0, "$nin": [102.3, None]},
    "nav_status": {"$exists": True, "$nin": ["", "nan", None, "Undefined"]}
}
    
    # pull only the data that matches the query and the config limits
    cursor = db[config.MONGO_COLLECTION].find(query, {"_id": 0}).batch_size(config.BATCH_SIZE)
    
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= config.BATCH_SIZE:
            try:
                db[config.TARGET_COL].insert_many(batch, ordered=False)

            
                rows_inserted += len(batch)

            except errors.BulkWriteError as e:
                # if row exists, it will throw a BulkWriteError due to the unique index
                # counts the successful inserts before the error and adds to total
                successful_inserts = len(batch) - len(e.details.get("writeErrors", []))
                rows_inserted += successful_inserts

                print(
                    f"[Worker {pid}] Bulk insert warning: "
                    f"{len(e.details.get('writeErrors', []))} duplicate/skipped rows"
                )
            batch = [] #reset the batch
            
    if batch:
        # For catch the last batch which may be smaller than BATCH_SIZE
        try:
            db[config.TARGET_COL].insert_many(batch, ordered=False)

            rows_inserted += len(batch)

        except errors.BulkWriteError as e:
            successful_inserts = len(batch) - len(e.details.get("writeErrors", []))
            rows_inserted += successful_inserts

            print(
                f"[Worker {pid}] Bulk insert warning: "
                f"{len(e.details.get('writeErrors', []))} duplicate/skipped rows"
            )

        except Exception as e:
            print(f"[Worker {pid}] Final batch insert error: {e}")  

    return pid, rows_inserted

def chunk_list(lst, n):
    """Helper generator to break a list into chunks of size n"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_task_3_filtering():
    print("\n--- STARTING TASK 3: PARALLEL NOISE FILTERING ---")
    start_time = time.time()
    
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    
    # cleans old results in case of re-run
    print(f"Cleaning up old results in {config.TARGET_COL}...")
    db[config.TARGET_COL].drop()

    # re-sets the code
    print("Creating index...")

    # main filtering index
    db[config.MONGO_COLLECTION].create_index([("mmsi", 1),
                                              ("timestamp", 1)])

    # helps to coordinate filtering
    db[config.MONGO_COLLECTION].create_index([
        ("latitude", 1),
        ("longitude", 1)
    ])

    print("Making sure target collection has indexes")
    db[config.TARGET_COL].create_index([("mmsi", 1), ("timestamp", 1)],
                                        unique=True) # Unique index to prevent duplicates
    
    print("Analyzing rows to find vessels with >= 100 points")
    # counts the points and drop vessels with < 100 points
    pipeline = [
        {"$group": {"_id": "$mmsi", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": 100}}}
    ]
    
    valid_mmsis = [doc["_id"] for doc in db[config.MONGO_COLLECTION].aggregate(pipeline, allowDiskUse=True)]
    
    # early filter for the unique MMSIs in Python
    valid_mmsis = [mmsi for mmsi in valid_mmsis if _is_mmsi_valid(mmsi)]
    print(f"Found {len(valid_mmsis)} perfectly valid vessels with 100+ points")
    
    mmsi_chunks = list(chunk_list(valid_mmsis, 50))
    total_chunks = len(mmsi_chunks)
    total_inserted = 0
    
    print(f"Starting {config.WORKERS} parallel workers to filter the data")
    with Pool(processes=config.WORKERS, initializer=init_worker) as pool:
        for i, (pid, inserted) in enumerate(pool.imap_unordered(process_mmsi_chunk, mmsi_chunks)):
            total_inserted += inserted
            # logs progress every 10 chunks
            if (i + 1) % 10 == 0 or (i + 1) == total_chunks:
                print(f"[Worker {pid}] Processed chunk {i + 1}/{total_chunks} | Total Clean Rows: {total_inserted:,}")

    elapsed = round((time.time() - start_time) / 60, 2)
    print(f"--- TASK 3 COMPLETE ---")
    print(f"Total Clean Rows Saved: {total_inserted:,}")
    print(f"Execution Time: {elapsed} minutes")

if __name__ == "__main__":
    run_task_3_filtering()