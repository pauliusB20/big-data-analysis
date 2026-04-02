from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict, namedtuple
from tqdm import tqdm
import csv
import os


FILE_CSV = "aisdk-2025-02-28.csv"

# WARNING: Max chunk size = 1000
CHUNK_SIZE = 1000

# After which count, display log message
FINISHED_TASKS = 1000

# Max tasks workers can proccess
TASKS_PER_WORKER = 25

# NOTE: check how many cores your CPU has. If 8 cores, max 8 proccesses
# WARNING: tested Max 7 workers for 8 cores and no memory overflow
WORKERS = 6

# Quick Classes
ShipRow = namedtuple("ShipRow", ["mmsi", "timestamp"])

def _is_mmsi_valid(mmsi: str) -> bool:
    return (
        mmsi.isdigit() 
        and 2 <= int(mmsi[0]) <= 7 
        and len(mmsi) == 9
        and len(set(mmsi)) > 1
    )


def _read_chunks(file_path: str, chunk_size: int) -> Iterable[dict]:    
    with open(file_path) as csv_file:
        reader = csv.reader(csv_file)
        headline = next(reader)
        
        # columns to select
        mmsi_idx = headline.index("MMSI")
        timestamp_idx = headline.index("# Timestamp")
        
        chunk = []
        for row in reader:            
            if _is_mmsi_valid(row[mmsi_idx]):
                chunk.append(ShipRow(row[mmsi_idx], row[timestamp_idx]))  
                       
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
            
    if chunk:
        yield chunk

def _get_mapped_chunk(chunk: list[dict]):
    # grouping
    ships = defaultdict(list)
     
    for row in chunk:
        ships[row.mmsi].append(row.timestamp)    
    
    for mmsi in ships:
        ships[mmsi].sort()    
    
    return ships

def process_ship_chunk(chunk: list[dict]) -> tuple[int, dict]:            
    result = _get_mapped_chunk(chunk)
    pid = os.getpid()
    
    # TODO:
    # add code for anomaly finding
    
    return pid, result 
    
    # end_time = datetime.now()
    # execution_time = (end_time - start_time).seconds

    # return result, pid, execution_time

if __name__ == "__main__":
    
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    global_states = defaultdict(list)
    start_time = datetime.now()
    print("Starting SHIP Data tool")
    
    task_counts = defaultdict(list)
    
    with Pool(processes=WORKERS) as pool:
        for pid, mapped in tqdm(
            pool.imap_unordered(process_ship_chunk, reader_cursor, chunksize=TASKS_PER_WORKER),
            desc="Processing chunk"
        ):
            task_counts[pid] = task_counts.get(pid, 0) + 1
            if task_counts[pid] % FINISHED_TASKS == 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Proccess = {pid} Finished proccesing {FINISHED_TASKS} at {timestamp}")                
            
            # merge to global
            for mmsi, timestamps in mapped.items():
                global_states[mmsi].extend(timestamps)
            
    print(f"Proccesed ships {len(global_states)}")
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            