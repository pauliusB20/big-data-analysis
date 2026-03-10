from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict
from tqdm import tqdm
import csv
import os

# TODO: 
# Optimize

FILE_CSV = "aisdk-2025-02-28.csv"

# WARNING: This is max value. If you increase, than more memory is used exponentially
CHUNK_SIZE = 1000
WORKERS = 3


def _is_mmsi_valid(mmsi: str) -> bool:
    return (
        mmsi.isdigit() 
        and 2 <= int(mmsi[0]) <= 7 
        and len(mmsi) == 9
        and len(set(mmsi)) > 1
    )

def _read_chunks(file_path: str, chunk_size: int) -> Iterable[dict]:    
    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        chunk = []
        for row in reader:            
            if _is_mmsi_valid(row["MMSI"]):
                chunk.append({"mmsi" : row["MMSI"], "Timestamp" : row["# Timestamp"]})     
                       
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
            
    if chunk:
        yield chunk

def _get_mapped_chunk(chunk: list[dict]):
    # grouping
    ships = defaultdict(list)
     
    for row in chunk:
        ships[row["mmsi"]].append(row["Timestamp"])    
    
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
    
    with Pool(processes=WORKERS) as pool:
        for pid, mapped in tqdm(
            pool.imap_unordered(process_ship_chunk, reader_cursor, chunksize=1),
            desc="Processing chunk"
        ):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Task = {pid} Finished proccesing at {timestamp}")
            
            # merge to global
            for mmsi, timestamps in mapped.items():
                global_states[mmsi].extend(timestamps)
            
    for mmsi in global_states:
        global_states[mmsi].sort()
        
    print(f"Proccesed ships {len(global_states)}")
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            