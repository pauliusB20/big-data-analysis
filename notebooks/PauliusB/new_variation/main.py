from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
import csv
import re

# TODO: 
# Optimize

FILE_CSV = "aisdk-2025-02-28.csv"

# WARNING: This is max value. If you increase, than more memory is used exponentially
CHUNK_SIZE = 1000

MMSI_REGEX = re.compile(r"^[2-7][0-9]{8}$")
WORKERS = 3

def _is_mmsi_valid(mmsi: str) -> bool:
    return bool(MMSI_REGEX.match(mmsi))

def _get_valid_data_chunk(file_path: str, chunk_size: int) -> Iterable[dict]:    
    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        chunk = []
        for row in reader:            
            # Optimization: check during reading mmsi
            if _is_mmsi_valid(row["MMSI"]):
                chunk.append(row)     
                       
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
            
    if chunk:
        yield chunk

def process_ship_chunk(chunk: list[dict]) -> tuple[int, bool]:
    def _get_map_reduced_chunk(chunk: list[dict]):
        # grouping
        mmsi_mapped = {}   
        for row in chunk:
            mmsi_mapped.setdefault(row["MMSI"], []).append(row)
        
        return mmsi_mapped
            
    result = _get_map_reduced_chunk(chunk)
    return result 
    
    # end_time = datetime.now()
    # execution_time = (end_time - start_time).seconds

    # return result, pid, execution_time

if __name__ == "__main__":
    
    reader_cursor = _get_valid_data_chunk(FILE_CSV, CHUNK_SIZE)
    global_vessels = {}
    start_time = datetime.now()
    print("Starting SHIP Data tool")
    
    with Pool(processes=WORKERS) as pool:
        for mapped in pool.imap_unordered(process_ship_chunk, reader_cursor, chunksize=1):
            # merge to global
            for mmsi, records in mapped.items():
                global_vessels.setdefault(mmsi, []).extend(records)
            
        for mmsi in global_vessels:
            global_vessels[mmsi] = sorted(global_vessels[mmsi], key=lambda x: x["# Timestamp"])
        
    print(f"Proccesed ships {len(global_vessels.keys())}")
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            