from multiprocessing import Process, Queue
from collections.abc import Iterable
from datetime import datetime
import re
import csv

# TODO: 
# Task 1: 
# Task 2. filter out invalid transponders


# config params
FILE_CSV = "aisdk-2025-02-28.csv"
ITERATIONS = 5
line = 0
schema = []
records = []

CHUNK_SIZE = 1000000

MMSI_PATTERN = r"^[2-7][0-9]{8}$"
RUNNING_PROCCESS_LIMIT = 2

# DEBUG
PROCCESS_COUNT = 15

def _is_mmsi_valid(mmsi: str) -> bool:
    extracted_mmsi_pattern = re.match(
        MMSI_PATTERN, 
        str(mmsi)
    )
    if extracted_mmsi_pattern and extracted_mmsi_pattern.group():
        return True
    return False

def _get_data_chunk(file_path: str, chunk_size: int) -> Iterable[dict]:    
    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        chunk = []
        for row in reader:            
            if len(chunk) > chunk_size:
                yield chunk
                chunk = []
            chunk.append(row)            
    if chunk:
        yield chunk

def process_ship_chunk(pid: int, chunk: list[dict]) -> None:
    start_time = datetime.now()
    valid_mmsi = 0
    for line in chunk:
        mmsi = line["MMSI"]
        valid_mmsi += int(_is_mmsi_valid(mmsi))
            # print(f"Proccess PID={pid} found ship: {mmsi}. Has valid MMSI code!")
    
    end_time = datetime.now()
    execution_time = (end_time - start_time).seconds
    print(f"Proccess PID={pid} finished. Execution time: {execution_time:.2f}\n")
    print(f"Proccess PID={pid}. Valid ships: {valid_mmsi}")
    
# def pretty_print(record: dict) -> None:
#     print("\n-----")
#     for v, k in record.items():
#         value = "NA"
#         if k:
#             value = k
#         print(f"{v} = {value}")


if __name__ == "__main__":
    
    print("Started SHIP analysis tool")
    
    chunk_cursor = _get_data_chunk(FILE_CSV, chunk_size=CHUNK_SIZE)
    running_proccesses = []
    
    # prepearing procceses to run
    pid = 1
    for _ in range(PROCCESS_COUNT): 
        try:   
            if len(running_proccesses) >= RUNNING_PROCCESS_LIMIT:  
                print("Running proccess limit reached")
                for running_proccess in running_proccesses:
                    running_proccess.join()
                running_proccesses = []
                print("DONE - appending next proccess batch to run. Removed the finished ones")
                
            chunk_selected = next(chunk_cursor)
            process_new = Process(
                target=process_ship_chunk, 
                args=(pid, chunk_selected,)
            )
            running_proccesses.append(process_new)
            process_new.start()
            pid += 1
            
        except:
            # When we reach end of the file, we break the loop
            break
    
    
    # process_new.start()
    # process_new.join()
   
   
    
        
    # iteration = 0
    # while iteration < ITERATIONS:
    #     try:            
    #         record = next(record_cursor)
    #         pretty_print(record) 
    #         line += 1
            
    #     except Exception as error:
    #         print(f"Received error during read and write: {str(error)}")
        
    #     iteration += 1
        # pretty_print(record)
    
    
        
# TODO:
# Create dispatches
# Write a custom streaming partitioner that reads the file line-by-line and dispatches chunks to worker processes.

# 1. Task: Compare two ship recorded points (longitude and latitude) between two points before dissapearance and after reaparance
# pick one ship and analyze appearance and re-appearance example mmsi = '219002136'

# IDEA: create works for explore data using workers, generators or DuckDB.
# Goal to understand the properties of data and how generating ideas on how 
# write the algorithm

# TODO:
# Write multiprocess pipeline which works with large data chunks to find 
# anomalies regarding ships
    