from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict, namedtuple
from datetime import datetime
from haversine import haversine, Unit
# from geopy.distance import geodesic
from typing import Any
from tqdm import tqdm
import numpy as np
import json
import csv
import os

# TODO: 
# Spotted anomaly dark 118. Now check with loitering (B). Later (C), (D)
# Export all data to result.csv file
# Do profiling on data

# Profile
# https://github.com/bloomberg/memray

FILE_CSV = "aisdk-2025-02-28.csv"

# WARNING: Max chunk size = 1000
CHUNK_SIZE = 1000

# After which count, display log message
FINISHED_TASKS = 1000

FINISHED_TEMP_TASKS = 10

# Max tasks workers can proccess
TASKS_PER_WORKER = 26

# NOTE: check how many cores your CPU has. If 8 cores, max 8 proccesses
# WARNING: tested Max 7 workers for 8 cores and no memory overflow
WORKERS = 6

# For anomaly A, 4 hours in seconds
TIME_THRESHOLD = 14400

# Quick Classes
ShipRow = namedtuple("ShipRow", 
    [
        "mmsi", 
        "timestamp",
        "latitude",
        "longitude"
    ])

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
        longitude_idx = headline.index("Longitude")
        latitude_idx = headline.index("Latitude")
        
        chunk = set()
        for row in reader:  
            
            # validating long, lat            
                      
            if _is_mmsi_valid(row[mmsi_idx]) and row[timestamp_idx]:
                latitude = row[latitude_idx]
                longitude = row[longitude_idx]
                # timestamp = datetime.strptime(row[timestamp_idx], "%d/%m/%Y %H:%M:%S")
                
                chunk.add(
                        ShipRow(
                            row[mmsi_idx], 
                            row[timestamp_idx],
                            np.float64(latitude),
                            np.float64(longitude)
                        )
                    ) 
            if len(chunk) >= chunk_size:
                yield list(chunk)
                chunk = set()
            
    if chunk:
        yield chunk
        
# reading chunk ship mmsi
def _read_ship_records(file_path: str, chunk_size: int) -> Iterable[list]:
    records = []
    with open(file_path, "r", encoding="utf-8") as file_json:
        for row_dict in file_json:
            ship_data = json.loads(row_dict)
            for _, rows in ship_data.items():
                records.extend(rows)
    
            yield records
            records = []
            

def _get_mapped_chunk(chunk: list[Any]):
    # grouping
    ships = defaultdict(list)
     
    for row in chunk:
        ships[row.mmsi].append(row)    
    
    # for mmsi in ships:
    #     ships[mmsi].sort(key=lambda x: datetime.strptime(x.timestamp, "%d/%m/%Y %H:%M:%S"))    
    
    return ships

def _get_time_diff(time1: datetime, time2: datetime) -> int:
    return (time2 - time1).total_seconds()

def _get_coord(latitude: np.float64, longitude: np.float64) -> tuple|None:
    if latitude < -90 or latitude > 90:
        return None
    
    if longitude < -180 or longitude > 180:
        return None
    
    return (latitude, longitude)

# TODO: compare rows between workers. Do data sharing and check
# def _get_anomaly_a(chunk: dict) -> int:
#     anomalies = 0
#     for (_, rows) in chunk.items():
#         for i in range(1, len(rows)):
#             ship_a, ship_b = rows[i-1], rows[i]
#             point_a = _get_coord(ship_a)
#             point_b = _get_coord(ship_b)
            
#             if not point_a or not point_b:
#                 continue
            
#             point_distance = haversine(point_a, point_b, Unit.KILOMETERS)
#             anomalies += int(
#                 # point_distance >= 1 and
#                 _get_time_diff(ship_a.timestamp, ship_b.timestamp) >= TIME_THRESHOLD
#             )
#     return anomalies
            
                

def process_ship_chunk(chunk: list[dict]) -> tuple[int, dict]:            
    result = _get_mapped_chunk(chunk)
    pid = os.getpid()
    return pid, result

def process_ship_records(ship_records: list[list]) -> tuple[int, int]:
    # TODO:
    # add anomaly calculation here
    pid = os.getpid()    
    ship_records.sort(key=lambda x: datetime.strptime(x[1], "%d/%m/%Y %H:%M:%S"))
    anomalies = 0
    
    for i in range(1, len(ship_records)):
        ship_record_a, ship_record_b = ship_records[i-1], ship_records[i]
        
        # Calculate time difference
        ship_a_time = datetime.strptime(
            ship_record_a[1], 
            "%d/%m/%Y %H:%M:%S"
        )
        ship_b_time = datetime.strptime(
            ship_record_b[1], 
            "%d/%m/%Y %H:%M:%S"
        )        
        time_diff = _get_time_diff(ship_a_time, ship_b_time)
        
        # Calculate heaversine distance
        ship_a_point = _get_coord(
            np.float64(ship_record_a[2]),
            np.float64(ship_record_a[3])
        )
        
        ship_b_point = _get_coord(
            np.float64(ship_record_b[2]),
            np.float64(ship_record_b[3])
        )
        if ship_a_point and ship_b_point:
            distance = haversine(ship_a_point, ship_b_point, Unit.KILOMETERS)
            
            anomalies += int(time_diff > TIME_THRESHOLD and distance > 1)
    
    return pid, anomalies
    
    # end_time = datetime.now()
    # execution_time = (end_time - start_time).seconds

    # return result, pid, execution_time

if __name__ == "__main__":
    
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    global_states = defaultdict(list)
    start_time = datetime.now()
    print("Starting SHIP Data tool")
    
    task_counts = defaultdict(list)
    anomalies_total = 0
    
    # TODO: write to parquet file and load them for distance measurement
    with Pool(processes=WORKERS) as pool:
        for pid, result in tqdm(
            pool.imap(process_ship_chunk, reader_cursor, chunksize=TASKS_PER_WORKER),
            desc="Processing file chunks"
        ):
            task_counts[pid] = task_counts.get(pid, 0) + 1
            if task_counts[pid] % FINISHED_TASKS == 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Proccess = {pid} Finished proccesing {FINISHED_TASKS} at {timestamp}")                
            
            # anomalies_total += anomalies
            # merge to global
            for mmsi, timestamps in result.items():
                global_states[mmsi].extend(timestamps)
    
    task_counts = defaultdict(list)
    # print("Sorting results")
    # for mmsi, timestamps in result.items():
    #     global_states[mmsi].sort(key=lambda x: x.timestamp)
            
    # print(f"Spotted anomaly ships {anomalies_total}")
    
    ship_statistics = {mmsi : len(global_states[mmsi]) for mmsi in global_states}
    
    print(f"Ship statistics: {ship_statistics}")
    print(f"Ships {len(ship_statistics)}")
    
    # export to JSON file
    print("Exporting to temp jsonl file")
    with open("temp.jsonl", "w", encoding="utf-8") as file_json:
        for key, value in global_states.items():
            file_json.write(json.dumps({key: value}) + "\n")
            
    print("Reading from temp file ship data")
    
    temp_reader_cusor = _read_ship_records(file_path="temp.jsonl", chunk_size=CHUNK_SIZE)
    anomalies_total = 0
    with Pool(processes=WORKERS) as pool:
        for pid, anomalies in tqdm(
            pool.imap(process_ship_records, temp_reader_cusor, chunksize=TASKS_PER_WORKER),
            desc="Processing ship chunks"
        ):
            task_counts[pid] = task_counts.get(pid, 0) + 1
            
            # TODO: need seperate divider for temp read like FINISHED_TASKS_TEMP or smth
            if task_counts[pid] % 10 == 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Proccess = {pid} Finished temp proccesing {FINISHED_TEMP_TASKS} at {timestamp}")                
            
            anomalies_total += anomalies
    
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    
    print(f"Spotted anomaly count: {anomalies_total}")    
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            