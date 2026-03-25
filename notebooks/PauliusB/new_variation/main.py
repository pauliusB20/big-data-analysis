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
# FILE_CSV = "aisdk-2025-03-01.csv"

# WARNING: Max chunk size = 1000
CHUNK_SIZE = 1000

SUB_CHUNK_SIZE = 100

# After which count, display log message
FINISHED_TASKS = 1000

FINISHED_TEMP_TASKS = 100

# Max tasks workers can proccess
TASKS_PER_WORKER = 26

# NOTE: check how many cores your CPU has. If 8 cores, max 8 proccesses
# WARNING: tested Max 7 workers for 8 cores and no memory overflow
WORKERS = 6

# For anomaly A, 4 hours in seconds
TIME_THRESHOLD = 14400

TEMP_CSV_FILE = "temp.csv"
# TEMP_CSV_FILE = "temp"

CALCULATE_A = False
CALCULATE_B = True
CALCULATE_B_V2 = False
EXPORT_FILE = True
EXPORT_FILE_MULTI = False

TEMP_FILE_AMOUNT = 3


# Quick Classes
ShipRow = namedtuple("ShipRow", 
    [
        "mmsi", 
        "timestamp",
        "latitude",
        "longitude",
        "sog"
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
        sog_idx = headline.index("SOG")
        
        chunk = set()
        for row in reader:  
            
            # validating long, lat            
                      
            if _is_mmsi_valid(row[mmsi_idx]) and row[timestamp_idx]:
                latitude = row[latitude_idx]
                longitude = row[longitude_idx]
                sog = row[sog_idx]
                if not sog:
                    continue
                # timestamp = datetime.strptime(row[timestamp_idx], "%d/%m/%Y %H:%M:%S")
                
                chunk.add(
                        ShipRow(
                            row[mmsi_idx], 
                            row[timestamp_idx],
                            np.float64(latitude),
                            np.float64(longitude),
                            np.float64(sog)
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

def _read_ship_data(chunks: Iterable[int]) -> Iterable[list]:
    chunk_size = next(chunks)   
    chunk = []
    with open(TEMP_CSV_FILE, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []            
            chunk.append(row)
    
    if chunk:
        yield chunk           
                

def process_ship_chunk(chunk: list[dict]) -> tuple[int, dict]:            
    result = _get_mapped_chunk(chunk)
    pid = os.getpid()
    return pid, result

def _read_csv_temp_file_chunk(chunk_size: int) -> Iterable[list]:
    chunk = []
    with open(TEMP_CSV_FILE, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
            
            chunk.append(row)
    
    if chunk:
        yield chunk
                

def process_ship_chunk_anomalies(chunk: list[list]) -> dict:    
    anomaly_count = 0
    
    # anomaly counting
    for i in range(1, len(chunk)):
        (
            _,
            ship_time_previous,
            ship_latitude_previous,
            ship_longitude_previous,
            _
        ) = chunk[i - 1]
        
        point_previous = _get_coord(
            np.float64(ship_latitude_previous), 
            np.float64(ship_longitude_previous)
        )
        if not point_previous:
            continue
        
        (
            _,
            ship_time_current,
            ship_latitude_current,
            ship_longitude_current,
            _
        ) = chunk[i]
        
        point_current = _get_coord(
            np.float64(ship_latitude_current), 
            np.float64(ship_longitude_current)
        )
        if not point_current:
            continue
        
        time_diff = _get_time_diff(
            datetime.strptime(ship_time_previous, "%d/%m/%Y %H:%M:%S"), 
            datetime.strptime(ship_time_current, "%d/%m/%Y %H:%M:%S")
        )
        
        anomaly_count += (
            haversine(
                point_previous, 
                point_current, 
                Unit.KILOMETERS
            ) > 1 and 
            time_diff > TIME_THRESHOLD
        )
    
    pid = os.getpid()
    
    return pid, anomaly_count

def process_ship_chunk_loitering(chunk_a: list[list]) -> dict:    
    loitering_count = 0
    
    # For loitering 
    for i in range(1, len(chunk_a)):
        timestamp_previous_a = chunk_a[i - 1][1]
        (
            mmsi_a, 
            timestamp_a, 
            latitude_a, 
            longitude_a, 
            sog_a
        ) = chunk_a[i]
        point_a = _get_coord(
            np.float64(latitude_a), 
            np.float64(longitude_a)
        )
        if not point_a:
            continue
        
        sog_a = np.float64(sog_a) 
        # ship a
        ship_a_previous_time = datetime.strptime(timestamp_previous_a, "%d/%m/%Y %H:%M:%S") 
        ship_a_current_time = datetime.strptime(timestamp_a, "%d/%m/%Y %H:%M:%S")
        ship_a_time_diff = _get_time_diff(ship_a_previous_time, ship_a_current_time)   
        if ship_a_time_diff < 7200:
            continue
        
        # comparing with other ships
        for chunk_b in _read_csv_temp_file_chunk(SUB_CHUNK_SIZE):
            for j in range(1, len(chunk_b)):
                (
                    mmsi_b_previous, 
                    timestamp_b_previous, 
                    _, _, _
                ) = chunk_b[j - 1]                
                
                (
                    mmsi_b, 
                    timestamp_b, 
                    latitude_b, 
                    longitude_b, 
                    sog_b
                ) = chunk_b[j]
                
                if mmsi_b_previous != mmsi_b:
                   continue 
               
                if mmsi_a == mmsi_b:
                    continue
                
                timestamp_b_previous = datetime.strptime(timestamp_b_previous, "%d/%m/%Y %H:%M:%S") 
                timestamp_b = datetime.strptime(timestamp_b, "%d/%m/%Y %H:%M:%S")
                ship_b_time_diff = _get_time_diff(
                    timestamp_b_previous, 
                    timestamp_b
                )   
                if ship_b_time_diff < 7200:
                    continue
                
                point_b = _get_coord(
                    np.float64(latitude_b), 
                    np.float64(longitude_b)
                )
                if not point_b:
                    continue
                
                ship_near_distance = haversine(point_a, point_b, Unit.MILES)
                loitering_count += int(
                    ship_near_distance <= 500 and 
                    np.float64(sog_a) < 1.0 and
                    np.float64(sog_b) < 1.0
                )
                
                
                # if (
                #     previous_check and 
                #     previous_check[0] != mmsi_a and 
                #     previous_check[0] == mmsi_b and 
                #     ship_distance <= 500 and 
                #     np.float64(sog_a) < 1.0 and
                #     np.float64(sog_b) < 1.0 and 
                # ): 
                    
                        
                #     # ship b
                #     previous_time = datetime.strptime(previous_check[1], "%d/%m/%Y %H:%M:%S") 
                #     current_time = datetime.strptime(timestamp_b, "%d/%m/%Y %H:%M:%S")
                #     time_diff_compare = _get_time_diff(previous_time, current_time)               
                #     loitering_count += int(time_diff_compare >= 7200)               
                    
                

    
    pid = os.getpid()
    
    return pid, loitering_count


if __name__ == "__main__":    
    anomaly_total = 0
    loitering_total = 0
    
    print("Starting SHIP Data tool")
    
    print(f"1. Reading data for processing using path={FILE_CSV}")
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    ships_mapped = defaultdict(list)
    start_time = datetime.now()   
    
    task_counts = defaultdict(list)
    task_ship_counts = defaultdict(list)
    
    # TODO: write to parquet file and load them for distance measurement
    with Pool(processes=WORKERS) as pool:
        for pid, ships in tqdm(
            pool.imap(process_ship_chunk, reader_cursor, chunksize=TASKS_PER_WORKER),
            desc="Processing file chunks"
        ):
            task_counts[pid] = task_counts.get(pid, 0) + 1
            if task_counts[pid] % FINISHED_TASKS == 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Proccess = {pid} Finished proccesing {FINISHED_TASKS} at {timestamp}")                
            
            # merge to global
            for mmsi, ship_records in ships.items():
                ships_mapped[mmsi].extend(ship_records)
    
    print("2. Sorting temp cached results")
    for mmsi in ships_mapped:
        ships_mapped[mmsi].sort(key=lambda x: x.timestamp)    
    
    if EXPORT_FILE:
        print("3. Exporting to temp csv file")
        with open(TEMP_CSV_FILE, "w") as file:
            writer = csv.writer(file)
            for mmsi, ship_records in ships_mapped.items():
                writer.writerows(ship_records)
    
    
    if EXPORT_FILE_MULTI:
        file_paths = []
        
        for idx in range(TEMP_FILE_AMOUNT):
            written_records = 0
            temp_file_new = TEMP_CSV_FILE + str(idx) + ".csv"
            with open(temp_file_new, "w") as file:
                writer = csv.writer(file)
                for mmsi, ship_records in ships_mapped.items():
                    writer.writerows(ship_records)
                    written_records += len(ship_records)        
            file_paths.append(temp_file_new)        
        
    # Prepearing chunk size for reading
    ship_chunks = []
    for mmsi, ship_records in ships_mapped.items():
        ship_chunks.append(len(ship_records))
    
    if CALCULATE_A:
        print("4. Reading data from temp file and calculating anomalies")
        chunks = iter(ship_chunks)
        ship_reader_cursor = _read_ship_data(chunks)    
        
        with Pool(processes=WORKERS) as pool:
            for pid, anomalies in tqdm(
                pool.imap(process_ship_chunk_anomalies, ship_reader_cursor, chunksize=10),
                desc="Processing file chunks ship anomalies"
            ):
                task_ship_counts[pid] = task_ship_counts.get(pid, 0) + 1
                if task_ship_counts[pid] % FINISHED_TEMP_TASKS == 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Proccess = {pid} Finished anomalies proccesing {FINISHED_TEMP_TASKS} at {timestamp}")                
                
                anomaly_total += anomalies   
    
    if CALCULATE_B:
        print("5. Reading data from temp file and calculating loitering")
        chunks = iter(ship_chunks)           
        ship_reader_cursor = _read_ship_data(chunks) 
        with Pool(processes=WORKERS) as pool:
            for pid, loitering in tqdm(
                pool.imap(process_ship_chunk_loitering, ship_reader_cursor, chunksize=3),
                desc="Processing file chunks ship loitering"
            ):
                task_ship_counts[pid] = task_ship_counts.get(pid, 0) + 1
                if task_ship_counts[pid] % FINISHED_TEMP_TASKS == 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Proccess = {pid} Finished loitering proccesing {FINISHED_TEMP_TASKS} at {timestamp}")                
                
                loitering_total += loitering  
                
    if CALCULATE_B_V2:
        pass
    
    print("Results")
    print(f"A) anomaly_total: {anomaly_total}")
    print(f"B) loitering: {loitering_total}")
    
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    
    # print(f"Spotted anomaly count: {anomalies_total}")    
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            