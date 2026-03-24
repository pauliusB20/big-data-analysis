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

TEMP_CSV_FILE = "temp.csv"

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

def _read_csv_temp_file_bulk() -> Iterable[list]:
    with open(TEMP_CSV_FILE, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            yield row

def process_ship_chunk_stats(chunk: list[list]) -> dict:    
    close_ships = 0
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
            ) > 0.5 and 
            time_diff > TIME_THRESHOLD
        )
    
    # For loitering 
    for j in range(1, len(chunk)):
        (
            mmsi_previous_a, 
            timestamp_previous_a, 
            latitude_previous_a, 
            longitude_previous_a, 
            sog_previous_a
        ) = chunk[i - 1]
        (
            mmsi_a, 
            timestamp_a, 
            latitude_a, 
            longitude_a, 
            sog_a
        ) = chunk[i]
        point_previous_a = _get_coord(
            np.float64(latitude_previous_a), 
            np.float64(longitude_previous_a)
        )
        if not point_previous_a:
            continue
        
        point_a = _get_coord(
            np.float64(latitude_a), 
            np.float64(longitude_a)
        )
        if not point_a:
            continue
        
        sog_a = np.float64(sog_a)     
        previous_check = None
        temp_reader = _read_csv_temp_file_bulk()
        checked_mmsi = []
        for row in temp_reader:
            (mmsi_b, timestamp_b, latitude_b, longitude_b, sog_b) = row
            point_b = _get_coord(np.float64(latitude_b), np.float64(longitude_b))
            if not point_b:
                continue
            
            ship_distance = haversine(point_a, point_b, Unit.MILES)
            if (
                previous_check and 
                previous_check[0] != mmsi_a and 
                previous_check[0] == mmsi_b and 
                ship_distance <= 500 and 
                np.float64(sog_a) < 1.0 and
                np.float64(sog_b) < 1.0 and 
                mmsi_b not in checked_mmsi
            ): 
               # ship a
               ship_a_previous_time = datetime.strptime(timestamp_previous_a, "%d/%m/%Y %H:%M:%S") 
               ship_a_current_time = datetime.strptime(timestamp_a, "%d/%m/%Y %H:%M:%S")
               ship_a_time_diff = _get_time_diff(ship_a_previous_time, ship_a_current_time)   
                
               # ship b
               previous_time = datetime.strptime(previous_check[1], "%d/%m/%Y %H:%M:%S") 
               current_time = datetime.strptime(timestamp_b, "%d/%m/%Y %H:%M:%S")
               time_diff_compare = _get_time_diff(previous_time, current_time)
               
               close_ships += int(ship_a_time_diff > 7200 and time_diff_compare > 7200)
               
               
               checked_mmsi.append(mmsi_b)
            
            if mmsi_a != mmsi_b:
                previous_check = row
            
            # sog_b = np.float64(sog_b)
            # ship_loitered = []
            
            # # if len(ship_loitered) >= 2:
            # #     start_time, end_time = ship_loitered
            # #     start_time = datetime.strptime(start_time, "%d/%m/%Y %H:%M:%S")
            # #     end_time = datetime.strptime(end_time, "%d/%m/%Y %H:%M:%S")
            # #     time_diff = _get_time_diff(start_time, end_time)
            # #     if time_diff >= 7200:
            # #         close_ships += 1
            # #     ship_loitered = []
                
            # if mmsi_a != mmsi_b and mmsi_b not in checked_mmsi:
        #         ship_distance = haversine(point_a, point_b, Unit.MILES)
                
        #         # TODO 500 to variable
        #         appropriate_distance_sog = (
        #             ship_distance <= 500 and 
        #             sog_b < 1.0 and 
        #             sog_a < 1.0 
        #         )
        # # if appropriate_distance_sog and len(ship_loitered) <= 2:
        #         close_ships += int(appropriate_distance_sog)
        #         checked_mmsi.append(mmsi_b)
                    # ship_loitered.append(timestamp_a)  
    
    pid = os.getpid()
    
    result = {
        "anomalies" : anomaly_count,
        "close_ships" : close_ships
    }
    
    return pid, result

# def process_ship_records(ship_records: list[list]) -> tuple[int, int]:
#     # TODO:
#     # add anomaly calculation here
#     pid = os.getpid()    
#     ship_records.sort(key=lambda x: datetime.strptime(x[1], "%d/%m/%Y %H:%M:%S"))
#     anomalies = 0
    
#     for i in range(1, len(ship_records)):
#         ship_record_a, ship_record_b = ship_records[i-1], ship_records[i]
        
#         # Calculate time difference
#         ship_a_time = datetime.strptime(
#             ship_record_a[1], 
#             "%d/%m/%Y %H:%M:%S"
#         )
#         ship_b_time = datetime.strptime(
#             ship_record_b[1], 
#             "%d/%m/%Y %H:%M:%S"
#         )        
#         time_diff = _get_time_diff(ship_a_time, ship_b_time)
        
#         # Calculate heaversine distance
#         ship_a_point = _get_coord(
#             np.float64(ship_record_a[2]),
#             np.float64(ship_record_a[3])
#         )
        
#         ship_b_point = _get_coord(
#             np.float64(ship_record_b[2]),
#             np.float64(ship_record_b[3])
#         )
#         if ship_a_point and ship_b_point:
#             distance = haversine(ship_a_point, ship_b_point, Unit.KILOMETERS)
            
#             anomalies += int(time_diff > TIME_THRESHOLD and distance > 1)
    
#     return pid, anomalies
    
    # end_time = datetime.now()
    # execution_time = (end_time - start_time).seconds

    # return result, pid, execution_time

if __name__ == "__main__":
    
    print("Starting SHIP Data tool")
    
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    ships = defaultdict(list)
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
            
            # anomalies_total += anomalies
            # merge to global
            for mmsi, ship_records in ships.items():
                ships[mmsi].extend(ship_records)
    
    print("Sorting results")
    for mmsi in ships:
        ships[mmsi].sort(key=lambda x: x.timestamp)
            
    print("Exporting to temp csv file")
    chunks = []
    with open(TEMP_CSV_FILE, "w") as file:
        writer = csv.writer(file)
        for mmsi, ships in ships.items():
            writer.writerows(ships)
            chunks.append(len(ships))
    
    chunks = iter(chunks)
    print("Calculating statistics")
    ship_reader_cursor = _read_ship_data(chunks)
    
    close_ships_total = 0
    anomaly_total = 0
    
    with Pool(processes=WORKERS) as pool:
        for pid, result in tqdm(
            pool.imap(process_ship_chunk_stats, ship_reader_cursor, chunksize=10),
            desc="Processing temp file chunks"
        ):
            task_ship_counts[pid] = task_ship_counts.get(pid, 0) + 1
            if task_ship_counts[pid] % FINISHED_TEMP_TASKS == 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Proccess = {pid} Finished temp proccesing {FINISHED_TEMP_TASKS} at {timestamp}")                
            
            anomaly_total += result["anomalies"]    
            close_ships_total += result["close_ships"]   
    
    print(f"A) anomaly_total: {anomaly_total}")
    print(f"B) close_ships: {close_ships_total}")
    
    
    
    # # export to JSON file
    # print("Exporting to temp jsonl file")
    # with open("temp.jsonl", "w", encoding="utf-8") as file_json:
    #     for key, value in global_states.items():
    #         file_json.write(json.dumps({key: value}) + "\n")
            
    # print("Reading from temp file ship data")
    
    # temp_reader_cusor = _read_ship_records(file_path="temp.jsonl", chunk_size=CHUNK_SIZE)
    # anomalies_total = 0
    # with Pool(processes=WORKERS) as pool:
    #     for pid, anomalies in tqdm(
    #         pool.imap(process_ship_records, temp_reader_cusor, chunksize=TASKS_PER_WORKER),
    #         desc="Processing ship chunks"
    #     ):
    #         task_counts[pid] = task_counts.get(pid, 0) + 1
            
    #         # TODO: need seperate divider for temp read like FINISHED_TASKS_TEMP or smth
    #         if task_counts[pid] % 10 == 0:
    #             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #             print(f"Proccess = {pid} Finished temp proccesing {FINISHED_TEMP_TASKS} at {timestamp}")                
            
    #         anomalies_total += anomalies
    
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    
    # print(f"Spotted anomaly count: {anomalies_total}")    
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            