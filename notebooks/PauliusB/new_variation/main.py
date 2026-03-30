from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict, namedtuple
from datetime import datetime
from haversine import haversine, Unit
from collections import deque
from typing import Any
from tqdm import tqdm
import numpy as np
import json
import csv
import os
import heapq

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
FINISHED_TASKS = 10000

FINISHED_TEMP_TASKS = 100

# Max tasks workers can proccess
TASKS_PER_WORKER = 10

# NOTE: check how many cores your CPU has. If 8 cores, max 8 proccesses
# WARNING: tested Max 7 workers for 8 cores and no memory overflow
WORKERS = 6

# For anomaly A, 4 hours in seconds
TIME_THRESHOLD = 14400

TEMP_CSV_FILE = "temp.csv"
ANOMALY_A_TEMP_FILE = "anomaly_temp.csv"
# TEMP_CSV_FILE = "temp"

CALCULATE_A = False
CALCULATE_B = True
EXPORT_FILE = True

TEMP_FILE_AMOUNT = 3


TIME_TOLERANCE = 300   # 5 minutes
MAX_KEEP_TIME = 900    # 15 minutes
MAX_GAP = 900
MIN_DURATION = 7200

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

def _is_valid_latitude(latitude: int) -> bool:
    return -90 < latitude < 90

def _is_valid_longitude(longitude: int) -> bool:
    return -180 < longitude < 180


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
                latitude = float(latitude)                
                if not _is_valid_latitude(latitude):
                    continue
                
                longitude = float(longitude)
                if not _is_valid_longitude(longitude):
                    continue
                
                sog = float(sog)
                
                chunk.add(
                        ShipRow(
                            row[mmsi_idx], 
                            row[timestamp_idx],
                            latitude,
                            longitude,
                            sog
                        )
                    ) 
            if len(chunk) >= chunk_size:
                yield list(chunk)
                chunk = set()
            
    if chunk:
        yield chunk
        

def _get_mapped_chunk(chunk: list[Any]):
    # grouping
    ships = defaultdict(list)
     
    for row in chunk:
        ships[row.mmsi].append(row)    
    
    for mmsi in ships:
        ships[mmsi].sort(key=lambda x: x.timestamp)
    
    return ships

def _get_time_diff(time1: datetime, time2: datetime) -> int:
    return (time2 - time1).total_seconds()

def _get_coord(latitude: np.float64, longitude: np.float64) -> tuple|None:
    return (
        np.float64(latitude), 
        np.float64(longitude)
    )

def _read_ship_data(chunk_size: int = 0) -> Iterable[list]:
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

def _read_ship_data_anomaly_a(chunk_sizes: dict[int]) -> Iterable[list]:
    chunk = []
    with open(ANOMALY_A_TEMP_FILE, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if len(chunk) >= chunk_sizes[row[0]]:
                yield chunk
                chunk = []            
            chunk.append(row)
    
    if chunk:
        yield chunk       
                

def process_chunk_mapped(chunk: list[dict]) -> list:            
    result = []
    mapped_chunk = _get_mapped_chunk(chunk)
    pid = os.getpid()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%d")
    
    # print(f"Task ID = {pid} completed, timestamp = {timestamp}")
    
    for row in mapped_chunk.values():
        result.extend(row)
    
    return result

def process_ordered_chunks(chunk: list[list]) -> dict[str, list]:
    result = defaultdict(list)
    pid = os.getpid()
    
    for (mmsi, timestamp, latitude, longitude, sog) in chunk:
        result[mmsi].append(
            ShipRow(
                mmsi,
                datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S"),
                float(latitude),
                float(longitude),
                float(sog)
            )
        )
    
    return pid, result

def process_ship_chunk_anomalies(chunk: list[list]) -> dict:    
    anomaly_mmsi = 0
    
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
            ship_latitude_previous, 
            ship_longitude_previous
        )
        if not point_previous:
            continue
        
        (
            anomaly_mmsi,
            ship_time_current,
            ship_latitude_current,
            ship_longitude_current,
            _
        ) = chunk[i]
        
        point_current = _get_coord(
            ship_latitude_current, 
            ship_longitude_current
        )
        if not point_current:
            continue
        
        time_diff = _get_time_diff(
            datetime.strptime(ship_time_previous, "%d/%m/%Y %H:%M:%S"), 
            datetime.strptime(ship_time_current, "%d/%m/%Y %H:%M:%S")
        )
        
        is_anomaly = (
            haversine(
                point_previous, 
                point_current, 
                Unit.KILOMETERS
            ) > 1 and 
            time_diff > TIME_THRESHOLD
        )
        anomaly_mmsi += int(is_anomaly)
    
    pid = os.getpid()
    
    return pid, anomaly_mmsi

def _perform_vacuum(temp_file: str) -> None:
    os.remove(temp_file)
    print("Temp file removed and cleared space")


def process_ship_chunk_pairs(chunk: list[list]) -> dict: 
    time_window = deque()  # holds (mmsi, ts, lat, lon, sog)
    pid = os.getpid()
    close_pairs = {}
    
    for (mmsi, ts_a, lat, long, sog) in chunk:
        
        timestamp = datetime.strptime(ts_a, "%d/%m/%Y %H:%M:%S")
        if time_window:            
            first_ship_time = time_window[0][1]
            time_difference = (timestamp - first_ship_time).total_seconds()
            while time_difference > MAX_KEEP_TIME:
                time_window.popleft()
                if not time_window:
                    break
                
                first_ship_time = time_window[0][1]
                time_difference = (timestamp - first_ship_time).total_seconds()
                    
        point_a = _get_coord(lat, long)
        if not point_a:
            continue
            
        for (mmsi_b, ts_b, lat2, long2, sog2) in time_window:
            if mmsi == mmsi_b:
                continue
            
            if abs((timestamp - ts_b).total_seconds()) > TIME_TOLERANCE:
                continue
            
            point_b = _get_coord(lat2, long2)
            if not point_b:
                continue
            
            sog = np.float64(sog)
            sog2 =  np.float64(sog2)
            if sog < 1 and sog2 < 1 and haversine(point_a, point_b) <= 500:
                pair = tuple(sorted((mmsi, mmsi_b)))

                if pair not in close_pairs:
                    close_pairs[pair] = {"start": timestamp, "last": timestamp}
                else:
                    pair_time = close_pairs[pair]
                    if (pair_time["last"] - timestamp).total_seconds() <= MAX_KEEP_TIME:
                        pair_time["last"] = timestamp
                    else:
                        pair_time["start"] = timestamp
                        pair_time["last"] = timestamp
        
        ship_time_point = (mmsi, timestamp, lat, long, sog)
        time_window.append(
           ship_time_point 
        )
    return pid, close_pairs

if __name__ == "__main__":    
    anomalies_total = 0
    loitering_total = 0
    
    print("Starting SHIP Data tool")
    
    print(f"1. Reading data for processing using path={FILE_CSV}")
    ships_mapped = defaultdict(list)
    start_time = datetime.now()   
    
    task_counts = defaultdict(list)
    task_anomalies_ships = defaultdict(list)
    task_loitered_ships = defaultdict(list)
    ship_counts = defaultdict(list)
    ship_pairs = []
    
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    with Pool(processes=WORKERS) as pool:
        sorted_chunks = list(
            pool.imap(
                process_chunk_mapped, 
                reader_cursor, 
                chunksize=TASKS_PER_WORKER)
            )

    print("Merged to heap")
    merged_result = heapq.merge(*sorted_chunks, key=lambda x: x.timestamp)
    
    print("Exporting to temp csv file") 
    with open(TEMP_CSV_FILE, "w") as file:
        writer = csv.writer(file)
        for row in merged_result:
            writer.writerow(row)
            
    if CALCULATE_A:
        ships_mapped = defaultdict(list)
        print("Calculating anomaly A")
        ship_reader_cursor = _read_ship_data(chunk_size=CHUNK_SIZE//2)
        with Pool(processes=WORKERS//2) as pool:
            for pid, mapped_ships in tqdm(
                pool.imap(
                   process_ordered_chunks, 
                   ship_reader_cursor, 
                   chunksize=2
                ) 
            ):
              task_counts[pid] = task_counts.get(pid, 0) + 1
              if task_counts[pid] % FINISHED_TASKS == 0:
                  timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                  print(f"Proccess = {pid} Finished reading temp file {FINISHED_TASKS} at {timestamp}") 

              for mmsi, ships in mapped_ships.items():
                  ships_mapped[mmsi].extend(ships)
        
        for mmsi, ships in ships_mapped.items():
            ship_counts[mmsi].sort(key=lambda x: x.timestamp)
              
        print("Exporting data to anomaly A temp csv file")
        exported_ship_counts = defaultdict(int)
        with open(ANOMALY_A_TEMP_FILE, "w") as file:
            writer = csv.writer(file)
            for mmsi, ship_records in ships_mapped.items():
                writer.writerows(ship_records)
                exported_ship_counts[mmsi] = len(ship_records)
        
        reader_cursor = _read_ship_data_anomaly_a(
            exported_ship_counts
        )
        
        print("Reading from temp file and calculating anomaly A")
        with Pool(processes=WORKERS//2) as pool:
            for pid, anomalies in tqdm(
                pool.imap(process_ship_chunk_anomalies, reader_cursor, chunksize=3),
                desc="Processing file chunks"
            ):
                task_anomalies_ships[pid] = task_anomalies_ships.get(pid, 0) + 1
                if task_anomalies_ships[pid] % FINISHED_TASKS == 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Proccess = {pid} Finished proccesing {FINISHED_TASKS} at {timestamp}")                
                
                anomalies_total += anomalies
        
    # TODO: optimize
    if CALCULATE_B:
        ship_pairs = defaultdict(dict)
        print("Reading ship data from temp file for loitering calculation") 
        ship_reader_cursor = _read_ship_data(chunk_size=CHUNK_SIZE) 
        with Pool(processes=WORKERS) as pool:
            for pid, pair_mappings in tqdm(
                pool.imap(process_ship_chunk_pairs, ship_reader_cursor, chunksize=10),
                desc="Processing file chunks ship loitering"
            ):
                task_loitered_ships[pid] = task_loitered_ships.get(pid, 0) + 1
                if task_loitered_ships[pid] % FINISHED_TEMP_TASKS == 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"Proccess = {pid} Finished loitering proccesing {FINISHED_TEMP_TASKS} at {timestamp}")   
                    
                for pair, timestamps in pair_mappings:
                    ship_pairs[pair] = timestamps
        
        print("Calculating loitering anomalies")
        for coord, pair_time in ship_pairs.items():
            if (pair_time["last"] - pair_time["start"]).total_seconds() >= MIN_DURATION:
                loitering_total += 1
                
    
    print("Results")
    print(f"A) anomaly_total: {anomalies_total}")
    print(f"B) loitering: {loitering_total}")
    
    
    _perform_vacuum(TEMP_CSV_FILE)
    _perform_vacuum(ANOMALY_A_TEMP_FILE)
    end_time = datetime.now()
    execution_difference = (end_time - start_time).seconds
    
    # print(f"Spotted anomaly count: {anomalies_total}")    
    print(f"Execution time: {execution_difference} s")
    
    print("DONE")
            