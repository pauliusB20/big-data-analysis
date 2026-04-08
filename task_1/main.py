import heapq
import os
import csv
from collections import namedtuple
from multiprocessing import Pool
from sqlite3 import connect
from datetime import datetime
from parser import run_ais_parsers
from workers import AISWorkerD
from config import Config
from helper import DBHelper
from tqdm import tqdm
from parser import run_ais_parsers, AISParser
# from haversine import haversine, Unit
from collections import defaultdict
from config import Config
from helper import DBHelper
from workers import AISWorkerC, AISWorkerD, AISWorkerA
from tqdm import tqdm
import numpy as np
import csv
import os            
from anomaly_B import run_anomaly_b #module for B
from pathlib import Path
import haversine as hs
from helper import LocationHelper


def _run_anomaly_a_analysis(config: Config) -> None:
    """
    Anomaly A analysis
    """

    file_path = Path(config.WRITE_TO_FILE_A)

    if file_path.exists():
        file_path.unlink()
        print(f"Successfully deleted {config.WRITE_TO_FILE_A}")
    else:
        print(f"The file {config.WRITE_TO_FILE_A} does not exist.")



    helper = LocationHelper()
    coastal_buffer = helper.create_coastal_buffer(config.COSTAL_FILE, nm_distance=12)

    if file_path.exists():
        file_path.unlink()
        print(f"Successfully deleted {config.WRITE_TO_FILE_A}")
    else:
        print(f"The file {config.WRITE_TO_FILE_A} does not exist.")

    print("------------STARTING anomaly A -------------")
    print("Finding flagging ships by 4 hours black out and movement")


    db_helper = DBHelper()
    for file_name in config.CSV_FILE_SOURCE:
        db_name = db_helper._get_db_from_file_name(file_name)
        written_total = 0

        db_records = db_helper._fetch_records_db_by_chunk_long(
            db_name=db_name,
            chunk_size=config.CHUNK_SIZE
        )

        # MATCH THIS with the Worker.process unpacking
        worker_input = ((chunk, coastal_buffer) for chunk in db_records)

        with Pool(processes=config.ANOMALY_A_PROCESSES) as pool:
            for pid, written in pool.imap(
                    func=AISWorkerA.process,
                    iterable=worker_input,
                    chunksize=config.ANOMALY_A_CHUNKSIZE
            ):
                if written > 0:
                    print(f"Anomaly A> Proccess PID={pid} Flagged ships {written}")
                written_total += written

        print(f"Saved anomaly A report in {config.WRITE_TO_FILE_A}")
        print(f"Written total: {written_total}")
    pass


def _run_anomaly_b_analysis(config: Config) -> None:
    """
    Anomaly B analysis
    """
    print("\n--- Running Anomaly B ---")

    for file_name in config.CSV_FILE_SOURCE:
        db_name = DBHelper._get_db_from_file_name(file_name)

        print(f"Processing DB: {db_name}")

        run_anomaly_b(db_name, config)
    
    pass


def _run_anomaly_c_analysis(config: Config) -> None:
    """
    Anomaly C analysis
    
    Goal:
    Anomaly C (Draft Changes at Sea): 
    Detect vessels whose draught (depth in water) 
    changes by more than 5% during an AIS blackout of > 2 hours 
    (implying cargo was loaded/unloaded illegally).
    """
    
    print("------------STARTING anomaly C -------------")
    print("Finding flagging ships by draught 5 percent and 2 hours blackout")
    
    start_time = datetime.now()
    
    db_helper = DBHelper()
    location_helper = LocationHelper()
    coastal_buffer = location_helper.create_coastal_buffer(config.COSTAL_FILE, nm_distance=12)
    
    # delete results anomaly C file before running the anomaly detection C again
    if os.path.exists(config.WORKERS_C_RESULT_FILE):
        print("Removing old anomaly C results...")
        os.remove(config.WORKERS_C_RESULT_FILE)
    
    print("Running anomaly C detection...")
    # TODO: Anomaly C detection
    for file_name in config.CSV_FILE_SOURCE:
        db_name = db_helper._get_db_from_file_name(file_name)
        # task_completed = 0
        written_total = 0
        
        db_records = db_helper._fetch_records_db_by_chunk_long(
            db_name=db_name,
            chunk_size=config.CHUNK_SIZE
        )

        # MATCH THIS with the Worker.process unpacking
        worker_input = ((chunk, coastal_buffer) for chunk in db_records)

        
        # creating multiple workers
        with Pool(processes=config.WORKERS_C) as pool:
            for pid, written in pool.imap(
                func = AISWorkerC.process,
                iterable=worker_input,
                chunksize=config.WORKER_C_TASKS            
            ):
                if written > 0:
                    print(f"Anomaly C> Proccess PID={pid} Flagged ships {written} with illegal cargo")
                written_total += written
        
        if written_total > 0:
            print(f"Saved anomaly C report in {config.WORKERS_C_RESULT_FILE}")
        else:
            print("No anomaly C were saved in results file")
            
        print(f"Written anomaly C records: {written_total}")
    
    end_time = datetime.now()
    execution_total = int((end_time - start_time).total_seconds())
    
    print(f"Total execution time: {execution_total}")
    print("-----DONE---------")
    

def _run_anomaly_d_analysis(config: Config) -> None:
    """
    Anomaly D analysis
    """
    print("\nRunning Anomaly D — Identity Clone detection ...")
    helper = DBHelper()
    db_paths = [DBHelper._get_db_from_file_name(f) for f in config.CSV_FILE_SOURCE]

    all_d = []
    dfsi_results = []

    with Pool(processes=config.WORKERS) as pool:
        for result in tqdm(
            pool.imap_unordered(
                AISWorkerD.process,
                helper._get_db_ship_pairs(db_paths, config.DB_TABLE),
                chunksize=config.TAKS_PER_WORKER
            ),
            desc="Anomaly D – analysing vessels",
            unit="vessel",
        ):
            all_d.extend(result["d"])
            if result["dfsi"]:
                dfsi_results.append(result["dfsi"])

    # TODO: save results to CSV liko other anomalies?
    dfsi_results.sort(key=lambda x: x["dfsi"], reverse=True)

    with open("anomaly_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["mmsi", "dfsi", "clones", "total_jump_nm", "artifacts_excluded"])
        writer.writeheader()
        writer.writerows(dfsi_results)

    print(f"  Anomaly D Identity Clone:   {len(all_d):>7,} events")
    print(f"  Flagged vessels (DFSI > 0): {len(dfsi_results):>7,}")

    print("\n  Top 10 suspects by DFSI:")
    for vessel in dfsi_results[:10]:
        print(
            f"    {vessel['mmsi']}  "
            f"DFSI={vessel['dfsi']}  "
            f"jumps={vessel['clones']}  "
            f"total_nm={vessel['total_jump_nm']}"
        )

def _run_cleanup_process(config: Config) -> None:
    print("ENDING: Deleting the database")

    for csv_file in config.CSV_FILE_SOURCE:

        sanitized_name = csv_file.replace('-', '_')

        db_file = Path(sanitized_name).with_suffix('.db')

        if db_file.exists():
            db_file.unlink()
            print(f"Deleted: {db_file}")
        else:
            print(f"Skipped: {db_file} (File not found)")


def run_anomaly_analysis(config: Config) -> None:
    """
    Anomaly analysis
    """
    
    start_time = datetime.now()
    
    _run_anomaly_a_analysis(config)
    _run_anomaly_b_analysis(config)
    _run_anomaly_c_analysis(config)
    _run_anomaly_d_analysis(config)


if __name__ == "__main__":
    config = Config()

    print("-----STARTING AIS DATA ANALYZER-----")

    start_time = datetime.now()
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"Started run time at: {start_time_str}"
        "-------------------"
    )
    
    # Skip parsing if DB files already exist — saves ~20 min during development
    db_paths = [DBHelper._get_db_from_file_name(f) for f in config.CSV_FILE_SOURCE]
    if all(os.path.exists(p) for p in db_paths):
        print("DB files already exist — skipping parsing, running anomaly detection only")
    else:
        run_ais_parsers(config)
    
    run_anomaly_analysis(config)

    
    end_time = datetime.now()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    execution_time = (end_time - start_time).seconds

    print(f"Total execution time: {execution_time}s")
    print("------------------")
    print(f"Finished runtime at {end_time_str}")
    print("DONE")