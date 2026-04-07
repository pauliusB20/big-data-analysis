from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict
from dataclasses import astuple
from datetime import datetime
from parser import run_ais_parsers
# from haversine import haversine, Unit
from config import Config
from helper import DBHelper
from workers import AISWorkerB
from tqdm import tqdm
import numpy as np
import csv
import os
from multiprocessing import Manager
      

# from anomaly_B import run_anomaly_b #module for B



def _run_anomaly_a_analysis(config: Config) -> None:
    """
    Anomaly A analysis
    """
    
    # TODO: Anomaly A detection
    
    pass

# TODO: optimize shared memory
def _run_anomaly_b_analysis(config: Config) -> None:
    """
    Anomaly B analysis
    """
    
    print("\n--- Running Anomaly B ---")

    for file_name in config.CSV_FILE_SOURCE:
        db_name = DBHelper._get_db_from_file_name(file_name)

        print(f"Processing DB: {db_name}")

        with Manager() as manager:
            low_sog_tracker = manager.dict()
            proximity_tracker = manager.dict()
            db_helper = DBHelper()
            # run_anomaly_b(db_name, config)
            db_reader = db_helper._fetch_records_db_by_chunk_global(
                db_name, 
                config.CHUNK_SIZE
            )   
            
            written_total = 0
            # task_completed = 0
            # creating multiple workers
            
            
            with Pool(processes=config.WORKERS_B, initializer=AISWorkerB.init_worker, initargs=(low_sog_tracker, proximity_tracker)) as pool:
                for pid, written in pool.imap(
                    func = AISWorkerB.process,
                    iterable=db_reader,
                    chunksize=config.WORKER_B_TASKS            
                ):
                    if written > 0:
                    # if task_completed % config.LOG_EVERY_B == 0:
                        print(f"Anomaly B> Proccess PID={pid} Flagged ships {written}")
                        written_total += written
                    # task_completed += 1
            
            print(f"Saved anomaly B report in {config.WORKERS_B_RESULT_FILE}")
            print(f"Written anomaly B records: {written_total}")


def _run_anomaly_c_analysis(config: Config) -> None:
    """
    Anomaly C analysis
    """
    
    # TODO: Anomaly C detection
    
    pass


def _run_anomaly_d_analysis(config: Config) -> None:
    """
    Anomaly D analysis
    """
    
    # TODO: Anomaly D detection
    
    pass

def run_anomaly_analysis(config: Config) -> None:

    """
    Anomaly analysis
    """
    
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
    
    # run_ais_parsers(config)
    
    run_anomaly_analysis(config)

    
    end_time = datetime.now()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    execution_time = (end_time - start_time).seconds
    print(f"Total execution time: {execution_time}s")
    print(
        "------------------\n"
        f"Finished runtime at {end_time_str}"
    )
    print("DONE")