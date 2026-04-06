from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict
from dataclasses import astuple
from datetime import datetime
from parser import run_ais_parsers
# from haversine import haversine, Unit
from collections import defaultdict
from config import Config
from helper import DBHelper
from workers import AISWorkerC
from tqdm import tqdm
import numpy as np
import csv
import os            



def _run_anomaly_a_analysis(config: Config) -> None:
    """
    Anomaly A analysis
    """
    
    # TODO: Anomaly A detection
    
    pass

def _run_anomaly_b_analysis(config: Config) -> None:
    """
    Anomaly B analysis
    """
    
    # TODO: Anomaly B detection
    
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
    
    # TODO: Anomaly C detection
    for file_name in config.CSV_FILE_SOURCE:
        db_name = db_helper._get_db_from_file_name(file_name)
        task_completed = 0
        written_total = 0
        db_reader = db_helper._fetch_records_db_by_chunk_long(
            db_name=db_name, 
            chunk_size=config.CHUNK_SIZE
        )
        
        # creating multiple workers
        with Pool(processes=config.WORKERS_C) as pool:
            for pid, written in pool.imap(
                func = AISWorkerC.process,
                iterable=db_reader,
                chunksize=config.WORKER_C_TASKS            
            ):
                if task_completed % config.LOG_EVERY_C == 0:
                    print(f"Anomaly C> Proccess PID={pid} Flagged ships {written}")
                written_total += written
        
        print(f"Saved anomaly C report in {config.WORKERS_C_RESULT_FILE}")
        print(f"Written anomaly C records: {written_total}")
    
    end_time = datetime.now()
    execution_total = int((end_time - start_time).total_seconds())
    
    print(f"Total execution time: {execution_total}")
    print("-----DONE---------")
    


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
    
    run_ais_parsers(config)
    
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