from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict
from dataclasses import astuple
from datetime import datetime
from parser import run_ais_parsers, AISParser
# from haversine import haversine, Unit
from collections import defaultdict
from config import Config
from helper import DBHelper
from tqdm import tqdm
import numpy as np
import csv
import os
from worker import AIC_worker_A
from pathlib import Path



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

    print("------------STARTING anomaly A -------------")
    print("Finding flagging ships by 4 hours black out and movement")

    start_time = datetime.now()

    db_helper = DBHelper()

    # TODO: add delete csv before running anomaly workers
    for file_name in config.CSV_FILE_SOURCE:

        db_name = db_helper._get_db_from_file_name(file_name)
        task_completed = 0

        written_total = 0
        with Pool(processes=config.ANOMALY_A_PROCESSES) as pool:
            for pid, written in pool.imap(
                    func=AIC_worker_A.process,
                    iterable=db_helper._fetch_records_db_by_chunk_long(
                        db_name=db_name,
                        chunk_size=config.CHUNK_SIZE
                    ),
                    chunksize=config.ANOMALY_A_CHUNKSIZE
            ):
                if task_completed and written > 0:
                    print(f"Anomaly A> Proccess PID={pid} Flagged ships {written}")


                written_total += written

    print("Deleting the database")

    for csv_file in config.CSV_FILE_SOURCE:

        sanitized_name = csv_file.replace('-', '_')

        db_file = Path(sanitized_name).with_suffix('.db')

        if db_file.exists():
            db_file.unlink()
            print(f"Deleted: {db_file}")
        else:
            print(f"Skipped: {db_file} (File not found)")



        print(f"Saved anomaly A report in Anomaly_A_result.csv")
        print(f"Written total: {written_total}")

    end_time = datetime.now()
    execution_total = int((end_time - start_time).total_seconds())

    print(f"Total execution time: {execution_total}")
    print("-----DONE---------")

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