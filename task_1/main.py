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
from tqdm import tqdm
import numpy as np
import csv
import os
from worker import AIC_worker_A



def _run_anomaly_a_analysis(config: Config) -> None:
    """
    Anomaly A analysis
    """

    config = Config()

    print("------------STARTING anomaly A -------------")
    print("Finding flagging ships by 4 hours black out and movement")

    start_time = datetime.now()

    db_helper = DBHelper()


    for file_name in config.CSV_FILE_SOURCE:
        db_name = db_helper._get_db_from_file_name(file_name)
        task_completed = 0


        with Pool(processes=6) as pool:
            for pid, written in pool.imap(
                    func=AIC_worker_A.process,
                    iterable=db_helper._fetch_records_db_by_chunk(
                        db_name=db_name,
                        chunk_size=config.CHUNK_SIZE
                    ),
                    chunksize=2
            ):
                if task_completed % 10 == 0:
                    print(f"Anomaly A> Proccess PID={pid} Flagged ships {written}")

        print(f"Saved anomaly A report in Anomaly_A_result.csv")

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