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

from anomaly_B_final import run_anomaly_b #module for B



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
    print("\n--- Running Anomaly B ---")

    for file_name in config.CSV_FILE_SOURCE:
        db_name = DBHelper._get_db_from_file_name(file_name)

        print(f"Processing DB: {db_name}")

        run_anomaly_b(db_name, config)
    
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
    
    #run_ais_parsers(config)
    
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