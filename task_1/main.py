from multiprocessing import Pool
from datetime import datetime
from collections.abc import Iterable
from collections import defaultdict
from dataclasses import astuple
from datetime import datetime
from parser import run_parser
# from haversine import haversine, Unit
from collections import defaultdict
from config import Config
from tqdm import tqdm
import numpy as np
import csv
import os            

# TODO
def _retrieve_records(CHUNK: int) -> list:
    pass
    
if __name__ == "__main__":
    
    
    config = Config()
    
    print("-----STARTING AIS DATA ANALYZER-----")
    
    start_time = datetime.now()
    run_parser(config)
    
    # Anomaly detection goes here 
    
    
    end_time = datetime.now()
    execution_time = (end_time - start_time).seconds
    print(f"Total execution time: {execution_time}s")
    
    print("DONE")