from collections.abc import Iterable
from collections import defaultdict
from multiprocessing import Pool
from dataclasses import astuple
from sqlite3 import connect
from datetime import datetime
from config import Config
from helper import FileReader, DBHelper
from models import ShipRow
import numpy as np
import os, csv, heapq

class AISWorkerC:
    
    """
        AIS Worker for anomaly C
        detection
    """
    
    @staticmethod
    def process(chunk: list[tuple]) -> tuple[int, int]:
        config = Config()
        db_helper = DBHelper()
        size = len(chunk)
        pid = os.getpid()
        total_written = 0
        
        with open(config.WORKERS_C_RESULT_FILE, "a", newline="") as writer:
            writer_csv = csv.writer(writer)
            
            for i in range(1, size):
                previous = ShipRow(*chunk[i - 1])
                current = ShipRow(*chunk[i])
                
                if previous.mmsi == current.mmsi:
                    draught_change_rate = abs(current.draught - previous.draught) / previous.draught
                    
                    previous_timestamp = datetime.strptime(previous.timestamp, "%Y-%m-%d %H:%M:%S")
                    current_timestamp = datetime.strptime(current.timestamp, "%Y-%m-%d %H:%M:%S")
                    
                    difference_seconds = db_helper._get_time_diff(previous_timestamp, current_timestamp)
                    # difference_hours = (difference_seconds / 3600)
                    if draught_change_rate >= 0.05 and difference_seconds > 7200:
                        # saving anomaly
                        writer_csv.writerow(previous._as_tuple_db())
                        writer_csv.writerow(current._as_tuple_db())
                        total_written += 1      
                
        return pid, total_written