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
        
        with open(config.WORKERS_C_RESULT_FILE, "w") as writer:
            writer_csv = csv.writer(writer)
            
            flagged_by_draught = []
            for i in range(1, size):
                previous = ShipRow(*chunk[i - 1])
                current = ShipRow(*chunk[i])
                
                if previous.mmsi == current.mmsi:
                    draught_change_rate = (current.draught - previous.draught) / current.draught
                    draught_change_rate_perc = np.round(
                        draught_change_rate * 100, 
                        decimals=2
                    )
                    difference_seconds = db_helper._get_time_diff(previous.timestamp, current.timestamp)
                    difference_hours = (difference_seconds / 3600)
                    if draught_change_rate_perc >= 5 and  difference_hours >= config.HOUR_LIMIT:
                        heapq.heappush(flagged_by_draught, current)
                        total_written += 1
                
                
        flagged_by_draught_sorted = sorted(flagged_by_draught)        
        writer_csv.writerows(flagged_by_draught_sorted)
        return pid, total_written