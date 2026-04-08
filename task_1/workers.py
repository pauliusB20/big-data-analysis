from collections.abc import Iterable
from collections import defaultdict
from multiprocessing import Pool
from dataclasses import astuple
from sqlite3 import connect
from datetime import datetime
from config import Config
from helper import FileReader, DBHelper
from haversine import haversine, Unit
from models import ShipRow
import haversine as hs
import numpy as np
import os, csv, heapq
import os
import csv
from datetime import datetime
import haversine as hs
from helper import LocationHelper


# Assuming LocationHelper and ShipRow are imported/defined in your scope
class AISWorkerA:
    @staticmethod
    def process(args):
        # Unpack only what is actually being passed
        chunk, coastal_buffer = args

        config = Config()
        size = len(chunk)
        pid = os.getpid()
        total_written = 0
        Going_dark = []
        seen_mmsis = set()

        for i in range(1, size):
            previous = ShipRow(*chunk[i - 1])
            current = ShipRow(*chunk[i])

            if previous.mmsi == current.mmsi:
                # Coastal Check
                is_open_sea = LocationHelper.is_outside_buffer(
                    previous.latitude, previous.longitude, coastal_buffer
                )
                if not is_open_sea:
                    continue

                k
                from_dt = datetime.fromisoformat(previous.timestamp)
                to_dt = datetime.fromisoformat(current.timestamp)
                difference_hours = (to_dt - from_dt).total_seconds() / 3600
                distance = hs.haversine(previous.point, current.point)

                if difference_hours > config.DIFFERENCE_HOURS and distance > config.DISTANCE:
                    # Logic: only flag if this isn't the first time we've seen this MMSI
                    if previous.mmsi in seen_mmsis:
                        Going_dark.append(previous)
                        Going_dark.append(current)
                        total_written += 1

                seen_mmsis.add(previous.mmsi)

        if Going_dark:

            Going_dark.sort(key=lambda ship: (ship.mmsi, ship.timestamp))
            rows_to_write = [ship._as_tuple_db() for ship in Going_dark]
            with open(config.WRITE_TO_FILE_A, "a", newline='') as writer:
                csv.writer(writer).writerows(rows_to_write)

        return pid, total_written

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

class AISWorkerD:  
    
    """
        AIS Worker for anomaly D
        detection
    """
    
    @staticmethod 
    def process(args: tuple) -> dict:
        mmsi, history = args
        config = Config()
        d_list = []

        for i in range(1, len(history)):
            p1 = history[i - 1]
            p2 = history[i]

            t_h = (p2.timestamp - p1.timestamp) / 3600
            if t_h <= 0:
                continue

            dist = haversine(
                p1.point,
                p2.point,
                unit=Unit.NAUTICAL_MILES
            )
            speed = dist / t_h

            if speed > config.CLONE_SPEED_KT:
                d_list.append({
                    "mmsi": mmsi,
                    "implied_knots": round(speed, 1),
                    "dist_nm": round(dist, 3),
                    "spoofing_artifact": dist > config.CLONE_MAX_DIST_NM,
                    "ts_start": p1.timestamp,
                    "lat1": p1.latitude,
                    "lon1": p1.longitude,
                    "ts_end": p2.timestamp,
                    "lat2": p2.latitude,
                    "lon2": p2.longitude,
                })

        dfsi_row = None
        if d_list:
            real_clone = [x for x in d_list if not x["spoofing_artifact"]]
            total_jump = sum(x["dist_nm"] for x in real_clone)
            dfsi_row = {
                "mmsi": mmsi,
                "dfsi": round(total_jump * config.DFSI_W_JUMP, 3),
                "total_jump_nm": round(total_jump, 3),
                "clones": len(real_clone),
                "artifacts_excluded": len(d_list) - len(real_clone),
            }

        return {"d": d_list, "dfsi": dfsi_row}
