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
import numpy as np
import os, csv, heapq

class AISWorkerD:
    
    """
        AIS Worker for anomaly C
        detection
    """
    
    @staticmethod
    def detect_anomaly_d(args: tuple) -> dict:
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