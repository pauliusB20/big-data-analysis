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
import haversine as hs


class AIC_worker_A:

    @staticmethod
    def process(chunk):
        config = Config()
        db_helper=DBHelper()
        size=len(chunk)
        pid=os.getpid()
        total_written=0
        Going_dark= []
        with open("Anomaly_A_result.csv", "w") as writer:
            writer_csv = csv.writer(writer)
            for i in range(1,size):
                previous=ShipRow(*chunk[i-1])
                current=ShipRow(*chunk[i])
                if previous.mmsi==current.mmsi:

                    from_dt = datetime.fromisoformat(previous.timestamp)
                    to_dt = datetime.fromisoformat(current.timestamp)

                    print(from_dt, to_dt)

                    difference_delta = to_dt-from_dt

                    difference_seconds = difference_delta.total_seconds()
                    difference_hours = (difference_seconds / 3600)
                    distance=float(hs.haversine((previous.latitude,previous.longitude), (current.latitude,current.longitude)))
                    print(difference_hours)
                    if difference_hours>0.1 and distance>0:
                        Going_dark.append(current)
                        total_written=total_written+1


            going_dark_sorted = sorted(Going_dark, key=lambda ship: ship.timestamp)

            rows_to_write = [ship._as_tuple() for ship in going_dark_sorted]
            writer_csv.writerows(rows_to_write)
            return pid, total_written









