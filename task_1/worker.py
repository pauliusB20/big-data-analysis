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
        size = len(chunk)
        pid = os.getpid()
        total_written = 0
        Going_dark = []

        # Keep track of which ships we have already processed in this chunk
        seen_mmsis = set()

        for i in range(1, size):
            previous = ShipRow(*chunk[i - 1])
            current = ShipRow(*chunk[i])

            if previous.mmsi == current.mmsi:
                # Calculate time and distance
                from_dt = datetime.fromisoformat(previous.timestamp)
                to_dt = datetime.fromisoformat(current.timestamp)
                difference_hours = (to_dt - from_dt).total_seconds() / 3600
                distance = hs.haversine(previous.point, current.point)

                if difference_hours > config.DIFFERENCE_HOURS and distance > config.DISTANCE:
                    # ONLY write if 'previous' was NOT the very first time
                    # we saw this ship in the current chunk.
                    if previous.mmsi in seen_mmsis:
                        Going_dark.append(previous)
                        Going_dark.append(current)
                        total_written += 1

                # Mark this MMSI as 'seen' AFTER the first potential comparison
                seen_mmsis.add(previous.mmsi)
            else:
                # If the MMSI changed, the 'previous' ship is gone.
                # We don't add to seen_mmsis here because the 'current'
                # ship is now the new 'first' observation for its ID.
                pass

        # Sort by MMSI and Timestamp
        Going_dark.sort(key=lambda ship: (ship.mmsi, ship.timestamp))

        rows_to_write = [ship._as_tuple_db() for ship in Going_dark]

        with open(config.WRITE_TO_FILE_A, "a", newline='') as writer:
            writer_csv = csv.writer(writer)
            writer_csv.writerows(rows_to_write)

        return pid, total_written








