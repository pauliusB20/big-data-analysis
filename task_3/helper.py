"""
Helper module for processing AIS SQLite data in chunks.

This module provides utilities for:
- Reading database records
- Chunked processing
- Multiprocessing pipelines
"""

from collections.abc import Iterable
from datetime import datetime
from models import ShipRow
import numpy as np
import heapq
import csv
import os
# import geopandas as gpd
# from shapely.geometry import Point, box

        
class FileReader:
    
    """
    Helper class for reading data from file
    """
    
    def __init__(self, file_path: str, chunk_size: int) -> None:
        self.file_path = file_path
        self.chunk_size = chunk_size
    
    def _read_csv(self) -> Iterable[dict]:
        """
        Iterator for reading data from CSV
        """
        chunk = []
        with open(self.file_path, newline="") as reader:
            csv_reader = csv.reader(reader)
            headline = next(csv_reader)
            
            # columns to select 
            mmsi_idx = headline.index("MMSI")
            timestamp_idx = headline.index("# Timestamp")
            longitude_idx = headline.index("Longitude")
            latitude_idx = headline.index("Latitude")
            sog_idx = headline.index("SOG")
            draught_idx = headline.index("Draught")
            cargo_type_idx = headline.index("Cargo type")
            ship_type_idx = headline.index("Ship type")
            
            nav_status_idx = headline.index("Navigational status") #added for anomaly B
            type_idx = headline.index("Type of mobile") #added for anomaly B 

            for row in csv_reader:
                
                if len(chunk) > self.chunk_size:
                    yield {self.file_path : chunk}
                    chunk = []
                
                timestamp = row[timestamp_idx]
                
                try:
                    if timestamp:                    
                        timestamp = datetime.strptime(
                            timestamp, 
                            "%d/%m/%Y %H:%M:%S"
                        )
                         
                    mmsi = row[mmsi_idx]
                    longitude = np.float32(row[longitude_idx])
                    latitude = np.float32(row[latitude_idx])
                    sog = np.float32(row[sog_idx])
                    draught = np.float32(row[draught_idx])
                    cargo_type = row[cargo_type_idx]
                    ship_type = row[ship_type_idx]
                    nav_status = str(row[nav_status_idx])
                    vessel_type = str(row[type_idx])
                    
                    new_row = ShipRow(
                        mmsi=mmsi,
                        timestamp=timestamp,
                        longitude=longitude,
                        latitude=latitude,
                        sog=sog,
                        draught=draught,
                        cargo_type=cargo_type,
                        ship_type=ship_type,
                        nav_status=nav_status, #New, added for anomaly B
                        vessel_type=vessel_type #New, added for anomaly B
                    ) 
                    chunk.append(new_row)
                
                except Exception as exception:
                    continue
                
        if chunk:
            yield {self.file_path : chunk}