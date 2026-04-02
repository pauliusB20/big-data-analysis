from collections.abc import Iterable
from datetime import datetime
from models import ShipRow
import numpy as np
import csv

        
class FileReader:
    
    "Helper class for reading data from file"
    
    def __init__(self, file_path: str, chunk_size: int) -> None:
        self.file_path = file_path
        self.chunk_size = chunk_size
    
    def _read_csv(self) -> Iterable:
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
            
            for row in csv_reader:
                
                if len(chunk) > self.chunk_size:
                    yield chunk
                    chunk = []
                
                if not row[timestamp_idx]:
                    continue
                
                try:
                    timestamp = datetime.strptime(
                        row[timestamp_idx], 
                        "%d/%m/%Y %H:%M:%S"
                    )
                    if (
                        not row[latitude_idx] or
                        not row[latitude_idx] or 
                        not row[mmsi_idx] or
                        not row[sog_idx] or
                        not row[draught_idx]
                    ):
                        continue
                         
                    mmsi = row[mmsi_idx]
                    latitude = np.float32(row[latitude_idx])
                    longitude = np.float32(row[longitude_idx])
                    sog = np.float32(row[sog_idx])
                    draught = np.float32(row[draught_idx])
                    
                    new_row = ShipRow(
                        mmsi=mmsi,
                        timestamp=timestamp,
                        longitude=longitude,
                        latitude=latitude,
                        sog=sog,
                        draught=draught
                    )
                    chunk.append(new_row)
                
                except Exception as exception:
                    continue
                
        if chunk:
            yield chunk