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
from sqlite3 import connect
from config import Config
import numpy as np
import heapq
import csv
import os
import geopandas as gpd
from shapely.geometry import Point, box


        
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
            nav_status_idx = headline.index("Navigational status") #added for anomaly B
            type_idx = headline.index("Type of mobile") #added for anomaly B 

            for row in csv_reader:
                
                if len(chunk) > self.chunk_size:
                    yield {self.file_path : chunk}
                    chunk = []
                
                if not row[timestamp_idx]:
                    continue
                
                try:
                    timestamp = datetime.strptime(
                        row[timestamp_idx], 
                        "%d/%m/%Y %H:%M:%S"
                    )
                    if (
                        not row[longitude_idx] or 
                        not row[latitude_idx] or
                        not row[mmsi_idx] or
                        not row[sog_idx] or
                        not row[draught_idx] or
                        not row[nav_status_idx] or 
                        not row[type_idx]
                    ):
                        continue
                         
                    mmsi = row[mmsi_idx]
                    longitude = np.float32(row[longitude_idx])
                    latitude = np.float32(row[latitude_idx])
                    sog = np.float32(row[sog_idx])
                    draught = np.float32(row[draught_idx])
                    nav_status = str(row[nav_status_idx])
                    vessel_type = str(row[type_idx])
                    
                    new_row = ShipRow(
                        mmsi=mmsi,
                        timestamp=timestamp,
                        longitude=longitude,
                        latitude=latitude,
                        sog=sog,
                        draught=draught,
                        nav_status=nav_status, #New, added for anomaly B
                        vessel_type=vessel_type #New, added for anomaly B
                    ) 
                    chunk.append(new_row)
                
                except Exception as exception:
                    continue
                
        if chunk:
            yield {self.file_path : chunk}
            
class DBHelper:
    """    
      Helper class for working with SQLITE datasets
    """
    
    def __init__(self):
        self.config = Config()
        
    def _get_time_diff(self, datetime_a: datetime, datetime_b: datetime) -> int:
        difference = (datetime_b - datetime_a).total_seconds() 
        return int(difference)
        
    def _get_record_limit(self, db_name: str) -> int:
        """
        Getting record count from parser database system table

        Parameters
        ----------
        db_name : str
            Database file name

        Returns
        -------
        int
            record count
        """       
        
        with connect(db_name) as conn:
            record_count = conn.execute(
                f"SELECT COUNT(*) FROM {self.config.DB_TABLE}"
            ).fetchone()[0]
            return record_count
        
    # records sorted by MMSI, timestamp
    def _fetch_records_db_by_chunk_long(self, db_name: str, chunk_size: int) -> Iterable[list]:
        """
        Getting records by chunks from database file

        Parameters
        ----------
        db_name : str
            Database file name        

        Returns
        -------
        Iterable[list]
            chunk of ship records 
        """  

        with connect(db_name) as conn:
            cursor = conn.cursor()

            cursor.execute(
                f"SELECT * FROM {self.config.DB_TABLE} ORDER BY MMSI, timestamp"
            )

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                # skip first column if needed
                yield [row for row in rows]
    
    # records sorted by timestamp
    def _fetch_records_db_by_chunk_global(self, db_name: str, chunk_size: int) -> Iterable[list]:
        """
        Getting records by chunks from database file

        Parameters
        ----------
        db_name : str
            Database file name        

        Returns
        -------
        Iterable[list]
            chunk of ship records 
        """  

        with connect(db_name) as conn:
            cursor = conn.cursor()

            cursor.execute(
                f"SELECT * FROM {self.config.DB_TABLE} ORDER BY timestamp"
            )

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                yield [row for row in rows]
    
    def _get_timestamp(self, timestamp_str: str|int) -> int:
        """Convert timestamp to Unix int seconds — handles both string and int."""
        if isinstance(timestamp_str, int):
            return timestamp_str
        timestamp_float = datetime.fromisoformat(timestamp_str).timestamp()
        return int(timestamp_float)

    
    def _get_db_ship_pairs(self, db_paths: list[str], table_name: str):
        def _get_similar_by_time_ships(rows: list[ShipRow]) -> list:
            config = Config()
            rows.sort(key=lambda r: r.timestamp)
            kept = [rows[0]]
            prev_ts = rows[0].timestamp
            prev_lat = rows[0].latitude
            prev_lon = rows[0].longitude

            for r in rows[1:]:
                if (r.timestamp - prev_ts) < config.HOUR_LIMIT_D:
                    continue
                if r.latitude == prev_lat and r.longitude == prev_lon:
                    continue
                kept.append(r)
                prev_ts = r.timestamp
                prev_lat = r.latitude
                prev_lon = r.longitude

            return kept
        
        conns, cursors = [], []
        for path in db_paths:
            conn = connect(f"file:{path}?mode=ro", uri=True)
            conns.append(conn)
            cursors.append(
                conn.execute(
                    f"SELECT mmsi, timestamp, longitude, latitude, sog, draught "
                    f"FROM {table_name} ORDER BY mmsi, timestamp"
                )
            )

        heap = []
        for idx, cur in enumerate(cursors):
            row = cur.fetchone()
            if row:
                (
                    mmsi, 
                    timestamp_str, 
                    longitude, 
                    latitude, 
                    sog, 
                    draught
                ) = row
                timestamp = self._get_timestamp(timestamp_str)
                heapq.heappush(heap, 
                               (
                                   mmsi, 
                                   timestamp, 
                                   longitude, 
                                   latitude, 
                                   sog, 
                                   draught, 
                                   idx
                                ))

        current_mmsi = None
        current_rows = []        

        while heap:
            (
                mmsi, 
                timestamp, 
                longitude, 
                latitude, 
                sog, 
                draught, 
                idx
            ) = heapq.heappop(heap)

            if mmsi != current_mmsi:
                if current_mmsi is not None and current_rows:
                    yield (
                        current_mmsi, 
                        _get_similar_by_time_ships(
                            current_rows
                        )
                    )
                current_mmsi = mmsi
                current_rows = []

            current_rows.append(
                ShipRow(
                    mmsi=mmsi, 
                    timestamp=timestamp, 
                    longitude=longitude, 
                    latitude=latitude, 
                    sog=sog, 
                    draught=draught
                ))

            next_ping = cursors[idx].fetchone()
            if next_ping:
                (
                    mmsi2, 
                    timestamp_str, 
                    longitude_2, 
                    latitude_2, 
                    sog_2, 
                    draught_2
                ) = next_ping
                timestamp_2 = self._get_timestamp(timestamp_str)
                heapq.heappush(heap, (
                    mmsi2, 
                    timestamp_2, 
                    longitude_2, 
                    latitude_2, 
                    sog_2, 
                    draught_2, 
                    idx))

        if current_mmsi is not None and current_rows:
            yield (
                current_mmsi, 
                _get_similar_by_time_ships(
                    current_rows
                )
            )

        for conn in conns:
            conn.close()
    
    
    @staticmethod       
    def _write_records_to_db(db_name: str, records: list[tuple], table: str="AIS_TABLE") -> bool:
        """
        Writing records to SQLITE database

        Parameters
        ----------
        db_name : str
            Database file name.
        
        records : list[tuple]
            records that need to be saved in database

        Returns
        -------
        Boolean
            written succesfully.
        """
               
        with connect(db_name) as conn:
            cursor = conn.cursor()
            
            cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    mmsi VARCHAR(100),
                    timestamp DATETIME,
                    longitude REAL,
                    latitude REAL,
                    sog REAL,
                    draught REAL,
                    nav_status VARCHAR(100),
                    vessel_type VARCHAR(100)
                )            
            """)
            
            cursor.executemany(
                f"""
                INSERT INTO {table}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                """,
                records
            )
            conn.commit()
        
        return True            
    
    
    @staticmethod
    def _get_db_from_file_name(file_name: str) -> str:
        """
        Helper for getting db name from source files (CSV, JSON and etc.)

        Parameters
        ----------
        file_name : str
            Data file name with extension

        Returns
        -------
        str
            Returns file.db sqlite3 file name
        """
        db_table_name, _ = os.path.splitext(file_name)
        db_table_name = db_table_name.replace("-", "_")
        return db_table_name + ".db"




class LocationHelper:
    """
    A utility class for geographic spatial filtering in maritime analysis.
    """

    @staticmethod
    def create_coastal_buffer(coastline_path, nm_distance=12):
        """
        Creates a buffer specifically for the North/Baltic Sea region:
        Lat: 50.0 to 70.0
        Lon: 5.0 to 35.0

        Parameters
        -----------
        Coastline_path : pathlib.Path
            Path to coastline file

        nm_distance : float
            Distance in miles from the coast

        Return
        -----------
        coastal_buffer_final: Shapely geometry object
            Returns the object which creates a buffer from the coastline

        """

        world = gpd.read_file(coastline_path)


        analysis_region = box(5.0, 50.0, 35.0, 70.0)


        regional_coast = world.clip(analysis_region)


        region_metric = regional_coast.to_crs(epsg=3035)


        meters_distance = nm_distance * 1852
        coastal_buffer = region_metric.buffer(meters_distance)


        coastal_buffer_final = coastal_buffer.to_crs(epsg=4326).unary_union

        return coastal_buffer_final

    @staticmethod
    def is_outside_buffer(lat, lon, buffer_polygon):
        """
        Checks if a specific coordinate is outside the coastal buffer.

        Parameters
        ----------
        lat : float
            Latitude of coordinate
        lon : float
            Longitude of coordinate
        buffer_polygon : shapely.geometry.Polygon
            Polygon to check if a specific coordinate is outside the coastal buffer

        Return
        -------
        is_near_shore: bool
            Returns True if the coordinate is outside the coastal buffer
        """

        ship_point = Point(lon, lat)


        is_near_shore = buffer_polygon.contains(ship_point)


        return not is_near_shore
