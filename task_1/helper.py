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
import csv
import os

        
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
            yield {self.file_path : chunk}
            
class DBHelper:
    """    
      Helper class for working with SQLITE datasets
    """
    
    def __init__(self):
        self.config = Config()
        
        
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

                # skip first column if needed
                yield [row for row in rows]
    
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
                    draught REAL
                )            
            """)
            
            cursor.executemany(
                f"""
                INSERT INTO {table}
                VALUES (?, ?, ?, ?, ?, ?)
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
