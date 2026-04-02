from collections.abc import Iterable
from collections import defaultdict
from multiprocessing import Pool
from dataclasses import astuple
from sqlite3 import connect
from datetime import datetime
from config import Config
from helper import FileReader
import os

class AISWorker:
    
    """
    Worker class for processing data
    """
    
    @staticmethod
    def write_to_db(chunk: list) -> None:
        config = Config()
        
        with connect(config.DB_NAME) as connection:
            cursor = connection.cursor()
            cursor.executemany(f"""
                INSERT 
                INTO 
                {config.DB_TABLE} 
                VALUES (?, ?, ?, ?, ?, ?)             
            """, chunk)
            connection.commit()
    
    @staticmethod
    def _get_rows(chunk: list[Iterable]) -> dict:
        mapped_ships = defaultdict(list)
        rows = []
        
        for row in chunk:
            mapped_ships[row.mmsi].append(row)
        
        for mmsi in mapped_ships:
            mapped_ships[mmsi].sort(key=lambda x: x.timestamp)
        
        rows = []
        for ships in mapped_ships.values():
            rows.extend(list(map(lambda x: astuple(x), ships)))
            
        return rows
    
    @staticmethod
    def process_file_chunk(chunk: list[Iterable]) -> int:
        chunk_ordered = AISWorker._get_rows(chunk)        
        AISWorker.write_to_db(chunk_ordered)
        pid = os.getpid()
        return pid
            
class AISParser:
    
    """
    AIS data parser - 
    used for creating ordered temp file for analysis
    """   
    
    def __init__(
        self, 
        file_reader: Iterable, 
        workers: int
    ) -> None:
        self.file_reader = file_reader
        self.workers = workers
        self.config = Config()
    
    def initialize_db_start(self) -> None:
        with connect(self.config.DB_NAME) as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.config.DB_TABLE} (
                    mmsi VARCHAR(100),
                    timestamp DATETIME,
                    longitude REAL,
                    latitude REAL,
                    sog REAL,
                    draught REAL
                )
                """
            )
            connection.commit()  
    
    def run(self):
        task_counts = 0
        with Pool(processes=self.workers) as parser_pool:
            for pid in parser_pool.imap_unordered(
                func = AISWorker.process_file_chunk,
                iterable = self.file_reader,
                chunksize=self.config.TAKS_PER_WORKER
            ):
               task_counts += 1 
               timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
               
               if task_counts % self.config.LOG_EVERY == 0:
                   print(f"Worker ID = {pid} finished at {timestamp}")
                

def run_parser(config: Config) -> None:
    for file_path in config.CSV_FILE_SOURCE:
        file_reader = FileReader(
            file_path=file_path,
            chunk_size=config.CHUNK_SIZE
        )
        parser = AISParser(
            file_reader=file_reader._read_csv(),
            workers=config.WORKERS
        )
        parser.initialize_db_start()
        parser.run()