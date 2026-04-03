from multiprocessing import Pool
from helper import DBHelper
from config import Config

CHUNK_SIZE = 10
WORKERS = 3
MAX_READ_TASKS = 10
TAKS_PER_WORKER = 2

def process_file_chunk(chunk: list[list]) -> list[tuple]:
    # Plainly returning for imap_unordered to work.
    
    # Add additional processing steps if needed
    
    return chunk


if __name__ == "__main__":
    
    print("Starting example 1: reading from database")
    
    helper = DBHelper()
    config = Config()
    
    selected_initial_chunks = []
    
    # Example how calculate how many records in db table SQLITE without count using multiproccess
    for file_name in config.CSV_FILE_SOURCE:
        
        db_name = helper._get_db_from_file_name(file_name)
        
        db_reader = helper._fetch_records_db_by_chunk(
            db_name=db_name,
            chunk_size=CHUNK_SIZE
        )

        print(f"\Counting rows in database {db_name}, table = {config.DB_TABLE}")
        
        check_rows = 0
        first_chunk = 0
        
        with Pool(processes=WORKERS) as parser_pool:
            for chunk in parser_pool.imap_unordered(
                func = process_file_chunk,
                iterable = db_reader,
                chunksize=TAKS_PER_WORKER
            ):
                # for second example
                if check_rows == 0:
                    first_chunk = chunk
                    
                check_rows += len(chunk) 
                    
        print(f"Counted rows: {check_rows}")
        selected_initial_chunks.append(first_chunk)
        
    # Writing to test databases 
    print("Starting simple writing example")
    db_names = [
        "database_test_1.db",
        "database_test_2.db"
    ]
    
    for (idx, chunk) in enumerate(selected_initial_chunks):
        db_name = db_names[idx]
        print(f"Writing data to db = {db_name}")
        helper._write_records_to_db(
            db_name = db_names[idx],
            records = chunk            
        )
        
    print("DONE")