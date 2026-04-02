class Config:
    
    """
    Config class for grouping config data
    
    """
    
    CHUNK_SIZE = 1000
    LOG_EVERY = 10000
    TAKS_PER_WORKER = 10
    WORKERS = 6
    DB_NAME = "AIS_DB.db"
    DB_TABLE = "AIS_TABLE"
    CSV_FILE_SOURCE = ["aisdk-2025-02-28.csv"]
    
    # TODO: add additional config params here for other anomalies