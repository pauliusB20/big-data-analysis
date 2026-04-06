class Config:
    
    """
    Config class for grouping config data
    
    """
    
    # Config data for AIS Data parser
    CHUNK_SIZE = 1000
    LOG_EVERY = 10000
    TAKS_PER_WORKER = 10
    WORKERS = 6
    DB_NAME = "AIS_DB.db"
    DB_TABLE = "AIS_TABLE"
    CSV_FILE_SOURCE = [
        "aisdk-2025-02-28.csv"
        # "aisdk-2025-03-01.csv"
    ]
    
    
    # anomaly A
    LOG_EVERY_A = 10000
    
    # TODO: add additional config params here for other anomalies if needed
    # For importing and using config.py, example:
    # from config import Config
    # 
    # config = Config()
    # print(config.CHUNK_SIZE)