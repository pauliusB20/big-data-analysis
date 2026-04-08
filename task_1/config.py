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
        "aisdk-2025-02-28.csv",
        "aisdk-2025-03-01.csv"
    ]
    
    
    # anomaly A
    LOG_EVERY_A = 10000
    WRITE_TO_FILE_A="ANOMALY_A_result.csv"
    DIFFERENCE_HOURS=4
    DISTANCE=0
    ANOMALY_A_PROCESSES=6
    ANOMALY_A_CHUNKSIZE=2

    COSTAL_FILE="ne_10m_coastline"
    
    # TODO: add additional config params here for other anomalies if needed
    # Anomaly D
    CLONE_SPEED_KT    = 60.0
    CLONE_MAX_DIST_NM = 3_000.0
    DFSI_W_JUMP       = 0.1
    HOUR_LIMIT_D = 5
    
    # For importing and using config.py, example:
    # from config import Config
    # 
    # config = Config()
    # print(config.CHUNK_SIZE)
    
    # Config anomaly C
    WORKERS_C = 3    
    WORKER_C_TASKS = 2    
    WORKERS_C_RESULT_FILE = "anomaly_c_draught.csv"
    HOUR_LIMIT = 2
    LOG_EVERY_C = 1000
