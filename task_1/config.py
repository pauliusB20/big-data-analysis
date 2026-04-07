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
        #"aisdk-2025-03-01.csv"
    ]
    
    # TODO: add additional config params here for other anomalies if needed
    # For importing and using config.py, example:
    # from config import Config
    # 
    # config = Config()
    # print(config.CHUNK_SIZE)

    #--------------------------
    # Config data for anomaly B
    SOG_THRESHOLD = 1.0
    B_HOURS = 2.0
    PROXIMITY_DIST = 0.5 # 500 meters max separation
    MAX_GAP_MINUTES = 10
    MIN_DISPLACEMENT = 0.3 # Must move 0.3km total (prevents harbor activity)
    AVG_SOG_MIN = 0 # Must have some drift (prevent harbour activity)
    MAX_DIST_VARIATION = 0.1 # 0.1 km, ships have to stay consistently close


    IGNORE_STATUS_B = {"Moored", "At anchor", "Aground", 
                       "Not under command"}
    VALID_MOBILE_TYPES = {"Class A"}
    #--------------------------