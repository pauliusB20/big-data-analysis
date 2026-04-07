import heapq
import os
from collections import namedtuple
from multiprocessing import Pool
from sqlite3 import connect
from datetime import datetime
from parser import run_ais_parsers
from workers import AISWorkerD
from config import Config
from helper import DBHelper
from tqdm import tqdm


def _run_anomaly_a_analysis(config: Config) -> None:
    """
    Anomaly A analysis
    """

    # TODO: Anomaly A detection
    pass


def _run_anomaly_b_analysis(config: Config) -> None:
    """
    Anomaly B analysis
    """

    # TODO: Anomaly B detection
    pass


def _run_anomaly_c_analysis(config: Config) -> None:
    """
    Anomaly C analysis
    """

    # TODO: Anomaly C detection
    pass


def _run_anomaly_d_analysis(config: Config) -> None:
    """
    Anomaly D analysis
    """
    print("\nRunning Anomaly D — Identity Clone detection ...")
    helper = DBHelper()
    db_paths = [DBHelper._get_db_from_file_name(f) for f in config.CSV_FILE_SOURCE]

    all_d = []
    dfsi_results = []

    with Pool(processes=config.WORKERS) as pool:
        for result in tqdm(
            pool.imap_unordered(
                AISWorkerD.detect_anomaly_d,
                helper._get_db_ship_pairs(db_paths, config.DB_TABLE),
                chunksize=config.TAKS_PER_WORKER
            ),
            desc="Anomaly D – analysing vessels",
            unit="vessel",
        ):
            all_d.extend(result["d"])
            if result["dfsi"]:
                dfsi_results.append(result["dfsi"])

    # TODO: save results to CSV liko other anomalies?
    dfsi_results.sort(key=lambda x: x["dfsi"], reverse=True)

    print(f"  Anomaly D Identity Clone:   {len(all_d):>7,} events")
    print(f"  Flagged vessels (DFSI > 0): {len(dfsi_results):>7,}")

    print("\n  Top 10 suspects by DFSI:")
    for vessel in dfsi_results[:10]:
        print(
            f"    {vessel['mmsi']}  "
            f"DFSI={vessel['dfsi']}  "
            f"jumps={vessel['clones']}  "
            f"total_nm={vessel['total_jump_nm']}"
        )


def run_anomaly_analysis(config: Config) -> None:
    """
    Anomaly analysis
    """
    _run_anomaly_a_analysis(config)
    _run_anomaly_b_analysis(config)
    _run_anomaly_c_analysis(config)
    _run_anomaly_d_analysis(config)


if __name__ == "__main__":
    config = Config()

    print("-----STARTING AIS DATA ANALYZER-----")

    start_time = datetime.now()
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Started run time at: {start_time_str}")
    print("-------------------")

    # Skip parsing if DB files already exist — saves ~20 min during development
    db_paths = [DBHelper._get_db_from_file_name(f) for f in config.CSV_FILE_SOURCE]
    if all(os.path.exists(p) for p in db_paths):
        print("DB files already exist — skipping parsing, running anomaly detection only")
    else:
        run_ais_parsers(config)

    run_anomaly_analysis(config)

    end_time = datetime.now()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    execution_time = (end_time - start_time).seconds

    print(f"Total execution time: {execution_time}s")
    print("------------------")
    print(f"Finished runtime at {end_time_str}")
    print("DONE")