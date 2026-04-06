import heapq
import os
from collections import namedtuple
from multiprocessing import Pool
from sqlite3 import connect
from datetime import datetime

from haversine import haversine as _haversine, Unit
from parser import run_ais_parsers
from config import Config
from helper import DBHelper
from tqdm import tqdm

_3600 = 3600.0
ShipRow = namedtuple("ShipRow", ["mmsi", "ts", "lat", "lon", "sog", "draught"])


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


def _to_ts(ts) -> int:
    """Convert timestamp to Unix int seconds — handles both string and int."""
    if isinstance(ts, int):
        return ts
    return int(datetime.fromisoformat(str(ts)).timestamp())


def _iter_vessels(db_paths: list[str], table_name: str):
    conns, cursors = [], []

    for path in db_paths:
        conn = connect(f"file:{path}?mode=ro", uri=True)
        conns.append(conn)
        cursors.append(
            conn.execute(
                f"SELECT mmsi, timestamp, latitude, longitude, sog, draught "
                f"FROM {table_name} ORDER BY mmsi, timestamp"
            )
        )

    heap = []
    for idx, cur in enumerate(cursors):
        row = cur.fetchone()
        if row:
            mmsi, ts, lat, lon, sog, dra = row
            heapq.heappush(heap, (mmsi, _to_ts(ts), lat, lon, sog, dra, idx))

    current_mmsi = None
    current_rows = []

    def _flush(rows):
        rows.sort(key=lambda r: r.ts)
        kept = [rows[0]]
        prev_ts = rows[0].ts
        prev_lat = rows[0].lat
        prev_lon = rows[0].lon

        for r in rows[1:]:
            if (r.ts - prev_ts) < 5:
                continue
            if r.lat == prev_lat and r.lon == prev_lon:
                continue
            kept.append(r)
            prev_ts = r.ts
            prev_lat = r.lat
            prev_lon = r.lon

        return kept

    while heap:
        mmsi, ts, lat, lon, sog, dra, idx = heapq.heappop(heap)

        if mmsi != current_mmsi:
            if current_mmsi is not None and current_rows:
                yield current_mmsi, _flush(current_rows)
            current_mmsi = mmsi
            current_rows = []

        current_rows.append(ShipRow(mmsi, ts, lat, lon, sog, dra))

        nxt = cursors[idx].fetchone()
        if nxt:
            mmsi2, ts2, lat2, lon2, sog2, dra2 = nxt
            heapq.heappush(heap, (mmsi2, _to_ts(ts2), lat2, lon2, sog2, dra2, idx))

    if current_mmsi is not None and current_rows:
        yield current_mmsi, _flush(current_rows)

    for conn in conns:
        conn.close()


def detect_anomaly_d(args: tuple) -> dict:
    mmsi, history = args
    config = Config()
    d_list = []

    for i in range(1, len(history)):
        p1 = history[i - 1]
        p2 = history[i]

        t_h = (p2.ts - p1.ts) / _3600
        if t_h <= 0:
            continue

        dist = _haversine(
            (p1.lat, p1.lon),
            (p2.lat, p2.lon),
            unit=Unit.NAUTICAL_MILES
        )
        speed = dist / t_h

        if speed > config.CLONE_SPEED_KT:
            d_list.append({
                "mmsi": mmsi,
                "implied_knots": round(speed, 1),
                "dist_nm": round(dist, 3),
                "spoofing_artifact": dist > config.CLONE_MAX_DIST_NM,
                "ts_start": p1.ts,
                "lat1": p1.lat,
                "lon1": p1.lon,
                "ts_end": p2.ts,
                "lat2": p2.lat,
                "lon2": p2.lon,
            })

    dfsi_row = None
    if d_list:
        real_clone = [x for x in d_list if not x["spoofing_artifact"]]
        total_jump = sum(x["dist_nm"] for x in real_clone)
        dfsi_row = {
            "mmsi": mmsi,
            "dfsi": round(total_jump * config.DFSI_W_JUMP, 3),
            "total_jump_nm": round(total_jump, 3),
            "clones": len(real_clone),
            "artifacts_excluded": len(d_list) - len(real_clone),
        }

    return {"d": d_list, "dfsi": dfsi_row}


def _run_anomaly_d_analysis(config: Config) -> None:
    """
    Anomaly D analysis
    """
    print("\nRunning Anomaly D — Identity Clone detection ...")
    db_paths = [DBHelper._get_db_from_file_name(f) for f in config.CSV_FILE_SOURCE]

    all_d = []
    dfsi_results = []

    with Pool(processes=config.WORKERS) as pool:
        for result in tqdm(
            pool.imap_unordered(
                detect_anomaly_d,
                _iter_vessels(db_paths, config.DB_TABLE),
                chunksize=config.TAKS_PER_WORKER
            ),
            desc="Anomaly D – analysing vessels",
            unit="vessel",
        ):
            all_d.extend(result["d"])
            if result["dfsi"]:
                dfsi_results.append(result["dfsi"])

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