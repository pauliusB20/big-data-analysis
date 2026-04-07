from collections import defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime
from haversine import haversine
from models import ShipRow, ProximityTrack, LowSOGTrack
from helper import DBHelper
from config import Config


def is_suspicious_encounter(p: ProximityTrack, config, ignore_types=set()) -> bool:
    """Helper function for detecting anomalies

    Args:
        p (ProximityTrack): ship anomaly range
        config (Config): config for detecting
        ignore_types (set, optional): for ignoring certain classes. Defaults to set().

    Returns:
        bool: status that informs if encounter is suspicious
    """
    if p.vessel_type_1 in ignore_types or p.vessel_type_2 in ignore_types:
        return False

    total_drift = haversine((p.start_lat, p.start_lon), (p.end_lat, p.end_lon))
    avg_sog = p.sog_sum / p.count if p.count > 0 else 0
    duration = (p.end - p.start).total_seconds() / 3600
    dist_variation = p.max_dist - p.min_dist

    return (
        duration >= config.B_HOURS and
        total_drift >= config.MIN_DISPLACEMENT and
        avg_sog >= config.AVG_SOG_MIN and
        dist_variation <= config.MAX_DIST_VARIATION
    )

# ------------------------
# MAIN FUNCTION
# ------------------------
def run_anomaly_b(db_name: str, config: Config) -> None:
    db_helper = DBHelper()
    low_sog_tracker = {}
    proximity_tracker = {}

    with open(config.WORKERS_C_RESULT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mmsi_1", "mmsi_2", "start", "end",
            "dur_hrs", "start_lat", "start_lon",
            "end_lat", "end_lon", "avg_sog", "drift_km"
        ])

        # PROCESS DATABASE IN CHUNKS
        for chunk in db_helper._fetch_records_db_by_chunk_global(
            db_name, 
            config.CHUNK_SIZE
        ):
            rows_by_mmsi = defaultdict(list)

            # FILTER AND STORE ONLY ESSENTIAL FIELDS
            for row in chunk:
                ship = ShipRow(*row)

                if ship.sog >= config.SOG_THRESHOLD:
                    continue
                if ship.nav_status in config.IGNORE_STATUS_B:
                    continue
                if ship.vessel_type not in config.VALID_MOBILE_TYPES:
                    continue

                # Store only needed info, not full object
                rows_by_mmsi[ship.mmsi].append(
                    (ship.timestamp, ship.latitude, ship.longitude, ship.sog, ship.vessel_type)
                )

            # UPDATE LOW_SOG_TRACKER
            for mmsi, ships in rows_by_mmsi.items():
                ships.sort(key=lambda x: x[0])  # sort by timestamp
                total_sog = sum(s[3] for s in ships)  # s[3] = sog
                vessel_type = ships[0][4]  # s[4] = vessel_type

                start_ts, _, _, _, _ = ships[0]
                end_ts, end_lat, end_lon, _, _ = ships[-1]

                if mmsi in low_sog_tracker:
                    old = low_sog_tracker[mmsi]
                    low_sog_tracker[mmsi] = LowSOGTrack(
                        start=old.start,
                        end=end_ts,
                        lat=end_lat,
                        lon=end_lon,
                        sog_sum=old.sog_sum + total_sog,
                        count=old.count + len(ships),
                        vessel_type=old.vessel_type
                    )
                else:
                    low_sog_tracker[mmsi] = LowSOGTrack(
                        start=start_ts,
                        end=end_ts,
                        lat=end_lat,
                        lon=end_lon,
                        sog_sum=total_sog,
                        count=len(ships),
                        vessel_type=vessel_type
                    )

            # PROXIMITY CHECK
            cands = list(low_sog_tracker.items())
            for i, (m1, d1) in enumerate(cands):
                for j in range(i + 1, len(cands)):
                    m2, d2 = cands[j]

                    if abs(d1.lat - d2.lat) > 0.01 or abs(d1.lon - d2.lon) > 0.01:
                        continue

                    dist_now = haversine(d1.point, d2.point)

                    if dist_now <= config.PROXIMITY_DIST:
                        pair = tuple(sorted((m1, m2)))
                        curr_t = max(d1.end, d2.end)

                        if pair in proximity_tracker:
                            p = proximity_tracker[pair]
                            gap_minutes = (curr_t - p.end).total_seconds() / 60

                            # FLUSH OLD ENCOUNTER
                            if gap_minutes > config.MAX_GAP_MINUTES:
                                if is_suspicious_encounter(p, config, config.IGNORE_STATUS_B):
                                    mmsi_a = pair[0]
                                    mmsi_b = pair[1]
                                    time_start = db_helper._get_timestamp_str(p.start)
                                    time_end = db_helper._get_timestamp_str(p.end)
                                    time_diff = round((p.end - p.start).total_seconds() / 3600, 2)
                                    avg_sog = round(p.sog_sum / p.count, 2)
                                    dist = round(haversine(p.start_point, p.end_point), 2)
                                    writer.writerow([
                                        mmsi_a, 
                                        mmsi_b,
                                        time_start,
                                        time_end,
                                        time_diff,
                                        p.start_lat, 
                                        p.start_lon,
                                        p.end_lat, 
                                        p.end_lon,
                                        avg_sog,
                                        dist
                                    ])
                                # Reset and keep only latest positions in memory
                                proximity_tracker[pair] = ProximityTrack(
                                    start=curr_t, 
                                    end=curr_t,
                                    start_lat=d1.lat, 
                                    start_lon=d1.lon,
                                    end_lat=d1.lat, 
                                    end_lon=d1.lon,
                                    sog_sum=d1.sog_sum + d2.sog_sum,
                                    count=d1.count + d2.count,
                                    min_dist=dist_now,
                                    max_dist=dist_now,
                                    vessel_type_1=d1.vessel_type,
                                    vessel_type_2=d2.vessel_type
                                )
                            else:
                                # CONTINUE ENCOUNTER
                                p.end = curr_t
                                p.end_lat = d1.lat
                                p.end_lon = d1.lon
                                p.sog_sum += d1.sog_sum + d2.sog_sum
                                p.count += d1.count + d2.count
                                p.min_dist = min(p.min_dist, dist_now)
                                p.max_dist = max(p.max_dist, dist_now)
                        else:
                            # FIRST TIME PAIR
                            proximity_tracker[pair] = ProximityTrack(
                                start=curr_t, 
                                end=curr_t,
                                start_lat=d1.lat, 
                                start_lon=d1.lon,
                                end_lat=d1.lat, 
                                end_lon=d1.lon,
                                sog_sum=d1.sog_sum + d2.sog_sum,
                                count=d1.count + d2.count,
                                min_dist=dist_now,
                                max_dist=dist_now,
                                vessel_type_1=d1.vessel_type,
                                vessel_type_2=d2.vessel_type
                            )

    print("Anomaly B detection complete.")