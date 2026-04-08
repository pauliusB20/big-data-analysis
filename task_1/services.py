import csv
import math
from datetime import datetime
from collections import defaultdict
from haversine import haversine
from models import ShipRow
from helper import DBHelper
from config import Config

def format_ts(ts: datetime) -> str:
    return ts.strftime('%Y-%m-%d %H:%M:%S')

def run_anomaly_b(db_name: str, config: Config) -> None:
    db_helper = DBHelper()
    all_loiter_windows = []    
    
    # We use a dict to group MMSI pings
    vessel_tracks = defaultdict(list)

    # GENERATE LOITER WINDOW
    for chunk in db_helper._fetch_records_db_by_chunk_global(db_name, config.CHUNK_SIZE):
        for row in chunk:
            ship = ShipRow(*row)
            
            # Type and Status Filters
            if ship.vessel_type not in config.VALID_MOBILE_TYPES:
                continue
            if ship.nav_status in config.IGNORE_STATUS_B:
                continue
                
            vessel_tracks[ship.mmsi].append(ship)

    # Convert tracks into candidate loiter windows (start/end of low SOG periods)
    for mmsi, history in vessel_tracks.items():
        # Keep time order for gap/duration
        history.sort(key=lambda x: x.timestamp)
        extract_loiters(history, all_loiter_windows, config)
    
    # Free up memory from raw tracks
    vessel_tracks.clear()

    # FIND CLOSE VESSELS (PROXIMITY)
    findings = find_proximity_pairs(all_loiter_windows, config)

    # WRITE TO CSV
    with open(config.RESULTS_ANOMALY_B, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mmsi_1", "mmsi_2", "start", "end", 
            "dur_hrs", "lat", "lon", "avg_sog", "dist_km"
        ])
        
        for res in findings:
            writer.writerow([
                res['m1'], res['m2'], res['start'], res['end'],
                res['dur'], res['lat'], res['lon'], res['sog'], res['dist']
            ])

def extract_loiters(history, windows, config) -> None:
    """Identifies stationary periods for a single vessel."""
    w_start = None
    w_end = None
    sog_sum = 0
    count = 0

    for ship in history:
        if ship.sog < config.SOG_THRESHOLD:
            if w_start is None:
                w_start = ship
            w_end = ship
            sog_sum += ship.sog
            count += 1
        else:
            if w_start is not None:
                duration = (w_end.timestamp - w_start.timestamp).total_seconds() / 3600
                if duration >= config.B_HOURS:
                    windows.append({
                        "mmsi": w_start.mmsi,
                        "ts_start": w_start.timestamp,
                        "ts_end": w_end.timestamp,
                        "lat": (w_start.latitude + w_end.latitude) / 2,
                        "lon": (w_start.longitude + w_end.longitude) / 2,
                        "sog_avg": sog_sum / count,
                        "start_pos": (w_start.latitude, w_start.longitude),
                        "end_pos": (w_end.latitude, w_end.longitude)
                    })
                w_start = w_end = None
                sog_sum = count = 0

def find_proximity_pairs(windows, config) -> list[dict]:
    """Optimized latitude check"""
    # lat band (same for long) of 0.01 is around 1 km, so vhecks in this band
    LAT_BAND = 0.01 
    findings = []
    
    # Sort by latitude so we only compare ships near each other
    wins = sorted(windows, key=lambda w: w["lat"])
    n = len(wins)

    for i in range(n):
        a = wins[i]
        for j in range(i + 1, n):
            b = wins[j]
            if b["mmsi"] == 245809000:
                import pdb; pdb.set_trace()
 
            
            # If the next ship is further than the band, stop checking
            if (b["lat"] - a["lat"]) > LAT_BAND:
                break
            
            if a["mmsi"] == b["mmsi"]: 
                continue
                
            # Longitude filter
            if abs(a["lon"] - b["lon"]) > LAT_BAND:
                continue
            
            # Time overlap check
            latest_start = max(a["ts_start"], b["ts_start"])
            earliest_end = min(a["ts_end"], b["ts_end"])
            
            if latest_start < earliest_end:
                overlap_dur = (earliest_end - latest_start).total_seconds() / 3600
                
                # Check if stayed together for long enough
                if overlap_dur >= config.B_HOURS:
                    dist = haversine((a["lat"], a["lon"]), (b["lat"], b["lon"]))
                    
                    if dist <= config.PROXIMITY_DIST:
                        # Drift check to avoid harbour activity
                        drift_a = haversine(a['start_pos'], a['end_pos'])
                        drift_b = haversine(b['start_pos'], b['end_pos'])
                        
                        if max(drift_a, drift_b) >= config.MIN_DISPLACEMENT:
                            findings.append({
                                "m1": a["mmsi"], "m2": b["mmsi"],
                                "start": format_ts(latest_start),
                                "end": format_ts(earliest_end),
                                "dur": round(overlap_dur, 2),
                                "lat": round((a["lat"] + b["lat"]) / 2, 5),
                                "lon": round((a["lon"] + b["lon"]) / 2, 5),
                                "sog": round((a["sog_avg"] + b["sog_avg"]) / 2, 2),
                                "dist": round(dist, 3)
                            })
    return findings