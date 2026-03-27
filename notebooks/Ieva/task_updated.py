"""
AIS Shadow Fleet Detection
===========================

"""

import csv
import os
import sys
from collections import defaultdict, namedtuple
from collections.abc import Iterable
from datetime import datetime
from multiprocessing import Pool, set_start_method

import numpy as np
import psutil
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration  (tune these without touching any logic below)
# ---------------------------------------------------------------------------
FILE_CSV          = "aisdk-2025-02-28.csv"
CHUNK_SIZE        = 1_000       
TASKS_PER_WORKER  = 25         
WORKERS           = 7           
LOG_EVERY         = 1_000       
REDUCE_BATCH      = 200         

# --- Anomaly thresholds -----------------------------------------------------
GAP_DARK_MIN_H    = 4.0
GAP_DARK_MIN_DIST = 1.0
LOITER_SOG_MAX    = 1.0
LOITER_MIN_H      = 2.0
LOITER_PROX_NM    = 0.27
DRAFT_CHANGE_PCT  = 0.05
DRAFT_GAP_MIN_H   = 2.0
CLONE_SPEED_KT    = 60.0
CLONE_MAX_DIST_NM = 3_000.0

# --- DFSI weights -----------------------------------------------------------
DFSI_W_GAP   = 0.5
DFSI_W_JUMP  = 0.1
DFSI_W_DRAFT = 15.0

# ---------------------------------------------------------------------------
# Memory profiling helpers  — Approach A: psutil
# ---------------------------------------------------------------------------
_PROC        = psutil.Process(os.getpid())
_mem_log: list[dict] = []          # [{phase, rss_mb, timestamp}, ...]


def _snap(phase: str) -> float:
   
    rss_mb = _PROC.memory_info().rss / 1024 ** 2
    _mem_log.append({
        "phase":     phase,
        "rss_mb":    round(rss_mb, 2),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    })
    return rss_mb


def _worker_rss(pool_pids: list[int]) -> dict[int, float]:
 
    result = {}
    for pid in pool_pids:
        try:
            rss_mb = psutil.Process(pid).memory_info().rss / 1024 ** 2
            result[pid] = round(rss_mb, 2)
        except psutil.NoSuchProcess:
            result[pid] = 0.0          # worker already cleaned up
    return result


def _print_mem_table() -> None:
    """Print the accumulated memory snapshots as a formatted table."""
    print("\n" + "=" * 58)
    print("MEMORY PROFILE  (main process RSS, psutil)")
    print("=" * 58)
    print(f"  {'Phase':<38} {'RSS (MB)':>8}  Timestamp")
    print("  " + "-" * 55)
    for entry in _mem_log:
        print(f"  {entry['phase']:<38} {entry['rss_mb']:>8.1f}  {entry['timestamp']}")
    print("=" * 58)


def _save_mem_csv(path: str = "memory_profile.csv") -> None:
    """Write the memory log to CSV — import into Excel / pandas for plotting."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["phase", "rss_mb", "timestamp"])
        writer.writeheader()
        writer.writerows(_mem_log)
    print(f"  -> memory profile saved to {path}")


# ---------------------------------------------------------------------------
# Lightweight record type
# ---------------------------------------------------------------------------
ShipRow = namedtuple("ShipRow", ["mmsi", "ts", "lat", "lon", "sog", "draught"])

# ---------------------------------------------------------------------------
# Task 1 - Streaming partitioner with dirty-data filtering
# ---------------------------------------------------------------------------
_BAD_MMSI = {"000000000", "111111111", "123456789", "999999999"}


def _is_mmsi_valid(mmsi: str) -> bool:
    return (
        mmsi.isdigit()
        and len(mmsi) == 9
        and 2 <= int(mmsi[0]) <= 7
        and mmsi not in _BAD_MMSI
        and len(set(mmsi)) > 1
    )


def _read_chunks(file_path: str, chunk_size: int) -> Iterable[list[ShipRow]]:
 
    chunk: list[ShipRow] = []

    with open(file_path, encoding="utf-8", newline="") as csv_file:
        reader   = csv.reader(csv_file)
        headline = next(reader)

        mmsi_idx = headline.index("MMSI")
        ts_idx   = headline.index("# Timestamp")
        lat_idx  = headline.index("Latitude")
        lon_idx  = headline.index("Longitude")
        sog_idx  = headline.index("SOG")
        dra_idx  = headline.index("Draught")

        for row in reader:
            mmsi = row[mmsi_idx].strip()
            if not _is_mmsi_valid(mmsi):
                continue

            ts_raw = row[ts_idx].strip()
            if not ts_raw:
                continue
            try:
                ts = datetime.strptime(ts_raw, "%d/%m/%Y %H:%M:%S")
            except ValueError:
                continue

            try:
                lat = float(row[lat_idx])
                lon = float(row[lon_idx])
                sog = float(row[sog_idx] or 0)
                dra = float(row[dra_idx] or 0)
            except (ValueError, IndexError):
                continue

            if not (-90  <= lat <= 90):    continue
            if not (-180 <= lon <= 180):   continue
            if not (0    <= sog <= 102.2): continue
            if dra < 0:                    continue
            if not np.all(np.isfinite([lat, lon, sog, dra])):
                continue

            chunk.append(ShipRow(mmsi, ts, lat, lon, sog, dra))

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

    if chunk:
        yield chunk

# ---------------------------------------------------------------------------
# Task 2 - Map step
# ---------------------------------------------------------------------------

def _dedup(recs: list[ShipRow]) -> list[ShipRow]:
    if not recs:
        return []
    kept = [recs[0]]
    for r in recs[1:]:
        prev = kept[-1]
        if (r.ts - prev.ts).total_seconds() < 5:
            continue
        if r.lat == prev.lat and r.lon == prev.lon:
            continue
        kept.append(r)
    return kept


def process_ship_chunk(chunk: list[ShipRow]) -> tuple[int, dict[str, list[ShipRow]]]:
    
    pid   = os.getpid()
    ships: dict[str, list[ShipRow]] = defaultdict(list)

    for row in chunk:
        ships[row.mmsi].append(row)

    result: dict[str, list[ShipRow]] = {}
    for mmsi, recs in ships.items():
        recs.sort(key=lambda r: r.ts)
        deduped = _dedup(recs)
        if deduped:
            result[mmsi] = deduped

    return pid, result

# ---------------------------------------------------------------------------
# Task 3 - Anomaly detection helpers
# ---------------------------------------------------------------------------
_R_NM = np.float64(3_440.065)


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    
    phi1 = np.radians(lat1);  phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return float(2 * _R_NM * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a)))


def detect_anomaly_a(mmsi: str, history: list[ShipRow]) -> list[dict]:
    findings = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        gap_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if gap_h <= GAP_DARK_MIN_H:
            continue
        dist = haversine(p1.lat, p1.lon, p2.lat, p2.lon)
        if dist > GAP_DARK_MIN_DIST:
            findings.append({
                "mmsi": mmsi, "gap_hours": round(gap_h, 2), "dist_nm": round(dist, 3),
                "ts_start": p1.ts, "lat_start": p1.lat, "lon_start": p1.lon,
                "ts_end":   p2.ts, "lat_end":   p2.lat, "lon_end":   p2.lon,
            })
    return findings


def _build_loiter_windows(mmsi: str, history: list[ShipRow]) -> list[dict]:
    windows: list[dict] = []
    w_start = w_end = None
    for rec in history:
        if rec.sog < LOITER_SOG_MAX:
            if w_start is None:
                w_start = rec
            w_end = rec
        else:
            if w_start is not None:
                _emit_window(mmsi, w_start, w_end, windows)
                w_start = w_end = None
    if w_start is not None:
        _emit_window(mmsi, w_start, w_end, windows)
    return windows


def _emit_window(mmsi, w_start, w_end, out):
    dur_h = (w_end.ts - w_start.ts).total_seconds() / 3600.0
    if dur_h > LOITER_MIN_H:
        out.append({
            "mmsi": mmsi, "ts_start": w_start.ts, "ts_end": w_end.ts,
            "lat": (w_start.lat + w_end.lat) / 2,
            "lon": (w_start.lon + w_end.lon) / 2,
        })


def cross_vessel_loiter_pairs(all_windows: list[dict]) -> list[dict]:
    LAT_BAND = 0.01
    findings: list[dict] = []
    wins = sorted(all_windows, key=lambda w: w["lat"])
    n    = len(wins)
    for i in range(n):
        a = wins[i]; j = i + 1
        while j < n and (wins[j]["lat"] - a["lat"]) <= LAT_BAND:
            b = wins[j]; j += 1
            if a["mmsi"] == b["mmsi"]:                                      continue
            if abs(a["lon"] - b["lon"]) > LAT_BAND:                        continue
            if a["ts_end"] < b["ts_start"] or b["ts_end"] < a["ts_start"]: continue
            dist = haversine(a["lat"], a["lon"], b["lat"], b["lon"])
            if dist <= LOITER_PROX_NM:
                findings.append({
                    "mmsi_a": a["mmsi"], "mmsi_b": b["mmsi"],
                    "dist_nm": round(dist, 4),
                    "ts_a": f"{a['ts_start']} -> {a['ts_end']}",
                    "ts_b": f"{b['ts_start']} -> {b['ts_end']}",
                    "lat": round((a["lat"] + b["lat"]) / 2, 4),
                    "lon": round((a["lon"] + b["lon"]) / 2, 4),
                })
    return findings


def detect_anomaly_c(mmsi: str, history: list[ShipRow]) -> list[dict]:
    findings = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        if p1.draught <= 0 or p2.draught <= 0:
            continue
        gap_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if gap_h <= DRAFT_GAP_MIN_H:
            continue
        pct = abs(p2.draught - p1.draught) / p1.draught
        if pct > DRAFT_CHANGE_PCT:
            findings.append({
                "mmsi": mmsi,
                "draught_before": p1.draught, "draught_after": p2.draught,
                "pct_change": round(pct * 100, 2), "gap_hours": round(gap_h, 2),
                "ts_start": p1.ts, "ts_end": p2.ts,
            })
    return findings


def detect_anomaly_d(mmsi: str, history: list[ShipRow]) -> list[dict]:
    findings = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        t_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if t_h <= 0:
            continue
        dist  = haversine(p1.lat, p1.lon, p2.lat, p2.lon)
        speed = dist / t_h
        if speed > CLONE_SPEED_KT:
            findings.append({
                "mmsi": mmsi, "implied_knots": round(speed, 1),
                "dist_nm": round(dist, 3),
                "spoofing_artifact": dist > CLONE_MAX_DIST_NM,
                "ts_start": p1.ts, "lat1": p1.lat, "lon1": p1.lon,
                "ts_end":   p2.ts, "lat2": p2.lat, "lon2": p2.lon,
            })
    return findings


def compute_dfsi(mmsi, a_list, c_list, d_list):
    max_gap     = max((x["gap_hours"] for x in a_list), default=0.0)
    real_clones = [x for x in d_list if not x.get("spoofing_artifact")]
    total_jump  = sum(x["dist_nm"] for x in real_clones)
    c_count     = len(c_list)
    dfsi = (max_gap * DFSI_W_GAP) + (total_jump * DFSI_W_JUMP) + (c_count * DFSI_W_DRAFT)
    return {
        "mmsi": mmsi, "dfsi": round(dfsi, 3),
        "max_gap_h": round(max_gap, 2), "total_jump_nm": round(total_jump, 3),
        "draft_changes": c_count, "going_dark": len(a_list),
        "clones": len(real_clones),
        "artifacts_excluded": len(d_list) - len(real_clones),
    }

# ---------------------------------------------------------------------------
# CSV output helper
# ---------------------------------------------------------------------------

def _write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        print(f"  (no data for {path})")
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {len(rows):,} rows  ->  {path}")

# ---------------------------------------------------------------------------
# Main pipeline (split into _run() so memray can wrap it cleanly)
# ---------------------------------------------------------------------------

def _run() -> None:
    global_states: dict[str, list[ShipRow]] = defaultdict(list)
    task_counts:   dict[int, int]            = defaultdict(int)

    print("Starting AIS Shadow Fleet Detection ...\n")

    # Snapshot 1: baseline - just imports + empty containers
    _snap("01_startup_baseline")

    # ------------------------------------------------------------------
    # Phase 1 + 2: stream -> map (parallel) -> reduce (main process)
    # ------------------------------------------------------------------
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    _snap("02_before_pool_open")

    pool_pids: list[int] = []

    with Pool(processes=WORKERS) as pool:
        # Collect worker PIDs for later RSS query
        pool_pids = [p.pid for p in pool._pool]   # type: ignore[attr-defined]
        _snap("03_pool_open_workers_spawned")

        buffer: list[dict[str, list[ShipRow]]] = []

        def _flush(buf: list, target: dict) -> None:
            for mapped in buf:
                for mmsi, recs in mapped.items():
                    target[mmsi].extend(recs)
            buf.clear()

        chunks_done = 0
        for pid, mapped in tqdm(
            pool.imap_unordered(
                process_ship_chunk, reader_cursor, chunksize=TASKS_PER_WORKER
            ),
            desc="Processing chunks",
            unit="chunk",
        ):
            task_counts[pid] += 1
            if task_counts[pid] % LOG_EVERY == 0:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"  PID {pid} finished {task_counts[pid]} chunks at {ts}")

            buffer.append(mapped)
            chunks_done += 1

            if len(buffer) >= REDUCE_BATCH:
                _flush(buffer, global_states)
                # Snapshot after each reduce flush - shows how global_states
                # grows in memory as more vessel histories are merged in
                _snap(f"04_reduce_flush_chunk_{chunks_done}")

        _flush(buffer, global_states)

    # Snapshot after pool closes - workers' pages released back to OS
    _snap("05_after_pool_closed")

    # Read worker RSS before the OS fully reclaims their pages
    worker_mem = _worker_rss(pool_pids)

    # Final cross-chunk sort + dedup
    for mmsi in list(global_states):
        recs = global_states[mmsi]
        recs.sort(key=lambda r: r.ts)
        kept = [recs[0]]
        for r in recs[1:]:
            prev = kept[-1]
            if (r.ts - prev.ts).total_seconds() < 5:
                continue
            if r.lat == prev.lat and r.lon == prev.lon:
                continue
            kept.append(r)
        global_states[mmsi] = kept

    _snap("06_after_global_dedup")
    print(f"\nUnique valid vessels: {len(global_states):,}")

    # ------------------------------------------------------------------
    # Phase 3: anomaly detection
    # ------------------------------------------------------------------
    print("Running anomaly detectors ...")
    _snap("07_before_anomaly_detection")

    all_a: list[dict] = []
    all_b_wins: list[dict] = []
    all_c: list[dict] = []
    all_d: list[dict] = []
    dfsi_results: list[dict] = []

    for mmsi, history in tqdm(
        global_states.items(), desc="Analysing vessels", unit="vessel"
    ):
        a = detect_anomaly_a(mmsi, history)
        b = _build_loiter_windows(mmsi, history)
        c = detect_anomaly_c(mmsi, history)
        d = detect_anomaly_d(mmsi, history)
        all_a.extend(a)
        all_b_wins.extend(b)
        all_c.extend(c)
        all_d.extend(d)
        if a or c or d:
            dfsi_results.append(compute_dfsi(mmsi, a, c, d))

    _snap("08_after_anomaly_detection")

    print(f"Loiter windows: {len(all_b_wins):,}  ->  computing cross-vessel pairs ...")
    all_b_pairs = cross_vessel_loiter_pairs(all_b_wins)
    dfsi_results.sort(key=lambda x: x["dfsi"], reverse=True)
    _snap("09_after_loiter_pairs")

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    t0      = datetime.fromisoformat(_mem_log[0]["timestamp"])
    elapsed = (datetime.now() - t0).total_seconds()

    print("\n" + "=" * 60)
    print("SHADOW FLEET REPORT")
    print("=" * 60)
    print(f"  Anomaly A  Going Dark:        {len(all_a):>7,} events")
    print(f"  Anomaly B  Loitering Pairs:   {len(all_b_pairs):>7,} vessel pairs")
    print(f"  Anomaly C  Draft Change:      {len(all_c):>7,} events")
    print(f"  Anomaly D  Identity Clone:    {len(all_d):>7,} events")
    print(f"  Flagged vessels (DFSI > 0):   {len(dfsi_results):>7,}")

    print("\nTop 10 suspects by DFSI:")
    print(f"  {'MMSI':<12} {'DFSI':>8}  {'MaxGap(h)':>10}  "
          f"{'JumpNM':>8}  {'DraftD':>7}  {'Dark':>6}  {'Clone':>6}  {'Artifact':>9}")
    print("  " + "-" * 78)
    for v in dfsi_results[:10]:
        print(f"  {v['mmsi']:<12} {v['dfsi']:>8.2f}  {v['max_gap_h']:>10.2f}  "
              f"{v['total_jump_nm']:>8.2f}  {v['draft_changes']:>7}  "
              f"{v['going_dark']:>6}  {v['clones']:>6}  {v['artifacts_excluded']:>9}")

    # Worker memory summary
    if worker_mem:
        print("\nWorker process memory (RSS queried at pool teardown):")
        for wpid, mb in sorted(worker_mem.items()):
            flag = "" if mb <= 1024 else "  WARNING: >1 GB!"
            print(f"  PID {wpid:>7}  {mb:>7.1f} MB{flag}")

    print(f"\nExecution time: {elapsed:.1f} s")

    # Memory profile table + CSV
    _snap("10_final")
    _print_mem_table()

    # ------------------------------------------------------------------
    # CSV output
    # ------------------------------------------------------------------
    print("\nWriting result CSVs ...")
    _write_csv("anomaly_a_going_dark.csv",     all_a)
    _write_csv("anomaly_b_loiter_pairs.csv",   all_b_pairs)
    _write_csv("anomaly_c_draft_change.csv",   all_c)
    _write_csv("anomaly_d_identity_clone.csv", all_d)
    _write_csv("dfsi_scores.csv",              dfsi_results)
    _save_mem_csv("memory_profile.csv")

    print("\nDONE")


if __name__ == "__main__":
    set_start_method("spawn", force=True)


    USE_MEMRAY = os.environ.get("MEMRAY", "0") == "1"

    if USE_MEMRAY:
        try:
            import memray
            MEMRAY_OUTPUT = "memray_ais.bin"
            print(f"[memray] Tracing enabled  ->  {MEMRAY_OUTPUT}")
            print("[memray] After run:  memray flamegraph memray_ais.bin")
            print("[memray]             memray summary   memray_ais.bin\n")
            with memray.Tracker(MEMRAY_OUTPUT, native_traces=True):
                _run()
        except ImportError:
            print("[memray] Not installed - falling back to psutil-only profiling.")
            print("[memray] Install with:  pip install memray\n")
            _run()
    else:
        _run()
