

import csv
import os
import sqlite3
import tempfile
from collections import defaultdict, namedtuple
from collections.abc import Iterable, Iterator
from datetime import datetime
from multiprocessing import Pool, set_start_method

import numpy as np
import psutil
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FILE_CSV          = "aisdk-2025-02-28.csv"
CHUNK_SIZE        = 1_000
TASKS_PER_WORKER  = 4           
WORKERS           = 7
LOG_EVERY         = 1_000
DB_DIR            = tempfile.gettempdir()

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
# Memory profiling — psutil RSS (always-on)
# ---------------------------------------------------------------------------
_PROC        = psutil.Process(os.getpid())
_mem_log: list[dict] = []


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
            result[pid] = round(psutil.Process(pid).memory_info().rss / 1024 ** 2, 2)
        except psutil.NoSuchProcess:
            result[pid] = 0.0
    return result


def _print_mem_table() -> None:
    print("\n" + "=" * 58)
    print("MEMORY PROFILE  (main process RSS, psutil)")
    print("=" * 58)
    print(f"  {'Phase':<38} {'RSS (MB)':>8}  Timestamp")
    print("  " + "-" * 55)
    for entry in _mem_log:
        print(f"  {entry['phase']:<38} {entry['rss_mb']:>8.1f}  {entry['timestamp']}")
    print("=" * 58)


def _save_mem_csv(path: str = "memory_profile.csv") -> None:
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
# Task 1 — Streaming file partitioner
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
    """Generator — yields validated ShipRow chunks, never loads the full file."""
    chunk: list[ShipRow] = []
    with open(file_path, encoding="utf-8", newline="") as fh:
        reader   = csv.reader(fh)
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
# Haversine (numpy)
# ---------------------------------------------------------------------------
_R_NM = np.float64(3_440.065)


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = np.radians(lat1);  phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return float(2 * _R_NM * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a)))

# ---------------------------------------------------------------------------
# Pass 1 MAP — write chunk to per-worker SQLite DB
# ---------------------------------------------------------------------------
_worker_db_conn: sqlite3.Connection | None = None
_worker_db_path: str = ""


def _get_worker_db() -> tuple[sqlite3.Connection, str]:
    """Lazily create one SQLite DB per worker process (identified by PID)."""
    global _worker_db_conn, _worker_db_path
    if _worker_db_conn is None:
        pid  = os.getpid()
        path = os.path.join(DB_DIR, f"ais_worker_{pid}.db")
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-32000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pings (
                mmsi    TEXT NOT NULL,
                ts      TEXT NOT NULL,
                lat     REAL NOT NULL,
                lon     REAL NOT NULL,
                sog     REAL NOT NULL,
                draught REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mmsi ON pings(mmsi, ts)")
        conn.commit()
        _worker_db_conn = conn
        _worker_db_path = path
    return _worker_db_conn, _worker_db_path


def write_chunk_to_db(chunk: list[ShipRow]) -> tuple[int, str, int]:
   
    pid = os.getpid()
    conn, path = _get_worker_db()
    with conn:
        conn.executemany(
            "INSERT INTO pings VALUES (?,?,?,?,?,?)",
            [
                (r.mmsi, r.ts.isoformat(), r.lat, r.lon, r.sog, r.draught)
                for r in chunk
                if _is_mmsi_valid(r.mmsi)
            ],
        )
    return pid, path, len(chunk)

# ---------------------------------------------------------------------------
# Pass 2 MAP — anomaly detection for ONE vessel
# ---------------------------------------------------------------------------

def analyse_vessel(args: tuple[str, list[ShipRow]]) -> dict:
   
    mmsi, history = args

    # Anomaly A — Going Dark
    a_list: list[dict] = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        gap_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if gap_h <= GAP_DARK_MIN_H:
            continue
        dist = haversine(p1.lat, p1.lon, p2.lat, p2.lon)
        if dist > GAP_DARK_MIN_DIST:
            a_list.append({
                "mmsi": mmsi, "gap_hours": round(gap_h, 2), "dist_nm": round(dist, 3),
                "ts_start": p1.ts, "lat_start": p1.lat, "lon_start": p1.lon,
                "ts_end":   p2.ts, "lat_end":   p2.lat, "lon_end":   p2.lon,
            })

    # Anomaly B — Loiter windows (cross-vessel pairing done in main process)
    b_wins: list[dict] = []
    w_start = w_end = None
    for rec in history:
        if rec.sog < LOITER_SOG_MAX:
            if w_start is None:
                w_start = rec
            w_end = rec
        else:
            if w_start is not None:
                dur_h = (w_end.ts - w_start.ts).total_seconds() / 3600.0
                if dur_h > LOITER_MIN_H:
                    b_wins.append({
                        "mmsi": mmsi, "ts_start": w_start.ts, "ts_end": w_end.ts,
                        "lat": (w_start.lat + w_end.lat) / 2,
                        "lon": (w_start.lon + w_end.lon) / 2,
                    })
                w_start = w_end = None
    if w_start is not None:
        dur_h = (w_end.ts - w_start.ts).total_seconds() / 3600.0
        if dur_h > LOITER_MIN_H:
            b_wins.append({
                "mmsi": mmsi, "ts_start": w_start.ts, "ts_end": w_end.ts,
                "lat": (w_start.lat + w_end.lat) / 2,
                "lon": (w_start.lon + w_end.lon) / 2,
            })

    # Anomaly C — Draft Change at Sea
    c_list: list[dict] = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        if p1.draught <= 0 or p2.draught <= 0:
            continue
        gap_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if gap_h <= DRAFT_GAP_MIN_H:
            continue
        pct = abs(p2.draught - p1.draught) / p1.draught
        if pct > DRAFT_CHANGE_PCT:
            c_list.append({
                "mmsi": mmsi,
                "draught_before": p1.draught, "draught_after": p2.draught,
                "pct_change": round(pct * 100, 2), "gap_hours": round(gap_h, 2),
                "ts_start": p1.ts, "ts_end": p2.ts,
            })

    # Anomaly D — Identity Cloning
    d_list: list[dict] = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        t_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if t_h <= 0:
            continue
        dist  = haversine(p1.lat, p1.lon, p2.lat, p2.lon)
        speed = dist / t_h
        if speed > CLONE_SPEED_KT:
            d_list.append({
                "mmsi": mmsi, "implied_knots": round(speed, 1),
                "dist_nm": round(dist, 3),
                "spoofing_artifact": dist > CLONE_MAX_DIST_NM,
                "ts_start": p1.ts, "lat1": p1.lat, "lon1": p1.lon,
                "ts_end":   p2.ts, "lat2": p2.lat, "lon2": p2.lon,
            })

    # DFSI
    dfsi_row: dict | None = None
    if a_list or c_list or d_list:
        max_gap    = max((x["gap_hours"] for x in a_list), default=0.0)
        real_clone = [x for x in d_list if not x.get("spoofing_artifact")]
        total_jump = sum(x["dist_nm"] for x in real_clone)
        c_count    = len(c_list)
        dfsi = (max_gap * DFSI_W_GAP) + (total_jump * DFSI_W_JUMP) + (c_count * DFSI_W_DRAFT)
        dfsi_row = {
            "mmsi": mmsi, "dfsi": round(dfsi, 3),
            "max_gap_h": round(max_gap, 2), "total_jump_nm": round(total_jump, 3),
            "draft_changes": c_count, "going_dark": len(a_list),
            "clones": len(real_clone),
            "artifacts_excluded": len(d_list) - len(real_clone),
        }

    return {"a": a_list, "b_wins": b_wins, "c": c_list, "d": d_list, "dfsi": dfsi_row}



def _iter_vessels(db_paths: list[str]) -> Iterator[tuple[str, list[ShipRow]]]:
  
    conns = []
    for path in db_paths:
        if os.path.exists(path):
            c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            c.execute("PRAGMA cache_size=-8000")   # 8 MB per connection
            conns.append(c)

    if not conns:
        return

    # Collect all MMSIs across all DBs (tiny strings only)
    all_mmsi: set[str] = set()
    for c in conns:
        for (mmsi,) in c.execute("SELECT DISTINCT mmsi FROM pings"):
            all_mmsi.add(mmsi)

    for mmsi in all_mmsi:
        rows: list[ShipRow] = []
        for c in conns:
            cur = c.execute(
                "SELECT ts, lat, lon, sog, draught FROM pings WHERE mmsi=? ORDER BY ts",
                (mmsi,),
            )
            for ts_str, lat, lon, sog, dra in cur:
                rows.append(ShipRow(mmsi, datetime.fromisoformat(ts_str),
                                    lat, lon, sog, dra))

        # Sort + dedup across all DB partials
        rows.sort(key=lambda r: r.ts)
        if not rows:
            continue
        kept = [rows[0]]
        for r in rows[1:]:
            prev = kept[-1]
            if (r.ts - prev.ts).total_seconds() < 5:
                continue
            if r.lat == prev.lat and r.lon == prev.lon:
                continue
            kept.append(r)

        yield mmsi, kept
        # 'rows' and 'kept' go out of scope here — GC can reclaim immediately

    for c in conns:
        c.close()

# ---------------------------------------------------------------------------
# Anomaly B — cross-vessel loiter pairs
# ---------------------------------------------------------------------------

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
# Main pipeline
# ---------------------------------------------------------------------------

def _run() -> None:
    print("Starting AIS Shadow Fleet Detection ...\n")
    _snap("01_startup_baseline")

    # -----------------------------------------------------------------------
    # PASS 1 — Stream rows -> write to per-worker SQLite DBs
    # Main process RAM: flat ~70 MB throughout
    # -----------------------------------------------------------------------
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)
    _snap("02_before_pass1_pool")

    task_counts:  dict[int, int] = defaultdict(int)
    db_path_set:  set[str]       = set()
    pool_pids_p1: list[int]      = []
    chunks_done   = 0

    with Pool(processes=WORKERS) as pool:
        pool_pids_p1 = [p.pid for p in pool._pool]  # type: ignore[attr-defined]
        _snap("03_pass1_pool_spawned")

        for pid, db_path, _ in tqdm(
            pool.imap_unordered(write_chunk_to_db, reader_cursor,
                                chunksize=TASKS_PER_WORKER),
            desc="Pass 1 – writing to DB",
            unit="chunk",
        ):
            task_counts[pid] += 1
            db_path_set.add(db_path)
            chunks_done += 1
            if task_counts[pid] % LOG_EVERY == 0:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"  PID {pid} finished {task_counts[pid]} chunks at {ts}")
            if chunks_done % 2000 == 0:
                _snap(f"04_pass1_chunk_{chunks_done}")

    _snap("05_pass1_pool_closed")
    worker_mem_p1 = _worker_rss(pool_pids_p1)

    db_paths = sorted(db_path_set)
    print(f"\nWorker DB files: {[os.path.basename(p) for p in db_paths]}")

    # Count distinct vessels without loading any pings
    total_mmsi = 0
    seen: set[str] = set()
    for path in db_paths:
        if os.path.exists(path):
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            for (m,) in conn.execute("SELECT DISTINCT mmsi FROM pings"):
                seen.add(m)
            conn.close()
    total_mmsi = len(seen)
    del seen
    print(f"Unique valid vessels: {total_mmsi:,}")

    # -----------------------------------------------------------------------
    # PASS 2 — Stream one vessel at a time from DBs -> detect anomalies
   
    # -----------------------------------------------------------------------
    print("Running anomaly detectors (Pass 2) ...")
    _snap("06_before_pass2_pool")

    all_a:        list[dict] = []
    all_b_wins:   list[dict] = []
    all_c:        list[dict] = []
    all_d:        list[dict] = []
    dfsi_results: list[dict] = []

    pool_pids_p2: list[int] = []

    with Pool(processes=WORKERS) as pool:
        pool_pids_p2 = [p.pid for p in pool._pool]  # type: ignore[attr-defined]
        _snap("07_pass2_pool_spawned")

        vessel_stream = _iter_vessels(db_paths)   # generator — no data loaded yet

        for result in tqdm(
            pool.imap_unordered(analyse_vessel, vessel_stream,
                                chunksize=TASKS_PER_WORKER),
            desc="Pass 2 – analysing vessels",
            unit="vessel",
            total=total_mmsi,
        ):
            all_a.extend(result["a"])
            all_b_wins.extend(result["b_wins"])
            all_c.extend(result["c"])
            all_d.extend(result["d"])
            if result["dfsi"]:
                dfsi_results.append(result["dfsi"])

            # Snapshot every 500 vessels to show flat memory during Pass 2
            if (len(dfsi_results) % 50) == 0:
                _snap(f"07b_pass2_vessels_processed")

    _snap("08_pass2_pool_closed")
    worker_mem_p2 = _worker_rss(pool_pids_p2)

    # Clean up worker DB files
    for p in db_paths:
        try:
            os.remove(p)
        except OSError:
            pass

    print(f"Loiter windows: {len(all_b_wins):,}  ->  computing cross-vessel pairs ...")
    all_b_pairs = cross_vessel_loiter_pairs(all_b_wins)
    dfsi_results.sort(key=lambda x: x["dfsi"], reverse=True)
    _snap("09_after_loiter_pairs")

    # -----------------------------------------------------------------------
    # Report
    # -----------------------------------------------------------------------
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

    def _worker_summary(label: str, mem: dict[int, float]) -> None:
        if not mem:
            return
        print(f"\nWorker RSS — {label}:")
        for wpid, mb in sorted(mem.items()):
            flag = "" if mb <= 1024 else "  WARNING: >1 GB!"
            print(f"  PID {wpid:>7}  {mb:>7.1f} MB{flag}")

    _worker_summary("Pass 1 (writing to DB)", worker_mem_p1)
    _worker_summary("Pass 2 (anomaly detection)", worker_mem_p2)

    print(f"\nExecution time: {elapsed:.1f} s")

    _snap("10_final")
    _print_mem_table()

    # -----------------------------------------------------------------------
    # CSV output
    # -----------------------------------------------------------------------
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
