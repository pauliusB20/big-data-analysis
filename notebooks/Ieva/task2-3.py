"""
AIS Shadow Fleet Detection
===========================
Task 1 : Low-Memory Parallel Partitioning  (your friend's streaming approach)
Task 2 : Parallel Map-Reduce               (your friend's pool pattern + tqdm)
Task 3 : Anomaly A-D + DFSI scoring        (full detection logic)
"""

import csv
import math
import os
from collections import defaultdict, namedtuple
from collections.abc import Iterable
from datetime import datetime
from multiprocessing import Pool, set_start_method
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration  (tune these without touching any logic below)
# ---------------------------------------------------------------------------
FILE_CSV          = "aisdk-2025-02-28.csv"
CHUNK_SIZE        = 1_000       # rows per chunk  ← your friend's safe limit
TASKS_PER_WORKER  = 25          # imap chunksize  ← your friend's pattern
WORKERS           = 7           # worker processes ← your friend's setting
LOG_EVERY         = 1_000       # print a heartbeat every N chunks per worker
REDUCE_BATCH      = 200         # flush the merge buffer every N chunks
                                # — keeps the main process from blocking workers

# --- Anomaly thresholds -----------------------------------------------------
GAP_DARK_MIN_H    = 4.0         # Anomaly A: min AIS gap (hours)
GAP_DARK_MIN_DIST = 1.0         # Anomaly A: min movement during gap (nm)
LOITER_SOG_MAX    = 1.0         # Anomaly B: "stopped" speed threshold (knots)
LOITER_MIN_H      = 2.0         # Anomaly B: min loiter duration (hours)
LOITER_PROX_NM    = 0.27        # Anomaly B: proximity radius (~500 m)
DRAFT_CHANGE_PCT  = 0.05        # Anomaly C: 5 % draught change
DRAFT_GAP_MIN_H   = 2.0         # Anomaly C: min blackout before draft check
CLONE_SPEED_KT    = 60.0        # Anomaly D: impossible speed threshold (knots)
CLONE_MAX_DIST_NM = 3_000.0     # Anomaly D: cap — jumps beyond this are GPS/data
                                 #   artifacts, not real clones (Earth ~10,800 nm
                                 #   across; 3,000 nm ≈ London→Cairo, still extreme)

# --- DFSI weights -----------------------------------------------------------
DFSI_W_GAP   = 0.5              # per max-gap hour
DFSI_W_JUMP  = 0.1              # per total jump nm
DFSI_W_DRAFT = 15.0             # per draft-change event

# ---------------------------------------------------------------------------
# Lightweight record type (your friend's namedtuple idea, extended)
# ---------------------------------------------------------------------------
ShipRow = namedtuple("ShipRow", ["mmsi", "ts", "lat", "lon", "sog", "draught"])

# ---------------------------------------------------------------------------
# Task 1 – Streaming partitioner with dirty-data filtering
# ---------------------------------------------------------------------------
_BAD_MMSI = {"000000000", "111111111", "123456789", "999999999"}

def _is_mmsi_valid(mmsi: str) -> bool:
    """
    Reject non-digits, wrong length, bad first digit,
    known placeholder values, and all-same-digit strings.
    """
    return (
        mmsi.isdigit()
        and len(mmsi) == 9
        and 2 <= int(mmsi[0]) <= 7
        and mmsi not in _BAD_MMSI
        and len(set(mmsi)) > 1
    )

def _read_chunks(file_path: str, chunk_size: int) -> Iterable[list[ShipRow]]:
    """
    Generator — yields lists of at most `chunk_size` validated ShipRow records.
    Reads strictly line-by-line so the full file is never in memory at once.

    Dirty-data guards applied per row:
      - MMSI validity (see _is_mmsi_valid)
      - Timestamp present and parseable
      - Latitude  in [-90,  90]
      - Longitude in [-180, 180]
      - SOG       in [0, 102.2]  (AIS spec maximum)
      - Draught   >= 0
      - All floats must be finite (no inf / NaN)
    """
    chunk: list[ShipRow] = []

    with open(file_path, encoding="utf-8", newline="") as csv_file:
        reader   = csv.reader(csv_file)
        headline = next(reader)

        # Resolve column indices once — fast and order-independent
        mmsi_idx = headline.index("MMSI")
        ts_idx   = headline.index("# Timestamp")
        lat_idx  = headline.index("Latitude")
        lon_idx  = headline.index("Longitude")
        sog_idx  = headline.index("SOG")
        dra_idx  = headline.index("Draught")

        for row in reader:
            # Guard 1: MMSI
            mmsi = row[mmsi_idx].strip()
            if not _is_mmsi_valid(mmsi):
                continue

            # Guard 2: Timestamp
            ts_raw = row[ts_idx].strip()
            if not ts_raw:
                continue
            try:
                ts = datetime.strptime(ts_raw, "%d/%m/%Y %H:%M:%S")
            except ValueError:
                continue

            # Guards 3-7: numeric fields + range checks
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
            if not all(math.isfinite(v) for v in (lat, lon, sog, dra)):
                continue

            chunk.append(ShipRow(mmsi, ts, lat, lon, sog, dra))

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

    if chunk:
        yield chunk

# ---------------------------------------------------------------------------
# Task 2 – Map step (runs inside each worker process)
# ---------------------------------------------------------------------------

def _dedup(recs: list[ShipRow]) -> list[ShipRow]:
    """Remove pings < 5 s apart or at identical position (multi-receiver noise)."""
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
    """
    MAP step — groups rows by MMSI, sorts each vessel's timeline,
    and deduplicates. Doing this inside the worker reduces IPC payload size.
    """
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
# Task 3 – Anomaly detection helpers
# ---------------------------------------------------------------------------

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R    = 3_440.065
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# --- Anomaly A: Going Dark --------------------------------------------------

def detect_anomaly_a(mmsi: str, history: list[ShipRow]) -> list[dict]:
    """AIS gaps > GAP_DARK_MIN_H hours where the vessel clearly kept moving."""
    findings = []
    for i in range(1, len(history)):
        p1, p2 = history[i-1], history[i]
        gap_h = (p2.ts - p1.ts).total_seconds() / 3600.0
        if gap_h <= GAP_DARK_MIN_H:
            continue
        dist = haversine(p1.lat, p1.lon, p2.lat, p2.lon)
        if dist > GAP_DARK_MIN_DIST:
            findings.append({
                "mmsi":      mmsi,
                "gap_hours": round(gap_h, 2),
                "dist_nm":   round(dist, 3),
                "ts_start":  p1.ts, "lat_start": p1.lat, "lon_start": p1.lon,
                "ts_end":    p2.ts, "lat_end":   p2.lat, "lon_end":   p2.lon,
            })
    return findings


# --- Anomaly B: Loitering & Transfers ---------------------------------------

def _build_loiter_windows(mmsi: str, history: list[ShipRow]) -> list[dict]:
    """Collapse consecutive low-speed pings into loiter windows > LOITER_MIN_H."""
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


def _emit_window(mmsi: str, w_start: ShipRow, w_end: ShipRow,
                 out: list[dict]) -> None:
    dur_h = (w_end.ts - w_start.ts).total_seconds() / 3600.0
    if dur_h > LOITER_MIN_H:
        out.append({
            "mmsi":     mmsi,
            "ts_start": w_start.ts,
            "ts_end":   w_end.ts,
            "lat":      (w_start.lat + w_end.lat) / 2,
            "lon":      (w_start.lon + w_end.lon) / 2,
        })


def cross_vessel_loiter_pairs(all_windows: list[dict]) -> list[dict]:
    """
    Find pairs of different-MMSI loiter windows that overlap in time
    and are within LOITER_PROX_NM of each other.

    Uses a latitude band-scan (sort + forward walk) instead of an O(n^2)
    double-loop — only windows within 0.01 degrees latitude are compared
    precisely with haversine.
    """
    LAT_BAND = 0.01     # ~0.6 nm — wide enough to catch all real pairs
    findings: list[dict] = []
    wins = sorted(all_windows, key=lambda w: w["lat"])
    n    = len(wins)

    for i in range(n):
        a = wins[i]
        j = i + 1
        while j < n and (wins[j]["lat"] - a["lat"]) <= LAT_BAND:
            b = wins[j]; j += 1
            if a["mmsi"] == b["mmsi"]:
                continue
            if abs(a["lon"] - b["lon"]) > LAT_BAND:
                continue
            if a["ts_end"] < b["ts_start"] or b["ts_end"] < a["ts_start"]:
                continue
            dist = haversine(a["lat"], a["lon"], b["lat"], b["lon"])
            if dist <= LOITER_PROX_NM:
                findings.append({
                    "mmsi_a":  a["mmsi"], "mmsi_b": b["mmsi"],
                    "dist_nm": round(dist, 4),
                    "ts_a":    f"{a['ts_start']} -> {a['ts_end']}",
                    "ts_b":    f"{b['ts_start']} -> {b['ts_end']}",
                    "lat":     round((a["lat"] + b["lat"]) / 2, 4),
                    "lon":     round((a["lon"] + b["lon"]) / 2, 4),
                })
    return findings


# --- Anomaly C: Draft Changes at Sea ----------------------------------------

def detect_anomaly_c(mmsi: str, history: list[ShipRow]) -> list[dict]:
    """Draught change > DRAFT_CHANGE_PCT during a blackout > DRAFT_GAP_MIN_H."""
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
                "mmsi":           mmsi,
                "draught_before": p1.draught,
                "draught_after":  p2.draught,
                "pct_change":     round(pct * 100, 2),
                "gap_hours":      round(gap_h, 2),
                "ts_start":       p1.ts,
                "ts_end":         p2.ts,
            })
    return findings


# --- Anomaly D: Identity Cloning / Teleportation ----------------------------

def detect_anomaly_d(mmsi: str, history: list[ShipRow]) -> list[dict]:
    """
    Consecutive pings implying speed > CLONE_SPEED_KT (impossible travel).

    Jump distance cap: events where dist_nm > CLONE_MAX_DIST_NM are almost
    certainly corrupt GPS fixes or data-feed artifacts (e.g. a receiver in
    Denmark picking up a spoofed position broadcast from the Pacific).
    They are still recorded but tagged spoofing_artifact=True so the DFSI
    calculation can exclude them from the jump-distance score.
    """
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
                "mmsi":             mmsi,
                "implied_knots":    round(speed, 1),
                "dist_nm":          round(dist, 3),
                "spoofing_artifact": dist > CLONE_MAX_DIST_NM,
                "ts_start":         p1.ts, "lat1": p1.lat, "lon1": p1.lon,
                "ts_end":           p2.ts, "lat2": p2.lat, "lon2": p2.lon,
            })
    return findings


# --- DFSI -------------------------------------------------------------------

def compute_dfsi(mmsi: str, a_list: list[dict],
                 c_list: list[dict], d_list: list[dict]) -> dict:
    max_gap    = max((x["gap_hours"] for x in a_list), default=0.0)
    # Only count jumps that are NOT flagged as data artifacts
    real_clones  = [x for x in d_list if not x.get("spoofing_artifact")]
    total_jump   = sum(x["dist_nm"] for x in real_clones)
    c_count      = len(c_list)
    dfsi = (max_gap * DFSI_W_GAP) + (total_jump * DFSI_W_JUMP) + (c_count * DFSI_W_DRAFT)
    return {
        "mmsi":            mmsi,
        "dfsi":            round(dfsi, 3),
        "max_gap_h":       round(max_gap, 2),
        "total_jump_nm":   round(total_jump, 3),
        "draft_changes":   c_count,
        "going_dark":      len(a_list),
        "clones":          len(real_clones),
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
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    set_start_method("spawn", force=True)

    start_time    = datetime.now()
    global_states: dict[str, list[ShipRow]] = defaultdict(list)
    task_counts:   dict[int, int]            = defaultdict(int)

    print("Starting AIS Shadow Fleet Detection ...\n")

    # ------------------------------------------------------------------
    # Phase 1 + 2: stream -> map (parallel) -> reduce (main process)
    # ------------------------------------------------------------------
    reader_cursor = _read_chunks(FILE_CSV, CHUNK_SIZE)

    with Pool(processes=WORKERS) as pool:
        # Buffer incoming results and flush in batches.
        # Without batching, the main process does a dict lookup + list.extend
        # on EVERY chunk result as it arrives, which becomes the bottleneck
        # when workers are fast — causing the slowdown seen around chunk 13k.
        # Buffering REDUCE_BATCH results first, then merging in one sweep,
        # keeps the main process from falling behind the worker pool.
        buffer: list[dict[str, list[ShipRow]]] = []

        def _flush(buf: list, target: dict) -> None:
            for mapped in buf:
                for mmsi, recs in mapped.items():
                    target[mmsi].extend(recs)
            buf.clear()

        for pid, mapped in tqdm(
            pool.imap_unordered(
                process_ship_chunk, reader_cursor, chunksize=TASKS_PER_WORKER
            ),
            desc="Processing chunks",
            unit="chunk",
        ):
            # Heartbeat logging (your friend's pattern)
            task_counts[pid] += 1
            if task_counts[pid] % LOG_EVERY == 0:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"  PID {pid} finished {task_counts[pid]} chunks at {ts}")

            buffer.append(mapped)
            if len(buffer) >= REDUCE_BATCH:
                _flush(buffer, global_states)

        # Flush any remaining buffered results
        _flush(buffer, global_states)

    # Final cross-chunk sort + dedup (same MMSI can span many chunks)
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

    print(f"\nUnique valid vessels: {len(global_states):,}")

    # ------------------------------------------------------------------
    # Phase 3: anomaly detection
    # ------------------------------------------------------------------
    print("Running anomaly detectors ...")

    all_a:        list[dict] = []
    all_b_wins:   list[dict] = []
    all_c:        list[dict] = []
    all_d:        list[dict] = []
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

    print(f"Loiter windows: {len(all_b_wins):,}  ->  computing cross-vessel pairs ...")
    all_b_pairs = cross_vessel_loiter_pairs(all_b_wins)
    dfsi_results.sort(key=lambda x: x["dfsi"], reverse=True)

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - start_time).total_seconds()

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

    print(f"\nExecution time: {elapsed:.1f} s")

    # ------------------------------------------------------------------
    # CSV output
    # ------------------------------------------------------------------
    print("\nWriting result CSVs ...")
    _write_csv("anomaly_a_going_dark.csv",    all_a)
    _write_csv("anomaly_b_loiter_pairs.csv",  all_b_pairs)
    _write_csv("anomaly_c_draft_change.csv",  all_c)
    _write_csv("anomaly_d_identity_clone.csv", all_d)
    _write_csv("dfsi_scores.csv",             dfsi_results)

    print("\nDONE")
