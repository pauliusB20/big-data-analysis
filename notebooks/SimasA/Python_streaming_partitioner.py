import multiprocessing as mp
import csv
import math
from datetime import datetime
import zlib
from collections import defaultdict
import time
import resource

# --- CONFIGURATION ---
N_WORKERS = 4
INVALID_MMSI = {"000000000", "111111111", "123456789"}

# Analysis Thresholds
MIN_ENCOUNTER_DURATION_SEC = 7200  # 2 hours
MAX_TIME_DIFF_SECONDS = 3600  # 1 hour
GAP_TIME_THRESHOLD_MIN = 240  # 4 hours
GAP_DIST_THRESHOLD_KM = 1.0  # 1 kilometer

# Index Mapping
IDX_TIMESTAMP = 0
IDX_MMSI = 2
IDX_LAT = 3
IDX_LON = 4
IDX_SOG = 7
IDX_DRAFT=18


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def parse_time(ts_str):
    return datetime.strptime(ts_str.strip(), "%d/%m/%Y %H:%M:%S")


def is_valid_row(row):
    try:
        if len(row) < 8: return False
        mmsi = row[IDX_MMSI].strip()
        if mmsi in INVALID_MMSI or not mmsi.isdigit(): return False
        float(row[IDX_LAT]), float(row[IDX_LON]), float(row[IDX_SOG])
        return True
    except:
        return False


def worker_process(queue, results_queue, worker_id, mode):
    vessel_tracks = defaultdict(list)
    active_encounters = {}
    last_seen_gap = {}
    chunk_count = 0
    total_processed = 0

    print(f"[Worker {worker_id}] Initialized in '{mode}' mode.")

    while True:
        chunk = queue.get() #Recieve data from the queue
        if chunk is None: #If there is no more data shut the process down.
            print(
                f"[Worker {worker_id}] Received shutdown signal. Final ship count: {len(vessel_tracks) if mode == 'loitering' else len(last_seen_gap)}")
            break

        chunk_count += 1
        total_processed += len(chunk)

        # Heartbeat every 10 chunks
        if chunk_count % 10 == 0:
            print(
                f"[Worker {worker_id}] Processed {total_processed} rows. Tracking {len(vessel_tracks) if mode == 'loitering' else len(last_seen_gap)} vessels.")

        if mode == "gap": #Calculating the gap
            for row in chunk:
                try:
                    mmsi = row[IDX_MMSI].strip() ##MMSI
                    curr_t = parse_time(row[IDX_TIMESTAMP]) #Current time
                    curr_lat, curr_lon = float(row[IDX_LAT]), float(row[IDX_LON])#Current position

                    if mmsi in last_seen_gap:
                        prev = last_seen_gap[mmsi] #Previous location of MMSI
                        if curr_t > prev["time"]:
                            tdelta = (curr_t - prev["time"]).total_seconds() #Calculate the time between instances
                            if tdelta >= (GAP_TIME_THRESHOLD_MIN * 60):
                                dist = haversine(prev["lat"], prev["lon"], curr_lat, curr_lon) #See if distance os not 0
                                if (dist / 1000) > GAP_DIST_THRESHOLD_KM:
                                    print(
                                        f"  [GAP DETECTED] Worker {worker_id} | MMSI: {mmsi} | {round(tdelta / 3600, 1)} hrs")
                                    results_queue.put({
                                        "type": "GAP", "mmsi": mmsi,
                                        "gap_hours": round(tdelta / 3600, 2),
                                        "dist_km": round(dist / 1000, 2),
                                        "start": prev["time"], "end": curr_t
                                    }) #Put the data into multiprocesser
                    last_seen_gap[mmsi] = {"time": curr_t, "lat": curr_lat, "lon": curr_lon} #Update the list
                except:
                    continue

        elif mode == "loitering": #Detecting loitering
            for row in chunk: #Iterate through each row and get data
                try:
                    mmsi = row[IDX_MMSI].strip()
                    dt = parse_time(row[IDX_TIMESTAMP])
                    lat, lon, sog = float(row[IDX_LAT]), float(row[IDX_LON]), float(row[IDX_SOG])
                    vessel_tracks[mmsi].append({"time": dt, "lat": lat, "lon": lon, "sog": sog})
                except:
                    continue

            mmsis = list(vessel_tracks.keys()) #All the unique ships in this chunk
            for i in range(len(mmsis)):
                for j in range(i + 1, len(mmsis)):
                    m1, m2 = mmsis[i], mmsis[j] #Take a pair of vessels
                    p1, p2 = vessel_tracks[m1][-1], vessel_tracks[m2][-1] #Take the last position of the vessels

                    if abs((p1['time'] - p2['time']).total_seconds()) > MAX_TIME_DIFF_SECONDS: #If the difference is more than 1 hours we can ignore the ships
                        continue

                    if p1['sog'] < 2.0 and p2['sog'] < 2.0: #Check the ships speed
                        dist = haversine(p1['lat'], p1['lon'], p2['lat'], p2['lon'])
                        if dist <= 500: #Check their distance
                            pair = tuple(sorted([m1, m2]))
                            if pair not in active_encounters:
                                print(f"    [*] Potential Encounter Started: {m1} & {m2} (Worker {worker_id})")
                                active_encounters[pair] = {"start": p1['time'], "reported": False} #Start tracking the ship
                            else:
                                duration = (p1['time'] - active_encounters[pair]["start"]).total_seconds() #Check how long they have been together
                                if duration >= MIN_ENCOUNTER_DURATION_SEC and not active_encounters[pair]["reported"]:
                                    print(
                                        f"    [!!!] TARGET ENCOUNTER: {m1} & {m2} (Duration: {round(duration / 3600, 1)}h)")
                                    results_queue.put({
                                        "type": "ENCOUNTER", "mmsi_a": pair[0], "mmsi_b": pair[1],
                                        "duration_hrs": round(duration / 3600, 2), "lat": p1['lat']
                                    }) #Print the loitering event
                                    active_encounters[pair]["reported"] = True
                        else:
                            active_encounters.pop(tuple(sorted([m1, m2])), None) #If they are moving away we can remove them
            #Delete the previous data points for a ship
            for m in mmsis:
                vessel_tracks[m] = vessel_tracks[m][-1:]
        elif mode == "draft":  # Detecting draft
            for row in chunk:
                try:
                    mmsi = row[IDX_MMSI].strip() #Getting MMSI
                    curr_t = parse_time(row[IDX_TIMESTAMP]) #Getting current time

                    val = row[IDX_DRAFT].strip() #Get draft

                    if not val: continue

                    curr_draft = float(val) #Current draft

                    if mmsi in last_seen_gap:
                        prev = last_seen_gap[mmsi] #Update the previous value with the new one

                        #Only compare if the row is newer
                        if curr_t > prev["time"]:
                            tdelta = (curr_t - prev["time"]).total_seconds() #Look at black out


                            if tdelta > 7200:
                                #Check if drought changed by 5%
                                if prev["draft"] > 0:
                                    change_amount = abs(curr_draft - prev["draft"])
                                    percent_change = (change_amount / prev["draft"]) * 100

                                    if percent_change > 5.0:
                                        print(
                                            f"  [DRAFT ANOMALY] MMSI: {mmsi} | Change: {round(percent_change, 2)}% | Gap: {round(tdelta / 3600, 1)}h")
                                        results_queue.put({
                                            "type": "DRAFT_ANOMALY",
                                            "mmsi": mmsi,
                                            "old_draft": prev["draft"],
                                            "new_draft": curr_draft,
                                            "percent": round(percent_change, 2),
                                            "gap_hrs": round(tdelta / 3600, 2)
                                        })

                    # Update state for this ship
                    last_seen_gap[mmsi] = {"time": curr_t, "draft": curr_draft}
                except (ValueError, IndexError):
                    continue



def stream_partition(file_path, mode):
    final_list = []
    queues = [mp.Queue(maxsize=50) for _ in range(N_WORKERS)]
    results_queue = mp.Queue()
    total_rows_read = 0

    def drain():
        while not results_queue.empty():
            try:
                final_list.append(results_queue.get_nowait())
            except:
                break

    processes = []
    for i in range(N_WORKERS):
        p = mp.Process(target=worker_process, args=(queues[i], results_queue, i, mode))
        p.start()
        processes.append(p)

    print(f"[Main] Reading file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        buffers = [[] for _ in range(N_WORKERS)]

        for row in reader:
            if not is_valid_row(row): continue
            total_rows_read += 1

            if total_rows_read % 100000 == 0:
                print(f"[Main] Rows distributed: {total_rows_read}...")

            if mode == "gap" or mode == "draft":
                worker_id = int(row[IDX_MMSI]) % N_WORKERS
            else:
                grid_key = f"{round(float(row[IDX_LAT]), 1)}_{round(float(row[IDX_LON]), 1)}"
                worker_id = zlib.adler32(grid_key.encode()) % N_WORKERS

            buffers[worker_id].append(row)
            if len(buffers[worker_id]) >= 5000:
                queues[worker_id].put(buffers[worker_id])
                buffers[worker_id] = []
                drain()

    print(f"[Main] End of file reached. Flushing remaining buffers...")
    for i in range(N_WORKERS):
        if buffers[i]: queues[i].put(buffers[i])
        queues[i].put(None)

    for p in processes: p.join()
    drain()
    return final_list


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    FILE = "aisdk-2025-02-28.csv"
    RUN_MODE = "draft"  # Set to "gap" , "loitering",draft

    start_time = time.time()
    results = stream_partition(FILE, mode=RUN_MODE)
    total_time = time.time() - start_time

    print(f"\n--- {RUN_MODE.upper()} ANALYSIS COMPLETE ---")
    print(f"Time elapsed: {round(total_time, 2)} seconds")
    print(f"Total events detected: {len(results)}")

    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    if hasattr(resource, 'linux_distribution'):
        peak_gb = peak_memory / (1024 * 1024)
    else:
        peak_gb = peak_memory / (1024 * 1024 * 1024)

    print(f"Peak Memory Usage: {round(peak_gb, 2)} GB")

