import multiprocessing as mp
import csv
import math
from datetime import datetime
from collections import defaultdict

N_WORKERS = 4
CHUNK_SIZE = 10000

INVALID_MMSI = {"000000000", "111111111", "123456789"}


def haversine(lat1, lon1, lat2, lon2):

    R = 6371000

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi/2)**2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    )

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def parse_time(ts_str):
    # Adjust format to your dataset, e.g., "YYYY-MM-DD HH:MM:SS"
    return datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")



def check_for_gap(prev_data, current_time, current_lat, current_lon):
    """
    Analyzes two points in time for a specific vessel.
    Returns a result dictionary if a gap is found, otherwise None.
    """
    # Guard: Don't process out-of-order data
    if current_time <= prev_data["time"]:
        return None

    # 1. Calculate Time Gap
    tdelta = (current_time - prev_data["time"]).total_seconds()
    gap_minutes = tdelta / 60

    # 2. Calculate Distance
    dist_meters = haversine(
        prev_data["lat"], prev_data["lon"],
        current_lat, current_lon
    )
    dist_km = dist_meters / 1000

    # 3. Decision: 4 hours AND 1 kilometer
    if gap_minutes >= 240 and dist_km > 1:
        return {
            "gap_hours": round(gap_minutes / 60, 2),
            "distance_km": round(dist_km, 2),
            "start_time": prev_data["time"],
            "end_time": current_time
        }

    return None




def worker_process(queue,results_queue, worker_id):
    """
    This function runs ONCE per worker.
    The dictionary 'last_seen' stays in memory until the script ends.
    """
    # MEMORY: This persists across all chunks sent to this worker
    last_seen = {}

    while True:
        # Wait for a new chunk of data
        chunk = queue.get()

        # If we receive None, it means there is no more data
        if chunk is None:
            break

        for row in chunk:
            try:
                mmsi = row[2].strip()
                current_time = parse_time(row[0])
                current_lat = float(row[3])
                current_lon = float(row[4])

                if mmsi in last_seen:
                    # CALL THE EXTERNAL LOGIC FUNCTION
                    gap_report = check_for_gap(
                        last_seen[mmsi],
                        current_time,
                        current_lat,
                        current_lon
                    )

                    if gap_report:
                        # Add identifying info to the report before sending
                        gap_report["mmsi"] = mmsi
                        print(f"[Worker {worker_id}] Found Gap: {mmsi}")
                        results_queue.put(gap_report)

                # UPDATE STATE: Only if this is the newest data
                if mmsi not in last_seen or current_time > last_seen[mmsi]["time"]:
                    last_seen[mmsi] = {
                        "time": current_time,
                        "lat": current_lat,
                        "lon": current_lon
                    }



            except Exception:
                continue



def is_valid_row(row):
    """
    Returns True if row passes basic validation.
    This is the producer-side filtering to prevent skew.
    """
    try:
        mmsi = row[2].strip()
        if mmsi in INVALID_MMSI or not mmsi.isdigit() or len(mmsi) != 9:
            return False

        lat = float(row[3])
        lon = float(row[4])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return False

        # Can add more checks (speed, timestamp, etc.)
        return True
    except Exception:
        return False


def stream_partition(file_path):
    N_WORKERS = 4
    # Create one communication channel (Queue) per worker
    queues = [mp.Queue(maxsize=10) for _ in range(N_WORKERS)]
    results_queue = mp.Queue()
    processes = []

    # Start the persistent workers
    for i in range(N_WORKERS):
        p = mp.Process(target=worker_process, args=(queues[i],results_queue, i))
        p.start()
        processes.append(p)

    with open(file_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        buffers = [[] for _ in range(N_WORKERS)]

        for row in reader:
            if not is_valid_row(row):
                print("Value is not valid")
                continue
            try:
                # Use the MMSI to route to the correct worker
                mmsi_val = int(row[2])
                worker_id = mmsi_val % N_WORKERS

                buffers[worker_id].append(row)

                # Send a 'chunk' once a buffer hits 5000 rows
                if len(buffers[worker_id]) >= 5000:
                    print("Executing the worker")
                    queues[worker_id].put(buffers[worker_id])
                    buffers[worker_id] = []
            except:
                continue

    # Final cleanup: send remaining data and shutdown signal
    for i in range(N_WORKERS):
        if buffers[i]:
            queues[i].put(buffers[i])
        queues[i].put(None)  # The 'None' tells the while loop to break

    for p in processes:
        p.join()
    final_list = []
    while not results_queue.empty():
        final_list.append(results_queue.get())

    return final_list  # Now it returns a standard Python list


if __name__ == "__main__":
    mp.set_start_method("fork",force=True)
    dark_periods=stream_partition("aisdk-2025-02-28.csv")


dark_periods
