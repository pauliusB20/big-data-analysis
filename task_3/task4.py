from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import config
import time

def calculate_delta_t():
    print("\n--- STARTING TASK 4: DELTA T CALCULATION ---")
    start_time = time.time()

    client = MongoClient(config.MONGO_URI)
    col = client[config.MONGO_DB][config.TARGET_COL]

    mmsi_list = col.distinct("mmsi")
    print(f"INFO: Found {len(mmsi_list)} vessels in filtered collection")

    all_deltas = []

    for mmsi in mmsi_list:
        docs = list(col.find(
            {"mmsi": mmsi},
            {"_id": 0, "timestamp": 1}
        ).sort("timestamp", 1))

        for i in range(1, len(docs)):
            t0 = docs[i - 1]["timestamp"]
            t1 = docs[i]["timestamp"]

            if isinstance(t0, str):
                fmt = "%d/%m/%Y %H:%M:%S"
                t0 = datetime.strptime(t0, fmt)
                t1 = datetime.strptime(t1, fmt)

            delta_ms = int((t1 - t0).total_seconds() * 1000)
            if delta_ms > 0:
                all_deltas.append(delta_ms)

    print(f"INFO: Total delta t values: {len(all_deltas):,}")
    print(f"INFO: Median: {int(np.median(all_deltas)):,} ms")
    print(f"INFO: Mean:   {int(np.mean(all_deltas)):,} ms")
    print(f"INFO: Max:    {int(np.max(all_deltas)):,} ms")

    # Histogram
    MAX_MS = 300_000
    BIN_MS = 10_000
    clipped = [d for d in all_deltas if d <= MAX_MS]
    overflow = len(all_deltas) - len(clipped)

    bins = list(range(0, MAX_MS + BIN_MS, BIN_MS))
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.hist(clipped, bins=bins, color="#378ADD", edgecolor="#185FA5", linewidth=0.4)
    ax.set_xlabel("Delta t (seconds)")
    ax.set_ylabel("Frequency")
    ax.set_title(
        f"Distribution of Delta t between consecutive AIS records\n"
        f"n={len(all_deltas):,} | "
        f"median={int(np.median(all_deltas)):,} ms | "
        f">{MAX_MS//1000}s omitted: {overflow:,}"
    )
    ax.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: f"{int(v/1000)}s")
    )
    plt.tight_layout()
    plt.savefig("delta_t_histogram.png", dpi=150)
    print("INFO: Histogram saved to delta_t_histogram.png")
    plt.show()

    elapsed = round((time.time() - start_time) / 60, 2)
    print(f"--- TASK 4 COMPLETE --- Execution Time: {elapsed} minutes")

if __name__ == "__main__":
    calculate_delta_t()