import csv
import multiprocessing as mp
import time
import zlib

BAD_MMSI = {"000000000", "111111111", "123456789", "999999999"} # The list of invalid numbers

def ok_mmsi(m):
    return m and m.isdigit() and len(m) == 9 and m not in BAD_MMSI # Numbers only, length is 9,no wrong numbers

def worker(wid, q): # MMSI handler picks up a 'box' of ship data and counts the rows inside.
    total = 0       # starting at 0
    while True:      # Continuously checking for new data batches
        chunk = q.get() # Grabbing the next 'box' from the queue
        if chunk is None:    # check is done until its none, then it breaks
            break
        total += len(chunk)
    print(f"worker {wid}: {total:,} rows", flush=True)

def run_task(files, workers=4, chunk_rows=20000, queue_max=5): # Processing 2 days of data using 4 parallel MMSI handlers and 20,000-row batches
    print("Starting Multi-Day Partitioning...", flush=True)
    t0 = time.time() # Checking how fast my code runs

    
    kept = 0        # Count of good data we are keeping
    skip_len = 0    # Count of broken rows 
    skip_base = 0   # Count of land stations (buildings that don't move)
    skip_ts = 0     # Count of data with no clock time 
    skip_mmsi = 0   # Count of fake ID numbers (the "ghost" ships)

    qs = [mp.Queue(maxsize=queue_max) for _ in range(workers)] # Create a dedicated "queue for each MMSI handler to receive data
    ps = [mp.Process(target=worker, args=(i, qs[i])) for i in range(workers)] # Use the MMSI handlers and tell  which queue to watch
    for p in ps: # Sending MMSI handlers to their stations and starting the work
        p.start()

    buf = [[] for _ in range(workers)]   # Create an empty buffer for each of the 4 MMSI handlers

    for file in files:   # Show which file we are starting to process 
        print(f"\n=== START {file} ===", flush=True)

        f_kept = f_skip = 0 # Start a fresh count for this file so previous file results don't mix in

        with open(file, newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            h = next(r)    # Read the top row of the file (the labels)
            i_ts, i_typ, i_mmsi = h.index("# Timestamp"), h.index("Type of mobile"), h.index("MMSI") # Figure out which column holds the Time, Type, and Ship ID

            for row_num, row in enumerate(r, start=1):
                if len(row) != len(h):                            #If a row is missing data or broken, you skip it.
                    skip_len += 1; f_skip += 1
                    continue

                if row[i_typ] == "Base Station":                 #throw away any row that identifies as a "Base Station./land based stations ( only need moving ships)
                    skip_base += 1; f_skip += 1
                    continue

                if not row[i_ts].strip():                        #throw away ships that dont report time
                    skip_ts += 1; f_skip += 1
                    continue

                m = row[i_mmsi].strip()                         #skip if bad ID
                if not ok_mmsi(m):
                    skip_mmsi += 1; f_skip += 1
                    continue

                wid = zlib.adler32(m.encode("utf-8")) % workers #  assign each ship to a specific MMSI handler (by ID)
                buf[wid].append(row)                             #collect ship signals in their bin/waiting area until you have 20,000
                kept += 1                                       # Keep a running total for the whole project
                f_kept += 1                                    # Keep a running total for just the file we are currently reading

                if row_num % 5_000_000 == 0:                               # # Every 5 million rows, tell  how many we have processed
                    print(f"{file}: read {row_num:,} rows", flush=True)

                if len(buf[wid]) >= chunk_rows:                             #if  bin has 20,000 signals, it is time to hand it to the MMSI hanflrt."
                    qs[wid].put(buf[wid])   
                    buf[wid] = []

        # # After finishing all files, check if any dock still has leftover data
        for wid in range(workers):
            if buf[wid]:          # If the dock isn't empty
                qs[wid].put(buf[wid])        # Send the last few items to the worker
                buf[wid] = []                 # Clear the dock completely

        print(f"=== DONE {file} === kept={f_kept:,} skipped={f_skip:,}", flush=True)

    # shutdown
    for q in qs:
        q.put(None) # # Signal MMSI handlers to shut down by sending a "None" value
    for p in ps:      # Wait for all MMSI handlers to finish their final tasks before closing
        p.join() 

    dt = time.time() - t0   #  Calculate total processing time and summarize the filtering results
    skipped = skip_len + skip_base + skip_ts + skip_mmsi        # Combine all specific skip counts into one "Grand Total"

    print("\n=== FILTER SUMMARY (ALL FILES) ===", flush=True)
    print(f"kept={kept:,} skipped={skipped:,} time={dt:.1f}s", flush=True)
    print(f"skip_len(malformed)={skip_len:,}", flush=True)
    print(f"skip_base(Base Station)={skip_base:,}", flush=True)
    print(f"skip_ts(missing timestamp)={skip_ts:,}", flush=True)
    print(f"skip_mmsi(bad/default MMSI)={skip_mmsi:,}", flush=True)

if __name__ == "__main__":  # Starting the process on two files with the instructions defined above
    run_task(["aisdk-2025-02-28.csv", "aisdk-2025-03-01.csv"], workers=4, chunk_rows=20000)
