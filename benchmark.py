#!/usr/bin/env python3
import os
import time
import json
import threading
import importlib.util
import inspect
import sys
from datetime import datetime

import docker  # requires `pip install docker`

# --- Configuration ---

# For each database, point to:
#   1) its adapter filename (without “.py”)
#   2) the class inside that file
#   3) the Docker container name to monitor
DATABASES = [
    {
        "key": "postgres",
        "adapter_file": "postgres",         # postgres.py must live in the same folder
        "adapter_class": "PostgresAdapter",
        "container_name": "postgres-mds",
    },
    {
        "key": "mongodb",
        "adapter_file": "mongodb",          # mongodb.py
        "adapter_class": "MongoDbAdapter",
        "container_name": "mongodb-mds",
    },
    {
        "key": "clickhouse",
        "adapter_file": "clickhouse",       # clickhouse.py
        "adapter_class": "ClickHouseAdapter",
        "container_name": "clickhouse-mds",
    },
]

# Polling interval (seconds) for Docker stats
POLL_INTERVAL = 1.0

# Where to append results
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "benchmark_results.jsonl")


# --- Helper: Load a class from a .py file by name ---
def load_class_from_file(py_filename_stem: str, class_name: str):
    """
    Dynamically load `class_name` from `<py_filename_stem>.py` located in this script's folder.
    Returns the class object, or exits if not found.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(script_dir, py_filename_stem + ".py")

    if not os.path.exists(file_path):
        print(f"Error: Cannot find `{py_filename_stem}.py` in {script_dir}")
        sys.exit(1)

    spec = importlib.util.spec_from_file_location(py_filename_stem, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, class_name):
        print(f"Error: `{class_name}` not found inside `{py_filename_stem}.py`")
        sys.exit(1)

    return getattr(module, class_name)


# --- Helper: Monitor one container continuously in a thread ---
def monitor_container(container_name: str, stop_event: threading.Event, samples: list):
    """
    Periodically polls Docker for stats (non-streaming), every POLL_INTERVAL seconds.
    Appends dicts like:
      {
        "timestamp": <epoch-sec>,
        "cpu_percent": <float>,
        "mem_mb": <float>,
        "blkio_bytes": <int>
      }
    """
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
    except docker.errors.NotFound:
        print(f"Error: Container '{container_name}' not found. Exiting monitor.")
        return

    while not stop_event.is_set():
        try:
            stat = container.stats(stream=False)

            # CPU % (using Docker’s formula)
            cpu_delta = stat["cpu_stats"]["cpu_usage"]["total_usage"] - \
                        stat["precpu_stats"]["cpu_usage"]["total_usage"]
            system_delta = stat["cpu_stats"]["system_cpu_usage"] - \
                           stat["precpu_stats"]["system_cpu_usage"]
            online_cpus = stat["cpu_stats"].get("online_cpus", 1) or 1
            if system_delta > 0 and cpu_delta > 0:
                cpu_percent = (cpu_delta / system_delta) * online_cpus * 100.0
            else:
                cpu_percent = 0.0

            # Memory usage (bytes → MiB)
            mem_usage = stat["memory_stats"].get("usage", 0)
            mem_mb = mem_usage / (1024 ** 2)

            # Block I/O: sum of all read+write bytes
            blkio_total = 0
            blkio_list = stat.get("blkio_stats", {}).get("io_service_bytes_recursive", [])
            for entry in blkio_list:
                # entry looks like {"major":8,"minor":0,"op":"Read","value":12345}
                blkio_total += entry.get("value", 0)

            samples.append({
                "timestamp": time.time(),
                "cpu_percent": cpu_percent,
                "mem_mb": mem_mb,
                "blkio_bytes": blkio_total
            })

        except Exception as e:
            # If stats failed, still record a marker (all zeroes) so summary knows we had an attempt
            samples.append({
                "timestamp": time.time(),
                "cpu_percent": 0.0,
                "mem_mb": 0.0,
                "blkio_bytes": 0
            })

        # Sleep until next poll (or until stop_event is set)
        for _ in range(int(POLL_INTERVAL / 0.1)):
            if stop_event.is_set():
                break
            time.sleep(0.1)


# --- Helper: Summarize a list of samples into avg/peak for CPU, RAM, and total blkio delta ---
def summarize(samples: list):
    """
    Given a list of samples with keys:
       {timestamp, cpu_percent, mem_mb, blkio_bytes}
    returns a dict with:
       avg_cpu, peak_cpu, avg_mem, peak_mem, total_blkio, samples_count.
    """
    if not samples:
        return {
            "avg_cpu": 0.0, "peak_cpu": 0.0,
            "avg_mem": 0.0, "peak_mem": 0.0,
            "total_blkio": 0,
            "samples": 0
        }

    cpu_vals = [s["cpu_percent"] for s in samples]
    mem_vals = [s["mem_mb"] for s in samples]
    blkio_vals = [s["blkio_bytes"] for s in samples]

    avg_cpu = sum(cpu_vals) / len(cpu_vals)
    peak_cpu = max(cpu_vals)
    avg_mem = sum(mem_vals) / len(mem_vals)
    peak_mem = max(mem_vals)

    # The container’s blkio counter is cumulative, so “total delta” =
    #   last value – first value
    total_blkio = max(blkio_vals) - min(blkio_vals)

    return {
        "avg_cpu": round(avg_cpu, 2),
        "peak_cpu": round(peak_cpu, 2),
        "avg_mem": round(avg_mem, 2),
        "peak_mem": round(peak_mem, 2),
        "total_blkio": total_blkio,
        "samples": len(samples)
    }


# --- Main Benchmarking Logic ---
def main():
    # Check Docker is reachable
    try:
        docker.from_env().ping()
    except Exception as e:
        print("Error: Cannot connect to Docker daemon. Is Docker running?")
        print(f"Details: {e}")
        sys.exit(1)

    results = []
    run_id = datetime.utcnow().isoformat()

    for db in DATABASES:
        key = db["key"]
        adapter_file = db["adapter_file"]
        adapter_cls_name = db["adapter_class"]
        container_name = db["container_name"]

        print(f"\n=== {key.upper()} ===")
        # 1) Load adapter class
        AdapterClass = load_class_from_file(adapter_file, adapter_cls_name)
        adapter = AdapterClass()

        # 2) Find all methods that start with "usecase"
        usecases = sorted(
            [name for (name, obj) in inspect.getmembers(adapter)
             if inspect.ismethod(obj) and name.startswith("usecase")]
        )

        if not usecases:
            print(f"No usecase...() methods found in {adapter_cls_name}. Skipping.")
            continue

        print(f"Found usecases: {usecases}")

        for method_name in usecases:
            print(f"→ Running {method_name} ...", end="", flush=True)
            # 3) Start background thread to collect stats
            samples = []
            stop_event = threading.Event()
            monitor = threading.Thread(
                target=monitor_container,
                args=(container_name, stop_event, samples),
                daemon=True
            )
            monitor.start()

            # 4) Run the actual usecase, timing it
            start_t = time.perf_counter()
            status = "Success"
            error_msg = None

            try:
                getattr(adapter, method_name)()
            except Exception as e:
                status = "Failed"
                error_msg = str(e)

            elapsed = time.perf_counter() - start_t

            # 5) Stop monitoring
            stop_event.set()
            monitor.join()

            # 6) Summarize stats
            stats_summary = summarize(samples)

            # 7) Record a single JSON line
            entry = {
                "run_id": run_id,
                "database": key,
                "container": container_name,
                "usecase": method_name,
                "status": status,
                "error": error_msg,
                "execution_time_s": round(elapsed, 4),
                "avg_cpu_percent": stats_summary["avg_cpu"],
                "peak_cpu_percent": stats_summary["peak_cpu"],
                "avg_mem_mb": stats_summary["avg_mem"],
                "peak_mem_mb": stats_summary["peak_mem"],
                "total_blkio_bytes": stats_summary["total_blkio"],
                "samples": stats_summary["samples"],
                "timestamp_utc": datetime.utcnow().isoformat()
            }
            results.append(entry)

            print(f" done. (t={entry['execution_time_s']:.2f}s, "
                  f"avgCPU={entry['avg_cpu_percent']:.1f}%, "
                  f"peakCPU={entry['peak_cpu_percent']:.1f}%, "
                  f"avgMem={entry['avg_mem_mb']:.1f}MiB, "
                  f"peakMem={entry['peak_mem_mb']:.1f}MiB, "
                  f"blkio={entry['total_blkio_bytes']}B)")

        # 8) If the adapter has a close() method, call it
        if hasattr(adapter, "close"):
            try:
                adapter.close()
            except Exception:
                pass

    # 9) Append all results to the JSONL file
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, "a", encoding="utf-8") as f:
        for obj in results:
            f.write(json.dumps(obj) + "\n")

    print(f"\nAll done. {len(results)} entries appended to {RESULTS_FILE}.")


if __name__ == "__main__":
    main()
