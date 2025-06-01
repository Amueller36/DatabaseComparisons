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

DATABASES = [
    {
        "key": "postgres",
        "adapter_file": "postgres",
        "adapter_class": "PostgresAdapter",
        "container_name": "postgres-mds",
    },
    {
        "key": "mongodb",
        "adapter_file": "mongodb",
        "adapter_class": "MongoDbAdapter",
        "container_name": "mongodb-mds",
    },
    {
        "key": "clickhouse",
        "adapter_file": "clickhouse",
        "adapter_class": "ClickHouseAdapter",
        "container_name": "clickhouse-mds",
    },
]

POLL_INTERVAL = 1.0

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

def load_class_from_file(py_filename_stem: str, class_name: str):
    file_path = os.path.join(SCRIPT_DIR, py_filename_stem + ".py")
    if not os.path.exists(file_path):
        print(f"Error: Cannot find `{py_filename_stem}.py` in {SCRIPT_DIR}")
        sys.exit(1)

    spec = importlib.util.spec_from_file_location(py_filename_stem, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, class_name):
        print(f"Error: `{class_name}` not found inside `{py_filename_stem}.py`")
        sys.exit(1)

    return getattr(module, class_name)

def monitor_container(container_name: str, stop_event: threading.Event, samples: list):
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
    except docker.errors.NotFound:
        print(f"Error: Container '{container_name}' not found. Exiting monitor.")
        return

    while not stop_event.is_set():
        try:
            stat = container.stats(stream=False)

            cpu_delta = stat["cpu_stats"]["cpu_usage"]["total_usage"] - \
                        stat["precpu_stats"]["cpu_usage"]["total_usage"]
            system_delta = stat["cpu_stats"]["system_cpu_usage"] - \
                           stat["precpu_stats"]["system_cpu_usage"]
            online_cpus = stat["cpu_stats"].get("online_cpus", 1) or 1
            if system_delta > 0 and cpu_delta > 0:
                cpu_percent = (cpu_delta / system_delta) * online_cpus * 100.0
            else:
                cpu_percent = 0.0

            mem_usage = stat["memory_stats"].get("usage", 0)
            mem_mb = mem_usage / (1024 ** 2)

            blkio_total = 0
            blkio_list = stat.get("blkio_stats", {}).get("io_service_bytes_recursive", [])
            for entry in blkio_list:
                blkio_total += entry.get("value", 0)

            # For richer plotting: collect more (see below!)
            samples.append({
                "timestamp": time.time(),
                "cpu_percent": cpu_percent,
                "mem_mb": mem_mb,
                "blkio_bytes": blkio_total,
                # More for plotting:
                "net_rx_bytes": stat.get("networks", {}).get("eth0", {}).get("rx_bytes", None),
                "net_tx_bytes": stat.get("networks", {}).get("eth0", {}).get("tx_bytes", None),
                # add more interfaces if needed
            })

        except Exception:
            samples.append({
                "timestamp": time.time(),
                "cpu_percent": 0.0,
                "mem_mb": 0.0,
                "blkio_bytes": 0,
                "net_rx_bytes": 0,
                "net_tx_bytes": 0,
            })

        for _ in range(int(POLL_INTERVAL / 0.1)):
            if stop_event.is_set():
                break
            time.sleep(0.1)

def summarize(samples: list):
    if not samples:
        return {
            "avg_cpu": 0.0, "peak_cpu": 0.0,
            "avg_mem": 0.0, "peak_mem": 0.0,
            "total_blkio_mb": 0.0,
            "samples": 0
        }

    cpu_vals = [s["cpu_percent"] for s in samples]
    mem_vals = [s["mem_mb"] for s in samples]
    blkio_vals = [s["blkio_bytes"] for s in samples]

    avg_cpu = sum(cpu_vals) / len(cpu_vals)
    peak_cpu = max(cpu_vals)
    avg_mem = sum(mem_vals) / len(mem_vals)
    peak_mem = max(mem_vals)

    total_blkio_bytes = max(blkio_vals) - min(blkio_vals)
    total_blkio_mb = total_blkio_bytes / (1024 ** 2)

    return {
        "avg_cpu": round(avg_cpu, 2),
        "peak_cpu": round(peak_cpu, 2),
        "avg_mem": round(avg_mem, 2),
        "peak_mem": round(peak_mem, 2),
        "total_blkio_mb": round(total_blkio_mb, 2),
        "samples": len(samples)
    }

def find_next_run_number(results_dir, db_name, base_ts):
    """Find next available run number for this timestamp (avoid overwriting if script is super-fast!)"""
    n = 1
    while True:
        fname = f"{db_name}_{base_ts}_Run{n}.jsonl"
        if not os.path.exists(os.path.join(results_dir, fname)):
            return n
        n += 1

def main():
    try:
        docker.from_env().ping()
    except Exception as e:
        print("Error: Cannot connect to Docker daemon. Is Docker running?")
        print(f"Details: {e}")
        sys.exit(1)

    run_id = datetime.utcnow().isoformat()

    for db in DATABASES:
        key = db["key"]
        adapter_file = db["adapter_file"]
        adapter_cls_name = db["adapter_class"]
        container_name = db["container_name"]

        print(f"\n=== {key.upper()} ===")
        AdapterClass = load_class_from_file(adapter_file, adapter_cls_name)
        adapter = AdapterClass()

        usecases = sorted(
            [name for (name, obj) in inspect.getmembers(adapter)
             if inspect.ismethod(obj) and name.startswith("usecase")]
        )

        if not usecases:
            print(f"No usecase...() methods found in {adapter_cls_name}. Skipping.")
            continue

        print(f"Found usecases: {usecases}")

        # Create result filename
        now = datetime.now()
        base_ts = now.strftime("%d-%m-%Y-%H-%M-%S")
        run_num = find_next_run_number(RESULTS_DIR, key, base_ts)
        db_results_file = os.path.join(RESULTS_DIR, f"{key}_{base_ts}_Run{run_num}.jsonl")
        print(f"Results will go to {db_results_file}")

        db_entries = []
        entry_index = 1

        for method_name in usecases:
            print(f"→ Running {method_name} ...", end="", flush=True)

            samples = []
            stop_event = threading.Event()
            monitor = threading.Thread(
                target=monitor_container,
                args=(container_name, stop_event, samples),
                daemon=True
            )
            monitor.start()

            start_t = time.perf_counter()
            status = "Success"
            error_msg = None

            try:
                getattr(adapter, method_name)()
            except Exception as e:
                status = "Failed"
                error_msg = str(e)

            elapsed = time.perf_counter() - start_t

            stop_event.set()
            monitor.join()

            stats_summary = summarize(samples)
            entry = {
                "index": entry_index,
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
                "total_blkio_mb": stats_summary["total_blkio_mb"],
                "samples": stats_summary["samples"],
                "timestamp_utc": datetime.utcnow().isoformat(),
                # For plotting: Save all raw samples for this usecase!
                "raw_samples": samples
            }
            db_entries.append(entry)

            print(
                f" done. (#{entry['index']}, t={entry['execution_time_s']:.2f}s, "
                f"avgCPU={entry['avg_cpu_percent']:.1f}%, "
                f"peakCPU={entry['peak_cpu_percent']:.1f}%, "
                f"avgMem={entry['avg_mem_mb']:.1f}MiB, "
                f"peakMem={entry['peak_mem_mb']:.1f}MiB, "
                f"blkio={entry['total_blkio_mb']:.1f}MiB)"
            )

            entry_index += 1

        if hasattr(adapter, "close"):
            try:
                adapter.close()
            except Exception:
                pass

        # Write results for this DB/usecase run (include full time series for each usecase!)
        with open(db_results_file, "w", encoding="utf-8") as f:
            for obj in db_entries:
                f.write(json.dumps(obj) + "\n")

        print(f"→ {len(db_entries)} entries written to {db_results_file}.")

    print("\nAll databases processed.")

if __name__ == "__main__":
    main()
