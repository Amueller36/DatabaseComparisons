#!/usr/bin/env python3

import os
import time
import subprocess
import re
import threading
import json
import importlib.util
import sys
import inspect
from datetime import datetime

# --- Configuration ---

# Database connection and adapter details
DATABASE_CONFIG = {
    "postgres": {
        "adapter_file": "postgres",  # Corresponds to postgres.py
        "adapter_class": "PostgresAdapter",
        "container_name": "postgres-mds",
    },
    "mongodb": {
        "adapter_file": "mongodb",  # Corresponds to mongodb.py
        "adapter_class": "MongoDbAdapter",
        "container_name": "mongodb-mds",
    },
    "clickhouse": {
        "adapter_file": "clickhouse",  # Corresponds to clickhouse.py
        "adapter_class": "ClickHouseAdapter",
        "container_name": "clickhouse-mds",
    },
}

# File to store benchmark results (appended to on each run)
RESULTS_FILE = "benchmark_results.jsonl"

# Prefix for methods in adapter classes that should be run as use cases
USE_CASE_METHOD_PREFIX = "usecase"

# Polling interval for docker stats (in seconds)
DOCKER_STATS_POLLING_INTERVAL = 1.0

# Directory where the benchmark script and adapter scripts are located
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# --- Helper Functions ---

def convert_to_mb(value, unit):
    """Converts a memory value to megabytes (MB)."""
    unit_lower = unit.lower()
    if 'kib' in unit_lower or 'kb' in unit_lower:  # KiB or KB
        return value / 1024.0
    elif 'mib' in unit_lower or 'mb' in unit_lower:  # MiB or MB
        return value
    elif 'gib' in unit_lower or 'gb' in unit_lower:  # GiB or GB
        return value * 1024.0
    elif 'tib' in unit_lower or 'tb' in unit_lower:  # TiB or TB
        return value * 1024.0 * 1024.0
    elif 'b' in unit_lower and not unit_lower.endswith('ib') and not unit_lower.endswith('b'):  # Bytes
        return value / (1024.0 * 1024.0)
    elif 'b' in unit_lower:  # Bytes (if it's just 'B')
        return value / (1024.0 * 1024.0)
    else:
        print(f"Warning: Unknown memory unit '{unit}'. Assuming MB.")
        return value


def monitor_container_stats_threaded(container_name, stop_event, stats_list, polling_interval):
    """
    Monitors Docker container stats in a separate thread.
    Appends dicts like {"timestamp": time.time(), "cpu_percentage": X, "ram_mb": Y} to stats_list.
    Appends {"cpu_percentage": -1.0, "ram_mb": -1.0} on error for a sample.
    """
    while not stop_event.is_set():
        sample_collected_in_iteration = False
        try:
            cmd = [
                "docker", "stats", "--no-stream",
                "--format", "{{.Name}},{{.CPUPerc}},{{.MemUsage}}",
                container_name
            ]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
            stdout, stderr = process.communicate(timeout=polling_interval + 1)  # Timeout slightly larger than interval

            if process.returncode == 0 and stdout.strip():
                name, cpu_perc_str, mem_usage_str = stdout.strip().split(',')

                if name != container_name:
                    print(f"Warning: Stats received for '{name}' instead of '{container_name}'. Skipping this sample.")
                    # Do not add error sample here, just skip.
                else:
                    cpu_perc = 0.0
                    if cpu_perc_str != "--" and cpu_perc_str:
                        try:
                            cpu_perc = float(cpu_perc_str.replace('%', ''))
                        except ValueError:
                            print(
                                f"Warning: Could not parse CPU percentage '{cpu_perc_str}' for {container_name}. Assuming 0.0.")

                    mem_mb = 0.0
                    if mem_usage_str != "--" and mem_usage_str:
                        mem_part = mem_usage_str.split('/')[0].strip()  # Get the usage part
                        try:
                            mem_value_str = "".join(
                                re.findall(r"[\d\.]+", mem_part))  # Handles cases like "1,234.5MiB" by removing comma
                            mem_unit_match = re.findall(r"[a-zA-Z]+", mem_part)
                            if mem_unit_match:
                                mem_unit = mem_unit_match[0]
                                mem_mb = convert_to_mb(float(mem_value_str), mem_unit)
                            elif mem_value_str:  # If only number, assume bytes
                                mem_mb = convert_to_mb(float(mem_value_str), 'B')
                            else:  # If mem_part is empty or unparseable
                                print(
                                    f"Warning: Could not parse memory value from '{mem_part}' for {container_name}. Assuming 0.0 MB.")

                        except (IndexError, ValueError) as e:
                            print(
                                f"Warning: Could not parse memory usage '{mem_part}' for {container_name}. Error: {e}. Assuming 0.0 MB.")

                    stats_list.append({
                        "timestamp": time.time(),
                        "cpu_percentage": cpu_perc,
                        "ram_mb": mem_mb
                    })
                    sample_collected_in_iteration = True

            elif stderr.strip():
                # print(f"Error polling stats for {container_name}: {stderr.strip()}")
                # This can happen if container is briefly inaccessible. Don't flood with errors.
                pass  # Error will be indicated by lack of valid samples or error sample below

        except subprocess.TimeoutExpired:
            print(f"Timeout polling docker stats for {container_name}.")
        except Exception as e:
            print(f"Exception in monitor_container_stats for {container_name}: {e}")

        if not sample_collected_in_iteration and not stop_event.is_set():
            # Add an error marker if no valid sample was collected and we are not about to stop
            stats_list.append({
                "timestamp": time.time(),
                "cpu_percentage": -1.0,  # Indicate error
                "ram_mb": -1.0  # Indicate error
            })

        # Sleep for the polling interval, but check stop_event more frequently
        for _ in range(int(polling_interval / 0.1) if polling_interval >= 0.1 else 1):
            if stop_event.is_set():
                break
            time.sleep(min(0.1, polling_interval))  # Sleep in small chunks or full interval if small
        if stop_event.is_set():
            break


def calculate_resource_summary(stats_list):
    """Calculates avg/peak CPU and RAM from a list of stats samples."""
    if not stats_list:
        return {"avg_cpu": 0, "peak_cpu": 0, "avg_ram": 0, "peak_ram": 0, "samples": 0, "valid_samples": 0}

    valid_stats = [s for s in stats_list if s.get("cpu_percentage", -1.0) >= 0 and s.get("ram_mb", -1.0) >= 0]

    total_samples = len(stats_list)
    valid_samples_count = len(valid_stats)

    if not valid_stats:
        return {"avg_cpu": 0, "peak_cpu": 0, "avg_ram": 0, "peak_ram": 0, "samples": total_samples, "valid_samples": 0}

    cpu_percentages = [s["cpu_percentage"] for s in valid_stats]
    ram_mbs = [s["ram_mb"] for s in valid_stats]

    avg_cpu = sum(cpu_percentages) / valid_samples_count if valid_samples_count else 0
    peak_cpu = max(cpu_percentages) if cpu_percentages else 0
    avg_ram = sum(ram_mbs) / valid_samples_count if valid_samples_count else 0
    peak_ram = max(ram_mbs) if ram_mbs else 0

    return {
        "avg_cpu": round(avg_cpu, 2),
        "peak_cpu": round(peak_cpu, 2),
        "avg_ram": round(avg_ram, 2),
        "peak_ram": round(peak_ram, 2),
        "samples": total_samples,
        "valid_samples": valid_samples_count
    }


def load_adapter_class(module_file_name_stem, class_name):
    """Loads an adapter class from a .py file located in SCRIPT_DIR."""
    module_file_path = os.path.join(SCRIPT_DIR, module_file_name_stem + ".py")
    module_name = f"adapters.{module_file_name_stem}"  # Give it a unique module name

    try:
        spec = importlib.util.spec_from_file_location(module_name, module_file_path)
        if spec is None:
            print(f"Error: Could not create module spec for {module_file_path}")
            return None

        adapter_module = importlib.util.module_from_spec(spec)
        if module_name in sys.modules:  # Avoid re-inserting if script is run multiple times in same python env (unlikely for cron)
            del sys.modules[module_name]
        sys.modules[module_name] = adapter_module  # Add to sys.modules before exec

        spec.loader.exec_module(adapter_module)
        AdapterClass = getattr(adapter_module, class_name)
        return AdapterClass
    except FileNotFoundError:
        print(f"Error: Adapter file {module_file_path} not found.")
    except AttributeError:
        print(f"Error: Class {class_name} not found in {module_file_path}.")
    except Exception as e:
        print(f"Error loading adapter '{class_name}' from '{module_file_path}': {e}")
    return None


def get_use_case_methods(adapter_instance, prefix):
    """Gets a list of method names from an adapter instance that match the prefix."""
    methods = []
    for member_name, member_value in inspect.getmembers(adapter_instance):
        if inspect.isroutine(member_value) and member_name.startswith(prefix) and not member_name.startswith('_'):
            # Check if it's bound to the instance (method) vs. a static/class method if needed
            # For instance methods, inspect.ismethod is true. For functions defined in class, inspect.isfunction.
            # inspect.isroutine covers both.
            methods.append(member_name)
    return sorted(methods)  # Sort for consistent execution order




def run_benchmarks():
    benchmark_run_id = datetime.now().isoformat()  # Unique ID for this entire benchmark script execution
    all_run_results = []

    print(f"Starting benchmark run ID: {benchmark_run_id}")
    print(f"Results will be saved to: {os.path.join(SCRIPT_DIR, RESULTS_FILE)}")
    print(f"Looking for adapter use case methods prefixed with: '{USE_CASE_METHOD_PREFIX}'")
    print("-" * 40)

    for db_key, config in DATABASE_CONFIG.items():
        db_name = db_key  # e.g. "postgres"
        print(f"\n--- Benchmarking Database: {db_name.upper()} ---")

        adapter_file_stem = config["adapter_file"]
        adapter_class_name = config["adapter_class"]
        container_name = config["container_name"]

        AdapterClass = load_adapter_class(adapter_file_stem, adapter_class_name)
        if not AdapterClass:
            print(f"Skipping {db_name} due to adapter loading error.")
            continue

        adapter_instance = None
        try:
            adapter_instance = AdapterClass()
            print(f"  Successfully instantiated adapter: {adapter_class_name}")
        except Exception as e:
            print(f"  Error instantiating adapter {adapter_class_name} for {db_name}: {e}")
            print(f"  Skipping {db_name}.")
            continue

        use_case_method_names = get_use_case_methods(adapter_instance, USE_CASE_METHOD_PREFIX)
        if not use_case_method_names:
            print(f"  No use case methods found in {adapter_class_name} for {db_name}.")
        else:
            print(f"  Found use cases for {db_name}: {', '.join(use_case_method_names)}")

        for uc_method_name in use_case_method_names:
            print(f"    Running Use Case: {uc_method_name} ...")

            raw_stats_list = []
            stop_monitoring_event = threading.Event()

            monitor_thread = threading.Thread(
                target=monitor_container_stats_threaded,
                args=(container_name, stop_monitoring_event, raw_stats_list, DOCKER_STATS_POLLING_INTERVAL),
                daemon=True  # Allow main program to exit even if thread is stuck (though we join)
            )

            use_case_start_time = time.perf_counter()
            monitor_thread.start()

            use_case_failed = False
            error_message = None
            try:
                use_case_method = getattr(adapter_instance, uc_method_name)
                use_case_method()  # Execute the use case
            except Exception as e:
                print(f"      ERROR executing use case {uc_method_name} for {db_name}: {e}")
                use_case_failed = True
                error_message = str(e)

            use_case_end_time = time.perf_counter()
            execution_time = use_case_end_time - use_case_start_time

            stop_monitoring_event.set()
            monitor_thread.join(timeout=DOCKER_STATS_POLLING_INTERVAL * 2 + 2)  # Wait for thread
            if monitor_thread.is_alive():
                print(f"      Warning: Monitoring thread for {container_name} did not finish cleanly after use case.")

            resource_summary = calculate_resource_summary(raw_stats_list)

            result_entry = {
                "benchmark_run_id": benchmark_run_id,
                "database": db_name,
                "container_name": container_name,
                "use_case_method": uc_method_name,
                "status": "Failed" if use_case_failed else "Success",
                "error_message": error_message if use_case_failed else None,
                "execution_time_seconds": round(execution_time, 4),
                "avg_cpu_percentage": resource_summary["avg_cpu"],
                "peak_cpu_percentage": resource_summary["peak_cpu"],
                "avg_ram_mb": resource_summary["avg_ram"],
                "peak_ram_mb": resource_summary["peak_ram"],
                "resource_samples_collected": resource_summary["samples"],
                "resource_samples_valid": resource_summary["valid_samples"],
                "measurement_timestamp_utc": datetime.utcnow().isoformat()
            }
            all_run_results.append(result_entry)
            status_emoji = "❌" if use_case_failed else "✅"
            print(f"    {status_emoji} Finished: {uc_method_name} "
                  f"Time={result_entry['execution_time_seconds']:.2f}s, "
                  f"AvgCPU={result_entry['avg_cpu_percentage']:.1f}%, PeakCPU={result_entry['peak_cpu_percentage']:.1f}%, "
                  f"AvgRAM={result_entry['avg_ram_mb']:.1f}MB, PeakRAM={result_entry['peak_ram_mb']:.1f}MB "
                  f"({result_entry['resource_samples_valid']}/{result_entry['resource_samples_collected']} valid stats)")

        if hasattr(adapter_instance, "close"):
            try:
                print(f"  Closing adapter for {db_name}...")
                adapter_instance.close()
                print(f"  Adapter for {db_name} closed.")
            except Exception as e:
                print(f"  Error closing adapter for {db_name}: {e}")
        print("-" * 20)

    # Save all results from this run
    results_file_path = os.path.join(SCRIPT_DIR, RESULTS_FILE)
    try:
        with open(results_file_path, "a", encoding='utf-8') as f:
            for res in all_run_results:
                f.write(json.dumps(res) + "\n")
        print(f"\nBenchmark finished. Results appended to {results_file_path}")
    except IOError as e:
        print(f"\nBenchmark finished, but FAILED to write results to {results_file_path}: {e}")
        print("Results data:")
        for res in all_run_results:
            print(json.dumps(res))


if __name__ == "__main__":
    # Example: Ensure Docker is running
    try:
        subprocess.run(["docker", "ps"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print("Error: Docker does not seem to be running or accessible.")
        print("Please ensure Docker is installed, running, and the `docker` command is in your PATH.")
        print(f"Details: {e}")
        sys.exit(1)

    run_benchmarks()
