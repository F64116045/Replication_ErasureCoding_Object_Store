import requests
import subprocess
import time
import json
import uuid

# ==========================================
# Configuration
# ==========================================
BASE_URL = "http://localhost:8000"
# Set to 800KB to stay safely under Nginx's default 1MB client_max_body_size
FILE_SIZE_KB = 800 
NUM_FILES = 10
LOGICAL_TOTAL = FILE_SIZE_KB * 1024 * NUM_FILES

# The base names of your containers in docker-compose
CONTAINER_PREFIX = "replication_erasurecoding_object_store-storage_node_"

# Realistic Scenario: IoT Device Telemetry
# Hot Fields: device_id, battery_level, status_code (Frequent access)
# Cold Field: sensor_raw_log (Bulk data, infrequent access)
def generate_realistic_payload(i):
    return {
        # --- Hot Fields (Assumed to be in internal/config/config.go:HotFields) ---
        "device_id": f"sensor-gh-{i:04d}",
        "battery_level": 85 - (i % 20),
        "status_code": 200 if i % 5 != 0 else 500,
        "is_active": True,
        "last_sync_ts": int(time.time()),
        "firmware_version": "v2.4.1-stable",
        # --- Cold Field (Bulk Data) ---
        "sensor_raw_log": "x" * (FILE_SIZE_KB * 1024) 
    }

def get_total_docker_storage_size():
    """Calculate total physical file size (Bytes) inside all 6 storage containers"""
    total_bytes = 0
    for i in range(1, 7):
        container_name = f"{CONTAINER_PREFIX}{i}-1"
        try:
            # -s: summary, -b: bytes
            cmd = ["docker", "exec", container_name, "du", "-sb", "/data"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                size_str = result.stdout.split()[0]
                total_bytes += int(size_str)
            else:
                # Fallback naming pattern if prefix differs
                alt_name = f"storage_node_{i}"
                cmd = ["docker", "exec", alt_name, "du", "-sb", "/data"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    size_str = result.stdout.split()[0]
                    total_bytes += int(size_str)
        except Exception:
            pass
    return total_bytes

def run_test(strategy_name, strategy_param):
    print(f"\n[Test] Running {strategy_name} Strategy...")
    start_size = get_total_docker_storage_size()
    
    print(f"   -> Writing {NUM_FILES} files with IoT Telemetry structure...")
    for i in range(NUM_FILES):
        key = f"iot-data-{strategy_param}-{uuid.uuid4()}"
        payload = generate_realistic_payload(i)
        url = f"{BASE_URL}/write?key={key}&strategy={strategy_param}"
        
        try:
            res = requests.post(url, json=payload)
            if res.status_code != 200:
                error_snippet = res.text[:100].replace('\n', '')
                print(f"   Write failed (Status {res.status_code}): {error_snippet}...")
        except Exception as e:
            print(f"   Request exception: {e}")

    # Wait for background I/O workers to flush to disk
    print("   -> Waiting 3s for background flush...")
    time.sleep(3)
    end_size = get_total_docker_storage_size()
    
    physical_growth = end_size - start_size
    ratio = physical_growth / LOGICAL_TOTAL if LOGICAL_TOTAL > 0 else 0
    
    print("-" * 55)
    print(f"   Logical Write Size: {LOGICAL_TOTAL / 1024 / 1024:.2f} MB")
    print(f"   Physical Growth:    {physical_growth / 1024 / 1024:.2f} MB")
    print(f"   Storage Overhead:   {ratio:.2f}x")
    print("-" * 55)
    
    return ratio

if __name__ == "__main__":
    print("=== IoT Telemetry Storage Efficiency Benchmark ===")
    print(f"Payload: 6 Hot Metadata Fields + 1 Cold Log Field ({FILE_SIZE_KB}KB)")
    
    # Pre-flight check for docker
    try:
        subprocess.run(["docker", "ps"], capture_output=True, check=True)
    except:
        print("Error: Docker is not running or not accessible.")
        exit(1)

    # Execute benchmarks for each strategy
    ratio_rep = run_test("Replication", "replication")
    ratio_ec = run_test("Erasure Coding", "ec")
    ratio_hybrid = run_test("Field Hybrid", "field_hybrid")
    
    print("\n=== Final Report: Storage Amplification Factor ===")
    print(f"{'Strategy':<20} | {'Overhead':<10} | {'Expected':<10}")
    print("-" * 50)
    print(f"{'Replication':<20} | {ratio_rep:.2f}x       | ~3.00x")
    print(f"{'Erasure Coding':<20} | {ratio_ec:.2f}x       | ~1.50x")
    print(f"{'Field Hybrid':<20} | {ratio_hybrid:.2f}x       | ~1.50x")
    print("-" * 50)
    print("Note: Hybrid overhead is slightly > 1.50x due to replicated metadata.")