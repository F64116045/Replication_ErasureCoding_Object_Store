# Healer Service: Manual Testing Guide

This guide provides step-by-step instructions to verify the **Safety (Concurrency Control)** and **Self-Healing (Auto-Repair)** capabilities of the Healer service.

We will use manual CLI tools (`curl` and `docker-compose exec`) to simulate failures and observe the system's recovery behavior.

## Prerequisites

1. **Start the Cluster**
Ensure all microservices are running.
    
    ```
    docker-compose up --build
    ```
    
2. **Prepare Test Data**
Ensure `test/payload.json` exists in your project root.
3. **Create "V2" Data (The Valid State)**
We first create a valid object using the Hybrid strategy. This represents the "latest successful write".
    
    ```
    # 1. Cleanup (Optional, ensures fresh start)
    curl -X DELETE http://localhost:8000/delete/healer_full_test
    
    # 2. Write Data
    curl -X POST "http://localhost:8000/write?key=healer_full_test&strategy=field_hybrid" \
         -H "Content-Type: application/json" \
         -d @test/payload.json
    
    # Response should be {"status": "ok", ...}
    ```
    
4. **Monitor Healer Logs**
Open a **new terminal window** to watch the Healer in action. Keep this open throughout the tests.
    
    ```
    docker-compose logs -f healer
    ```
    

## Test 1: Race Condition Safety

**Objective:** Verify that the Healer does **NOT** delete valid data (V2) even if it finds a stale "FAILED" transaction log (V1) for the same key.

### Step 1: Inject Fake Stale Log

We manually inject a "FAILED" transaction record into Etcd, mimicking a scenario where an old write failed, but a newer write succeeded.

1. Open a new terminal.
2. Enter the Etcd container:
    
    ```
    docker-compose exec etcd0 /bin/sh
    ```
    
3. Insert the fake log using `etcdctl` (inside the container):
    
    ```
    # We create a FAILED log pointing to our valid key "healer_full_test"
    etcdctl put txn/fake-v1-failed-txn \
    '{"key_name": "healer_full_test", "strategy": "field_hybrid", "status": "FAILED", "timestamp": 1}'
    
    ```
    
4. Exit the container:
    
    ```
    exit
    ```
    

**Current State:**

- `metadata/healer_full_test`: Exists (Valid Data).
- `txn/fake-v1-failed-txn`: Exists (Fake Failed Log targeting the same key).

### Step 2: Observe Healer Logs

Watch the terminal running `docker-compose logs -f healer`. Wait for the next cycle (approx. 60 seconds).

**Expected Output:**
The Healer should detect the conflict but **abort** the rollback to protect the data.

```
[HEALER] Detected inconsistent txn (Key: healer_full_test), checking rollback...
[HEALER] Safety Check: Metadata exists. This is a stale txn log. Skipping delete to prevent data loss.
[HEALER] Phase 1: Cleaned 1 failed txns.

```

### Step 3: Verify Data Integrity

Ensure the data is still accessible via the API.

```
curl http://localhost:8000/read/healer_full_test
```

**Pass Condition:** You receive the JSON data (200 OK).
**Fail Condition:** You receive a 404 Not Found (Data was wrongly deleted).

## Test 2: Self-Healing (Hot Data / Replication)

**Objective:** Verify the Healer detects a missing replica and copies it from a healthy node.

### Step 1: Simulate Data Loss

Hot data is stored on 3 nodes (`storage_node_1`, `_2`, `_3`). We will delete the file from **Node 2**.

```
# Delete the hot file directly from the container
docker-compose exec storage_node_2 rm /data/node2/healer_full_test_hot
```

*(Optional) Verify deletion:*

```
docker-compose exec storage_node_2 ls /data/node2
# 'healer_full_test_hot' should be missing.

```

### Step 2: Observe Healer Logs

Wait for the next Healer cycle.

**Expected Output:**

```
[HEALER] Auditing Replica: healer_full_test (Target: healer_full_test_hot)
[HEALER] Missing replica on http://storage_node_2:8002. Repairing...
[HEALER] Repair successful: http://storage_node_2:8002
```

### Step 3: Verify Repair

Check if the file is back on Node 2.

```
docker-compose exec storage_node_2 ls /data/node2
```

**Pass Condition:** The file `healer_full_test_hot` appears in the list.

## Test 3: Self-Healing (Cold Data / Erasure Coding)

**Objective:** Verify the Healer detects a missing EC shard and reconstructs it using Reed-Solomon logic.

### Step 1: Simulate Data Loss

Cold data is split across all 6 nodes (Indices 0-5). We will delete **Chunk 4** (located on `storage_node_5`).

```
# Delete chunk 4
docker-compose exec storage_node_5 rm /data/node5/healer_full_test_cold_chunk_4
```

**Current State:**

- Chunk 4 is missing.
- Chunks 0, 1, 2, 3, 5 exist (5 chunks > K=4). The data is recoverable.

### Step 2: Observe Healer Logs

Wait for the next Healer cycle.

**Expected Output:**

```
[HEALER] Auditing EC: healer_full_test (healer_full_test_cold_chunk_*)
[HEALER] Missing 1 chunk(s) for healer_full_test. Healthy: 5
[HEALER] Reconstructing missing chunks...
[HEALER] Uploading repaired chunk 4 to http://storage_node_5:8005...
[HEALER] Repair complete for healer_full_test
```

### Step 3: Verify Repair

Check if the chunk is back on Node 5.

```
docker-compose exec storage_node_5 ls /data/node5

```

**Pass Condition:** The file `healer_full_test_cold_chunk_4` appears in the list.