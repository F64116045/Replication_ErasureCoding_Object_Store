# Healer Service: Manual Testing & Verification Guide

This guide outlines the procedures to verify the Self-Healing, Consistency Checking, and High Availability capabilities of the Healer service.

The Healer operates using a **Polling Loop** mechanism with **Leader Election**:

- It scans Etcd metadata every **30 seconds** (default interval).
- Triggers are automatic based on time.
- Only the elected **Leader** performs the health checks; other instances remain in standby mode.

## Prerequisites

### 1. Start the Cluster

Ensure all services (Etcd, API, Storage Nodes, Healer) are running.

```
docker-compose up -d --build
```

### 2. Open Monitoring Terminal

Open a separate terminal to watch Healer's decision-making process in real-time.

```
docker-compose logs -f healer
```

## Test Case 1: Repairing Lost Replicas (Hot Data)

**Scenario:** A valid object exists, but one of the Storage Nodes suffers data loss (e.g., disk corruption). The Healer must detect this during its scheduled scan and restore the file.

### Step 1: Create Valid Data

Use the API to write a standard object using the Replication strategy.

```
curl -X POST "http://localhost:8000/write?key=repair_rep_test&strategy=replication" \
     -H "Content-Type: application/json" \
     -d '{"mission": "restore_this_data"}'
```

### Step 2: Sabotage a Node

Delete the actual data file from Storage Node 1 to simulate disk loss.

```
# Verify file exists first
docker-compose exec storage_node_1 ls /data/node1/repair_rep_test

# Delete it
docker-compose exec storage_node_1 rm /data/node1/repair_rep_test
```

### Step 3: Wait or Force Trigger

You have two options:

1. **Wait:** Wait up to 30 seconds for the next `[Loop]` interval.
2. **Force:** Restart the healer container to trigger an immediate scan on boot.

```
docker-compose restart healer
```

### Step 4: Verify Restoration

Watch the Monitoring Terminal (`docker-compose logs -f healer`).

**Expected Log Output:**

```
[Loop] Starting full system health scan...
[Loop] Found X objects in metadata...
[Healer] Repairing Replication for key: repair_rep_test. Missing on: [http://storage_node_1:8001]
[Healer] Successfully repaired replica on http://storage_node_1:8001
[Loop] Health scan completed.
```

**Verify Disk:**

```
docker-compose exec storage_node_1 ls /data/node1/repair_rep_test
# The file should be back!
```

## Test Case 2: Reconstructing EC Shards (Cold Data)

**Scenario:** A large file stored via Erasure Coding loses a chunk. The Healer must use Reed-Solomon logic to reconstruct the missing data from remaining shards.

### Step 1: Create Valid Hybrid Data

Write a large object to trigger the EC strategy.

```
curl -X POST "http://localhost:8000/write?key=repair_ec_test&strategy=field_hybrid" \
     -H "Content-Type: application/json" \
     -d '{
           "hot_field": 123,
           "cold_field": "A_LONG_STRING_DATA_FOR_EC_RECONSTRUCTION_TESTING_1234567890"
         }'
```

### Step 2: Sabotage a Chunk

EC chunks are distributed across nodes. Let's delete Chunk 2 from Storage Node 3.

```
# Delete the specific chunk file
docker-compose exec storage_node_3 rm /data/node3/repair_ec_test_cold_chunk_2
```

### Step 3: Trigger Repair

Wait for the next loop (30s) or restart the healer.

### Step 4: Verify Reconstruction

Watch the logs.

**Expected Log Output:**

```
[Loop] Starting full system health scan...
[Healer] Reconstructing EC chunks for repair_ec_test. Missing: [2]
[Healer] Successfully repaired chunk 2 on http://storage_node_3:8003
```

**Verify Disk:**

```
docker-compose exec storage_node_3 ls /data/node3/repair_ec_test_cold_chunk_2
```

## Test Case 3: High Availability (Leader Election)

**Scenario:** We run multiple Healer instances. Only one should work; the others should be on standby. If the leader dies, a standby should take over.

### Step 1: Scale Up Healer

Run a second instance of the Healer service.

```
docker-compose up -d --scale healer=2
```

### Step 2: Observe Election

Check logs for both containers.

```
docker-compose logs -f healer
```

**Expected Output:**

One container (e.g., `healer-1`) will show:

```
[Election] Campaigning for leadership...
[Election] WINNER! I am the Leader now.
[Healer] Entering Control Loop...
```

The other container (e.g., `healer-2`) will block at:

```
[Election] Campaigning for leadership...
```

*(It will NOT show "Entering Control Loop")*

### Step 3: Kill the Leader

Stop the container that won the election.

```
docker stop <container_id_of_leader>
```

### Step 4: Verify Failover

Within ~15 seconds (TTL), the second container should wake up.

**Expected Output on Standby Node:**

```
[Election] WINNER! I am the Leader now.
[Healer] Entering Control Loop...
[Loop] Starting full system health scan...
```