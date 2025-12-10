# Healer Service: Manual Testing & Verification Guide

This guide outlines the procedures to verify the **Self-Healing** and **Consistency Checking** capabilities of the Healer service within the Log-Centric architecture.

Since the Healer operates as a **Redpanda Consumer**, we trigger maintenance tasks by injecting specific log events into the WAL topic.

## Prerequisites

1. **Start the Cluster**
Ensure all services (Redpanda, Etcd, API, Storage Nodes, Healer) are running and healthy.
    
    ```
    docker-compose up -d --build
    
    ```
    
2. **Open Monitoring Terminal**
Open a separate terminal window to watch Healer's actions in real-time.
    
    ```
    docker-compose logs -f healer
    
    ```
    

## Test Case 1: Detecting "Zombie" Transactions

**Scenario:** The API Gateway crashes after writing to the WAL (Redpanda) but *before* committing Metadata to Etcd. The Healer must detect this inconsistency.

### Step 1: Manually Inject a "Zombie" Log

We use the `rpk` tool inside the Redpanda container to simulate a log entry for a key that does not exist in Etcd.

```
# Produce a fake WAL entry to the 'wal-events' topic
echo '{"txn_id":"txn:zombie-test","status":"PENDING","key_name":"ghost_data_key","strategy":"replication","timestamp":1700000000,"details":{}}' | \
docker-compose exec -T redpanda rpk topic produce wal-events
```

### Step 2: Verify Alert

Check your **Monitoring Terminal**. The Healer should consume this message, check Etcd, find no metadata, and raise an alert.

**Expected Log Output:**

```
[Healer] ALERT: Data Inconsistency Detected!
   Txn: txn:zombie-test
   Key: ghost_data_key
   Status: Missing in Etcd (Write Lost)
```

## Test Case 2: Repairing Lost Replicas (Hot Data)

**Scenario:** A valid object exists, but one of the Storage Nodes suffers data loss (e.g., disk corruption). The Healer must restore the file from healthy nodes.

### Step 1: Create Valid Data

Use the API to write a standard object using the **Replication** strategy.

```
curl -X POST "http://localhost:8000/write?key=repair_rep_test&strategy=replication" \
     -H "Content-Type: application/json" \
     -d '{"mission": "restore_this_data"}'

```

### Step 2: Sabotage a Node

Delete the actual data file from **Storage Node 1**.

```
# Verify file exists first
docker-compose exec storage_node_1 ls /data/node1/repair_rep_test

# Delete it
docker-compose exec storage_node_1 rm /data/node1/repair_rep_test

```

### Step 3: Trigger Repair (Replay Log)

Since the original WAL log was already consumed, we simulate a "Log Replay" or "Periodic Scan" by re-injecting the log intent.

```
# Inject the same log entry to trigger the Healer logic again
echo '{"txn_id":"txn:replay-01","status":"PENDING","key_name":"repair_rep_test","strategy":"replication","timestamp":1700000000,"details":{}}' | \
docker-compose exec -T redpanda rpk topic produce wal-events

```

### Step 4: Verify Restoration

**1. Check Logs:**

```
[Healer] Repairing Replication for key: repair_rep_test. Missing on: [http://storage_node_1:8001]
[Healer] Successfully repaired replica on http://storage_node_1:8001

```

**2. Check Disk:**

```
docker-compose exec storage_node_1 ls /data/node1/repair_rep_test
# Output should show the file name, indicating successful recovery.

```

## Test Case 3: Reconstructing EC Shards (Cold Data)

**Scenario:** A large file stored via Erasure Coding loses a chunk. The Healer must use Reed-Solomon logic to reconstruct the missing data from remaining shards.

### Step 1: Create Valid Hybrid Data

Write a large object to trigger the EC strategy (Cold Data).

```
curl -X POST "http://localhost:8000/write?key=repair_ec_test&strategy=field_hybrid" \
     -H "Content-Type: application/json" \
     -d '{
           "hot_field": 123,
           "cold_field": "A_LONG_STRING_DATA_FOR_EC_RECONSTRUCTION_TESTING_1234567890"
         }'

```

### Step 2: Sabotage a Chunk

EC chunks are distributed across nodes. Let's delete **Chunk 2** from **Storage Node 3**.

```
# Delete the specific chunk file
docker-compose exec storage_node_3 rm /data/node3/repair_ec_test_cold_chunk_2

```

### Step 3: Trigger Repair

Re-inject the log. Note that we must provide the correct `details` so the Healer knows the naming convention.

```
# JSON payload matching the Hybrid structure
echo '{"txn_id":"txn:replay-02","status":"PENDING","key_name":"repair_ec_test","strategy":"field_hybrid","timestamp":1700000000,"details":{"hot_key":"repair_ec_test_hot","cold_prefix":"repair_ec_test_cold_chunk_"}}' | \
docker-compose exec -T redpanda rpk topic produce wal-events

```

### Step 4: Verify Reconstruction

**1. Check Logs:**

```
[Healer] Reconstructing EC chunks for repair_ec_test. Missing: [2]
[Healer] Successfully repaired chunk 2 on http://storage_node_3:8003

```

**2. Check Disk:**

```
docker-compose exec storage_node_3 ls /data/node3/repair_ec_test_cold_chunk_2
# Output should show the file name.

```