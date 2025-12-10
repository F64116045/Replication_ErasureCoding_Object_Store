# **Replication + Erasure Coding Object Store** Architecture

This document provides a comprehensive architectural overview of the Replication + Erasure Coding Object Store. It details the system's design principles, component interactions, data flows, and consistency models.

## 1. System Overview

The Replication + Erasure Coding Object Store is designed to balance **Performance** (for hot data) and **Storage Efficiency** (for cold data) using a **Log-Centric Architecture**.

### 1.1 Design Goals

- **Durability**: Zero data loss once a write is acknowledged (via WAL).
- **Efficiency**: Minimize storage overhead for large blobs using Erasure Coding.
- **Self-Healing**: Automatic recovery from node failures without human intervention.
- **Consistency**: Strong consistency for metadata (Etcd), Eventual consistency for object blobs.

### 1.2 CAP Theorem Positioning

- **Metadata (CP)**: We use Etcd to guarantee linearizable consistency for file metadata and node locking.
- **Storage (AP)**: Storage nodes prioritize availability. Inconsistencies are resolved asynchronously by the **Healer**.

## 2. Core Components

The system separates concerns into three distinct planes:

### A. Control Plane

- **Etcd Cluster**:
    - Acts as the **Source of Truth**.
    - Stores file metadata (file size, strategy, node location maps).
    - Handles distributed locking for Leader Election.
    - Stores Service Discovery records (`nodes/health/`).
- **Healer Service**:
    - A stateless worker that performs reconciliation.
    - Active-Standby HA model via Etcd Election.
    - Responsible for repairing lost replicas and reconstructing missing EC shards.

### B. Event Plane

- **Redpanda** :
    - Acts as the **Write-Ahead Log (WAL)**.
    - Decouples ingestion from processing.
    - Ensures write requests are durable even if the API Gateway crashes mid-processing.

### C. Data Plane

- **Storage Nodes**:
    - Stateless HTTP servers.
    - Store raw bytes on the local filesystem.
    - No knowledge of the cluster topology.
- **API Gateway**:
    - Handles client HTTP requests.
    - Performs Erasure Coding (Reed-Solomon) encoding/decoding.
    - Manages sharding logic and node selection.

## 3. Data Flow: The Write Path (Log-Centric)

The write path is designed to be **Crash-Safe**.

### Workflow

```mermaid
sequenceDiagram
    participant C as Client
    participant API as API Gateway
    participant RP as Redpanda (WAL)
    participant Etcd as Etcd
    participant SN as Storage Nodes

    Note left of C: User Uploads JSON
    C->>API: POST /write

    Note over API: 1. Durability First
    API->>RP: Produce Event (Topic: wal-events)
    RP-->>API: ACK (Offset X)

    Note over API: 2. Strategy Analysis
    API->>API: Parse JSON -> Split Hot/Cold
    API->>API: Calculate Hash (SHA-256)

    Note over API: 3. Distribution
    par Write Replicas (Hot)
        API->>SN: POST /store (Node 1,2,3)
    and Write Shards (Cold)
        API->>SN: POST /store (Node 1..6)
    end

    Note over API: 4. Finalize
    alt All Writes Success
        API->>Etcd: PUT metadata/{key}
        API-->>C: 200 OK
    else Partial Failure
        API->>Etcd: PUT metadata/{key} (Marked Dirty)
        API-->>C: 202 Accepted (Repair Scheduled)
    end

```

## 4. Data Flow: The Read Path

The read path differs significantly based on the storage strategy.

### Workflow

```mermaid
sequenceDiagram
    participant C as Client
    participant API as API Gateway
    participant Etcd as Etcd
    participant SN as Storage Nodes

    C->>API: GET /read/{key}
    API->>Etcd: GET metadata/{key}
    Etcd-->>API: Returns Strategy & Map

    alt Strategy: Replication
        API->>SN: HEAD Request (Check latency)
        API->>SN: GET /retrieve/{key} (Fastest Node)
        SN-->>API: Raw JSON
    else Strategy: Erasure Coding
        Note over API: Need K shards to reconstruct
        par Fetch Shards
            API->>SN: GET /retrieve/{chunk_0}
            API->>SN: GET /retrieve/{chunk_1}
            API->>SN: ...
        end
        API->>API: Reed-Solomon Reconstruct
    end

    API->>API: Merge Hot + Cold fields
    API-->>C: Unified JSON Response

```

## 5. Storage Strategies

We employ a hybrid approach to optimize costs.

| Feature | Replication | Erasure Coding (RS 4+2) |
| --- | --- | --- |
| **Target Data** | Hot Data (Counters, Status, Small Metadata) | Cold Data (Bios, Long Descriptions, Blobs) |
| **Redundancy** | 3x Copies (300% storage overhead) | 1.5x Overhead (4 Data + 2 Parity) |
| **Fault Tolerance** | Tolerate 2 node failures | Tolerate 2 node failures |
| **Read Perf** | High (Low latency, parallel read) | Medium (CPU overhead for reconstruction) |
| **Write Perf** | High | Medium (CPU overhead for encoding) |

### Field-Level Hybrid Logic

When a JSON object is updated:

1. **Hash Check**: The system calculates the SHA-256 hash of the Cold fields.
2. **Deduplication**: If the hash matches the previous version in Etcd, the **EC Write is skipped entirely**. Only the small Hot data is updated via Replication.

## 6. The Healer (Self-Healing Controller)

The Healer ensures the system converges to a healthy state. It operates as a background daemon.

### 6.1 Architecture: Active-Standby

To prevent race conditions (e.g., two nodes trying to repair the same file simultaneously), Healer instances perform **Leader Election**:

- Uses `etcd/concurrency` session with a 15s TTL.
- Only the **Leader** enters the Control Loop.
- Others wait on the lock key `/healer/leader`.

### 6.2 The Control Loop (Polling)

Every 30 seconds, the Leader:

1. **Scans**: Lists keys in `metadata/`.
2. **Audits**: Checks existence of files on Storage Nodes via HTTP HEAD.
3. **Repairs**:
    - **Replication Repair**: Copies data from a healthy node -> missing node.
    - **EC Repair**: Downloads `K` shards -> Reconstructs `M` missing shards -> Uploads to new nodes.

## Limitations & Future Work

- **Sharding**: Currently, node selection is deterministic based on sorting. A Consistent Hashing Ring (e.g., Ringpop) is needed for dynamic node scaling.
- **Bitrot Detection**: Currently checks for file existence only. Future versions should implement Checksum verification on read/audit.