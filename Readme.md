# Replication + Erasure Coding Object Store

Fault-tolerant distributed object storage system written in Go. It features a novel **Field-Level Hybrid Storage** strategy within a **Log-Centric Architecture**, optimizing costs by automatically splitting object (JSON data) into "Hot" (Replicated) and "Cold" (Erasure Coded) partitions based on access patterns.

This system addresses the write amplification problem inherent in traditional object storage systems when handling large, structured data with frequent metadata updates.


## **Features**

- **Multi-Strategy Storage**
    - **Replication**: Ensures high availability for critical data.
    - **Erasure Coding (RS 4+2)**: Maximizes storage efficiency for larger data blobs.
    - **Field Hybrid**: Automatically separates frequently accessed fields (Hot) from bulk data (Cold).
        - **Automatic Tiering**: Automatically splits a JSON object into Hot Data (Replicated) and Cold Data (Erasure Coded RS 4+2).
        - **Pure Hot Update**: Calculates SHA-256 hashes to detect if cold data remains unchanged. If matched, it skips expensive EC encoding and backend I/O.

- **Self-Healing System**
    - **Leader Election**: Background `Healer` nodes compete for leadership to prevent race conditions.
    - **Auto-Repair**: Automatically reconstructs missing EC shards or copies missing replicas.
    - **Zombie Cleanup**: Rolls back stalled pending transactions automatically.

- **Log-Centric Architecture**
    - **Redpanda as WAL**: Decouples the Write-Ahead Log from metadata storage. It utilizes sequential disk I/O to provide high-throughput durability guarantees.
    - **Etcd for Metadata**: Ensures strong consistency for object location maps, hashes, and configuration, acting as the single source of truth for system state.
    - **Stateless API Gateway**: The API layer is purely computational, allowing for horizontal scaling without complex state coordination.

- **Async I/O Storage Engine**
    - **Non-blocking Write-Back**: Storage nodes utilize an in-memory `WriteQueue` and background I/O workers to handle disk operations.
    - **Latency Optimization**: Disk write latency is decoupled from the API response time, reducing storage node response latency from milliseconds to microseconds.


## Architecture

The system is deployed as a microservices cluster via Docker Compose:

```mermaid
graph TD
    User[Client] -->|HTTP :8000| LB[Nginx Load Balancer]
    LB --> API[API Gateway Cluster x3]

    subgraph Control Plane
        API -->|WAL Append | RP[Redpanda]
        API -->|Metadata Commit | Etcd[Etcd Cluster x3]
    end

    subgraph Data Plane
        API -->|Async Write| SN[Storage Nodes x6]
        SN -->|Background Flush| Disk[(Local Disk)]
    end

    subgraph Reliability
        Healer[Healer Service] -.->|Consume Log| RP
        Healer -.->|Repair Data| SN
    end
```

### Component Roles

| Service | Scale | Description |
| --- | --- | --- |
| **API Gateway** | 3x | Traffic entry point. Handles Hash calculation, EC sharding, and coordinates WAL/Storage writes. |
| **Redpanda** | 1x | **Distributed WAL**. Stores write intents (PENDING state) with high throughput using Kafka protocol. |
| **Etcd Cluster** | 3x | **Metadata Store**. Stores object locations and commit states. |
| **Storage Node** | 6x | **Data Warehouse**. Stores actual data blobs using non-blocking buffered writes (Async I/O). |
| **Healer** | 2x | **Recovery**. Reconciles inconsistencies between WAL and Data plane. |

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Go 1.24+ (for local development)

### Installation

1. **Clone the repository**
    
    ```
    cd Replication_ErasureCoding_Object_Store
    ```
    
2. **Start the Cluster**
    
    The system utilizes Init Containers to handle all setup tasks (e.g., Topic creation).
    
    ```
    docker-compose up --build
    ```
    
3. **Verify Status**
The API Gateway is exposed at `http://localhost:8000`.
    
    ```
    curl http://localhost:8000/health
    # Output: {"status":"healthy","service":"api_gateway"...}
    ```
    

## Usage Guide

### 1. Upload Data (Write)

You can choose a storage strategy via the `strategy` query parameter.

**Option A: Hybrid Storage (Recommended for JSON)**
This strategy automatically keeps "Hot Fields" (defined in config) on replicas and moves other fields to EC nodes.

```
curl -X POST "http://localhost:8000/write?key=user:1001&strategy=field_hybrid" \
     -H "Content-Type: application/json" \
     -d '{
           "user_id": 1001,
           "view_count": 50,
           "biography": "Long text data that belongs to cold storage..."
         }'
```

**Option B: Erasure Coding (For large files)**

```
curl -X POST "http://localhost:8000/write?key=image.png&strategy=ec" \
     -H "Content-Type: application/json" \
     -d '{"binary_data": "..."}'
```

**Option C: Replication (For critical data)** Best for small, critical data that requires low latency. Stores full copies on 3 nodes.

```
curl -X POST "http://localhost:8000/write?key=config:settings&strategy=replication" \
     -H "Content-Type: application/json" \
     -d '{"theme": "dark", "notifications": true}'
```

### 2. Retrieve Data (Read)

Simply request the key. The system automatically resolves the strategy, retrieves shards/replicas, reconstructs the data, and returns the original JSON.

```
curl http://localhost:8000/read/user:1001
```

### 3. Delete Data

Deletes both metadata and physical data.

```
curl -X DELETE http://localhost:8000/delete/user:1001
```

## Configuration

Key system parameters are defined in `internal/config/config.go`.

| Parameter | Default | Description |
| --- | --- | --- |
| `K` | 4 | Number of Data Shards (Reed-Solomon). |
| `M` | 2 | Number of Parity Shards. |
| `HotFields` | `view_count`, `like_count`... | Fields that are kept in Replication storage during Hybrid writes. |

## Testing

Simple integration tests are provided in the `test/` directory.

**Run the functional test suite:**
This script verifies all strategies (Replication, EC, Hybrid) and edge cases (invalid inputs, updates, deletes).
For all strategies

```
python3 test/simple_test.py
```

For only hybrid strategies

```
python3 test/hybrid_only.py
```

![Success1](./img/Success1.png)
![Success1](./img/Success2.png)

## Project Structure

```
.
├── cmd/                       # Entry points
│   ├── api/                   # API Gateway (Main Entry)
│   ├── healer/                # Self-healing service
│   └── storage_node/          # Async I/O Data Node
├── internal/                  # Private library code
│   ├── config/                # Configuration constants
│   ├── ec/                    # Reed-Solomon wrapper
│   ├── etcd/                  # Etcd client wrapper
│   ├── httpclient/            # Connection pooling client
│   ├── interfaces/            # Interface definitions
│   ├── monitoringservice/     # Node status logic
│   ├── mq/                    # Redpanda/Kafka Client Wrapper
│   ├── readservice/           # Read path logic
│   ├── storageops/            # Low-level operations
│   ├── utils/                 # Hashing & Serialization Tools
│   └── writeservice/          # Write path logic (WAL, Hybrid)
├── docs/                      # Documentation
├── test/                      # Legacy Python tests
├── benchmark.js               # K6 Performance Test Suite
├── docker-compose.yaml        # Container orchestration
└── nginx.conf                 # Load balancer config

```