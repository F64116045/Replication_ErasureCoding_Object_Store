# System Performance Benchmark Guide

This document explains how to use [k6](https://k6.io/) to perform stress testing and functional verification on the **Replication + Erasure Coding Object Store**.

The primary purpose of this test script is to verify the performance of the **Multi-Strategy Storage Engine**, specifically to demonstrate the advantages of the **Field-Level Hybrid (Pure Hot Update)** mechanism when handling updates to hot data.

## 1. Prerequisites

- **k6**: Please refer to the [k6 Installation Guide](https://k6.io/docs/get-started/installation/) to install the tool.
- **System Environment**: Ensure the Docker Cluster is running and the API Gateway is accessible via `http://localhost:8000`.
- **Docker & Docker Compose**: Installed on the host machine.

## 2. Configuration

The test script supports adjusting behavior via environment variables (`-e`):

| Variable Name | Default | Options | Description |
| --- | --- | --- | --- |
| `BASE_URL` | `http://localhost:8000` | - | The address of the API Gateway. |
| **`MODE`** | `replication` | `replication`, `ec`, `hybrid` | **Key Parameter**. Determines the write strategy for the test (see below). |
| `SIZE` | `800kb` | `10kb`, `100kb`, `800kb` | Simulates the size of the Cold Data. |

## 3. Controlled Environment Setup (Crucial)

To obtain accurate benchmarks, you must eliminate background noise and accumulated state.

### Why Reset?

- **Data accumulation**: Previous tests fill up storage and Etcd keys, potentially slowing down subsequent I/O.
- **Background Noise**: The `healer` service consumes CPU and locks while checking for inconsistencies. This interferes with pure write/read performance measurements.

### The Standard Testing Workflow

For **EACH** benchmark run (e.g., before switching from EC to Hybrid), follow this exact sequence:

1. **Full Reset (Wipe Data)**
Stop all containers and remove volumes to clear Etcd and Storage Node data.
    
    ```
    docker-compose down -v
    
    ```
    
2. **Start Core Cluster (Terminal 1)**
Start the cluster. **This will occupy the terminal** to stream logs.
    
    ```
    docker-compose up --build
    
    ```
    
3. **Stop Background Services (Terminal 2)OPEN A NEW TERMINAL.** The Healer service competes for Etcd locks and CPU. Stop it to measure pure API performance.
    
    ```
    docker-compose stop healer
    
    ```
    
4. **Warm-up / Wait**
Wait ~10-15 seconds for Etcd leader election and API Gateway initialization.

## 4. Scenarios and Expected Results

This benchmark includes three main modes, corresponding to the system's three write paths:

### A. Replication Mode (Baseline)

- **Reset & Prep:**
    1. **Terminal 1:** `docker-compose down -v`
    2. **Terminal 1:** `docker-compose up --build` (Wait for logs to flow)
    3. **Terminal 2:** `docker-compose stop healer`
- **Command (Terminal 2)**:
    
    ```
    k6 run -e MODE=replication benchmark.js
    
    ```
    
- **Behavior**: Simulates standard writes for critical data.
- **Expectation**: Serves as the performance baseline. Latency should be relatively low, primarily limited by network bandwidth.

### B. Erasure Coding (EC) Mode (Stress Test)

- **Reset & Prep:**
    1. **Terminal 1:** `docker-compose down -v`
    2. **Terminal 1:** `docker-compose up --build` (Wait for logs to flow)
    3. **Terminal 2:** `docker-compose stop healer`
- **Command (Terminal 2)**:
    
    ```
    k6 run -e MODE=ec benchmark.js
    
    ```
    
- **Behavior**: Forces the system to perform Reed-Solomon (4+2) encoding for every request.
- **Expectation**:
    - **Latency**: Highest (due to intensive CPU calculations).
    - **CPU**: Significant increase in API Gateway load.
    - This mode demonstrates the high cost of processing large file updates without optimization.

### C. Hybrid Mode (Core Verification)

- **Reset & Prep:**
    1. **Terminal 1:** `docker-compose down -v`
    2. **Terminal 1:** `docker-compose up --build` (Wait for logs to flow)
    3. **Terminal 2:** `docker-compose stop healer`
- **Command (Terminal 2)**:
    
    ```
    k6 run -e MODE=hybrid benchmark.js
    
    ```
    
- **Behavior**: Simulates a **Partial Update**. The payload contains a changing counter (Hot) and an unchanged large description file (Cold).
- **Verification Goal**: **Pure Hot Update Mechanism**.
- **Expectation**:
    - **Latency**: Should be close to Replication mode and **significantly lower than EC mode**.
    - This proves the system successfully detected that the cold data did not change, skipping expensive EC encoding and I/O operations.

## 5. Result Analysis

After execution, please check the terminal output summary:

**Performance Metric (`http_req_duration`)**:

- **p(95)**: Represents that 95% of requests were completed within this time.
- If `Hybrid p(95)` << `EC p(95)`, the system design is proven successful.