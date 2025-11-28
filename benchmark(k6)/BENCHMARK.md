# System Performance Benchmark Guide

This document explains how to use [k6](https://k6.io/) to perform stress testing and functional verification on the **Replication + Erasure Coding Object Store**.

The primary purpose of this test script is to verify the performance of the **Multi-Strategy Storage Engine**, specifically to demonstrate the advantages of the **Field-Level Hybrid (Pure Hot Update)** mechanism when handling updates to hot data.

## 1. Prerequisites

- **k6**: Please refer to the [k6 Installation Guide](https://k6.io/docs/get-started/installation/) to install the tool.
- **System Environment**: Ensure the Docker Cluster is running and the API Gateway is accessible via `http://localhost:8000`.

## 2. Configuration

The test script supports adjusting behavior via environment variables (`-e`):

| Variable Name | Default | Options | Description |
| --- | --- | --- | --- |
| `BASE_URL` | `http://localhost:8000` | - | The address of the API Gateway. |
| **`MODE`** | `replication` | `replication`, `ec`, `hybrid` | **Key Parameter**. Determines the write strategy for the test (see below). |
| `SIZE` | `800kb` | `10kb`, `100kb`, `800kb` | Simulates the size of the Cold Data. |

## 3. Scenarios and Expected Results

This benchmark includes three main modes, corresponding to the system's three write paths:

### A. Replication Mode (Baseline)

- **Command**: `k6 run -e MODE=replication benchmark.js`
- **Behavior**: Simulates standard writes for critical data.
- **Expectation**: Serves as the performance baseline. Latency should be relatively low, primarily limited by network bandwidth.

### B. Erasure Coding (EC) Mode (Stress Test)

- **Command**: `k6 run -e MODE=ec benchmark.js`
- **Behavior**: Forces the system to perform Reed-Solomon (4+2) encoding for every request.
- **Expectation**:
    - **Latency**: Highest (due to intensive CPU calculations).
    - **CPU**: Significant increase in API Gateway load.
    - This mode demonstrates the high cost of processing large file updates without optimization.

### C. Hybrid Mode (Core Verification)

- **Command**: `k6 run -e MODE=hybrid benchmark.js`
- **Behavior**: Simulates a **Partial Update**. The payload contains a changing counter (Hot) and an unchanged large description file (Cold).
- **Verification Goal**: **Pure Hot Update Mechanism**.
- **Expectation**:
    - **Latency**: Should be close to Replication mode and **significantly lower than EC mode**.
    - This proves the system successfully detected that the cold data did not change, skipping expensive EC encoding and I/O operations.

## 4. Quick Start Examples

**Scenario 1: Verify Hybrid Optimization**
This uses 10 concurrent users to repeatedly update objects with 800KB of cold data.

```
k6 run -e MODE=hybrid -e SIZE=800kb benchmark.js
```

**Scenario 2: Compare EC Overhead**
Run this command and record the Latency to compare with Scenario 1.

```
k6 run -e MODE=ec -e SIZE=800kb benchmarkt.js
```

## 5. Result Analysis

After execution, please check the terminal output summary:

    
**Performance Metric (`http_req_duration`)**:
- **p(95)**: Represents that 95% of requests were completed within this time.
- If `Hybrid p(95)` << `EC p(95)`, the system design is proven successful.

