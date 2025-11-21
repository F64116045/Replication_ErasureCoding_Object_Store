# API Reference

Base URL: `http://localhost:8000`

## Endpoints

### 1. Write Data

Writes a JSON object or binary data to the distributed store.

- **URL**: `/write`
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Query Parameters**:
    - `key` (Required): Unique identifier for the object.
    - `strategy` (Optional): `replication`, `ec`, or `field_hybrid` (default: `replication`).
    - `hot_only` (Optional): `true` to force a hot-only update (debug use).

**Request Body (Example)**:

```
{
    "user_id": 12345,
    "name": "Alice",
    "bio": "A very long string that will be erasure coded...",
    "login_count": 42
}

```

**Success Response (200 OK)**:

```
{
    "status": "ok",
    "key": "user:12345",
    "strategy": "field_hybrid",
    "latency_ms": 15,
    "is_pure_hot_update": false,
    "hot_nodes_written": 3,
    "cold_chunks_written": 6
}

```

### 2. Read Data

Retrieves data. The system automatically determines the storage strategy and reconstructs the object.

- **URL**: `/read/:key`
- **Method**: `GET`

**Success Response (200 OK)**:
Returns the original JSON object.

**Error Response (404 Not Found)**:

```
{
    "detail": "Key 'unknown' not found"
}

```

### 3. Delete Data

Permanently removes the object metadata and physical files.

- **URL**: `/delete/:key`
- **Method**: `DELETE`

**Success Response (200 OK)**:

```
{
    "status": "ok",
    "key": "user:12345",
    "strategy": "field_hybrid",
    "hot_nodes_deleted": 3,
    "cold_chunks_deleted": 6
}

```

### 4. System Status

**Node Health**:

- **URL**: `/node_status`
- **Method**: `GET`
- **Response**: Returns the status (Health/Size/Ops) of all active storage nodes.

**Storage Usage**:

- **URL**: `/storage_usage`
- **Method**: `GET`
- **Response**: Aggregated disk usage statistics.