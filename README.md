# 🌊 Data Engineering Zoomcamp — Module 7: Streaming with Kafka & Apache Flink

> My hands-on work for **Module 7** of the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) — covering real-time stream processing using **Redpanda** (Kafka-compatible), **Apache Flink** (PyFlink), and **PostgreSQL** as a streaming sink, processing simulated NYC Taxi ride events.

---

## 📖 Module Overview

Module 7 introduces **stream processing** — handling data as it arrives in real time rather than in batches. The module uses **Redpanda** (a Kafka-compatible message broker) as the event bus, **PyFlink** (Apache Flink's Python API) for stream processing, and **PostgreSQL** as the output sink. Two types of Flink jobs are built: a **pass-through job** that writes raw events to Postgres, and an **aggregation job** that computes 1-hour tumbling window statistics per pickup zone using event-time watermarks.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│   NYC Taxi Parquet (Nov 2025)  /  Realtime Simulator   │
└────────────────────┬────────────────────────────────────┘
                     │  Python KafkaProducer
                     ▼
┌─────────────────────────────────────────────────────────┐
│              REDPANDA (Kafka-compatible)                 │
│                  Topic: rides                           │
│              localhost:9092 / redpanda:29092            │
└──────────┬─────────────────────┬───────────────────────┘
           │                     │
     KafkaConsumer          PyFlink Jobs
   (notebook demo)     (pass-through / aggregation)
           │                     │
           ▼                     ▼
┌──────────────────────────────────────────────────────────┐
│                    POSTGRESQL                            │
│   processed_events          processed_events_aggregated  │
│   (raw rows)                (1-hour tumbling windows)    │
└──────────────────────────────────────────────────────────┘
```

---

## 🐳 Infrastructure Setup

The full stack runs via **Docker Compose** with 4 services:

| Service | Image | Purpose | Port(s) |
|---------|-------|---------|---------|
| `redpanda` | `redpandadata/redpanda:v25.3.9` | Kafka-compatible message broker | `9092` (external), `29092` (internal), `8082` (Proxy) |
| `postgres` | `postgres:18` | Streaming sink database | `5433` (host → 5432) |
| `jobmanager` | `pyflink-workshop` (custom) | Flink Job Manager — coordinates jobs | `8081` (Flink UI) |
| `taskmanager` | `pyflink-workshop` (custom) | Flink Task Manager — executes tasks | internal `6121`, `6122` |

**Flink cluster config:**
- JobManager memory: `1600m`
- TaskManager memory: `1728m`, JVM metaspace: `512m` (enlarged for PyFlink)
- Task slots per TaskManager: `15`
- Default parallelism: `3`

```bash
# Build the custom Flink image and start all services
docker compose up --build -d
```

Access the **Flink UI** at **http://localhost:8081**

---

## 🐋 Custom Flink Docker Image (`Dockerfile.flink`)

Built on top of `flink:2.2.0-scala_2.12-java17`, the image:

- Installs **`uv`** (from Astral) as the Python package manager
- Installs `openjdk-17-jdk` and `build-essential` for native compilation
- Installs **`apache-flink==2.2.0`** into a virtual environment via `uv sync`
- Downloads and bundles the following **Flink connector JARs** into `/opt/flink/lib/`:

| JAR | Purpose |
|-----|---------|
| `flink-json-2.2.0.jar` | JSON format support |
| `flink-sql-connector-kafka-4.0.1-2.0.jar` | Kafka/Redpanda source connector |
| `flink-connector-jdbc-core-4.0.0-2.0.jar` | JDBC sink core |
| `flink-connector-jdbc-postgres-4.0.0-2.0.jar` | PostgreSQL-specific JDBC connector |
| `postgresql-42.7.10.jar` | PostgreSQL JDBC driver |

---

## 📦 Data Model (`src/producers/models.py`, `notebooks/models.py`)

A `Ride` dataclass represents a single NYC taxi trip event:

```python
@dataclass
class Ride:
    PULocationID: int          # pickup taxi zone (1–263)
    DOLocationID: int          # dropoff taxi zone
    trip_distance: float       # distance in miles
    total_amount: float        # fare in USD
    tpep_pickup_datetime: int  # epoch milliseconds
```

The models file also provides:
- **`ride_from_row(row)`** — converts a pandas DataFrame row into a `Ride` object
- **`ride_serializer(ride)`** — serializes a `Ride` to UTF-8 JSON bytes for Kafka production
- **`ride_deserializer(data)`** — deserializes Kafka bytes back into a `Ride` object

---

## 📡 Producers

### Notebook Producer (`notebooks/producer.ipynb`)

An interactive notebook that teaches Kafka production step by step:

1. **Load real data** — downloads NYC Yellow Taxi November 2025 Parquet directly from the TLC website, keeps 1000 rows with only the 5 relevant columns
2. **Build a `Ride` object** — converts a DataFrame row using `ride_from_row()`
3. **Manual JSON serializer** — shows how to serialize Python dicts to bytes with `json.dumps(...).encode('utf-8')`
4. **`KafkaProducer` setup** — connects to `localhost:9092`, attaches the serializer
5. **Send a single message** — `producer.send(topic_name, value=ride)` + `producer.flush()`
6. **Bulk send 1000 rows** — loops through the DataFrame with a `10ms` sleep between sends, flushes at the end; logs the total time taken

### Realtime Producer (`src/producers/producer_realtime.py`)

A continuous event simulator that generates random ride events indefinitely:

- **20 realistic pickup locations** hard-coded from the top NYC taxi zones (JFK, LaGuardia, Times Square, Union Square, Midtown, etc.)
- Generates a new `Ride` every **0.5 seconds** with:
  - Random pickup/dropoff zones from the location pool
  - Random `trip_distance` between `0.5` and `20.0` miles
  - Random `total_amount` between `$5.00` and `$100.00`
- **Simulates ~20% late events** — randomly delays event timestamps by 3–10 seconds to mimic real-world out-of-order data
- Logs each event with `on time` or `LATE (Xs)` prefix and the pickup zone + timestamp

```bash
uv run python src/producers/producer_realtime.py
```

---

## ⚡ PyFlink Jobs

Both jobs use **Flink's Table API** with SQL DDL to define source and sink tables, and SQL `INSERT INTO ... SELECT` to define the streaming transformation.

### Job 1 — Pass-Through (`src/job/pass_through_job.py`)

Reads raw ride events from Kafka and writes them directly to PostgreSQL with a timestamp conversion — no aggregation, one row in = one row out.

**Source table (Kafka):**
```sql
CREATE TABLE events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime BIGINT
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'topic' = 'rides',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);
```

**Sink table (PostgreSQL):**
```sql
CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    pickup_datetime TIMESTAMP
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'processed_events',
    ...
);
```

**Transformation** — converts epoch milliseconds to a proper `TIMESTAMP`:
```sql
INSERT INTO processed_events
SELECT
    PULocationID, DOLocationID, trip_distance, total_amount,
    TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3) as pickup_datetime
FROM events;
```

- Checkpointing every **10 seconds** for fault tolerance

---

### Job 2 — Aggregation with Tumbling Windows (`src/job/aggregation_job.py`)

The more advanced job — computes **1-hour tumbling window aggregations** per pickup zone using **event-time** and **watermarks** to handle late-arriving events.

**Source table with watermark:**
```sql
CREATE TABLE events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime BIGINT,
    -- Computed column: converts epoch ms to event timestamp
    event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),
    -- Watermark: tolerate events up to 5 seconds late
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'scan.startup.mode' = 'earliest-offset',
    ...
);
```

**Sink table (PostgreSQL):**
```sql
CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    total_revenue DOUBLE,
    PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
) WITH ( 'connector' = 'jdbc', ... );
```

**Tumbling window aggregation:**
```sql
INSERT INTO processed_events_aggregated
SELECT
    window_start,
    PULocationID,
    COUNT(*)           AS num_trips,
    SUM(total_amount)  AS total_revenue
FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
)
GROUP BY window_start, PULocationID;
```

Key concepts applied:
- **Event time** — uses the timestamp embedded in the event (when the ride happened), not when Kafka received it
- **Watermark of 5 seconds** — Flink waits up to 5s beyond the window end for late events before closing and emitting the window result
- **Tumbling windows** — non-overlapping, fixed 1-hour buckets (00:00–01:00, 01:00–02:00, etc.)
- **Parallelism of 3** — 3 concurrent task slots process the stream
- **Checkpointing every 10s** — enables exactly-once recovery on failure

---

### Job 3 — Consumer Notebook (`notebooks/consumer.ipynb`)

An interactive notebook showing how to consume from Kafka using plain Python (no Flink):

1. Creates a `KafkaConsumer` on the `rides` topic with:
   - `auto_offset_reset='earliest'` — reads from the beginning of the topic
   - `group_id='rides-console'` — consumer group for offset tracking
   - Custom `ride_deserializer` to decode bytes → `Ride` objects
2. Connects to PostgreSQL using `psycopg2`
3. Loops indefinitely over incoming messages, inserting each `Ride` into `processed_events` and logging progress every 100 rows

---

## 🗂️ Repository Structure

```
.
├── docker-compose.yml                      # Redpanda + PostgreSQL + Flink cluster
├── Dockerfile.flink                        # Custom PyFlink image with connectors
├── flink-config.yaml                       # Flink cluster configuration
├── pyproject.toml                          # Python deps: kafka-python, pandas, psycopg2, pyarrow
├── pyproject.flink.toml                    # Flink container deps: apache-flink==2.2.0
├── .python-version                         # Python 3.12
├── main.py                                 # Project entrypoint placeholder
├── notebooks/
│   ├── models.py                           # Ride dataclass + serializer/deserializer
│   ├── producer.ipynb                      # Step-by-step Kafka producer notebook
│   └── consumer.ipynb                      # Kafka consumer → PostgreSQL notebook
└── src/
    ├── producers/
    │   ├── models.py                       # Ride model (production version)
    │   └── producer_realtime.py            # Continuous random ride event simulator
    └── job/
        ├── pass_through_job.py             # Flink: Kafka → PostgreSQL (raw events)
        └── aggregation_job.py              # Flink: Kafka → PostgreSQL (1-hr tumbling windows)
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12 | Runtime |
| Redpanda | v25.3.9 | Kafka-compatible message broker |
| Apache Flink | 2.2.0 | Stream processing engine |
| PyFlink | 2.2.0 | Python API for Flink |
| kafka-python | 3.0.7+ | Python Kafka producer/consumer client |
| PostgreSQL | 18 | Streaming sink / output store |
| psycopg2 | 2.9.12+ | Python PostgreSQL driver |
| pandas | 3.0.3+ | Loading source Parquet data |
| pyarrow | 24.0.0+ | Parquet file reading |
| uv | latest | Fast Python package manager |
| Docker / Docker Compose | — | Local environment |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.12+ with [uv](https://docs.astral.sh/uv/)

### 1. Build and start all services

```bash
docker compose up --build -d
```

This builds the custom PyFlink image (downloads JARs — takes a few minutes the first time).

### 2. Install local Python dependencies

```bash
uv sync
```

### 3. Start the Kafka producer

**Option A — Real NYC Taxi data (notebook):**
```bash
uv run jupyter notebook notebooks/producer.ipynb
```

**Option B — Continuous random event stream:**
```bash
uv run python src/producers/producer_realtime.py
```

### 4. Run a Flink job (submit to the Flink cluster)

```bash
# Pass-through: raw events → PostgreSQL
docker exec -it <jobmanager_container> \
  /opt/pyflink/.venv/bin/python /opt/src/job/pass_through_job.py

# Aggregation: 1-hour tumbling windows → PostgreSQL
docker exec -it <jobmanager_container> \
  /opt/pyflink/.venv/bin/python /opt/src/job/aggregation_job.py
```

Or via the **Flink UI** at **http://localhost:8081** → Submit New Job.

### 5. Query results in PostgreSQL

```bash
psql -h localhost -p 5433 -U postgres -d postgres
```

```sql
-- Raw events
SELECT * FROM processed_events LIMIT 10;

-- Aggregated 1-hour windows
SELECT * FROM processed_events_aggregated ORDER BY window_start DESC LIMIT 20;
```

---

## 📚 Resources

- [DataTalksClub DE Zoomcamp — Module 7](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-streaming)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [PyFlink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/)
- [Redpanda Documentation](https://docs.redpanda.com/)
- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [Course YouTube Playlist](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

---

## 🙌 Acknowledgements

Thanks to [Alexey Grigorev](https://linkedin.com/in/agrigorev) and the DataTalksClub team for this excellent free course.
