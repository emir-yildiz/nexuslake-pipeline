# NexusLake

End-to-end data lakehouse pipeline built on the Medallion Architecture (Bronze → Silver → Gold), orchestrated with Apache Airflow and served via a REST API.

---

## Architecture

```
Kafka  ──►  Bronze Service  ──►  Silver Service  ──►  Gold Service  ──►  Serving API
 (raw)       (ingestion)         (cleansing)          (aggregation)      (REST)
                │                     │                     │
           Delta Lake            Delta Lake            Delta Lake
           (raw events)         (cleansed)            (platform metrics)
```

Each layer runs as an independent FastAPI service. Apache Airflow orchestrates the pipeline by triggering each service and waiting for completion before moving to the next layer.

---

## Stack

| Component | Technology |
|---|---|
| Stream Processing | Apache Spark 3.5.0 (Structured Streaming) |
| Storage Format | Delta Lake |
| Message Broker | Apache Kafka (Confluent 7.5.0) |
| Orchestration | Apache Airflow 2.9.2 |
| API Framework | FastAPI + Uvicorn |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
nexuslake/
├── config/
│   ├── kafka_config.yaml
│   └── spark_config.yaml
├── deployments/
│   ├── airflow/
│   │   └── dags/
│   │       └── nexuslake_pipeline.py
│   └── docker/
│       └── base.Dockerfile
├── src/
│   ├── common/
│   │   ├── spark_session.py
│   │   └── logger.py
│   ├── ingestion/
│   │   └── producer.py
│   ├── bronze/
│   │   ├── bronze_layer.py
│   │   └── bronze_service.py
│   ├── silver/
│   │   ├── silver_layer.py
│   │   └── silver_service.py
│   ├── gold/
│   │   ├── gold_layer.py
│   │   └── gold_service.py
│   └── serving/
│       └── serving_layer.py
└── storage/
    ├── checkpoints/
    └── lakehouse/
        ├── bronze/
        ├── silver/
        └── gold/
```

---

## Services & Ports

| Service | Host Port | Description |
|---|---|---|
| Event Producer | `8000` | Publishes mock events to Kafka |
| Bronze Service | `8001` | Kafka → Delta Lake (raw ingestion) |
| Silver Service | `8002` | Bronze → Delta Lake (cleansed & deduplicated) |
| Gold Service | `8003` | Silver → Delta Lake (business aggregates) |
| Serving API | `8004` | Gold → REST API (analytics endpoints) |
| Airflow Webserver | `8080` | Pipeline orchestration UI |

---

## Pipeline Flow

```
[Airflow DAG: every 30 minutes]

trigger_bronze ──► wait_for_bronze
                         │
                   trigger_silver ──► wait_for_silver
                                            │
                                      trigger_gold ──► wait_for_gold
```

Each trigger uses `availableNow=True` mode — Spark processes all available data and stops. Airflow sensors poll the `/status` endpoint of each service until `COMPLETED` is returned.

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+

### 1. Clone the repository

```bash
git clone https://github.com/your-username/nexuslake.git
cd nexuslake
```

### 2. Set environment variables

```bash
cp .env.example .env
# Edit .env and set AIRFLOW_FERNET_KEY
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Start all services

```bash
docker compose up -d
```

### 4. Verify services are healthy

```bash
docker compose ps
```

All services should show `healthy` before the pipeline runs.

### 5. Access the interfaces

| Interface | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| Serving API Swagger | http://localhost:8004/docs |
| Event Producer Swagger | http://localhost:8000/docs |

---

## API Endpoints

### Serving API (`localhost:8004`)

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/metrics/platform` | Latest platform metrics from Gold layer |
| `GET` | `/metrics/platform?platform=mobile` | Filter by platform |
| `GET` | `/metrics/platform?limit=50` | Adjust result limit (max 500) |
| `GET` | `/metrics/summary` | Aggregated totals across all platforms |
| `GET` | `/health` | Spark session status |

### Example response — `/metrics/platform`

```json
{
  "status": "ok",
  "count": 10,
  "data": [
    {
      "window_start": "2026-05-08 22:00:00",
      "window_end": "2026-05-08 22:05:00",
      "platform": "mobile",
      "total_events": 1423,
      "total_revenue": 58320.5,
      "negative_price_count": 2,
      "gold_processed_at": "2026-05-08 22:06:01"
    }
  ]
}
```

---

## Layer Details

### Bronze — Raw Ingestion
- Reads from Kafka using Spark Structured Streaming
- Parses JSON payloads against a fixed schema
- Routes malformed records to a Dead Letter Queue (DLQ) Delta table
- Logs data quality metrics per batch (valid / corrupt counts)

### Silver — Cleansing & Deduplication
- Reads from Bronze Delta table
- Drops records with null `event_id` or `event_timestamp`
- Deduplicates by `event_id` within each micro-batch
- Upserts into Silver Delta table via `MERGE`

### Gold — Business Aggregates
- Reads from Silver Delta table
- Computes 5-minute windowed aggregations per platform
- Metrics: `total_events`, `total_revenue`, `negative_price_count`
- Upserts into Gold Delta table via `MERGE`

### Serving — REST API
- Reads Gold Delta table on demand (no caching)
- Supports platform filtering and result limiting
- Non-blocking reads via `run_in_executor`

---

## Configuration

### `config/spark_config.yaml`

```yaml
spark:
  app_name: NexusLake
  config:
    spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
    spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
    spark.sql.shuffle.partitions: 8
```

### `config/kafka_config.yaml`

```yaml
kafka:
  bootstrap_servers: kafka:29092
  topics:
    raw_events: nexuslake.raw.events
  consumer:
    auto_offset_reset: earliest
```

---

## Dead Letter Queue

Malformed Kafka messages are written to:

```
storage/lakehouse/bronze/dead_letter/
```

Each DLQ record contains the original raw message, topic, partition, offset, and a `failure_reason` field for debugging.

---

## License

MIT