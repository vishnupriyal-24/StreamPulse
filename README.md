# StreamPulse — Distributed Real-Time Data Intelligence Platform

A real-time streaming pipeline that ingests simulated device telemetry, processes it with windowed aggregation and deduplication, detects anomalies using both rule-based and ML approaches, and serves everything through a REST API and a live dashboard.

**Stack**: Apache Kafka · Spark Structured Streaming · PostgreSQL · Redis · MinIO (S3-compatible object storage) · FastAPI · scikit-learn · Docker Compose

---

## Architecture

```
                     EVENT PRODUCERS
                    (simulated devices)
                           │
                           ↓
                    ┌─────────────┐
                    │    Kafka    │
                    │  4 partitions,
                    │  keyed by device_id
                    └──────┬──────┘
                           │
                           ↓
              ┌────────────────────────┐
              │ Spark Structured       │
              │ Streaming              │
              │                        │
              │ Parse • Validate       │
              │ Watermark (10 min)     │
              │ Deduplicate (event_id) │
              │ 1-min tumbling windows │
              │ Rule-based alerts      │
              │ ML-based alerts        │
              └───────────┬────────────┘
                           │
             ┌─────────────┼──────────────┐
             ↓             ↓              ↓
        ┌─────────┐  ┌──────────┐  ┌──────────┐
        │ Redis   │  │PostgreSQL│  │  MinIO   │
        │ Latest  │  │Windowed  │  │Raw event │
        │ device  │  │metrics + │  │archive   │
        │ state   │  │alerts    │  │(Parquet) │
        └────┬────┘  └────┬─────┘  └────┬─────┘
             │            │              │
             └─────┬──────┘              │
                    ↓                    │
             ┌─────────────┐             │
             │  FastAPI    │             │
             └──────┬──────┘             │
                    ↓                    │
             ┌─────────────┐             │
             │  Dashboard  │             │
             │  (HTML/JS)  │             │
             └─────────────┘             │
                                          ↓
                                  ┌───────────────┐
                                  │ Offline model │
                                  │ training       │
                                  │ (Isolation     │
                                  │  Forest)       │
                                  └───────────────┘
```

---

## What it does

- **Simulates a fleet of devices** (`event_simulator.py`) producing telemetry — CPU, memory, temperature, network latency — with realistic healthy/degraded state transitions (each device has a small per-tick chance of entering a degraded state for several ticks, producing elevated metrics and occasional error codes).
- **Streams events through Kafka**, partitioned by `device_id` so all events for one device land on the same partition (ordering guarantee per device).
- **Spark Structured Streaming** consumes the stream, parses and validates JSON against an explicit schema, applies a 10-minute watermark, deduplicates by `event_id`, and computes 1-minute tumbling window aggregates (avg/max CPU, avg/max temperature, event count) per device.
- **Three sinks** run as independent concurrent streaming queries, each with its own checkpoint:
  - **Redis** — latest raw state per device, 1-hour TTL, for fast "what's happening right now" lookups.
  - **PostgreSQL** — windowed aggregates (upserted via `foreachBatch`, since Structured Streaming has no native Postgres sink) and alerts (append-only history).
  - **MinIO** — raw validated events archived as Parquet via the S3A connector, forming a data lake for offline analysis and model training.
- **Anomaly detection** runs two ways, both writing to the same `alerts` table:
  - **Rule-based**: CPU/temperature threshold checks plus explicit error-code detection, tagged `alert_type = 'RULE'`.
  - **ML-based**: an Isolation Forest trained offline (`train_anomaly_model.py`) on the MinIO Parquet archive, loaded once at startup and scored live against each micro-batch inside `foreachBatch`, tagged `alert_type = 'ML'`.
- **A FastAPI backend** (`api.py`) exposes device state, windowed metrics, and alerts via REST endpoints, and a **single-page dashboard** (`dashboard.html`) polls it every 5 seconds for a live view.

---

## Key design decisions

- **10-minute watermark**: chosen to tolerate realistic delay between an event's actual timestamp and when Spark processes it, without waiting so long that windows never finalize or state grows unbounded. Watermarks bound how long Spark needs to remember past state for both windowing and `dropDuplicates`.
- **Watermark applied exactly once**: an early version applied `withWatermark()` both before deduplication and again inside the windowing function, which Spark 3.5 rejects with `AnalysisException: Redefining watermark is disallowed`. The fix was to apply the watermark once right after parsing/validation, then reuse that single watermarked+deduplicated DataFrame (`deduped_df`) as the input to every downstream sink and the windowing step.
- **`foreachBatch` for Postgres and Redis**: Structured Streaming has no built-in streaming sink for either, so `foreachBatch` hands each micro-batch to normal Python code using `psycopg2`/`redis-py` directly — the same escape hatch used for the ML scoring step.
- **`ON CONFLICT` upsert for windowed metrics**: since window aggregates run in `update` output mode (a window's numbers change as more events arrive before it's finalized), the same `(device_id, window_start)` pair can be re-emitted multiple times. Upserting on that composite primary key avoids duplicate rows.
- **Rule-based alerts before ML**: proved the alerting plumbing (thresholds → Postgres) worked correctly before adding model complexity on top, and gave a rough sanity baseline to compare ML-flagged anomalies against.
- **ML threshold tuned empirically, not guessed**: the first `ML_THRESHOLD` value flagged effectively 100% of events as anomalous. After inspecting the actual score distribution in Postgres, the threshold was retuned to `-0.58`, after which flagged anomalies correlated clearly with genuinely elevated CPU/temperature readings rather than firing indiscriminately.
- **Redis uses `SCAN` instead of `KEYS`** in the API layer — `KEYS` blocks the entire Redis server while it runs; `SCAN` iterates incrementally without blocking, which matters as the keyspace grows.
- **API layer between dashboard and data stores**: the dashboard never talks to Redis/Postgres directly. This keeps database credentials off the client, decouples the frontend from schema/storage changes, and allows other consumers (CLI tools, other services) to reuse the same data without duplicating access logic.

---

## Benchmark results

Benchmarks were run on a single Ubuntu VM under VirtualBox using `benchmark.py`, which measures actual producer throughput and approximate end-to-end latency (time from the last event sent until that device's state is visible in Redis).

| Load (events/sec sent) | Actual achieved throughput | End-to-end latency (Kafka → Spark → Redis) | 
|---|---|---|
| 100   | 74.7 events/sec | 0.03s | 
| 1,000 (run 1) | 179.6 events/sec | 0.32s | 
| 1,000 (run 2) | 202.1 events/sec | 0.01s | 
| 1,000 (run 3) | 183.7 events/sec | 0.18s | 
| 5,000 | 658.4 events/sec | 0.03s | 

The 1,000 eps target was repeated three times to check consistency — throughput varied between ~180-202 events/sec across runs, showing some variance under local VM conditions rather than a single fixed ceiling. Achieved throughput increased as the target rate increased (74.7 → ~190 → 658.4 events/sec across 100/1,000/5,000 targets), suggesting the bottleneck is largely fixed per-event overhead in the Python producer loop rather than a hard capacity limit in the pipeline — at low target rates, that per-event overhead dominates and caps throughput well below the target; at higher targets, the same fixed overhead matters proportionally less, so achieved throughput climbs closer to (though still below) the requested rate.

**Parallelism testing** (`spark.sql.shuffle.partitions` at 2 / 4 / 8, fixed load of 1,000 events/sec target, 10s duration, Spark pipeline restarted between each run):

| Shuffle partitions | Throughput | Approx. E2E latency | 
|---|---|---|
| 2 | 179.6 events/sec | 0.32s | 
| 4 | 202.1 events/sec | 0.01s | 
| 8 | 183.7 events/sec | 0.18s | 

Throughput and latency did not scale meaningfully with shuffle partition count. At this load and on this VM, shuffle parallelism isn't the bottleneck — the limiting factor is more likely the single-threaded Python producer, Docker overhead, or the VM's available CPU cores, rather than how many partitions Spark shuffles across. Increasing `spark.sql.shuffle.partitions` past the number of available CPU cores adds task scheduling overhead without adding real parallel capacity, which may explain why 8 partitions didn't outperform 4.

---

## Failure testing

Both tests were run with the full pipeline (`spark_anomaly.py`) and simulator (`event_simulator.py`) live.

- **Kafka broker killed mid-stream** (`docker stop streampulse-kafka`, left down for ~30s, then `docker start streampulse-kafka`): Spark logged repeated connection warnings while Kafka was down, as expected since it couldn't reach the broker. After restarting Kafka, the pipeline resumed on its own within about 2-3 seconds — no manual restart of `spark_anomaly.py` was needed.
- **Spark job killed and restarted** (`Ctrl+C` on `spark_anomaly.py` while the simulator kept producing, left down for ~30s, then restarted): on restart, the pipeline processed a visible burst of backlogged events before settling back into its normal per-batch rate — consistent with resuming from its checkpointed Kafka offset and catching up on everything the simulator produced while it was down, rather than skipping that data or reprocessing from the beginning.

---

---

## Project structure

```
event_simulator.py       Step 1 — simulates devices, publishes to Kafka
spark_consumer.py        Step 2 — Kafka consume/parse/validate (console output)
spark_windowed.py        Step 3 — + windowing, watermark, deduplication
spark_sinks.py           Step 4 — + Redis/Postgres/MinIO sinks
train_anomaly_model.py   Step 6 — offline Isolation Forest training from MinIO data
spark_anomaly.py         Step 6 — full pipeline: sinks + rule-based + ML alerts
api.py                   Step 7 — FastAPI backend
dashboard.html           Step 7 — live dashboard (polls the API every 5s)
benchmark.py             Step 8 — throughput/latency benchmark tool
docker-compose.yml        Kafka, Redis, PostgreSQL, MinIO
anomaly_model.joblib      Trained Isolation Forest (generated by train_anomaly_model.py)
anomaly_scaler.joblib     Fitted StandardScaler (generated by train_anomaly_model.py)
```

`spark_anomaly.py` is the final, complete pipeline — the earlier `spark_consumer.py`/`spark_windowed.py`/`spark_sinks.py` scripts are kept as incremental snapshots showing how the pipeline was built up in stages.

---

## Running it

1. **Start infrastructure**:
   ```bash
   docker compose up -d
   ```

2. **Create the database schema** (one-time):
   ```bash
   docker exec -it streampulse-postgres psql -U streampulse -d streampulse
   ```
   Then create `device_metrics` and `alerts` tables (see Step 4 and Step 6 setup).

3. **Create the MinIO bucket** (one-time):
   ```bash
   python3 -c "
   import boto3
   s3 = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
   s3.create_bucket(Bucket='streampulse-raw')
   "
   ```

4. **Run the simulator** (separate terminal):
   ```bash
   source venv/bin/activate
   python3 event_simulator.py --devices 20 --eps 10
   ```

5. **Train the anomaly model** (after letting the simulator + pipeline run for a few minutes to archive data — one-time, or re-run periodically):
   ```bash
   python3 train_anomaly_model.py
   ```

6. **Run the full pipeline** (separate terminal):
   ```bash
   python3 spark_anomaly.py
   ```

7. **Run the API** (separate terminal):
   ```bash
   uvicorn api:app --reload --port 8000
   ```

8. **Serve and open the dashboard**:
   ```bash
   python3 -m http.server 8080
   ```
   Then open `http://localhost:8080/dashboard.html`.
