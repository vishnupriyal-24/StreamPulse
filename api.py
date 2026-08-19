"""
api.py

A small FastAPI backend exposing StreamPulse's data:
  - GET /devices              -> latest state of every device (from Redis)
  - GET /devices/{device_id}  -> latest state of one device (from Redis)
  - GET /metrics/{device_id}  -> recent windowed metrics history (from Postgres)
  - GET /alerts                -> recent alerts, optionally filtered (from Postgres)
  - GET /summary                -> counts for a dashboard header

Run with: uvicorn api:app --reload --port 8000
"""

import json
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

POSTGRES_CONFIG = dict(
    host="localhost", port=5432,
    dbname="streampulse", user="streampulse", password="streampulse",
)
REDIS_CONFIG = dict(host="localhost", port=6379, db=0)

app = FastAPI(title="StreamPulse API")

# CORS: allows a browser-based dashboard (served from a different
# port, e.g. a simple `python3 -m http.server`) to call this API.
# Without this, browsers block the request by default for
# security reasons (cross-origin requests).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_redis():
    return redis.Redis(**REDIS_CONFIG, decode_responses=True)


def get_postgres():
    return psycopg2.connect(**POSTGRES_CONFIG)


@app.get("/devices")
def list_devices():
    """
    Returns the latest known state of every device currently in
    Redis. Uses SCAN instead of KEYS -- KEYS blocks the whole Redis
    server while it runs, which is fine for our demo scale but is
    exactly the kind of thing worth knowing NOT to do in production
    with a large keyspace. SCAN iterates incrementally instead.
    """
    r = get_redis()
    devices = []
    for key in r.scan_iter("device:*"):
        value = r.get(key)
        if value:
            device_id = key.split(":", 1)[1]
            data = json.loads(value)
            data["device_id"] = device_id
            devices.append(data)
    return {"count": len(devices), "devices": devices}


@app.get("/devices/{device_id}")
def get_device(device_id: str):
    r = get_redis()
    value = r.get(f"device:{device_id}")
    if value is None:
        raise HTTPException(status_code=404, detail=f"No recent data for device {device_id}")
    data = json.loads(value)
    data["device_id"] = device_id
    return data


@app.get("/metrics/{device_id}")
def get_device_metrics(device_id: str, limit: int = Query(default=20, le=200)):
    """
    Returns the most recent windowed aggregate rows for one device
    from Postgres -- useful for a "CPU over time" chart.
    """
    conn = get_postgres()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT device_id, window_start, window_end,
                       avg_cpu, max_cpu, avg_temperature, max_temperature, event_count
                FROM device_metrics
                WHERE device_id = %s
                ORDER BY window_start DESC
                LIMIT %s;
                """,
                (device_id, limit),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No metrics found for device {device_id}")

    # Convert datetime objects to ISO strings so FastAPI can JSON-serialize them.
    for row in rows:
        row["window_start"] = row["window_start"].isoformat()
        row["window_end"] = row["window_end"].isoformat()

    return {"device_id": device_id, "metrics": rows}


@app.get("/alerts")
def list_alerts(
    limit: int = Query(default=50, le=500),
    severity: Optional[str] = Query(default=None),
    device_id: Optional[str] = Query(default=None),
):
    """
    Returns recent alerts, optionally filtered by severity
    (WARNING/CRITICAL) and/or device_id. This is the main endpoint
    a dashboard's "active alerts" panel would poll.
    """
    conn = get_postgres()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = "SELECT * FROM alerts WHERE 1=1"
            params = []

            if severity:
                query += " AND severity = %s"
                params.append(severity.upper())
            if device_id:
                query += " AND device_id = %s"
                params.append(device_id)

            query += " ORDER BY event_time DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    for row in rows:
        row["event_time"] = row["event_time"].isoformat()

    return {"count": len(rows), "alerts": rows}


@app.get("/summary")
def summary():
    """
    Aggregate counts for a dashboard header: total devices seen
    recently, active alert counts by severity, and how many events
    have been processed overall (approximated via total alert +
    metric event counts -- a real system might track this
    separately, but this is enough for a demo).
    """
    r = get_redis()
    device_count = sum(1 for _ in r.scan_iter("device:*"))

    conn = get_postgres()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT severity, COUNT(*) FROM alerts "
                "WHERE event_time > NOW() - INTERVAL '10 minutes' "
                "GROUP BY severity;"
            )
            severity_counts = dict(cur.fetchall())

            cur.execute("SELECT COUNT(*) FROM alerts;")
            total_alerts = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "active_devices": device_count,
        "recent_alerts_by_severity": severity_counts,
        "total_alerts_all_time": total_alerts,
        "timestamp": datetime.utcnow().isoformat(),
    }
