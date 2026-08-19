"""
spark_anomaly.py

StreamPulse Step 6 — Anomaly Detection

This pipeline extends the Step 4 sink pipeline with:

  1. PostgreSQL
     - Windowed device metrics
     - Upserted into device_metrics

  2. Redis
     - Latest state per device
     - TTL of 1 hour

  3. MinIO
     - Raw validated events
     - Archived as Parquet

  4. Rule-based anomaly detection
     - CPU / temperature thresholds
     - Error-code detection
     - Alerts written to PostgreSQL

  5. ML-based anomaly detection
     - Isolation Forest
     - Model trained offline using MinIO data
     - Live scoring inside foreachBatch
     - ML alerts written to PostgreSQL

IMPORTANT:
The event stream receives ONE watermark in main().
Do NOT add another withWatermark() downstream.
This avoids Spark 3.5's:
    "Redefining watermark is disallowed"
error.
"""

import json

import joblib
import pandas as pd
import psycopg2
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    max as spark_max,
    count,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# ===============================================================
# CONFIGURATION
# ===============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device-events"


POSTGRES_CONFIG = dict(
    host="localhost",
    port=5432,
    dbname="streampulse",
    user="streampulse",
    password="streampulse",
)


REDIS_CONFIG = dict(
    host="localhost",
    port=6379,
    db=0,
)


# ---------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------

MINIO_RAW_PATH = "s3a://streampulse-raw/events/"


# ---------------------------------------------------------------
# Checkpoints
#
# IMPORTANT:
# Every streaming query gets its own checkpoint.
# ---------------------------------------------------------------

POSTGRES_CHECKPOINT_PATH = (
    "/tmp/streampulse-checkpoints/postgres-metrics"
)

REDIS_CHECKPOINT_PATH = (
    "/tmp/streampulse-checkpoints/redis-state"
)

MINIO_CHECKPOINT_PATH = (
    "/tmp/streampulse-checkpoints/minio-raw"
)

RULE_ALERT_CHECKPOINT_PATH = (
    "/tmp/streampulse-checkpoints/alerts-rules"
)

ML_ALERT_CHECKPOINT_PATH = (
    "/tmp/streampulse-checkpoints/alerts-ml"
)


# ===============================================================
# RULE-BASED ANOMALY THRESHOLDS
# ===============================================================

CPU_CRITICAL = 90.0
CPU_WARNING = 75.0

TEMP_CRITICAL = 80.0
TEMP_WARNING = 65.0


# ===============================================================
# ML MODEL CONFIGURATION
# ===============================================================

ML_MODEL_PATH = "anomaly_model.joblib"
ML_SCALER_PATH = "anomaly_scaler.joblib"

ML_FEATURE_COLUMNS = [
    "cpu_usage",
    "memory_usage",
    "temperature",
    "network_latency",
]

# Initial threshold.
#
# IMPORTANT:
# This is a tuning parameter.
# After the pipeline works, you can inspect score distributions
# and adjust it.
ML_THRESHOLD = -0.58


# ===============================================================
# EVENT SCHEMA
# ===============================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("cpu_usage", DoubleType(), True),
    StructField("memory_usage", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("network_latency", DoubleType(), True),
    StructField("error_code", StringType(), True),
])


# ===============================================================
# LOAD ML MODEL
#
# Load ONCE when the program starts.
# Do NOT load the model inside every micro-batch.
# ===============================================================

try:
    ML_MODEL = joblib.load(ML_MODEL_PATH)
    ML_SCALER = joblib.load(ML_SCALER_PATH)

    print("[ML] Model loaded successfully")
    print(f"[ML] Model: {ML_MODEL_PATH}")
    print(f"[ML] Scaler: {ML_SCALER_PATH}")

except FileNotFoundError:
    print()
    print("=" * 70)
    print("[ML] ERROR: Model files not found.")
    print()
    print("Run this first:")
    print()
    print("    python3 train_anomaly_model.py")
    print()
    print("That should create:")
    print("    anomaly_model.joblib")
    print("    anomaly_scaler.joblib")
    print("=" * 70)
    print()

    raise


# ===============================================================
# SPARK SESSION
# ===============================================================

def build_spark_session() -> SparkSession:

    return (
        SparkSession.builder
        .appName("StreamPulse-Anomaly-Detection")

        .config(
            "spark.driver.memory",
            "2g"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        # Kafka connector + Hadoop S3A connector
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )

        # -------------------------------------------------------
        # MinIO / S3A configuration
        # -------------------------------------------------------

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000"
        )

        .config(
            "spark.hadoop.fs.s3a.access.key",
            "minioadmin"
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            "minioadmin"
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )

        .getOrCreate()
    )


# ===============================================================
# READ KAFKA STREAM
# ===============================================================

def read_raw_stream(spark: SparkSession):

    return (
        spark.readStream
        .format("kafka")

        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )

        .option(
            "subscribe",
            KAFKA_TOPIC
        )

        # Read existing Kafka data when starting from a fresh
        # checkpoint.
        .option(
            "startingOffsets",
            "earliest"
        )

        .load()
    )


# ===============================================================
# PARSE + VALIDATE EVENTS
# ===============================================================

def parse_and_validate(raw_df):

    parsed_df = (
        raw_df

        # Kafka value is binary.
        # Convert it to a JSON string.
        .selectExpr(
            "CAST(value AS STRING) as json_str"
        )

        # Convert JSON into our schema.
        .withColumn(
            "data",
            from_json(
                col("json_str"),
                EVENT_SCHEMA
            )
        )

        .select("data.*")
    )


    # -----------------------------------------------------------
    # Basic validation
    # -----------------------------------------------------------

    validated_df = parsed_df.filter(

        col("device_id").isNotNull()

        & col("event_id").isNotNull()

        & col("cpu_usage").isNotNull()

        & (col("cpu_usage") >= 0)

        & (col("cpu_usage") <= 100)

        & col("memory_usage").isNotNull()

        & (col("memory_usage") >= 0)

        & (col("memory_usage") <= 100)
    )


    # -----------------------------------------------------------
    # Convert string timestamp into Spark timestamp
    # -----------------------------------------------------------

    validated_df = validated_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )


    return validated_df


# ===============================================================
# WINDOWED AGGREGATES
# ===============================================================

def compute_windowed_aggregates(df):

    """
    IMPORTANT:

    Do NOT call withWatermark() here.

    The watermark is already applied once in main():

        clean_df.withWatermark("event_time", "10 minutes")

    Calling withWatermark() again was the reason for:

        AnalysisException:
        Redefining watermark is disallowed
    """

    windowed = (
        df

        .groupBy(
            window(
                col("event_time"),
                "1 minute"
            ),

            col("device_id"),
        )

        .agg(

            avg("cpu_usage").alias(
                "avg_cpu"
            ),

            spark_max("cpu_usage").alias(
                "max_cpu"
            ),

            avg("temperature").alias(
                "avg_temperature"
            ),

            spark_max("temperature").alias(
                "max_temperature"
            ),

            count("event_id").alias(
                "event_count"
            ),
        )

        .select(

            col("window.start").alias(
                "window_start"
            ),

            col("window.end").alias(
                "window_end"
            ),

            col("device_id"),

            "avg_cpu",
            "max_cpu",
            "avg_temperature",
            "max_temperature",
            "event_count",
        )
    )

    return windowed


# ===============================================================
# SINK 1
# POSTGRES — WINDOWED METRICS
# ===============================================================

def write_batch_to_postgres(batch_df, batch_id):

    """
    Receives a normal, non-streaming DataFrame for one
    micro-batch.

    Writes windowed metrics into PostgreSQL.

    Uses ON CONFLICT so the same device/window can be updated
    rather than duplicated.
    """

    rows = batch_df.collect()

    if not rows:
        return


    conn = psycopg2.connect(
        **POSTGRES_CONFIG
    )

    try:

        with conn.cursor() as cur:

            for row in rows:

                cur.execute(
                    """
                    INSERT INTO device_metrics
                        (
                            device_id,
                            window_start,
                            window_end,
                            avg_cpu,
                            max_cpu,
                            avg_temperature,
                            max_temperature,
                            event_count
                        )

                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )

                    ON CONFLICT
                        (device_id, window_start)

                    DO UPDATE SET

                        window_end =
                            EXCLUDED.window_end,

                        avg_cpu =
                            EXCLUDED.avg_cpu,

                        max_cpu =
                            EXCLUDED.max_cpu,

                        avg_temperature =
                            EXCLUDED.avg_temperature,

                        max_temperature =
                            EXCLUDED.max_temperature,

                        event_count =
                            EXCLUDED.event_count;
                    """,

                    (
                        row.device_id,
                        row.window_start,
                        row.window_end,
                        row.avg_cpu,
                        row.max_cpu,
                        row.avg_temperature,
                        row.max_temperature,
                        row.event_count,
                    ),
                )


        conn.commit()

        print(
            f"[Postgres] batch {batch_id}: "
            f"upserted {len(rows)} rows"
        )


    finally:

        conn.close()


# ===============================================================
# SINK 2
# REDIS — LATEST DEVICE STATE
# ===============================================================

def write_batch_to_redis(batch_df, batch_id):

    """
    Stores the latest event for each device in Redis.

    Example:

        device:D0001

    contains the most recent known state.
    """

    rows = batch_df.collect()

    if not rows:
        return


    r = redis.Redis(
        **REDIS_CONFIG
    )


    # Pipeline means multiple SET operations are sent
    # efficiently instead of making a network round trip
    # for every single event.

    pipe = r.pipeline()


    for row in rows:

        key = f"device:{row.device_id}"


        value = json.dumps({

            "cpu_usage":
                row.cpu_usage,

            "memory_usage":
                row.memory_usage,

            "temperature":
                row.temperature,

            "network_latency":
                row.network_latency,

            "error_code":
                row.error_code,

            "last_seen":
                row.timestamp,
        })


        pipe.set(
            key,
            value,
            ex=3600
        )


    pipe.execute()


    print(
        f"[Redis] batch {batch_id}: "
        f"updated {len(rows)} device states"
    )


# ===============================================================
# RULE-BASED ANOMALY DETECTION
# ===============================================================

def evaluate_rule(row):

    """
    Returns:

        (severity, reason)

    when the event is anomalous.

    Returns:

        None

    when the event is healthy.
    """


    # -----------------------------------------------------------
    # Rule 1: Explicit device error
    # -----------------------------------------------------------

    if row.error_code is not None:

        return (
            "CRITICAL",
            f"Device reported error code: "
            f"{row.error_code}"
        )


    # -----------------------------------------------------------
    # Rule 2: CPU AND temperature both critical
    # -----------------------------------------------------------

    if (
        row.cpu_usage >= CPU_CRITICAL
        and
        row.temperature >= TEMP_CRITICAL
    ):

        return (
            "CRITICAL",
            (
                f"CPU {row.cpu_usage}% and "
                f"temperature {row.temperature}°C "
                f"both critical"
            )
        )


    # -----------------------------------------------------------
    # Rule 3: Either CPU or temperature elevated
    # -----------------------------------------------------------

    if (
        row.cpu_usage >= CPU_WARNING
        or
        row.temperature >= TEMP_WARNING
    ):

        return (
            "WARNING",
            (
                f"CPU {row.cpu_usage}% or "
                f"temperature {row.temperature}°C "
                f"elevated"
            )
        )


    # -----------------------------------------------------------
    # Healthy
    # -----------------------------------------------------------

    return None


# ===============================================================
# SINK 4
# RULE-BASED ALERTS -> POSTGRES
# ===============================================================

def write_batch_alerts(batch_df, batch_id):

    """
    Evaluates every event in a micro-batch.

    Only anomalous events are written to PostgreSQL.
    """

    rows = batch_df.collect()

    if not rows:
        return


    alerts_to_insert = []


    for row in rows:

        result = evaluate_rule(row)


        if result is not None:

            severity, reason = result


            alerts_to_insert.append(

                (
                    row.device_id,
                    row.event_time,
                    "RULE",
                    severity,
                    reason,
                    row.cpu_usage,
                    row.temperature,
                )
            )


    # -----------------------------------------------------------
    # No alerts
    # -----------------------------------------------------------

    if not alerts_to_insert:

        print(
            f"[Alerts] batch {batch_id}: "
            f"0 alerts (all healthy)"
        )

        return


    # -----------------------------------------------------------
    # Write alerts to PostgreSQL
    # -----------------------------------------------------------

    conn = psycopg2.connect(
        **POSTGRES_CONFIG
    )


    try:

        with conn.cursor() as cur:

            cur.executemany(
                """
                INSERT INTO alerts
                    (
                        device_id,
                        event_time,
                        alert_type,
                        severity,
                        reason,
                        cpu_usage,
                        temperature
                    )

                VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    );
                """,

                alerts_to_insert,
            )


        conn.commit()


        print(
            f"[Alerts] batch {batch_id}: "
            f"{len(alerts_to_insert)} alerts written"
        )


    finally:

        conn.close()


# ===============================================================
# SINK 5
# ML ANOMALY DETECTION -> POSTGRES
# ===============================================================

def write_batch_ml_alerts(batch_df, batch_id):

    """
    Runs the pre-trained Isolation Forest against the current
    micro-batch.

    The model is NOT trained here.

    Training happens offline in:

        train_anomaly_model.py

    This function only performs inference.
    """

    rows = batch_df.collect()

    if not rows:
        return


    # -----------------------------------------------------------
    # Convert Spark Rows -> pandas DataFrame
    # -----------------------------------------------------------

    records = []


    for row in rows:

        records.append({

            "device_id":
                row.device_id,

            "event_time":
                row.event_time,

            "cpu_usage":
                row.cpu_usage,

            "memory_usage":
                row.memory_usage,

            "temperature":
                row.temperature,

            "network_latency":
                row.network_latency,
        })


    df = pd.DataFrame(records)


    if df.empty:
        return


    # -----------------------------------------------------------
    # ML requires all feature values to be present.
    #
    # This prevents StandardScaler / IsolationForest from
    # failing if a malformed event contains NULL temperature
    # or network latency.
    # -----------------------------------------------------------

    df = df.dropna(
        subset=ML_FEATURE_COLUMNS
    )


    if df.empty:

        print(
            f"[ML Alerts] batch {batch_id}: "
            f"0 anomalies (no complete ML feature rows)"
        )

        return


    # -----------------------------------------------------------
    # Extract model features
    # -----------------------------------------------------------

    features = df[
        ML_FEATURE_COLUMNS
    ]


    # -----------------------------------------------------------
    # Apply the SAME scaler used during training
    # -----------------------------------------------------------

    scaled_features = ML_SCALER.transform(
        features
    )


    # -----------------------------------------------------------
    # Isolation Forest scoring
    #
    # More negative = more anomalous
    # Less negative / closer to zero = more normal
    # -----------------------------------------------------------

    scores = ML_MODEL.score_samples(
        scaled_features
    )


    df["anomaly_score"] = scores


    # -----------------------------------------------------------
    # Keep only anomalies
    # -----------------------------------------------------------

    anomalies = df[
        df["anomaly_score"] < ML_THRESHOLD
    ]


    # -----------------------------------------------------------
    # No ML anomalies
    # -----------------------------------------------------------

    if anomalies.empty:

        print(
            f"[ML Alerts] batch {batch_id}: "
            f"0 anomalies"
        )

        return


    # -----------------------------------------------------------
    # Write ML alerts to PostgreSQL
    # -----------------------------------------------------------

    conn = psycopg2.connect(
        **POSTGRES_CONFIG
    )


    try:

        with conn.cursor() as cur:

            for _, row in anomalies.iterrows():

                cur.execute(
                    """
                    INSERT INTO alerts
                        (
                            device_id,
                            event_time,
                            alert_type,
                            severity,
                            reason,
                            cpu_usage,
                            temperature,
                            anomaly_score
                        )

                    VALUES
                        (
                            %s,
                            %s,
                            'ML',
                            'WARNING',
                            %s,
                            %s,
                            %s,
                            %s
                        );
                    """,

                    (
                        row["device_id"],

                        row["event_time"],

                        (
                            "Isolation Forest anomaly "
                            f"score "
                            f"{row['anomaly_score']:.3f}"
                        ),

                        row["cpu_usage"],

                        row["temperature"],

                        row["anomaly_score"],
                    ),
                )


        conn.commit()


        print(
            f"[ML Alerts] batch {batch_id}: "
            f"{len(anomalies)} anomalies written"
        )


    finally:

        conn.close()


# ===============================================================
# MAIN
# ===============================================================

def main():

    print()
    print("=" * 70)
    print("StreamPulse — Step 6: Anomaly Detection")
    print("=" * 70)
    print()


    # -----------------------------------------------------------
    # Start Spark
    # -----------------------------------------------------------

    spark = build_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )


    # -----------------------------------------------------------
    # Read Kafka
    # -----------------------------------------------------------

    raw_df = read_raw_stream(
        spark
    )


    # -----------------------------------------------------------
    # Parse + validate
    # -----------------------------------------------------------

    clean_df = parse_and_validate(
        raw_df
    )


    # ===========================================================
    # IMPORTANT WATERMARK
    #
    # Apply the watermark EXACTLY ONCE.
    #
    # The old Step 4 code applied another watermark inside
    # compute_windowed_aggregates(), which caused:
    #
    #   Redefining watermark is disallowed
    #
    # We do not do that anymore.
    # ===========================================================

    watermarked_df = (
        clean_df
        .withWatermark(
            "event_time",
            "10 minutes"
        )
    )


    # -----------------------------------------------------------
    # Deduplicate events
    # -----------------------------------------------------------

    deduped_df = (
        watermarked_df
        .dropDuplicates(
            ["event_id"]
        )
    )
    archive_df = clean_df
    # -----------------------------------------------------------
    # Windowed aggregates
    # -----------------------------------------------------------

    windowed_df = compute_windowed_aggregates(
        deduped_df
    )


    # ===========================================================
    # QUERY 1
    #
    # Windowed aggregates -> PostgreSQL
    # ===========================================================

    postgres_query = (

        windowed_df.writeStream

        .foreachBatch(
            write_batch_to_postgres
        )

        .outputMode(
            "update"
        )

        .option(
            "checkpointLocation",
            POSTGRES_CHECKPOINT_PATH
        )

        .trigger(
            processingTime="10 seconds"
        )

        .start()
    )


    # ===========================================================
    # QUERY 2
    #
    # Raw deduplicated events -> Redis
    # ===========================================================

    redis_query = (

        deduped_df.writeStream

        .foreachBatch(
            write_batch_to_redis
        )

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            REDIS_CHECKPOINT_PATH
        )

        .trigger(
            processingTime="5 seconds"
        )

        .start()
    )


    # ===========================================================
    # QUERY 3
    #
    # Raw deduplicated events -> MinIO
    # ===========================================================

    minio_query = (

        archive_df.writeStream

        .format(
            "parquet"
        )

        .option(
            "path",
            MINIO_RAW_PATH
        )

        .option(
            "checkpointLocation",
            MINIO_CHECKPOINT_PATH
        )

        .outputMode(
            "append"
        )

        .trigger(
            processingTime="30 seconds"
        )

        .start()
    )


    # ===========================================================
    # QUERY 4
    #
    # Rule-based anomaly detection -> PostgreSQL
    # ===========================================================

    alerts_query = (

        deduped_df.writeStream

        .foreachBatch(
            write_batch_alerts
        )

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            RULE_ALERT_CHECKPOINT_PATH
        )

        .trigger(
            processingTime="5 seconds"
        )

        .start()
    )


    # ===========================================================
    # QUERY 5
    #
    # ML anomaly detection -> PostgreSQL
    # ===========================================================

    ml_alerts_query = (

        deduped_df.writeStream

        .foreachBatch(
            write_batch_ml_alerts
        )

        .outputMode(
            "append"
        )

        .option(
            "checkpointLocation",
            ML_ALERT_CHECKPOINT_PATH
        )

        .trigger(
            processingTime="10 seconds"
        )

        .start()
    )


    # ===========================================================
    # STARTUP MESSAGE
    # ===========================================================

    print()
    print("=" * 70)
    print("All five streaming queries are running.")
    print()
    print("1. PostgreSQL  -> windowed metrics")
    print("2. Redis       -> latest device state")
    print("3. MinIO       -> raw Parquet archive")
    print("4. PostgreSQL  -> rule-based alerts")
    print("5. PostgreSQL  -> ML alerts")
    print()
    print("Press Ctrl+C to stop.")
    print("=" * 70)
    print()


    # -----------------------------------------------------------
    # Wait for streaming queries
    # -----------------------------------------------------------

    spark.streams.awaitAnyTermination()


# ===============================================================
# ENTRY POINT
# ===============================================================

if __name__ == "__main__":
    main()
