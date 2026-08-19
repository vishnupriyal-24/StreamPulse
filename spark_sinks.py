"""
spark_sinks.py

Extends the Step 3 windowed pipeline with three real sinks:
  - PostgreSQL: windowed aggregates (device_metrics table), upserted
    so re-emitted windows (from `update` mode) overwrite cleanly
  - Redis: latest raw state per device, for fast "what's happening
    right now" lookups
  - MinIO: raw validated events, archived as Parquet files (data lake)

Postgres and Redis use foreachBatch, since Spark has no native
streaming sink for either. MinIO uses Spark's native streaming file
sink pointed at MinIO's S3-compatible API via the hadoop-aws
connector.
"""

import psycopg2
import redis
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, max as spark_max, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device-events"

POSTGRES_CONFIG = dict(
    host="localhost", port=5432,
    dbname="streampulse", user="streampulse", password="streampulse",
)

REDIS_CONFIG = dict(host="localhost", port=6379, db=0)

MINIO_RAW_PATH = "s3a://streampulse-raw/events/"
MINIO_CHECKPOINT_PATH = "/tmp/streampulse-checkpoints/minio-raw"
POSTGRES_CHECKPOINT_PATH = "/tmp/streampulse-checkpoints/postgres-metrics"
REDIS_CHECKPOINT_PATH = "/tmp/streampulse-checkpoints/redis-state"

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


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamPulse-Sinks")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )
        # --- MinIO / S3A connection settings ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_raw_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_and_validate(raw_df):
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), EVENT_SCHEMA))
        .select("data.*")
    )

    validated_df = parsed_df.filter(
        col("device_id").isNotNull()
        & col("event_id").isNotNull()
        & col("cpu_usage").isNotNull()
        & (col("cpu_usage") >= 0) & (col("cpu_usage") <= 100)
        & col("memory_usage").isNotNull()
        & (col("memory_usage") >= 0) & (col("memory_usage") <= 100)
    )

    validated_df = validated_df.withColumn(
        "event_time", to_timestamp(col("timestamp"))
    )

    return validated_df


def compute_windowed_aggregates(df):
    windowed = (
        df
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("device_id"),
        )
        .agg(
            avg("cpu_usage").alias("avg_cpu"),
            spark_max("cpu_usage").alias("max_cpu"),
            avg("temperature").alias("avg_temperature"),
            spark_max("temperature").alias("max_temperature"),
            count("event_id").alias("event_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            "avg_cpu", "max_cpu", "avg_temperature", "max_temperature", "event_count",
        )
    )
    return windowed


# ---------------------------------------------------------------
# SINK 1: PostgreSQL — upsert windowed aggregates
# ---------------------------------------------------------------

def write_batch_to_postgres(batch_df, batch_id):
    """
    Called once per micro-batch by foreachBatch. batch_df is a
    REGULAR (non-streaming) DataFrame -- we can collect it and use
    normal Python/psycopg2 against it.

    Uses ON CONFLICT (Postgres upsert) so if the same device_id +
    window_start comes through again with updated numbers (because
    the window wasn't finalized yet), we overwrite instead of
    duplicating rows.
    """
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO device_metrics
                        (device_id, window_start, window_end, avg_cpu, max_cpu,
                         avg_temperature, max_temperature, event_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, window_start)
                    DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        avg_cpu = EXCLUDED.avg_cpu,
                        max_cpu = EXCLUDED.max_cpu,
                        avg_temperature = EXCLUDED.avg_temperature,
                        max_temperature = EXCLUDED.max_temperature,
                        event_count = EXCLUDED.event_count;
                    """,
                    (
                        row.device_id, row.window_start, row.window_end,
                        row.avg_cpu, row.max_cpu,
                        row.avg_temperature, row.max_temperature, row.event_count,
                    ),
                )
        conn.commit()
        print(f"[Postgres] batch {batch_id}: upserted {len(rows)} rows")
    finally:
        conn.close()


# ---------------------------------------------------------------
# SINK 2: Redis — latest state per device
# ---------------------------------------------------------------

def write_batch_to_redis(batch_df, batch_id):
    """
    Called once per micro-batch on the RAW (non-windowed) validated
    event stream. For each event, overwrites device:<id> in Redis
    with that event's values -- so Redis always holds only the most
    recent known state per device, not history.
    """
    rows = batch_df.collect()
    if not rows:
        return

    r = redis.Redis(**REDIS_CONFIG)
    pipe = r.pipeline()  # batches all SETs into one network round trip
    for row in rows:
        key = f"device:{row.device_id}"
        value = json.dumps({
            "cpu_usage": row.cpu_usage,
            "memory_usage": row.memory_usage,
            "temperature": row.temperature,
            "network_latency": row.network_latency,
            "error_code": row.error_code,
            "last_seen": row.timestamp,
        })
        pipe.set(key, value, ex=3600)  # expires after 1 hour if device goes silent
    pipe.execute()
    print(f"[Redis] batch {batch_id}: updated {len(rows)} device states")


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw_stream(spark)
    clean_df = parse_and_validate(raw_df)

    # Dedup once, shared by all three downstream sinks.
    deduped_df = clean_df.withWatermark("event_time", "10 minutes").dropDuplicates(["event_id"])

    windowed_df = compute_windowed_aggregates(deduped_df)

    # --- Query 1: windowed aggregates -> Postgres ---
    postgres_query = (
        windowed_df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", POSTGRES_CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Query 2: raw validated events -> Redis (latest state) ---
    redis_query = (
        deduped_df.writeStream
        .foreachBatch(write_batch_to_redis)
        .outputMode("append")
        .option("checkpointLocation", REDIS_CHECKPOINT_PATH)
        .trigger(processingTime="5 seconds")
        .start()
    )

    # --- Query 3: raw validated events -> MinIO (Parquet archive) ---
    minio_query = (
        deduped_df.writeStream
        .format("parquet")
        .option("path", MINIO_RAW_PATH)
        .option("checkpointLocation", MINIO_CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="30 seconds")  # archive in bigger, less frequent batches
        .start()
    )

    # awaitAnyTermination waits on ALL three running queries, and
    # returns/raises if any one of them stops or fails -- since we
    # now have three independent streaming queries running
    # concurrently in the same script.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
