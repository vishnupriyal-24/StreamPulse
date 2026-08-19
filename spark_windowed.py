"""
spark_windowed.py

Extends the Step 2 consumer with:
  - proper event-time timestamp parsing
  - a watermark to bound how late an event can arrive and still count
  - deduplication by event_id within the watermark bound
  - tumbling 1-minute windows aggregating cpu/temp/memory per device

We use 1-minute windows (instead of 5) so you can see multiple windows
finalize in a reasonable amount of time while testing/demoing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, max as spark_max, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device-events"

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
        .appName("StreamPulse-Windowed")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
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
    """
    Same as Step 2, but now we also convert the 'timestamp' string
    into a real TimestampType column, since windowing and watermarks
    both operate on actual timestamp types, not strings.
    """
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

    # Convert the ISO 8601 string timestamp into a real TimestampType.
    # This is the "event_time" column everything downstream will use.
    validated_df = validated_df.withColumn(
        "event_time", to_timestamp(col("timestamp"))
    )

    return validated_df


def deduplicate(df):
    """
    Applies a watermark on event_time, then drops duplicate events
    based on event_id. The watermark bounds how much state Spark has
    to keep around to check for duplicates -- without it, Spark would
    need to remember every event_id it has EVER seen, forever, which
    grows unboundedly in a real long-running stream.

    withWatermark MUST be called before dropDuplicates for this to
    work as a streaming-safe dedup (not just a batch-style one).
    """
    return (
        df.withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )


def compute_windowed_aggregates(df):
    """
    Groups by a 1-minute tumbling window (by event_time) and by
    device_id, computing avg/max cpu, avg temperature, and error
    count per device per window.

    Tumbling = non-overlapping, fixed-size windows: 10:00-10:01,
    10:01-10:02, etc. (as opposed to a sliding window, which would
    overlap, e.g. 10:00-10:01, 10:00:30-10:01:30, ...)
    """
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
            "avg_cpu",
            "max_cpu",
            "avg_temperature",
            "max_temperature",
            "event_count",
        )
    )
    return windowed


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw_stream(spark)
    clean_df = parse_and_validate(raw_df)
    deduped_df = deduplicate(clean_df)
    windowed_df = compute_windowed_aggregates(deduped_df)

    # NOTE: outputMode is "update" here, not "append". Windowed
    # aggregations change as more data arrives for a window that
    # hasn't been finalized yet (e.g. the avg_cpu for 10:00-10:01
    # keeps changing as more events for that minute stream in).
    # "update" mode shows you those in-progress changes each batch.
    # "append" mode would only show a window's row once it's
    # completely finalized (past the watermark) -- useful later when
    # writing to a real sink like Postgres, but less useful for
    # watching it work live in the console.
    query = (
        windowed_df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
