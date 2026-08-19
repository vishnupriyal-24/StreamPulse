"""
spark_consumer.py

Reads raw device events from the Kafka 'device-events' topic using
Spark Structured Streaming, parses them from JSON into a structured
DataFrame, validates/cleans them, and prints the result to console.

This is intentionally the simplest possible pipeline — no windowing,
no aggregation, no sinks other than console — to prove the Kafka to
Spark connection works before building anything more complex on top.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device-events"

# This must exactly match the JSON structure produced by
# event_simulator.py — same field names, same order doesn't matter,
# but same names and compatible types do.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # parsed as string here, converted later
    StructField("cpu_usage", DoubleType(), True),
    StructField("memory_usage", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("network_latency", DoubleType(), True),
    StructField("error_code", StringType(), True),
])


def build_spark_session() -> SparkSession:
    """
    Creates the SparkSession. The extra configs limit how much memory
    Spark's driver uses, which matters on a VM with limited RAM.
    """
    return (
        SparkSession.builder
        .appName("StreamPulse-Consumer")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # matches our Kafka partition count
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def read_raw_stream(spark: SparkSession):
    """
    Opens a streaming read against the Kafka topic. This does NOT
    start any processing yet — readStream just describes the source.
    Nothing actually runs until we call .start() later on a writer.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # read from the beginning of the topic
        .load()
    )


def parse_and_validate(raw_df):
    """
    Takes the raw Kafka DataFrame (columns: key, value, topic,
    partition, offset, timestamp, timestampType — all as bytes/meta)
    and:
      1. Casts the 'value' column (raw event bytes) to a string
      2. Parses that string as JSON using our schema
      3. Pulls the parsed fields out into top-level columns
      4. Drops any row that failed to parse or is missing required fields
    """
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), EVENT_SCHEMA))
        .select("data.*")  # flatten: promotes data.device_id -> device_id, etc.
    )

    # Basic validation: a row is only valid if the fields we truly
    # can't work without are present and numeric fields are sane.
    validated_df = parsed_df.filter(
        col("device_id").isNotNull()
        & col("event_id").isNotNull()
        & col("cpu_usage").isNotNull()
        & (col("cpu_usage") >= 0) & (col("cpu_usage") <= 100)
        & col("memory_usage").isNotNull()
        & (col("memory_usage") >= 0) & (col("memory_usage") <= 100)
    )

    return validated_df


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # reduce noisy INFO logs

    raw_df = read_raw_stream(spark)
    clean_df = parse_and_validate(raw_df)

    # Write the cleaned stream to the console so we can see it working.
    # outputMode("append") means: only show new rows since the last batch,
    # not the full accumulated result (that matters more once we add
    # aggregations in the next step).
    query = (
        clean_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")  # run a micro-batch every 5 seconds
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
