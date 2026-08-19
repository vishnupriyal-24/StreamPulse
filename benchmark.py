"""
benchmark.py

StreamPulse end-to-end benchmark.

Measures:
1. Actual event generation/producer throughput.
2. Approximate end-to-end latency from the final event being sent
   to the benchmark device appearing in Redis.

The Spark Structured Streaming pipeline must already be running
in another terminal before starting this benchmark.

Architecture:

    benchmark.py
          |
          v
       Kafka
          |
          v
       Spark
          |
          v
       Redis

A unique benchmark device is created for every run so that an
old Redis key can never make the latency measurement succeed
prematurely.

NOTE:
Redis stores only the latest device state, not event_id.
Therefore the measured latency is an approximate pipeline latency,
not an exact per-event acknowledgement latency.
"""

import argparse
import json
import time
import uuid

import redis
from kafka import KafkaProducer


# ===============================================================
# CONFIGURATION
# ===============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device-events"

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
}


# ===============================================================
# BENCHMARK
# ===============================================================

def run_benchmark(events_per_second: float, duration_seconds: int):

    if events_per_second <= 0:
        raise ValueError("events_per_second must be greater than 0")

    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be greater than 0")


    # -----------------------------------------------------------
    # Kafka producer
    # -----------------------------------------------------------

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )


    # -----------------------------------------------------------
    # Redis connection
    # -----------------------------------------------------------

    r = redis.Redis(
        **REDIS_CONFIG,
        decode_responses=True,
    )


    # -----------------------------------------------------------
    # Unique device for THIS benchmark run
    # -----------------------------------------------------------

    device_id = f"BENCH-{uuid.uuid4().hex[:8]}"

    redis_key = f"device:{device_id}"


    # -----------------------------------------------------------
    # Calculate workload
    # -----------------------------------------------------------

    delay = 1.0 / events_per_second

    total_events = int(
        events_per_second * duration_seconds
    )


    print()
    print("=" * 70)
    print("StreamPulse Benchmark")
    print("=" * 70)

    print(
        f"Target rate       : {events_per_second:.1f} events/sec"
    )

    print(
        f"Duration           : {duration_seconds} seconds"
    )

    print(
        f"Total events       : {total_events}"
    )

    print(
        f"Benchmark device   : {device_id}"
    )

    print(
        f"Redis key          : {redis_key}"
    )

    print("=" * 70)
    print()


    # -----------------------------------------------------------
    # Send events
    # -----------------------------------------------------------

    start_time = time.time()

    last_event_id = None
    last_send_time = None
    last_event_timestamp = None


    for i in range(total_events):

        event_id = str(uuid.uuid4())

        event_time = time.time()


        # UTC timestamp with microsecond precision
        timestamp = (
            time.strftime(
                "%Y-%m-%dT%H:%M:%S",
                time.gmtime(event_time),
            )
            + f".{int((event_time % 1) * 1_000_000):06d}+00:00"
        )


        event = {
            "event_id": event_id,

            "device_id": device_id,

            "timestamp": timestamp,

            "cpu_usage": 50.0,

            "memory_usage": 50.0,

            "temperature": 55.0,

            "network_latency": 15.0,

            "error_code": None,
        }


        producer.send(
            KAFKA_TOPIC,
            key=device_id,
            value=event,
        )


        last_event_id = event_id

        last_event_timestamp = event_time

        last_send_time = time.time()


        # Maintain requested approximate rate.
        time.sleep(delay)


    # -----------------------------------------------------------
    # Make sure all producer messages have been handed off
    # -----------------------------------------------------------

    producer.flush()

    send_end_time = time.time()


    # -----------------------------------------------------------
    # Sending statistics
    # -----------------------------------------------------------

    actual_send_duration = (
        send_end_time - start_time
    )

    actual_throughput = (
        total_events / actual_send_duration
    )


    print(
        f"Finished sending in "
        f"{actual_send_duration:.2f}s"
    )

    print(
        f"Actual producer throughput: "
        f"{actual_throughput:.1f} events/sec"
    )

    print(
        f"Last event_id sent: "
        f"{last_event_id}"
    )

    print()

    print(
        "Waiting for Spark -> Redis..."
    )

    print(
        f"Polling Redis key: {redis_key}"
    )

    print()


    # -----------------------------------------------------------
    # Wait for Redis
    # -----------------------------------------------------------

    poll_start = time.time()

    timeout = 60

    latency = None


    while time.time() - poll_start < timeout:

        value = r.get(redis_key)


        if value:

            try:
                data = json.loads(value)

            except json.JSONDecodeError:

                print(
                    "Redis value exists but could not be decoded."
                )

                time.sleep(0.5)

                continue


            last_seen = data.get("last_seen")


            if last_seen:

                latency = (
                    time.time() - last_send_time
                )


                print(
                    "Redis state received."
                )

                print(
                    f"Redis last_seen     : {last_seen}"
                )

                print(
                    f"Approx E2E latency  : "
                    f"{latency:.2f}s"
                )

                break


        time.sleep(0.5)


    # -----------------------------------------------------------
    # Timeout
    # -----------------------------------------------------------

    if latency is None:

        print(
            "WARNING: Timed out after "
            f"{timeout} seconds waiting for Redis."
        )


    # -----------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------

    producer.close()

    r.close()


    # -----------------------------------------------------------
    # Final benchmark summary
    # -----------------------------------------------------------

    print()

    print("=" * 70)
    print("Benchmark Result")
    print("=" * 70)

    print(
        f"Target throughput    : "
        f"{events_per_second:.1f} events/sec"
    )

    print(
        f"Actual throughput    : "
        f"{actual_throughput:.1f} events/sec"
    )

    if latency is not None:

        print(
            f"Approx E2E latency   : "
            f"{latency:.2f}s"
        )

    else:

        print(
            "Approx E2E latency   : TIMEOUT"
        )

    print("=" * 70)
    print()


# ===============================================================
# COMMAND-LINE INTERFACE
# ===============================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Benchmark StreamPulse Kafka -> Spark -> Redis pipeline"
    )


    parser.add_argument(
        "--eps",
        type=float,
        default=100,
        help="Target events per second",
    )


    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Benchmark duration in seconds",
    )


    args = parser.parse_args()


    run_benchmark(
        args.eps,
        args.duration,
    )
