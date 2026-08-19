"""
event_simulator.py

Simulates a fleet of devices (servers/sensors) each producing periodic
telemetry events, and publishes them to a Kafka topic.

Each device has a normal baseline for its metrics, but can randomly
enter a "degraded" state for a while to simulate real-world anomalies
(high CPU, high temperature, errors).
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Creates a Kafka producer configured to:
    - serialize event dicts as JSON bytes
    - serialize the partition key (device_id) as UTF-8 bytes
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


class Device:
    """
    Represents one simulated device with its own baseline metrics
    and a small chance of entering/leaving a degraded state.
    """

    def __init__(self, device_id: str):
        self.device_id = device_id

        # Each device has a slightly different normal baseline.
        self.baseline_cpu = random.uniform(20, 50)
        self.baseline_memory = random.uniform(30, 60)
        self.baseline_temp = random.uniform(40, 60)

        self.degraded = False
        self.degraded_ticks_remaining = 0

    def maybe_toggle_degraded_state(self):
        """
        Randomly decide whether this device enters or exits
        a degraded state.
        """
        if self.degraded:
            self.degraded_ticks_remaining -= 1

            if self.degraded_ticks_remaining <= 0:
                self.degraded = False

        else:
            # ~2% chance per tick that a healthy device degrades.
            if random.random() < 0.02:
                self.degraded = True

                # Stay degraded for 5-20 ticks.
                self.degraded_ticks_remaining = random.randint(5, 20)

    def generate_event(self) -> dict:
        """
        Builds one event for this device.
        """
        self.maybe_toggle_degraded_state()

        if self.degraded:
            cpu = min(
                100,
                self.baseline_cpu + random.uniform(30, 50)
            )

            memory = min(
                100,
                self.baseline_memory + random.uniform(20, 40)
            )

            temp = self.baseline_temp + random.uniform(20, 35)

            latency = random.uniform(100, 400)

            # Error while degraded.
            error_code = random.choice(
                [
                    "ERR_TIMEOUT",
                    "ERR_OVERHEAT",
                    "ERR_MEM_LIMIT",
                    None,
                    None,
                ]
            )

        else:
            cpu = max(
                0,
                self.baseline_cpu + random.uniform(-5, 5)
            )

            memory = max(
                0,
                self.baseline_memory + random.uniform(-5, 5)
            )

            temp = self.baseline_temp + random.uniform(-3, 3)

            latency = random.uniform(5, 30)

            error_code = None

        return {
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_usage": round(cpu, 2),
            "memory_usage": round(memory, 2),
            "temperature": round(temp, 2),
            "network_latency": round(latency, 2),
            "error_code": error_code,
        }


def run_simulator(
    num_devices: int,
    events_per_second: float,
    topic: str,
    bootstrap_servers: str,
):
    producer = create_producer(bootstrap_servers)

    devices = [
        Device(f"D{str(i).zfill(4)}")
        for i in range(1, num_devices + 1)
    ]

    # Delay between individual event sends.
    delay = 1.0 / events_per_second

    print(
        f"Starting simulator: {num_devices} devices, "
        f"{events_per_second} events/sec, topic='{topic}'"
    )

    print("Press Ctrl+C to stop.\n")

    sent_count = 0

    try:
        while True:
            # Pick a random device.
            device = random.choice(devices)

            # Generate event.
            event = device.generate_event()

            # Send event to Kafka.
            # device_id is used as the Kafka partition key.
            producer.send(
                topic,
                key=device.device_id,
                value=event,
            )

            sent_count += 1

            if sent_count % 20 == 0:
                print(
                    f"[{sent_count}] "
                    f"{event['device_id']} "
                    f"cpu={event['cpu_usage']} "
                    f"temp={event['temperature']} "
                    f"error={event['error_code']}"
                )

            time.sleep(delay)

    except KeyboardInterrupt:
        print(
            f"\nStopping. Sent {sent_count} events total."
        )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate device telemetry events into Kafka."
    )

    parser.add_argument(
        "--devices",
        type=int,
        default=20,
        help="Number of simulated devices",
    )

    parser.add_argument(
        "--eps",
        type=float,
        default=10,
        help="Events per second",
    )

    parser.add_argument(
        "--topic",
        type=str,
        default="device-events",
        help="Kafka topic",
    )

    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )

    args = parser.parse_args()

    run_simulator(
        num_devices=args.devices,
        events_per_second=args.eps,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
    )
