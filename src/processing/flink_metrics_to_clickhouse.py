import json
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "flink-metrics-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

KAFKA_TOPIC = "ride-metrics"

clickhouse = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="",
    database="ride_hailing",
)

consumer = Consumer(KAFKA_CONFIG)


def parse_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    consumer.subscribe([KAFKA_TOPIC])

    print("Flink metrics consumer started.")
    print("Waiting for Flink analytics events...")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            event = json.loads(message.value().decode("utf-8"))

            row = [
                parse_timestamp(event["window_start"]),
                parse_timestamp(event["window_end"]),
                event["pickup_zone"],
                int(event["total_rides"]),
            ]

            clickhouse.insert(
                "ride_metrics_by_zone",
                [row],
                column_names=[
                    "window_start",
                    "window_end",
                    "pickup_zone",
                    "total_rides",
                ],
            )

            consumer.commit(message=message, asynchronous=False)

            print(
                f"Saved metrics: {event['pickup_zone']} "
                f"| Rides: {event['total_rides']}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        clickhouse.close()
        print("Consumer stopped.")


if __name__ == "__main__":
    main()