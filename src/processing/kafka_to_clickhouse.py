import json
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException


KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ride-clickhouse-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

KAFKA_TOPIC = "ride-events"


clickhouse = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="",
    database="ride_hailing",
)

consumer = Consumer(KAFKA_CONFIG)


def parse_timestamp(value):
    """Convert an ISO timestamp into a Python datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    consumer.subscribe([KAFKA_TOPIC])

    print("Global Kafka-to-ClickHouse consumer started.")
    print("Saving ride events with geographic coordinates.")
    print("Waiting for ride events. Press Ctrl+C to stop.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            event = json.loads(message.value().decode("utf-8"))

            row = [
                event["event_id"],
                event["ride_id"],
                event["rider_id"],
                event["pickup_zone"],
                event["dropoff_zone"],
                float(event["estimated_distance_km"]),
                parse_timestamp(event["request_timestamp"]),
                event["status"],
                event.get("city", ""),
                event.get("country", ""),
                float(event.get("latitude", 0)),
                float(event.get("longitude", 0)),
            ]

            clickhouse.insert(
                "ride_events",
                [row],
                column_names=[
                    "event_id",
                    "ride_id",
                    "rider_id",
                    "pickup_zone",
                    "dropoff_zone",
                    "estimated_distance_km",
                    "event_timestamp",
                    "status",
                    "city",
                    "country",
                    "latitude",
                    "longitude",
                ],
            )

            consumer.commit(
                message=message,
                asynchronous=False
            )

            print(
                f"Saved ride {event['ride_id']} | "
                f"{event.get('city', 'Unknown City')}, "
                f"{event.get('country', 'Unknown Country')} | "
                f"Coordinates: "
                f"{event.get('latitude', 0)}, "
                f"{event.get('longitude', 0)}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        clickhouse.close()
        print("Consumer stopped.")


if __name__ == "__main__":
    main()