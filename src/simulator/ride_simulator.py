import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

ZONES = [
    "Downtown",
    "Airport",
    "Business_District",
    "University",
    "Railway_Station",
    "Residential_Area",
]

# Kafka configuration: connect from Windows to Docker
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "ride-simulator",
}

KAFKA_TOPIC = "ride-events"

# Save data in the project's data folder
OUTPUT_FILE = Path("data/ride_events.jsonl")


def generate_ride_event():
    """Generate one simulated ride request."""

    pickup_zone = random.choice(ZONES)
    dropoff_zone = random.choice(
        [zone for zone in ZONES if zone != pickup_zone]
    )

    return {
        "event_id": str(uuid.uuid4()),
        "ride_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "pickup_zone": pickup_zone,
        "dropoff_zone": dropoff_zone,
        "estimated_distance_km": round(random.uniform(1.0, 30.0), 2),
        "request_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "REQUESTED",
    }


def delivery_report(err, msg):
    """Report whether Kafka accepted the event."""
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka delivered event to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    producer = Producer(KAFKA_CONFIG)

    print("Ride simulator started!")
    print(f"Saving events to: {OUTPUT_FILE}")
    print(f"Publishing events to Kafka topic: {KAFKA_TOPIC}")
    print("Generating one ride every 2 seconds.")
    print("Press Ctrl+C to stop.\n")

    try:
        with OUTPUT_FILE.open("a", encoding="utf-8") as file:
            while True:
                event = generate_ride_event()
                event_json = json.dumps(event, ensure_ascii=False)

                # Print the event to the terminal
                print(event_json)

                # Save one JSON event per line
                file.write(event_json + "\n")
                file.flush()

                # Publish the event to Kafka
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event["ride_id"],
                    value=event_json.encode("utf-8"),
                    callback=delivery_report,
                )

                # Process delivery callbacks
                producer.poll(0)

                # Wait before generating the next ride
                time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping simulator...")

    finally:
        print("Waiting for pending Kafka messages...")
        producer.flush()
        print("Simulator stopped.")


if __name__ == "__main__":
    main()
