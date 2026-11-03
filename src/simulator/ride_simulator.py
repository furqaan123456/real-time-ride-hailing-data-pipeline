import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


# Pickup zones
ZONES = [
    "Downtown",
    "Airport",
    "Business_District",
    "University",
    "Railway_Station",
    "Residential_Area",
]


# Global cities and their approximate central coordinates.
# Coordinates are simulated and are not exact pickup addresses.

CITIES = {
    "Bengaluru": {
        "country": "India",
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    "Mumbai": {
        "country": "India",
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    "Delhi": {
        "country": "India",
        "latitude": 28.6139,
        "longitude": 77.2090,
    },
    "Hyderabad": {
        "country": "India",
        "latitude": 17.3850,
        "longitude": 78.4867,
    },
    "Dubai": {
        "country": "UAE",
        "latitude": 25.2048,
        "longitude": 55.2708,
    },
    "Abu Dhabi": {
        "country": "UAE",
        "latitude": 24.4539,
        "longitude": 54.3773,
    },
    "London": {
        "country": "UK",
        "latitude": 51.5072,
        "longitude": -0.1276,
    },
    "Manchester": {
        "country": "UK",
        "latitude": 53.4808,
        "longitude": -2.2426,
    },
    "New York": {
        "country": "USA",
        "latitude": 40.7128,
        "longitude": -74.0060,
    },
    "San Francisco": {
        "country": "USA",
        "latitude": 37.7749,
        "longitude": -122.4194,
    },
    "Chicago": {
        "country": "USA",
        "latitude": 41.8781,
        "longitude": -87.6298,
    },
    "Berlin": {
        "country": "Germany",
        "latitude": 52.5200,
        "longitude": 13.4050,
    },
    "Frankfurt": {
        "country": "Germany",
        "latitude": 50.1109,
        "longitude": 8.6821,
    },
    "Singapore": {
        "country": "Singapore",
        "latitude": 1.3521,
        "longitude": 103.8198,
    },
}


# Approximate coordinate offsets for different pickup zones.
# These are illustrative offsets around each city center.

ZONE_OFFSETS = {
    "Downtown": (0.000, 0.000),
    "Airport": (0.025, 0.035),
    "Business_District": (-0.010, 0.015),
    "University": (0.015, -0.020),
    "Railway_Station": (-0.015, -0.005),
    "Residential_Area": (0.010, 0.025),
}


# Kafka configuration: connect from Windows to Docker
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "global-ride-simulator",
}

KAFKA_TOPIC = "ride-events"


# Save data in the project's data folder
OUTPUT_FILE = Path("data/ride_events.jsonl")


def generate_ride_event():
    """Generate one simulated global ride request."""

    city = random.choice(list(CITIES.keys()))
    city_info = CITIES[city]

    pickup_zone = random.choice(ZONES)

    dropoff_zone = random.choice(
        [zone for zone in ZONES if zone != pickup_zone]
    )

    # Get approximate coordinates for the selected pickup zone
    latitude_offset, longitude_offset = ZONE_OFFSETS[pickup_zone]

    # Add small random variation to spread markers on the map
    latitude = (
        city_info["latitude"]
        + latitude_offset
        + random.uniform(-0.003, 0.003)
    )

    longitude = (
        city_info["longitude"]
        + longitude_offset
        + random.uniform(-0.003, 0.003)
    )

    return {
        "event_id": str(uuid.uuid4()),
        "ride_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "pickup_zone": pickup_zone,
        "dropoff_zone": dropoff_zone,
        "estimated_distance_km": round(random.uniform(1.0, 30.0), 2),
        "city": city,
        "country": city_info["country"],
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
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

    print("GLOBAL RIDE SIMULATOR STARTED!")
    print(f"Saving events to: {OUTPUT_FILE}")
    print(f"Publishing events to Kafka topic: {KAFKA_TOPIC}")
    print(f"Available cities: {len(CITIES)}")
    print("Generating one ride every 2 seconds.")
    print("Press Ctrl+C to stop.\n")

    try:
        with OUTPUT_FILE.open("a", encoding="utf-8") as file:

            while True:
                event = generate_ride_event()

                event_json = json.dumps(
                    event,
                    ensure_ascii=False
                )

                # Print generated event
                print(event_json)

                # Save event to JSON file
                file.write(event_json + "\n")
                file.flush()

                # Publish event to Kafka
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event["ride_id"],
                    value=event_json.encode("utf-8"),
                    callback=delivery_report,
                )

                producer.poll(0)

                time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopping global simulator...")

    finally:
        print("Waiting for pending Kafka messages...")
        producer.flush()
        print("Global simulator stopped.")


if __name__ == "__main__":
    main()