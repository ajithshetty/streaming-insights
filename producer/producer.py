"""
Generates dummy user-interaction events and publishes them to Kafka/Redpanda.

Event shape:
{
  "event_id": "uuid",
  "user_id": "user_042",
  "event_type": "view" | "click" | "add_to_cart" | "purchase" | "scroll",
  "page": "/home",
  "device": "mobile" | "desktop" | "tablet",
  "country": "PT",
  "session_id": "sess_...",
  "event_time": "2026-08-01T12:00:00.000Z"
}

A small "spike" mode is included so the aggregates (and therefore the Q&A
endpoint) have something interesting to talk about occasionally.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.environ.get("EVENTS_TOPIC", "user-events")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "8"))

EVENT_TYPES = ["view", "click", "add_to_cart", "purchase", "scroll"]
EVENT_WEIGHTS = [0.45, 0.25, 0.12, 0.06, 0.12]

PAGES = [
    "/home",
    "/search",
    "/product/101",
    "/product/205",
    "/product/318",
    "/cart",
    "/checkout",
    "/profile",
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.6, 0.32, 0.08]

COUNTRIES = ["PT", "ES", "FR", "DE", "BR", "PL", "RO"]

NUM_USERS = 400


def make_producer() -> KafkaProducer:
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[producer] waiting for kafka ({attempt+1}/30): {exc}")
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka broker")


def random_event(spike: bool) -> dict:
    user_id = f"user_{random.randint(1, NUM_USERS):04d}"
    event_type = random.choices(
        ["purchase", "add_to_cart"] if spike else EVENT_TYPES,
        weights=[0.5, 0.5] if spike else EVENT_WEIGHTS,
        k=1,
    )[0]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "page": random.choice(PAGES),
        "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
        "country": random.choice(COUNTRIES),
        "session_id": f"sess_{user_id}_{random.randint(1, 5)}",
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }


def main() -> None:
    producer = make_producer()
    print(f"[producer] publishing to '{TOPIC}' on {BOOTSTRAP_SERVERS} "
          f"at ~{EVENTS_PER_SECOND}/s")

    delay = 1.0 / max(EVENTS_PER_SECOND, 0.1)
    next_spike_at = time.time() + random.uniform(30, 90)
    spike_until = 0.0

    sent = 0
    while True:
        now = time.time()
        if now >= next_spike_at and spike_until == 0.0:
            spike_until = now + random.uniform(8, 15)
            print("[producer] triggering a purchase spike")
        spike = now < spike_until
        if spike_until and now >= spike_until:
            spike_until = 0.0
            next_spike_at = now + random.uniform(45, 120)

        event = random_event(spike)
        producer.send(TOPIC, key=event["user_id"], value=event)
        sent += 1
        if sent % 200 == 0:
            print(f"[producer] sent {sent} events")

        time.sleep(delay)


if __name__ == "__main__":
    main()