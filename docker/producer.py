#!/usr/bin/env python3
"""
SSL Kafka Producer — sends JSON events to test-ssl-topic.

Certificates are PEM files extracted from the JKS keystores:
  secrets/snakeoil-ca-1.crt   → CA certificate (trust anchor)
  secrets/producer.crt         → producer's signed certificate
  secrets/producer.key         → producer's private key
"""

import json
import time
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

SECRETS_DIR = os.path.join(os.path.dirname(__file__), "secrets")
TOPIC = "test-ssl-topic"
BROKERS = "localhost:19092,localhost:29092,localhost:39092"

SSL_CONFIG = {
    "bootstrap.servers": BROKERS,
    "security.protocol": "SSL",
    "ssl.ca.location": os.path.join(SECRETS_DIR, "snakeoil-ca-1.crt"),
    "ssl.certificate.location": os.path.join(SECRETS_DIR, "producer.crt"),
    "ssl.key.location": os.path.join(SECRETS_DIR, "producer.key"),
    "ssl.endpoint.identification.algorithm": "none",
}


def ensure_topic_exists():
    admin = AdminClient(SSL_CONFIG)
    existing = admin.list_topics(timeout=10).topics
    if TOPIC not in existing:
        result = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=3)])
        for topic, future in result.items():
            try:
                future.result()
                print(f"[admin] Created topic '{topic}'")
            except Exception as e:
                print(f"[admin] Topic '{topic}' already exists or error: {e}")
    else:
        print(f"[admin] Topic '{TOPIC}' already exists")


def delivery_report(err, msg):
    if err:
        print(f"[producer] Delivery FAILED: {err}")
    else:
        print(
            f"[producer] Delivered → topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def main():
    ensure_topic_exists()

    producer = Producer(SSL_CONFIG)
    print(f"\n[producer] Connected to brokers: {BROKERS}")
    print(f"[producer] Sending 5 messages to '{TOPIC}' over SSL...\n")

    for i in range(1, 6):
        event = {
            "event_id": i,
            "message": f"Hello SSL Kafka from Python — event {i}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        payload = json.dumps(event)
        producer.produce(
            topic=TOPIC,
            key=str(i),
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.5)

    print("\n[producer] Flushing...")
    producer.flush()
    print("[producer] Done.")


if __name__ == "__main__":
    main()
