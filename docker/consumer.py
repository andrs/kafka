#!/usr/bin/env python3
"""
SSL Kafka Consumer — reads JSON events from test-ssl-topic.

Certificates are PEM files extracted from the JKS keystores:
  secrets/snakeoil-ca-1.crt   → CA certificate (trust anchor)
  secrets/consumer.crt         → consumer's signed certificate
  secrets/consumer.key         → consumer's private key
"""

import json
import os
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException

SECRETS_DIR = os.path.join(os.path.dirname(__file__), "secrets")
TOPIC = "test-ssl-topic"
BROKERS = "localhost:19092,localhost:29092,localhost:39092"

SSL_CONFIG = {
    "bootstrap.servers": BROKERS,
    "security.protocol": "SSL",
    "ssl.ca.location": os.path.join(SECRETS_DIR, "snakeoil-ca-1.crt"),
    "ssl.certificate.location": os.path.join(SECRETS_DIR, "consumer.crt"),
    "ssl.key.location": os.path.join(SECRETS_DIR, "consumer.key"),
    "ssl.endpoint.identification.algorithm": "none",
    "group.id": "python-ssl-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

running = True


def shutdown(sig, frame):
    global running
    print("\n[consumer] Shutting down...")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    consumer = Consumer(SSL_CONFIG)
    consumer.subscribe([TOPIC])

    print(f"[consumer] Connected to brokers: {BROKERS}")
    print(f"[consumer] Subscribed to '{TOPIC}' | group.id='{SSL_CONFIG['group.id']}'")
    print("[consumer] Waiting for messages... (Ctrl+C to stop)\n")

    received = 0
    empty_polls = 0

    try:
        while running:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                empty_polls += 1
                if empty_polls >= 5:
                    print("[consumer] No more messages. Exiting.")
                    break
                continue

            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                        f"[consumer] End of partition: "
                        f"{msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                received += 1
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    print(
                        f"[consumer] Message #{received} "
                        f"| partition={msg.partition()} offset={msg.offset()} "
                        f"| key={msg.key().decode('utf-8') if msg.key() else None}"
                    )
                    print(f"           payload → {json.dumps(value, indent=2)}\n")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    print(f"[consumer] Raw message: {msg.value()}")

    finally:
        consumer.close()
        print(f"[consumer] Total messages received: {received}")


if __name__ == "__main__":
    main()
