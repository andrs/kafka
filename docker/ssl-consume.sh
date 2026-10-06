#!/bin/bash
set -euo pipefail

BROKER="localhost:19092"
TOPIC="test-ssl-topic"

echo "--- Consuming messages from '$TOPIC' (timeout 10s) ---"
docker exec docker-kafka-ssl-1-1 kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --consumer.config /etc/kafka/secrets/host.consumer.ssl.config \
  --from-beginning \
  --timeout-ms 10000

echo ""
echo "--- Done consuming ---"
