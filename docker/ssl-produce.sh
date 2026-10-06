#!/bin/bash
set -euo pipefail

BROKER="localhost:19092"
TOPIC="test-ssl-topic"
SECRETS_DIR="$(cd "$(dirname "$0")/secrets" && pwd)"

PRODUCER_CONFIG="$SECRETS_DIR/host.producer.ssl.config"

# Create a temporary config with host-local paths
TMPCONFIG=$(mktemp)
trap 'rm -f "$TMPCONFIG"' EXIT

cat > "$TMPCONFIG" <<EOF
bootstrap.servers=$BROKER
security.protocol=SSL
ssl.truststore.location=$SECRETS_DIR/kafka.producer.truststore.jks
ssl.truststore.password=confluent
ssl.keystore.location=$SECRETS_DIR/kafka.producer.keystore.jks
ssl.keystore.password=confluent
ssl.key.password=confluent
ssl.endpoint.identification.algorithm=
EOF

echo "--- Creating topic '$TOPIC' (if not exists) ---"
docker exec docker-kafka-ssl-1-1 kafka-topics \
  --bootstrap-server "$BROKER" \
  --command-config /etc/kafka/secrets/host.producer.ssl.config \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 3

echo ""
echo "--- Producing 3 test messages ---"
for i in 1 2 3; do
  MSG="Hello SSL Kafka - message $i ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "$MSG" | docker exec -i docker-kafka-ssl-1-1 kafka-console-producer \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC" \
    --producer.config /etc/kafka/secrets/host.producer.ssl.config
  echo "  Produced: $MSG"
done

echo ""
echo "--- Done producing ---"
