# Commands Used to Set Up Kafka SSL Cluster

## 1. Check Prerequisites

```bash
which keytool && which openssl && java -version
```

## 2. Generate SSL Certificates

```bash
# Make the script executable
chmod +x secrets/create-certs.sh

# Run from inside the secrets/ directory
cd secrets
bash create-certs.sh

# Clean up partial artifacts if re-running
cd secrets
rm -f *.jks *.crt *.key *.csr *.pem *.srl *_creds kafkacat.client.key kafkacat.client.req
```

## 3. Create .env File

```bash
# Create .env in docker/ directory
echo "KAFKA_SSL_SECRETS_DIR=./secrets" > .env
```

## 4. Validate Docker Compose Config

```bash
docker compose config
```

## 5. Add ubuntu User to docker Group

```bash
sudo usermod -aG docker ubuntu
```

## 6. Start the Cluster

```bash
docker compose up -d
```

## 7. Check Container Status

```bash
# Running containers only
docker compose ps

# All containers including stopped
docker compose ps -a
```

## 8. Check Container Logs

```bash
docker compose logs controller-1
docker compose logs controller-3
docker compose logs kafka-ssl-1
```

## 9. Restart the Cluster

```bash
docker compose down && docker compose up -d
```

## 10. Produce Test Messages

```bash
bash ssl-produce.sh
```

What it does internally (via `docker exec`):
```bash
# Create topic
docker exec docker-kafka-ssl-1-1 kafka-topics \
  --bootstrap-server localhost:19092 \
  --command-config /etc/kafka/secrets/host.producer.ssl.config \
  --create --if-not-exists \
  --topic test-ssl-topic \
  --partitions 3 \
  --replication-factor 3

# Produce a message
echo "Hello SSL Kafka" | docker exec -i docker-kafka-ssl-1-1 kafka-console-producer \
  --bootstrap-server localhost:19092 \
  --topic test-ssl-topic \
  --producer.config /etc/kafka/secrets/host.producer.ssl.config
```

## 11. Consume Test Messages

```bash
bash ssl-consume.sh
```

What it does internally (via `docker exec`):
```bash
docker exec docker-kafka-ssl-1-1 kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic test-ssl-topic \
  --consumer.config /etc/kafka/secrets/host.consumer.ssl.config \
  --from-beginning \
  --timeout-ms 10000
```

## Broker Endpoints

| Broker      | SSL Port |
|-------------|----------|
| kafka-ssl-1 | 19092    |
| kafka-ssl-2 | 29092    |
| kafka-ssl-3 | 39092    |

| Controller  | SSL Port |
|-------------|----------|
| controller-1 | 19093   |
| controller-2 | 29093   |
| controller-3 | 39093   |

## Notes

- Image pinned to `confluentinc/cp-kafka:7.7.0` — `latest` (v8.3.2) has a regression with controller-only KRaft nodes.
- `create-certs.sh` required `-noprompt` added to `keytool -import` commands to auto-trust the CA.
- All passwords are `confluent` (test/demo use only).
