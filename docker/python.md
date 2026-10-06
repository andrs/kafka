# Python SSL Producer & Consumer — How to Run

## Prerequisites

### 1. Create and activate a Python virtual environment

```bash
cd docker/

# Create the venv
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

The `requirements.txt` contains:
```
confluent-kafka==2.15.1
```

To deactivate the venv when you are done:
```bash
deactivate
```

### 2. Make sure the Kafka cluster is running

```bash
cd docker/
docker compose ps
```

All 6 containers must show `Up`:
```
docker-controller-1-1   Up
docker-controller-2-1   Up
docker-controller-3-1   Up
docker-kafka-ssl-1-1    Up
docker-kafka-ssl-2-1    Up
docker-kafka-ssl-3-1    Up
```

If not running, start it:
```bash
docker compose up -d
# Wait ~20 seconds for the cluster to be ready
```

### 3. Make sure PEM certificates exist

```bash
ls docker/secrets/producer.key producer.crt consumer.key consumer.crt snakeoil-ca-1.crt
```

If any are missing, extract them from the JKS keystores:

```bash
cd docker/secrets

# Producer certs
keytool -importkeystore \
  -srckeystore kafka.producer.keystore.jks \
  -destkeystore producer.p12 \
  -deststoretype PKCS12 \
  -srcalias producer \
  -deststorepass confluent -srcstorepass confluent -noprompt

openssl pkcs12 -in producer.p12 -nocerts -nodes -passin pass:confluent -out producer.key
openssl pkcs12 -in producer.p12 -nokeys          -passin pass:confluent -out producer.crt

# Consumer certs
keytool -importkeystore \
  -srckeystore kafka.consumer.keystore.jks \
  -destkeystore consumer.p12 \
  -deststoretype PKCS12 \
  -srcalias consumer \
  -deststorepass confluent -srcstorepass confluent -noprompt

openssl pkcs12 -in consumer.p12 -nocerts -nodes -passin pass:confluent -out consumer.key
openssl pkcs12 -in consumer.p12 -nokeys          -passin pass:confluent -out consumer.crt
```

---

## Run the Producer

Activate the venv first, then run the script:

```bash
cd docker/
source .venv/bin/activate
python producer.py
```

Or run it directly without activating:

```bash
cd docker/
.venv/bin/python producer.py
```

### Expected output

```
[admin] Topic 'test-ssl-topic' already exists

[producer] Connected to brokers: localhost:19092,localhost:29092,localhost:39092
[producer] Sending 5 messages to 'test-ssl-topic' over SSL...

[producer] Delivered → topic=test-ssl-topic partition=2 offset=1
[producer] Delivered → topic=test-ssl-topic partition=1 offset=3
[producer] Delivered → topic=test-ssl-topic partition=1 offset=4
[producer] Delivered → topic=test-ssl-topic partition=1 offset=5

[producer] Flushing...
[producer] Delivered → topic=test-ssl-topic partition=1 offset=6
[producer] Done.
```

Each `Delivered` line confirms the message was acknowledged by the broker. The partition and offset values may differ.

---

## Run the Consumer

```bash
cd docker/
source .venv/bin/activate
python consumer.py
```

Or directly without activating:

```bash
cd docker/
.venv/bin/python consumer.py
```

### Expected output

```
[consumer] Connected to brokers: localhost:19092,localhost:29092,localhost:39092
[consumer] Subscribed to 'test-ssl-topic' | group.id='python-ssl-consumer-group'
[consumer] Waiting for messages... (Ctrl+C to stop)

[consumer] Message #1 | partition=2 offset=1 | key=1
           payload → {
  "event_id": 1,
  "message": "Hello SSL Kafka from Python — event 1",
  "timestamp": "2026-10-06T12:14:10.447008+00:00"
}
...
[consumer] No more messages. Exiting.
[consumer] Total messages received: 5
```

The consumer reads from `auto.offset.reset=earliest`, so it always starts from the beginning of the topic. Press `Ctrl+C` at any time to stop it gracefully.

---

## Run Producer and Consumer Together (two terminals)

Open **Terminal 1** — start the consumer first so it is ready to receive:

```bash
cd docker/
source .venv/bin/activate
python consumer.py
```

Open **Terminal 2** — run the producer:

```bash
cd docker/
source .venv/bin/activate
python producer.py
```

Switch back to Terminal 1 and watch messages arrive in real time.

---

## SSL Config Reference

Both scripts resolve certificates from the `secrets/` folder automatically. The SSL parameters used are:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `security.protocol` | `SSL` | Encrypt all traffic with TLS |
| `ssl.ca.location` | `secrets/snakeoil-ca-1.crt` | CA cert to verify broker identity |
| `ssl.certificate.location` | `secrets/producer.crt` or `consumer.crt` | Client certificate sent to broker |
| `ssl.key.location` | `secrets/producer.key` or `consumer.key` | Client private key |
| `ssl.endpoint.identification.algorithm` | `none` | Disable hostname verification (self-signed CA) |

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'confluent_kafka'`
The venv is not activated or the package was not installed inside it.
```bash
cd docker/
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### `SSL handshake failed` or `certificate verify failed`
The PEM files may be missing or were generated from a different keystore than the running cluster.
Re-extract the certs (see Prerequisites step 3) and restart the cluster with `docker compose down -v && docker compose up -d`.

### `KafkaException: Failed to resolve 'localhost'`
The cluster is not running. Start it with `docker compose up -d` and wait 20 seconds.

### Consumer shows 0 messages
The consumer's `group.id` may have already committed offsets to the end.  
Change `group.id` in `consumer.py` to a new name (e.g. `python-ssl-group-2`) to reset.
