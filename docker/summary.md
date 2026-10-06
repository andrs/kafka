# Kafka SSL Cluster — Full Setup Explanation

## What Is This?

This project runs a **production-style Apache Kafka cluster secured with SSL/TLS** using Docker Compose. It uses the modern **KRaft mode** (no Zookeeper) with:

- **3 Controller nodes** — manage cluster metadata and leader election
- **3 Broker nodes** — handle message storage and client connections

All communication — between controllers, between brokers, and between clients and brokers — is encrypted with SSL/TLS using mutual authentication (both sides present a certificate).

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                    HOST MACHINE (localhost)               │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │              KRaft Controller Quorum                 │ │
│  │                                                      │ │
│  │  controller-1 :19093  controller-2 :29093            │ │
│  │              controller-3 :39093                     │ │
│  │         (SSL-encrypted Raft consensus)               │ │
│  └─────────────────────────────────────────────────────┘ │
│                          │ SSL                            │
│  ┌─────────────────────────────────────────────────────┐ │
│  │                  Kafka Brokers                       │ │
│  │                                                      │ │
│  │  kafka-ssl-1 :19092   kafka-ssl-2 :29092             │ │
│  │              kafka-ssl-3 :39092                      │ │
│  │         (SSL-encrypted client connections)           │ │
│  └─────────────────────────────────────────────────────┘ │
│                          │ SSL                            │
│             Producer / Consumer clients                   │
└──────────────────────────────────────────────────────────┘
```

All containers run with `network_mode: host`, so they share the host's network stack and communicate over `localhost` ports.

---

## SSL Certificate Architecture

The cluster uses a **self-signed CA** (Certificate Authority) to sign all certificates. Each node and each client gets its own keypair signed by this CA.

```
snakeoil-ca-1 (Root CA)
│
├── kafka.controller1.keystore.jks  (controller 1 identity)
├── kafka.controller2.keystore.jks  (controller 2 identity)
├── kafka.controller3.keystore.jks  (controller 3 identity)
├── kafka.broker1.keystore.jks      (broker 1 identity)
├── kafka.broker2.keystore.jks      (broker 2 identity)
├── kafka.broker3.keystore.jks      (broker 3 identity)
├── kafka.producer.keystore.jks     (producer client identity)
└── kafka.consumer.keystore.jks     (consumer client identity)

Each node also gets a truststore that trusts the Root CA:
  kafka.<node>.truststore.jks
```

**Keystores** hold the node's private key and certificate (proves "I am who I say I am").  
**Truststores** hold the CA certificate (proves "I trust certificates signed by this CA").

---

## Step-by-Step Guide to Try It

### Prerequisites

Make sure you have these installed on your machine:

```bash
# Verify tools are available
which openssl       # for generating CA and signing certificates
which keytool       # Java tool for managing keystores (comes with JDK)
java -version       # any JDK/JRE version works
docker compose version  # Docker Compose v2
```

---

### Step 1 — Generate SSL Certificates

Navigate to the `docker/` directory and run the certificate generation script:

```bash
cd docker/secrets
bash create-certs.sh
```

This script does the following:

1. **Generates a Root CA** (`snakeoil-ca-1.key` + `snakeoil-ca-1.crt`) — the trust anchor for the whole cluster.
2. **Generates a kafkacat client certificate** — for CLI tooling.
3. **For each node** (controller1, controller2, controller3, broker1, broker2, broker3, producer, consumer):
   - Creates a **keystore** with a new RSA keypair
   - Issues a **CSR** (Certificate Signing Request)
   - Signs the CSR with the Root CA → produces a signed certificate
   - Imports the CA cert + signed cert into the keystore (establishing the chain of trust)
   - Creates a **truststore** containing the Root CA certificate
   - Writes **credential files** (files containing the password `confluent`) so the brokers can read passwords without them being embedded in environment variables

After running, you should see files like:
```
kafka.broker1.keystore.jks       kafka.broker1.truststore.jks
kafka.controller1.keystore.jks   kafka.controller1.truststore.jks
kafka.producer.keystore.jks      kafka.producer.truststore.jks
kafka.consumer.keystore.jks      kafka.consumer.truststore.jks
broker1_keystore_creds           broker1_truststore_creds
...
```

---

### Step 2 — Set Up the Environment Variable

The `docker-compose.yml` uses a variable `${KAFKA_SSL_SECRETS_DIR}` for the volume mount path. Create a `.env` file in the `docker/` directory:

```bash
# From inside the docker/ directory
echo "KAFKA_SSL_SECRETS_DIR=./secrets" > .env
```

This tells Docker Compose to mount the `secrets/` folder into each container at `/etc/kafka/secrets`, which is where Kafka looks for SSL keystores.

---

### Step 3 — Validate the Configuration

Before starting anything, confirm the variable resolves correctly:

```bash
docker compose config
```

Look for a `volumes` section like this — it confirms the variable expanded to an absolute path:

```yaml
volumes:
  - type: bind
    source: /your/absolute/path/docker/secrets
    target: /etc/kafka/secrets
```

If you see `${KAFKA_SSL_SECRETS_DIR}` unexpanded, check that `.env` exists in the `docker/` directory.

---

### Step 4 — Add Your User to the Docker Group (if needed)

If you get `permission denied while trying to connect to the Docker daemon`:

```bash
sudo usermod -aG docker $USER
# Then use sg to apply it in the current session without logging out:
sg docker -c "docker compose up -d"
```

---

### Step 5 — Start the Cluster

```bash
docker compose up -d
```

This pulls `confluentinc/cp-kafka:7.7.0` (pinned — see Notes) and starts all 6 containers.

Wait about **15–20 seconds** for the KRaft controllers to elect a leader and for brokers to register.

---

### Step 6 — Verify Everything Is Running

```bash
docker compose ps -a
```

Expected output — all containers should show `Up`:

```
NAME                    IMAGE                         STATUS
docker-controller-1-1   confluentinc/cp-kafka:7.7.0   Up
docker-controller-2-1   confluentinc/cp-kafka:7.7.0   Up
docker-controller-3-1   confluentinc/cp-kafka:7.7.0   Up
docker-kafka-ssl-1-1    confluentinc/cp-kafka:7.7.0   Up
docker-kafka-ssl-2-1    confluentinc/cp-kafka:7.7.0   Up
docker-kafka-ssl-3-1    confluentinc/cp-kafka:7.7.0   Up
```

Check the controller logs to confirm quorum formed:

```bash
docker compose logs controller-3 | tail -20
```

Look for lines like:
```
[ControllerServer id=3] Kafka Server started
[QuorumController id=3] Replayed RegisterControllerRecord ...
```

Check broker logs to confirm SSL loaded and broker started:

```bash
docker compose logs kafka-ssl-1 | tail -20
```

Look for:
```
SSL is enabled.
[BrokerServer id=4] Transition from STARTING to STARTED
[KafkaRaftServer nodeId=4] Kafka Server started
```

---

### Step 7 — Produce Messages Over SSL

Run the producer script from the `docker/` directory:

```bash
bash ssl-produce.sh
```

This will:
1. Create the topic `test-ssl-topic` with 3 partitions and replication factor 3
2. Produce 3 timestamped messages to the topic

Expected output:
```
--- Creating topic 'test-ssl-topic' (if not exists) ---
Created topic test-ssl-topic.

--- Producing 3 test messages ---
  Produced: Hello SSL Kafka - message 1 (2026-10-06T12:03:59Z)
  Produced: Hello SSL Kafka - message 2 (2026-10-06T12:04:02Z)
  Produced: Hello SSL Kafka - message 3 (2026-10-06T12:04:04Z)

--- Done producing ---
```

**How SSL is applied in the producer:** the script passes `--producer.config /etc/kafka/secrets/host.producer.ssl.config` which configures:
- `security.protocol=SSL` — use SSL for the connection
- `ssl.keystore.location` — the producer's certificate (proves client identity to the broker)
- `ssl.truststore.location` — used to verify the broker's certificate
- `ssl.key.password` / `ssl.keystore.password` / `ssl.truststore.password` — all `confluent`

---

### Step 8 — Consume Messages Over SSL

```bash
bash ssl-consume.sh
```

Expected output:
```
--- Consuming messages from 'test-ssl-topic' (timeout 10s) ---
Hello SSL Kafka - message 1 (2026-10-06T12:03:59Z)
Hello SSL Kafka - message 2 (2026-10-06T12:04:02Z)
Hello SSL Kafka - message 3 (2026-10-06T12:04:04Z)
Processed a total of 3 messages

--- Done consuming ---
```

The `TimeoutException` at the end is **normal** — it means `--timeout-ms 10000` expired because there are no more messages to read.

---

### Step 9 — Stop the Cluster

```bash
docker compose down
```

To also remove all stored data (wipe topic data between runs):

```bash
docker compose down -v
```

---

## Troubleshooting

### Controllers exit immediately on startup

**Symptom:** `docker compose ps -a` shows controllers as `Exited (1)`.

**Cause 1:** `cp-kafka:latest` (v8.3.2+) rejects `KAFKA_ADVERTISED_LISTENERS` on controller-only nodes — use the pinned version:
```yaml
image: confluentinc/cp-kafka:7.7.0
```

**Cause 2:** Keystores don't exist yet — run `bash secrets/create-certs.sh` first.

**Cause 3:** `keytool -import` in `create-certs.sh` prompted interactively and failed — the fixed version includes `-noprompt` on all import commands.

---

### SSL Handshake errors in broker logs

**Symptom:** Logs contain `SSLHandshakeException` or `certificate_unknown`.

**Cause:** The certificates were regenerated but old container data still references the previous certificates.

**Fix:**
```bash
docker compose down -v   # remove volumes with old data
docker compose up -d
```

---

### Permission denied connecting to Docker

```bash
sudo usermod -aG docker $USER
sg docker -c "docker compose up -d"
```

---

## File Reference

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Defines all 6 containers (3 controllers + 3 brokers) |
| `.env` | Sets `KAFKA_SSL_SECRETS_DIR=./secrets` |
| `secrets/create-certs.sh` | Generates all SSL keystores, truststores, and credential files |
| `secrets/host.producer.ssl.config` | SSL config for producer clients |
| `secrets/host.consumer.ssl.config` | SSL config for consumer clients (adds `group.id=ssl-host`) |
| `ssl-produce.sh` | Creates topic and produces 3 test messages |
| `ssl-consume.sh` | Reads all messages from `test-ssl-topic` from the beginning |

---

## Key Concepts Demonstrated

| Concept | Detail |
|---------|--------|
| **KRaft mode** | No Zookeeper — controllers manage metadata via Raft consensus |
| **Mutual TLS** | Both brokers and clients authenticate with certificates |
| **JKS keystores** | Java KeyStore format used by Kafka for certificate storage |
| **Credential files** | Passwords stored in separate files, not hardcoded in env vars |
| **network_mode: host** | All containers share the host network — no bridge overhead |
| **Replication factor 3** | Every topic partition is replicated across all 3 brokers |
