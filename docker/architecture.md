# Kafka SSL Cluster Architecture — Detailed Explanation

## Overview

This Kafka cluster uses **KRaft mode** (Kafka Raft) — a modern architecture that eliminates the need for Apache Zookeeper. It consists of **6 containers** organized into two distinct layers:

- **3 Controller nodes** — manage cluster metadata using Raft consensus
- **3 Broker nodes** — store and serve message data to clients

All communication between nodes and clients is encrypted with **SSL/TLS mutual authentication**.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      KAFKA KRAFT CLUSTER                         │
│                     (All SSL/TLS Encrypted)                      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           CONTROLLER QUORUM (Metadata Layer)                │ │
│  │                                                             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │ │
│  │  │controller-1  │  │controller-2  │  │controller-3  │    │ │
│  │  │  Node ID: 1  │  │  Node ID: 2  │  │  Node ID: 3  │    │ │
│  │  │  Port: 19093 │  │  Port: 29093 │  │  Port: 39093 │    │ │
│  │  │              │  │              │  │              │    │ │
│  │  │   LEADER ★   │  │   FOLLOWER   │  │   FOLLOWER   │    │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘    │ │
│  │         ▲                 ▲                 ▲              │ │
│  │         └─────────────────┴─────────────────┘              │ │
│  │              Raft Consensus Protocol                       │ │
│  │          (Elects 1 active leader at any time)              │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              │ Metadata API (SSL)                │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │             BROKER CLUSTER (Data Layer)                     │ │
│  │                                                             │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │ │
│  │  │ kafka-ssl-1  │  │ kafka-ssl-2  │  │ kafka-ssl-3  │    │ │
│  │  │  Node ID: 4  │  │  Node ID: 5  │  │  Node ID: 6  │    │ │
│  │  │  Port: 19092 │  │  Port: 29092 │  │  Port: 39092 │    │ │
│  │  │              │  │              │  │              │    │ │
│  │  │  Partitions  │  │  Partitions  │  │  Partitions  │    │ │
│  │  │    0,1,2     │  │    0,1,2     │  │    0,1,2     │    │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘    │ │
│  │         ▲                 ▲                 ▲              │ │
│  │         └─────────────────┴─────────────────┘              │ │
│  │           Replication & Client Connections                 │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              ▲                                   │
│                              │ SSL (Client Protocol)             │
│                              │                                   │
│                    ┌─────────┴─────────┐                        │
│                    │                   │                        │
│               Producers           Consumers                     │
│             (Python/CLI)         (Python/CLI)                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Layer 1: Controller Quorum (Metadata Management)

### Container Details

```
NAME                    IMAGE                         STATUS    PORT
docker-controller-1-1   confluentinc/cp-kafka:7.7.0   Up        19093
docker-controller-2-1   confluentinc/cp-kafka:7.7.0   Up        29093
docker-controller-3-1   confluentinc/cp-kafka:7.7.0   Up        39093
```

### Responsibilities

Controllers manage the **cluster metadata** — the "brain" of the Kafka cluster:

| Metadata Type | Description |
|---------------|-------------|
| **Cluster membership** | Which brokers are alive and registered |
| **Topic configuration** | Which topics exist, partition count, replication factor |
| **Partition assignment** | Which brokers host which partition replicas |
| **Partition leadership** | Which broker is the leader for each partition |
| **Access control** | ACLs, quotas, user permissions |

**Important:** Controllers **do NOT store or serve message data**. They only manage configuration and state.

### Raft Consensus Protocol

The 3 controllers use the **Raft algorithm** to maintain a replicated log of all metadata changes:

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│controller-1  │       │controller-2  │       │controller-3  │
│   LEADER ★   │──────▶│   FOLLOWER   │──────▶│   FOLLOWER   │
│              │◀──────│              │◀──────│              │
└──────────────┘       └──────────────┘       └──────────────┘
     │                       │                       │
     │  1. Leader receives metadata change request   │
     │  2. Leader appends to its Raft log            │
     │  3. Leader replicates to followers            │
     │  4. Followers acknowledge replication         │
     │  5. Leader commits once majority acks         │
     └───────────────────────┴───────────────────────┘
```

### Leadership Election

At any given time, **exactly 1 controller is the active LEADER**:

**Normal state:**
```
controller-1: LEADER   (handles all metadata requests)
controller-2: FOLLOWER (syncs from leader, ready to take over)
controller-3: FOLLOWER (syncs from leader, ready to take over)
```

**After leader failure:**
```
controller-1: CRASHED
controller-2: NEW LEADER ★ (elected in ~500ms)
controller-3: FOLLOWER
```

**Election requirements:**
- **Quorum:** `(N/2) + 1` nodes must agree
  - With 3 nodes: **2 votes = majority** → can tolerate 1 failure
  - With 5 nodes: **3 votes = majority** → can tolerate 2 failures
  - With 1 node: **1 vote = majority** → single point of failure (not recommended)

**Election trigger:** If followers don't receive a heartbeat from the leader for >1 second (configurable), they start an election.

---

## Layer 2: Broker Cluster (Data Storage & Client Serving)

### Container Details

```
NAME                  IMAGE                         STATUS    PORT
docker-kafka-ssl-1-1  confluentinc/cp-kafka:7.7.0   Up        19092
docker-kafka-ssl-2-1  confluentinc/cp-kafka:7.7.0   Up        29092
docker-kafka-ssl-3-1  confluentinc/cp-kafka:7.7.0   Up        39092
```

### Responsibilities

Brokers are the **data workers** that:

1. **Accept producer writes** — receive messages over SSL on port 19092/29092/39092
2. **Serve consumer reads** — deliver messages to consumers
3. **Persist messages to disk** — store data in `/var/lib/kafka/data` inside each container
4. **Replicate partitions** — copy data across brokers for fault tolerance
5. **Report health to controllers** — send heartbeats to stay in the cluster

### Partition Leadership (Separate from Controller Leadership)

Each **topic partition** has its own leader among the brokers:

**Example:** `test-ssl-topic` with 3 partitions and replication factor 3:

```
Partition 0:
  Leader:   kafka-ssl-1 (port 19092)  ← producers/consumers connect here
  Replicas: [kafka-ssl-1, kafka-ssl-2, kafka-ssl-3]

Partition 1:
  Leader:   kafka-ssl-2 (port 29092)  ← producers/consumers connect here
  Replicas: [kafka-ssl-2, kafka-ssl-3, kafka-ssl-1]

Partition 2:
  Leader:   kafka-ssl-3 (port 39092)  ← producers/consumers connect here
  Replicas: [kafka-ssl-3, kafka-ssl-1, kafka-ssl-2]
```

**Key points:**
- **Producers always write to the partition leader**
- **Consumers read from the partition leader** (by default)
- **Replicas sync data in the background** from the leader
- If a leader fails, the controller promotes one of the **in-sync replicas (ISR)** to be the new leader

---

## Communication Flows (All SSL Encrypted)

### 1. Controller-to-Controller Communication

```
controller-1:19093 ←──SSL──→ controller-2:29093 ←──SSL──→ controller-3:39093
```

**Purpose:** Raft log replication
- Leader replicates every metadata change to followers
- Followers send heartbeat acknowledgments
- Election messages during leadership changes

**Protocol:** Internal Raft protocol over SSL
**Security:** Mutual TLS with keystores/truststores

---

### 2. Broker-to-Controller Communication

```
kafka-ssl-1 ──────SSL────────▶ controller-leader
            ◀────metadata────┘

kafka-ssl-2 ──────SSL────────▶ controller-leader
            ◀────metadata────┘

kafka-ssl-3 ──────SSL────────▶ controller-leader
            ◀────metadata────┘
```

**Purpose:**
- Brokers register with the controller on startup
- Brokers send periodic heartbeats to prove they are alive
- Brokers fetch metadata updates (topic changes, partition assignments)
- Brokers report partition replica status (in-sync, lagging)

**Security:** Mutual TLS authentication

---

### 3. Broker-to-Broker Replication

```
kafka-ssl-1:19092 ←──SSL──→ kafka-ssl-2:29092 ←──SSL──→ kafka-ssl-3:39092
```

**Purpose:** Partition replica synchronization
- Follower replicas fetch data from partition leaders
- Each broker acts as both a leader (for some partitions) and a follower (for others)

**Example data flow:**
```
Producer ──writes──▶ kafka-ssl-1 (partition 0 leader)
                           │
                           ├──replicates──▶ kafka-ssl-2 (follower)
                           └──replicates──▶ kafka-ssl-3 (follower)
```

**Security:** Mutual TLS with `KAFKA_INTER_BROKER_LISTENER_NAME: SSL`

---

### 4. Client-to-Broker Communication

```
Producer/Consumer ──SSL──▶ kafka-ssl-1:19092 (partition 0 leader)
                  ──SSL──▶ kafka-ssl-2:29092 (partition 1 leader)
                  ──SSL──▶ kafka-ssl-3:39092 (partition 2 leader)
```

**Producer flow:**
1. Producer connects to `bootstrap.servers: localhost:19092,localhost:29092,localhost:39092`
2. Producer fetches metadata from any broker → learns which broker leads which partition
3. Producer sends each message to the partition leader based on the key hash
4. Leader writes to local disk and waits for replicas to ack
5. Leader returns acknowledgment to producer

**Consumer flow:**
1. Consumer connects to bootstrap servers
2. Consumer joins a consumer group and gets partition assignments
3. Consumer fetches messages from partition leaders
4. Consumer commits offsets (either to Kafka or auto-commit)

**Security:**
- Client presents certificate from `producer.crt` / `consumer.crt`
- Broker presents certificate from `broker1.keystore.jks`
- Both sides verify via `snakeoil-ca-1.crt` (CA certificate)

---

## Fault Tolerance Scenarios

### Scenario 1: Controller Leader Failure

**Initial state:**
```
controller-1: LEADER
controller-2: FOLLOWER
controller-3: FOLLOWER
brokers: all connected to controller-1
```

**Failure occurs:**
```
1. controller-1 crashes or loses network
2. controller-2 and controller-3 detect missing heartbeats (timeout: ~1 second)
3. Election starts → controller-2 wins (has most up-to-date log)
4. controller-2 becomes the new LEADER
5. Brokers reconnect to controller-2
6. Cluster continues operating normally
```

**Downtime:** ~500ms to 2 seconds (imperceptible to clients)

**Data loss:** None — Raft guarantees all committed metadata changes survive

---

### Scenario 2: Broker Failure

**Initial state:**
```
Partition 0 → Leader: kafka-ssl-1, Replicas: [kafka-ssl-1, kafka-ssl-2, kafka-ssl-3]
Partition 1 → Leader: kafka-ssl-2, Replicas: [kafka-ssl-2, kafka-ssl-3, kafka-ssl-1]
Partition 2 → Leader: kafka-ssl-3, Replicas: [kafka-ssl-3, kafka-ssl-1, kafka-ssl-2]
```

**kafka-ssl-1 crashes:**
```
1. Broker stops sending heartbeats to controller
2. Controller detects failure after heartbeat timeout (~10 seconds)
3. Controller promotes new leaders for partitions led by kafka-ssl-1:
   - Partition 0: kafka-ssl-2 becomes new leader
4. Controller updates metadata and notifies all brokers
5. Producers/consumers automatically reconnect to new leaders
6. Partition 0 is now under-replicated (2/3 replicas instead of 3/3)
```

**When kafka-ssl-1 recovers:**
```
7. kafka-ssl-1 rejoins the cluster
8. It becomes a follower for partition 0
9. It syncs missing data from the current leader (kafka-ssl-2)
10. Once caught up, it rejoins the in-sync replica set (ISR)
11. Partition 0 is now fully replicated again (3/3)
```

**Data loss:** None (assuming `min.insync.replicas=2` and `acks=all` in producer config)

---

### Scenario 3: Network Partition

**Split-brain scenario:**
```
controller-1 ──X──  controller-2, controller-3
```

**What happens:**
- controller-1 can no longer reach the other controllers
- controller-2 and controller-3 (majority = 2/3) elect a new leader
- controller-1 steps down automatically (cannot reach quorum)
- Brokers connected to controller-1 reconnect to the new leader

**Result:** The majority partition (2 controllers) continues operating. The isolated controller shuts down its leadership role.

---

## Why KRaft Instead of Zookeeper?

### Old Architecture (Kafka < 3.0)

```
┌──────────────────────┐
│  Zookeeper Cluster   │  ← Separate system (3–5 nodes)
│  (External Quorum)   │
└──────────────────────┘
           │
           │ TCP (port 2181)
           ▼
┌──────────────────────┐
│   Kafka Brokers      │  ← 3+ nodes
│   (No controllers)   │
└──────────────────────┘
```

**Problems:**
- **Two systems to manage** — Zookeeper and Kafka
- **Slow failover** — Controller election via Zookeeper takes 10–30 seconds
- **Scalability limits** — Zookeeper struggles with millions of partitions
- **Operational complexity** — different tuning, monitoring, and upgrade cycles

---

### New Architecture (Kafka 3.0+, KRaft Mode)

```
┌──────────────────────────────┐
│  Controller Quorum (Raft)    │  ← Built into Kafka
│  (controllers 1, 2, 3)       │
└──────────────────────────────┘
           │
           │ Internal Kafka protocol
           ▼
┌──────────────────────────────┐
│   Broker Nodes               │
│   (brokers 1, 2, 3)          │
└──────────────────────────────┘
```

**Benefits:**
- **Single system** — No Zookeeper dependency
- **Fast failover** — Raft election in <1 second
- **Better scalability** — Supports millions of partitions
- **Simpler operations** — Unified monitoring, logging, and configuration
- **Lower latency** — Metadata changes propagate faster

---

## Configuration Summary

### Node Roles

| Node | Kafka Node ID | Process Role | Port | Purpose |
|------|---------------|--------------|------|---------|
| controller-1 | 1 | `controller` | 19093 | Metadata management |
| controller-2 | 2 | `controller` | 29093 | Metadata management |
| controller-3 | 3 | `controller` | 39093 | Metadata management |
| kafka-ssl-1 | 4 | `broker` | 19092 | Data storage + client serving |
| kafka-ssl-2 | 5 | `broker` | 29092 | Data storage + client serving |
| kafka-ssl-3 | 6 | `broker` | 39092 | Data storage + client serving |

### SSL Configuration

Every node has its own identity certificate:

| Node | Keystore | Truststore | Password |
|------|----------|------------|----------|
| controller-1 | `kafka.controller1.keystore.jks` | `kafka.controller1.truststore.jks` | `confluent` |
| controller-2 | `kafka.controller2.keystore.jks` | `kafka.controller2.truststore.jks` | `confluent` |
| controller-3 | `kafka.controller3.keystore.jks` | `kafka.controller3.truststore.jks` | `confluent` |
| broker-1 | `kafka.broker1.keystore.jks` | `kafka.broker1.truststore.jks` | `confluent` |
| broker-2 | `kafka.broker2.keystore.jks` | `kafka.broker2.truststore.jks` | `confluent` |
| broker-3 | `kafka.broker3.keystore.jks` | `kafka.broker3.truststore.jks` | `confluent` |
| producer | `producer.crt` + `producer.key` (PEM) | `snakeoil-ca-1.crt` | — |
| consumer | `consumer.crt` + `consumer.key` (PEM) | `snakeoil-ca-1.crt` | — |

All certificates are signed by the same CA: `snakeoil-ca-1.crt`

---

## Capacity Planning

### Fault Tolerance

| Failure | Can Cluster Survive? | Minimum Quorum |
|---------|----------------------|----------------|
| 1 controller down | ✅ Yes (2/3 quorum remains) | 2 controllers |
| 2 controllers down | ❌ No (1/3 < quorum) | Cluster freezes metadata updates |
| 1 broker down | ✅ Yes (2/3 replicas remain) | All data accessible |
| 2 brokers down | ✅ Yes (1/3 replica remains) | All data accessible (assuming RF=3) |
| 3 brokers down | ❌ No | Data unavailable |

### Scaling Considerations

**To increase capacity:**
- Add more **brokers** (scale horizontally for storage/throughput)
- Brokers can be added without changing controller count

**To increase metadata reliability:**
- Add more **controllers** (5 controllers = tolerate 2 failures)
- Must be an odd number (3, 5, 7) for Raft quorum

**Typical production setups:**
- **Small cluster:** 3 controllers, 3–6 brokers
- **Medium cluster:** 3 controllers, 10–50 brokers
- **Large cluster:** 5 controllers, 100+ brokers

---

## Network Mode: Host

All containers use `network_mode: host`, which means:

- Containers **share the host's network stack**
- No Docker bridge or NAT
- Containers bind directly to `localhost` ports
- Lower latency, simpler routing

**Port allocation:**
```
Host ports used:
  19092, 29092, 39092  ← broker SSL listeners
  19093, 29093, 39093  ← controller SSL listeners
```

**Trade-off:**
- ✅ Better performance
- ❌ Requires unique ports per container (can't run multiple clusters on the same host easily)

---

## Security Model

### Mutual TLS (mTLS)

Every connection requires **both sides to authenticate**:

```
Client                          Broker
  │                               │
  ├────── SSL Handshake ──────────▶
  │  "Here's my certificate"      │
  │   (producer.crt)              │
  │                               │
  ◀────── SSL Challenge ──────────│
     "Here's my certificate"      │
      (broker1 certificate)       │
  │                               │
  ├──── Verify via CA ────────────▶
  │   (snakeoil-ca-1.crt)         │
  │                               │
  ◀──── Verify via CA ────────────│
  │   (snakeoil-ca-1.crt)         │
  │                               │
  ├────── Encrypted Data ─────────▶
```

### Certificate Chain

```
snakeoil-ca-1.crt (Root CA)
     │
     ├─── Signs ───▶ controller1.crt
     ├─── Signs ───▶ controller2.crt
     ├─── Signs ───▶ controller3.crt
     ├─── Signs ───▶ broker1.crt
     ├─── Signs ───▶ broker2.crt
     ├─── Signs ───▶ broker3.crt
     ├─── Signs ───▶ producer.crt
     └─── Signs ───▶ consumer.crt
```

Every node trusts the Root CA → can verify any certificate signed by it.

---

## Summary

This Kafka cluster demonstrates a **modern, production-grade architecture**:

✅ **Resilient** — survives single node failures in both layers  
✅ **Scalable** — can add brokers independently of controllers  
✅ **Secure** — all communication encrypted with mutual TLS  
✅ **Fast** — sub-second metadata failover with Raft  
✅ **Simple** — no external dependencies (no Zookeeper)  

The separation of **controllers (metadata)** and **brokers (data)** allows independent scaling and specialization of each layer.
