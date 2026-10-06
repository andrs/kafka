# Event-Driven Architecture with Python & Apache Kafka (SSL)

> A fully working, SSL-secured, multi-service event-driven system built with Python and Apache Kafka.
> Each microservice communicates exclusively through Kafka topics — no direct service-to-service calls.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Event Flow](#event-flow)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Services](#services)
- [Kafka Topics](#kafka-topics)
- [Schemas](#schemas)
- [Configuration](#configuration)
- [Running the Examples](#running-the-examples)
- [Documentation](#documentation)

---

## Overview

This project demonstrates a **food delivery platform** built entirely on event-driven principles:

- A customer places an order via a REST API
- The order flows automatically through payment, food preparation, delivery, and notification services
- Every step is validated against an **Avro schema** before being published or consumed
- All Kafka traffic is encrypted with **mutual TLS (SSL)**
- Failed events are never lost — they are routed to a **dead-letter queue**
- Fault tolerance is provided by a **circuit breaker** on every consumer

| Feature | Implementation |
|---------|---------------|
| Message broker | Apache Kafka 7.7 (KRaft mode, 3 brokers) |
| Security | Mutual TLS — SSL on all broker ports |
| Schema validation | Apache Avro (`fastavro`) |
| REST API | Flask 3.1 |
| Fault tolerance | `pybreaker` circuit breaker + retry with exponential backoff |
| Idempotency | Dead-letter queue + processed-events deduplication table |
| Partition strategy | Sticky partition assignor |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kafka Cluster (SSL)                         │
│                                                                     │
│   broker-1 :19092    broker-2 :29092    broker-3 :39092            │
│   controller-1       controller-2       controller-3               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Topics
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   order.updates        payment.updates      food.updates
   dead.letter          delivery.updates
          │
┌─────────▼──────────────────────────────────────────────────────────┐
│                          Services                                   │
│                                                                     │
│  [UI / Flask API]                                                   │
│       │  POST /initiate_orders                                      │
│       │  validates: order_created_schema v1.0                       │
│       ▼                                                             │
│  [Order Service] ──── listens: order/payment/food/delivery.updates  │
│       │               publishes: order.updates                      │
│       ▼                                                             │
│  [Payment Service] ── listens: order.updates (order_created)        │
│       │               publishes: payment.updates                    │
│       ▼                                                             │
│  [Food Service] ───── listens: order.updates (order_confirmed)      │
│       │               publishes: food.updates                       │
│       ▼                                                             │
│  [Delivery Service] ─ listens: order/delivery.updates               │
│       │               publishes: delivery.updates                   │
│       ▼                                                             │
│  [Notification] ───── listens: order/delivery.updates               │
│  [Logging] ─────────── listens: ALL topics (observer)              │
└────────────────────────────────────────────────────────────────────┘
```

---

## Event Flow

```
Customer
   │
   │  POST /initiate_orders
   ▼
UI Service ──────────────────────────► order.updates
                                         event: order_initiated
                                              │
                                              ▼
                                       Order Service ──► order.updates
                                         validates &        event: order_created
                                         persists                │
                                                                 ▼
                                                        Payment Service ──► payment.updates
                                                          charges customer    event: payment_processed
                                                                                   │
                                                                                   ▼
                                                                           Order Service ──► order.updates
                                                                             confirms order    event: order_confirmed
                                                                                                   │
                                                                                                   ▼
                                                                                          Food Service ──► food.updates
                                                                                           prepares food   event: order_prepared
                                                                                                               │
                                                                                                               ▼
                                                                                                      Order Service ──► order.updates
                                                                                                        marks ready      event: order_ready
                                                                                                                             │
                                                                                                                             ▼
                                                                                                                    Delivery Service ──► delivery.updates
                                                                                                                     picks up & delivers  event: delivery_completed
                                                                                                                                               │
                                                                                                                                               ▼
                                                                                                                                    Order Service ──► order.updates
                                                                                                                                     closes order      event: order_completed
                                                                                                                                          │
                                                                                                                                          ▼
                                                                                                                                 Notification Service
                                                                                                                                  notifies customer
```

At every step, if **schema validation fails** the event is diverted to `dead.letter` and never processed.

---

## Project Structure

```
Chapter09/
│
├── base_consumer_config.py         Shared Kafka consumer config (SSL, brokers, sticky partition)
├── base_producer_config.py         Shared Kafka producer config (SSL, acks=all, gzip, retries)
├── kafka_util.py                   SSL context, serializers, circuit breaker, DLQ helper
├── validators.py                   AvroEventValidator — version check + field validation
├── requirements.txt                Python dependencies
├── docker-compose.yaml             Single-node Kafka (no SSL, for local dev)
│
├── schema/                         Avro schema contracts (.avsc)
│   ├── order_created_schema.avsc
│   ├── order_prepared_confirmed_schema.avsc
│   ├── order_complete_schema.avsc
│   ├── order_cancel_schema.avsc
│   ├── payment_schema.avsc
│   ├── delivery_picked_completed_schema.avsc
│   └── inventory_updated.avsc
│
├── user/
│   └── ui_service_producer.py      Flask REST API — entry point for new orders
│
├── order/
│   ├── order_service_consumer.py   Drives the full order state machine
│   └── order_service_producer.py   Publishes order state transitions
│
├── payment/
│   ├── payment_service_consumer.py Listens for order_created, initiates payment
│   └── payment_service_producer.py Publishes payment_processed
│
├── food/
│   ├── food_service_consumer.py    Listens for order_confirmed, starts food prep
│   └── food_service_producer.py    Publishes order_prepared
│
├── delivery/
│   ├── delivery_service_consumer.py Listens for order_ready, manages pickup/delivery
│   └── delivery_service_producer.py Publishes delivery_picked / delivery_completed
│
├── notification/
│   └── notification_service_consumer.py Sends (mocked) customer notifications
│
├── logging/
│   └── logging_service_consumer.py Subscribes to all topics — prints every event
│
├── docker/                         SSL Kafka cluster (3 brokers + 3 controllers)
│   ├── docker-compose.yml          KRaft cluster with mutual TLS
│   ├── secrets/                    Generated SSL certs & keystores (git-ignored)
│   └── create-certs.sh             Script that generates all SSL material
│
├── build/                          Legacy SSL artefacts (git-ignored)
│
├── README.md                       This file
├── run.md                          Step-by-step run guide
├── schemas.md                      Schema reference + 10 usage examples
├── commands.md                     All CLI commands used in this chapter
└── results.md                      Output of every schema example run
```

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10+ | |
| Docker | 24+ | For Kafka cluster |
| Docker Compose | v2 | `docker compose` (not `docker-compose`) |

---

## Quick Start

### 1. Clone and enter the directory

```bash
git clone <repo-url>
cd kafka
```

### 2. Generate SSL certificates

```bash
cd docker
bash secrets/create-certs.sh
cd ..
```

### 3. Start the Kafka cluster

```bash
cd docker
sudo docker compose up -d
cd ..
```

Verify 6 containers are running:

```bash
sudo docker ps --format "table {{.Names}}\t{{.Status}}"
```

```
NAMES                   STATUS
docker-kafka-ssl-1-1    Up ...
docker-kafka-ssl-2-1    Up ...
docker-kafka-ssl-3-1    Up ...
docker-controller-1-1   Up ...
docker-controller-2-1   Up ...
docker-controller-3-1   Up ...
```

### 4. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5. Set PYTHONPATH

```bash
export PYTHONPATH="$(pwd)"
```

### 6. Start the services

Open a **separate terminal** for each service. Run the setup line (`source` + `export`) in each one first.

```bash
# Terminal 1 — Logging (watch all events)
cd logging && python3 logging_service_consumer.py

# Terminal 2 — Order service
cd order && python3 order_service_consumer.py

# Terminal 3 — Payment service
cd payment && python3 payment_service_consumer.py

# Terminal 4 — Food service
cd food && python3 food_service_consumer.py

# Terminal 5 — Delivery service
cd delivery && python3 delivery_service_consumer.py

# Terminal 6 — Notification service
cd notification && python3 notification_service_consumer.py

# Terminal 7 — UI (Flask API)
cd user && python3 ui_service_producer.py
```

### 7. Send a test order

```bash
curl -s -X POST http://localhost:5000/initiate_orders \
  -H "Content-Type: application/json" \
  -d '{
    "items": [{"item_id": "pizza", "quantity": 2, "price": 15.99}],
    "customer_id": "CUSTOMER-456",
    "restaurant_id": "RESTAUR-123",
    "delivery_address": "123 Main St",
    "payment_method": "credit_card",
    "total_amount": 31.98
  }'
```

Expected response:

```json
{"order_id": "ORDER-123", "status": "order_initiated"}
```

Watch the **Logging terminal** — you will see all events cascade through the system in real time.

---

## Services

| Service | File | Port / Topic | Responsibility |
|---------|------|-------------|----------------|
| UI | `user/ui_service_producer.py` | `POST :5000/initiate_orders` | Accept HTTP orders, validate, publish to Kafka |
| Order | `order/order_service_consumer.py` | `order/payment/food/delivery.updates` | Central state machine — drives the full lifecycle |
| Payment | `payment/payment_service_consumer.py` | `order.updates` → `payment.updates` | Process payment, publish result |
| Food | `food/food_service_consumer.py` | `order.updates` → `food.updates` | Prepare food after order confirmed |
| Delivery | `delivery/delivery_service_consumer.py` | `order/delivery.updates` → `delivery.updates` | Pick up and deliver the order |
| Notification | `notification/notification_service_consumer.py` | `order/delivery.updates` | Notify customer on completion and pickup |
| Logging | `logging/logging_service_consumer.py` | ALL topics | Observe and print every event (debugging) |

---

## Kafka Topics

| Topic | Producers | Consumers | Events carried |
|-------|-----------|-----------|---------------|
| `order.updates` | UI, Order | Order, Payment, Food, Delivery, Notification, Logging | `order_initiated`, `order_created`, `order_confirmed`, `order_ready`, `order_completed` |
| `payment.updates` | Payment | Order, Logging | `payment_processed` |
| `food.updates` | Food | Order, Logging | `order_prepared` |
| `delivery.updates` | Delivery | Order, Notification, Logging | `delivery_picked`, `delivery_completed` |
| `dead.letter` | All services | — | Any event that fails validation or exhausts retries |

---

## Schemas

All event contracts live in `schema/` as Avro `.avsc` files.
Every event is validated **before being produced** and **before being consumed**.

| Schema file | Event name(s) | Key fields |
|-------------|--------------|-----------|
| `order_created_schema.avsc` | `order_initiated`, `order_created` | `order_id`, `order_items[]`, `total_amount` |
| `order_prepared_confirmed_schema.avsc` | `order_confirmed`, `order_ready` | `order_id`, `restaurant_id`, `status` |
| `order_complete_schema.avsc` | `order_completed` | `order_id`, `delivery_person_id`, `status` |
| `order_cancel_schema.avsc` | `order_canceled` | `order_id`, `cancellation_reason` |
| `payment_schema.avsc` | `payment_processed` | `payment_id`, `payment_status` (enum) |
| `delivery_picked_completed_schema.avsc` | `delivery_picked`, `delivery_completed` | `order_id`, `delivery_person_id`, `status` |
| `inventory_updated.avsc` | `inventory_updated` | `item_id`, `quantity_change` |

Smoke-test all schemas at any time:

```bash
python3 -c "
import os
from fastavro.schema import load_schema
for f in sorted(os.listdir('schema')):
    if f.endswith('.avsc'):
        s = load_schema(f'schema/{f}')
        print(f'OK  {f}  v{s.get(\"version\")}')
"
```

See `schemas.md` for the full schema reference and `results.md` for example outputs.

---

## Configuration

### Kafka brokers

| Broker | SSL port |
|--------|---------|
| kafka-ssl-1 | `localhost:19092` |
| kafka-ssl-2 | `localhost:29092` |
| kafka-ssl-3 | `localhost:39092` |

Configured in `base_producer_config.py` and `base_consumer_config.py`.

### SSL setup

Certificates are resolved automatically by `kafka_util.py` from `./docker/secrets/`:

| File | Purpose |
|------|---------|
| `snakeoil-ca-1.crt` | Certificate Authority — trust anchor |
| `producer.crt` / `producer.key` | Client certificate and private key |

> TLS is forced to **1.2** via `ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2`
> to avoid a known socket-selector bug in `kafka-python-ng` under TLS 1.3.

### Producer settings

| Setting | Value | Why |
|---------|-------|-----|
| `acks` | `all` | Wait for all ISR replicas — no data loss |
| `retries` | `3` | Retry transient failures |
| `compression_type` | `gzip` | Reduce network bandwidth |
| `max_in_flight_requests_per_connection` | `1` | Preserve message ordering |

### Consumer settings

| Setting | Value | Why |
|---------|-------|-----|
| `enable_auto_commit` | `False` | Manual commit — only after successful processing |
| `auto_offset_reset` | `earliest` | Never miss a message on first start |
| `partition_assignment_strategy` | `StickyPartitionAssignor` | Minimize partition reassignment on rebalance |
| `max_poll_interval_ms` | `420000` | Allow time for slow DB operations |

---

## Running the Examples

All schema validation examples from `schemas.md` run as inline Python from the Chapter09 root.

```bash
# Setup
source .venv/bin/activate
export PYTHONPATH="$(pwd)"

# Example — validate a correct order event
python3 -c "
from validators import AvroEventValidator
order = {
    'order_id': 'O1', 'customer_id': 'C1', 'restaurant_id': 'R1',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'pizza', 'quantity': 2, 'price': 15.99}],
    'delivery_address': '123 Main St', 'payment_method': 'cash',
    'total_amount': 15.99, 'timestamp': '2026-10-06T12:00:00.000'
}
print(AvroEventValidator().validate_event('schema/order_created_schema.avsc', 1.0, order))
"
```

See `run.md` for all 10 examples with full copy-paste commands.

---

## Documentation

| File | Contents |
|------|----------|
| `run.md` | Complete step-by-step guide to run every service and example |
| `schemas.md` | Avro schema reference — all 7 schemas, validation flow, 10 usage examples |
| `commands.md` | Every CLI command used in this chapter |
| `results.md` | Actual output of all 10 schema examples |

---

## Stopping Everything

```bash
# Kill all service processes
pkill -f "logging_service_consumer.py"
pkill -f "order_service_consumer.py"
pkill -f "payment_service_consumer.py"
pkill -f "food_service_consumer.py"
pkill -f "delivery_service_consumer.py"
pkill -f "notification_service_consumer.py"
pkill -f "ui_service_producer.py"

# Stop Kafka
cd docker && sudo docker compose down
```
