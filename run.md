# How to Run — Chapter09

All commands must be run from the **Chapter09** root directory.

---

## Prerequisites

### 1. Start Kafka (Docker)

```bash
cd ./docker
sudo docker compose up -d
cd ..
```

Verify the 6 containers are running:

```bash
sudo docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output:
```
NAMES                   STATUS
docker-kafka-ssl-1-1    Up ...
docker-kafka-ssl-2-1    Up ...
docker-kafka-ssl-3-1    Up ...
docker-controller-1-1   Up ...
docker-controller-2-1   Up ...
docker-controller-3-1   Up ...
```

### 2. Activate the Virtual Environment

```bash
source .venv/bin/activate
```

If the venv does not exist yet, create it first:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Set PYTHONPATH

Run this once per terminal session from the Chapter09 root:

```bash
export PYTHONPATH="$(pwd)"
```

> Every service must be started from its **own subdirectory** but needs
> `PYTHONPATH` pointing back to Chapter09 so shared modules (`kafka_util`,
> `base_producer_config`, `validators`, etc.) are importable.

---

## Project Structure

```
Chapter09/
├── base_consumer_config.py       shared consumer config (SSL, brokers)
├── base_producer_config.py       shared producer config (SSL, brokers)
├── kafka_util.py                 SSL context, serializers, circuit breaker
├── validators.py                 AvroEventValidator class
├── requirements.txt              Python dependencies
├── schema/                       Avro schema files (.avsc)
│
├── user/
│   └── ui_service_producer.py    Flask HTTP API → publishes order_initiated
├── order/
│   ├── order_service_consumer.py listens: order/payment/food/delivery.updates
│   └── order_service_producer.py publishes: order_created/confirmed/ready/completed
├── payment/
│   ├── payment_service_consumer.py listens: order.updates
│   └── payment_service_producer.py publishes: payment.updates
├── food/
│   ├── food_service_consumer.py  listens: order.updates + food.updates
│   └── food_service_producer.py  publishes: food.updates
├── delivery/
│   ├── delivery_service_consumer.py listens: order.updates + delivery.updates
│   └── delivery_service_producer.py publishes: delivery.updates
├── notification/
│   └── notification_service_consumer.py listens: order.updates + delivery.updates
└── logging/
    └── logging_service_consumer.py listens: all topics, prints every event
```

---

## Running the Services

Each service runs as a long-lived process. Open a **separate terminal** for each one. In every terminal run the setup first:

```bash
cd ./Chapter09
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
```

### Logging Consumer
Subscribes to all topics and prints every event. Good to keep running to see the full event flow.

```bash
cd ./logging
python3 logging_service_consumer.py
```

### Order Consumer + Producer
The central service. Listens on 4 topics and drives the order state machine.

```bash
cd ./order
python3 order_service_consumer.py
```

### Payment Consumer + Producer
Listens for `order_created` events and produces `payment_processed`.

```bash
cd ./payment
python3 payment_service_consumer.py
```

### Food Consumer + Producer
Listens for `order_confirmed` events and produces `order_prepared`.

```bash
cd ./food
python3 food_service_consumer.py
```

### Delivery Consumer + Producer
Listens for `order_ready` events and produces `delivery_picked` / `delivery_completed`.

```bash
cd ./delivery
python3 delivery_service_consumer.py
```

### Notification Consumer
Listens for `order_completed` and `delivery_picked` events and sends (mocked) notifications.

```bash
cd ./notification
python3 notification_service_consumer.py
```

### UI Service (Flask API)
Exposes a REST endpoint that accepts an order and publishes `order_initiated`.

```bash
cd ./user
python3 ui_service_producer.py
```

Flask starts on `http://localhost:5000`.

---

## Sending a Test Order

With the UI service running, send an order from any terminal:

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

---

## Running the Schema Examples

All examples from `schemas.md` run as inline Python from the Chapter09 root.

### Setup (once per terminal)

```bash
cd ./Chapter09
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
```

### Example 1 — Validate a correct order event

```bash
python3 -c "
from validators import AvroEventValidator

order = {
    'order_id': 'ORDER-001',
    'customer_id': 'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'pizza', 'quantity': 2, 'price': 15.99}],
    'delivery_address': '123 Main St',
    'payment_method': 'credit_card',
    'total_amount': 15.99,
    'timestamp': '2026-10-06T12:00:00.000'
}
print(AvroEventValidator().validate_event('schema/order_created_schema.avsc', 1.0, order))
"
```

### Example 2 — Catch a missing required field

```bash
python3 -c "
from validators import AvroEventValidator
from fastavro._validation import ValidationError

order = {
    'order_id': 'ORDER-002', 'customer_id': 'C1', 'restaurant_id': 'R1',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'burger', 'quantity': 1, 'price': 9.99}],
    'payment_method': 'cash', 'total_amount': 9.99,
    'timestamp': '2026-10-06T12:00:00.000'
    # delivery_address intentionally missing
}
try:
    AvroEventValidator().validate_event('schema/order_created_schema.avsc', 1.0, order)
except ValidationError as e:
    print('CAUGHT:', e)
"
```

### Example 3 — Catch a version mismatch

```bash
python3 -c "
from validators import AvroEventValidator
from fastavro._validation import ValidationError

order = {
    'order_id': 'ORDER-003', 'customer_id': 'C1', 'restaurant_id': 'R1',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'pasta', 'quantity': 1, 'price': 12.50}],
    'delivery_address': '456 Oak Ave', 'payment_method': 'debit_card',
    'total_amount': 12.50, 'timestamp': '2026-10-06T12:00:00.000'
}
try:
    AvroEventValidator().validate_event('schema/order_created_schema.avsc', 2.0, order)
except ValidationError as e:
    print('CAUGHT:', e)
"
```

### Example 4 — Validate a payment event (enum)

```bash
python3 -c "
from validators import AvroEventValidator
from fastavro._validation import ValidationError

v = AvroEventValidator()
payment = {
    'payment_id': 'PAY-9001', 'order_id': 'ORDER-001',
    'customer_id': 'C1', 'restaurant_id': 'R1',
    'amount_paid': 39.47, 'payment_status': 'PROCESSED',
    'payment_timestamp': '2026-10-06T12:05:00.000'
}
print('valid  :', v.validate_event('schema/payment_schema.avsc', 1.0, payment))
try:
    v.validate_event('schema/payment_schema.avsc', 1.0, {**payment, 'payment_status': 'REFUNDED'})
except ValidationError:
    print('invalid: CAUGHT — REFUNDED not in [INITIATED, PROCESSED, FAILED]')
"
```

### Example 5 — Validate a delivery event

```bash
python3 -c "
from validators import AvroEventValidator

delivery = {
    'order_id': 'ORDER-001', 'customer_id': 'C1',
    'delivery_person_id': 'DRIVER-55', 'status': 'DELIVERED',
    'timestamp': '2026-10-06T13:30:00.000'
}
print(AvroEventValidator().validate_event('schema/delivery_picked_completed_schema.avsc', 1.0, delivery))
"
```

### Example 6 — Validate and produce to Kafka

```bash
python3 -c "
import uuid
from datetime import datetime
from kafka import KafkaProducer
import base_producer_config, kafka_util
from validators import AvroEventValidator

producer = KafkaProducer(**base_producer_config.producer_config)
order = {
    'order_id': 'ORDER-LIVE-001', 'customer_id': 'C1', 'restaurant_id': 'R1',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'pizza', 'quantity': 2, 'price': 15.99}],
    'delivery_address': '123 Main St', 'payment_method': 'credit_card',
    'total_amount': 31.98, 'timestamp': str(datetime.now())
}
if AvroEventValidator().validate_event('schema/order_created_schema.avsc', 1.0, order):
    headers = [('event_name', b'order_initiated'), ('version', b'1.0'),
               ('event_id', str(uuid.uuid4()).encode())]
    r = producer.send('order.updates', key=order['order_id'], value=order, headers=headers).get(timeout=10)
    producer.flush()
    print(f'Sent: partition={r.partition} offset={r.offset}')
producer.close()
"
```

### Example 7 — Consume and validate from Kafka

```bash
python3 -c "
from kafka import KafkaConsumer
import base_consumer_config
from validators import AvroEventValidator

config = base_consumer_config.consumer_config.copy()
config.update({'group_id': 'example-consumer', 'auto_offset_reset': 'earliest', 'consumer_timeout_ms': 8000})
consumer = KafkaConsumer(**config)
consumer.subscribe(['order.updates'])

schema_map = {
    'order_initiated': 'schema/order_created_schema.avsc',
    'order_created':   'schema/order_created_schema.avsc',
    'order_confirmed': 'schema/order_prepared_confirmed_schema.avsc',
    'order_ready':     'schema/order_prepared_confirmed_schema.avsc',
    'order_completed': 'schema/order_complete_schema.avsc',
}
v = AvroEventValidator()
for msg in consumer:
    event_name = dict(msg.headers).get('event_name', b'unknown').decode()
    if event_name in schema_map:
        v.validate_event(schema_map[event_name], 1.0, msg.value)
        print(f'VALID  {event_name:<22} order_id={msg.value[\"order_id\"]}')
        consumer.commit()
consumer.close()
"
```

### Example 8 — Inspect a schema programmatically

```bash
python3 -c "
from fastavro.schema import load_schema

schema = load_schema('schema/order_created_schema.avsc')
full_name = schema['name']
print(f'Name      : {full_name.split(\".\")[-1]}')
print(f'Namespace : {\".\" .join(full_name.split(\".\")[:-1])}')
print(f'Version   : {schema.get(\"version\")}')
print('Fields    :')
for f in schema['fields']:
    print(f'  - {f[\"name\"]:<20} type={f[\"type\"]}')
"
```

### Example 9 — Add and validate a new schema

```bash
python3 -c "
import json
from validators import AvroEventValidator
from fastavro._validation import ValidationError

# Write new schema
schema = {
    'type': 'record', 'name': 'rating_submitted', 'version': 1.0,
    'namespace': 'com.fandutech.events',
    'fields': [
        {'name': 'order_id',    'type': 'string'},
        {'name': 'customer_id', 'type': 'string'},
        {'name': 'rating',      'type': 'int'},
        {'name': 'comment',     'type': ['null', 'string'], 'default': None},
        {'name': 'timestamp',   'type': 'string', 'logicalType': 'timestamp-millis'}
    ]
}
with open('schema/rating_submitted.avsc', 'w') as f:
    json.dump(schema, f, indent=2)

v = AvroEventValidator()
base = {'order_id': 'ORDER-001', 'customer_id': 'C1', 'rating': 5,
        'comment': 'Great!', 'timestamp': '2026-10-06T14:00:00.000'}

print('with comment   :', v.validate_event('schema/rating_submitted.avsc', 1.0, base))
print('null comment   :', v.validate_event('schema/rating_submitted.avsc', 1.0, {**base, 'comment': None}))
try:
    v.validate_event('schema/rating_submitted.avsc', 1.0, {**base, 'rating': 'five'})
except ValidationError:
    print('bad type       : CAUGHT — rating must be int')
"
```

### Example 10 — Smoke test all schemas

```bash
python3 -c "
import os
from fastavro.schema import load_schema

for f in sorted(os.listdir('schema')):
    if not f.endswith('.avsc'):
        continue
    try:
        s = load_schema(f'schema/{f}')
        fields = [x['name'] for x in s.get('fields', [])]
        print(f'OK  {f:<45} v{s.get(\"version\")}  {fields}')
    except Exception as e:
        print(f'ERR {f}: {e}')
"
```

---

## Running All Examples at Once

```bash
python3 -c "
from validators import AvroEventValidator
from fastavro._validation import ValidationError
import os
from fastavro.schema import load_schema

v = AvroEventValidator()

order = {
    'order_id': 'O1', 'customer_id': 'C1', 'restaurant_id': 'R1', 'status': 'INITIATED',
    'order_items': [{'item_id': 'pizza', 'quantity': 1, 'price': 10.0}],
    'delivery_address': 'Addr', 'payment_method': 'cash',
    'total_amount': 10.0, 'timestamp': '2026-10-06T12:00:00.000'
}

# 1 valid
assert v.validate_event('schema/order_created_schema.avsc', 1.0, order) == True
print('Ex1  PASS — valid order')

# 2 missing field
try:
    bad = {k: val for k, val in order.items() if k != 'delivery_address'}
    v.validate_event('schema/order_created_schema.avsc', 1.0, bad)
except ValidationError:
    print('Ex2  PASS — missing field caught')

# 3 version mismatch
try:
    v.validate_event('schema/order_created_schema.avsc', 2.0, order)
except ValidationError:
    print('Ex3  PASS — version mismatch caught')

# 4 enum
payment = {'payment_id': 'P1', 'order_id': 'O1', 'customer_id': 'C1', 'restaurant_id': 'R1',
           'amount_paid': 10.0, 'payment_status': 'PROCESSED', 'payment_timestamp': '2026-10-06T12:05:00.000'}
assert v.validate_event('schema/payment_schema.avsc', 1.0, payment) == True
print('Ex4  PASS — valid payment enum')
try:
    v.validate_event('schema/payment_schema.avsc', 1.0, {**payment, 'payment_status': 'REFUNDED'})
except ValidationError:
    print('Ex4  PASS — invalid enum caught')

# 5 delivery
delivery = {'order_id': 'O1', 'customer_id': 'C1', 'delivery_person_id': 'D1',
            'status': 'DELIVERED', 'timestamp': '2026-10-06T13:00:00.000'}
assert v.validate_event('schema/delivery_picked_completed_schema.avsc', 1.0, delivery) == True
print('Ex5  PASS — valid delivery event')

# 8 inspect
s = load_schema('schema/order_created_schema.avsc')
assert 'order_id' in [f['name'] for f in s['fields']]
print('Ex8  PASS — schema introspection')

# 10 smoke test
ok = err = 0
for f in os.listdir('schema'):
    if f.endswith('.avsc'):
        try:   load_schema(f'schema/{f}'); ok += 1
        except: err += 1
print(f'Ex10 PASS — {ok} schemas OK, {err} ERR')

print()
print('All examples passed.')
"
```

---

## Stopping Services

Kill all running service processes:

```bash
pkill -f "logging_service_consumer.py"
pkill -f "order_service_consumer.py"
pkill -f "payment_service_consumer.py"
pkill -f "food_service_consumer.py"
pkill -f "delivery_service_consumer.py"
pkill -f "notification_service_consumer.py"
pkill -f "ui_service_producer.py"
```

Stop Kafka containers:

```bash
cd ./docker
sudo docker compose down
```

---

## Quick Reference

| Script | Directory | Command | Listens to | Publishes to |
|--------|-----------|---------|-----------|-------------|
| `ui_service_producer.py` | `user/` | `python3 ui_service_producer.py` | HTTP POST | `order.updates` |
| `order_service_consumer.py` | `order/` | `python3 order_service_consumer.py` | `order/payment/food/delivery.updates` | `order.updates` |
| `payment_service_consumer.py` | `payment/` | `python3 payment_service_consumer.py` | `order.updates` | `payment.updates` |
| `food_service_consumer.py` | `food/` | `python3 food_service_consumer.py` | `order/food.updates` | `food.updates` |
| `delivery_service_consumer.py` | `delivery/` | `python3 delivery_service_consumer.py` | `order/delivery.updates` | `delivery.updates` |
| `notification_service_consumer.py` | `notification/` | `python3 notification_service_consumer.py` | `order/delivery.updates` | — |
| `logging_service_consumer.py` | `logging/` | `python3 logging_service_consumer.py` | all topics | — |
