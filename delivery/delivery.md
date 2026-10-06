# Delivery Service — API Reference

## Overview

The delivery service has two callable functions and one long-running consumer.

| Component | File | Role |
|-----------|------|------|
| `pickup_delivery(event)` | `delivery_service_producer.py` | Publishes `delivery_picked` to `delivery.updates` |
| `complete_delivery(event)` | `delivery_service_producer.py` | Publishes `delivery_completed` to `delivery.updates` |
| Consumer loop | `delivery_service_consumer.py` | Listens on `order.updates` + `delivery.updates`, calls both functions automatically |

---

## Schema — `delivery_picked_completed_schema.avsc` v1.0

All calls validate against this schema before publishing:

| Field | Type | Required |
|-------|------|----------|
| `order_id` | string | yes |
| `customer_id` | string | yes |
| `delivery_person_id` | string | yes |
| `status` | string | yes |
| `timestamp` | string | yes |

---

## Setup (run in every terminal before any call)

```bash
cd /home/ubuntu/src/Ultimate-Event-Driven-Architecture-with-Python-and-Apache-Kafka/Chapter09
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
cd delivery
```

---

## Function: `pickup_delivery(event)`

### What it does

1. Builds a delivery payload with `status = "PICKEDUP"`
2. Validates the payload against `delivery_picked_completed_schema v1.0`
3. Publishes event `delivery_picked` to topic `delivery.updates`
4. On validation failure → sends to `dead.letter` topic

### Input fields used from `event`

| Field | Used for |
|-------|----------|
| `order_id` | Kafka message key + payload field |
| `customer_id` | Payload field |

### Call

```python
from delivery_service_producer import pickup_delivery

event = {
    'order_id':      'ORDER-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T10:00:00.000'
}

pickup_delivery(event)
```

### What it publishes to Kafka

```python
topic   = 'delivery.updates'
key     = 'ORDER-001'
value   = {
    'order_id':           'ORDER-001',
    'customer_id':        'CUSTOMER-101',
    'delivery_person_id': 'delivery_person_id',
    'status':             'PICKEDUP',
    'timestamp':          '2026-10-06 10:00:01.123456'
}
headers = {
    'event_name': 'delivery_picked',
    'version':    '1.0',
    'event_id':   '<uuid>'
}
```

---

## Function: `complete_delivery(event)`

### What it does

1. Builds a delivery payload with `status = "COMPLETED"`
2. Validates the payload against `delivery_picked_completed_schema v1.0`
3. Publishes event `delivery_completed` to topic `delivery.updates`
4. On validation failure → sends to `dead.letter` topic

### Input fields used from `event`

| Field | Used for |
|-------|----------|
| `order_id` | Kafka message key + payload field |
| `customer_id` | Payload field |

### Call

```python
from delivery_service_producer import complete_delivery

event = {
    'order_id':      'ORDER-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T10:00:00.000'
}

complete_delivery(event)
```

### What it publishes to Kafka

```python
topic   = 'delivery.updates'
key     = 'ORDER-001'
value   = {
    'order_id':           'ORDER-001',
    'customer_id':        'CUSTOMER-101',
    'delivery_person_id': 'delivery_person_id',
    'status':             'COMPLETED',
    'timestamp':          '2026-10-06 10:00:02.654321'
}
headers = {
    'event_name': 'delivery_completed',
    'version':    '1.0',
    'event_id':   '<uuid>'
}
```

---

## Call both functions in sequence (one terminal)

```bash
python3 -c "
from delivery_service_producer import pickup_delivery, complete_delivery

event = {
    'order_id':      'ORDER-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T10:00:00.000'
}

pickup_delivery(event)
print('pickup_delivery called')

complete_delivery(event)
print('complete_delivery called')
"
```

Expected output:
```
pickup_delivery called
complete_delivery called
```

---

## Trigger via Kafka (end-to-end)

Instead of calling the functions directly, publish an `order_ready` event.
The consumer will call `pickup_delivery` and then `complete_delivery` automatically.

```bash
cd /home/ubuntu/src/Ultimate-Event-Driven-Architecture-with-Python-and-Apache-Kafka/Chapter09
python3 -c "
import uuid
from kafka import KafkaProducer
import base_producer_config

producer = KafkaProducer(**base_producer_config.producer_config)

event = {
    'order_id':      'ORDER-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T10:00:00.000'
}
headers = [
    ('event_name', b'order_ready'),
    ('version',    b'1.0'),
    ('event_id',   str(uuid.uuid4()).encode())
]
r = producer.send('order.updates', key=event['order_id'], value=event, headers=headers).get(timeout=10)
producer.flush()
producer.close()
print(f'Published order_ready -> partition={r.partition} offset={r.offset}')
"
```

---

## Consumer — `delivery_service_consumer.py`

### Topics subscribed

| Topic | Events handled |
|-------|---------------|
| `order.updates` | `order_ready` |
| `delivery.updates` | `delivery_picked` |

### Event handler mapping

```python
EVENT_HANDLERS = {
    'order_ready':      handle_order_ready,    # validates order_prepared_confirmed_schema → calls pickup_delivery()
    'delivery_picked':  handle_pickup_delivery # validates delivery_picked_completed_schema → calls complete_delivery()
}
```

### Start the consumer

```bash
cd delivery
python3 delivery_service_consumer.py
```

### Consumer event flow

```
order_ready received on order.updates
    │
    ├─ validate: order_prepared_confirmed_schema v1.0
    ├─ process_with_circuit_breaker → INSERT into eda.orders (Postgres)
    └─ pickup_delivery(event) → publishes delivery_picked to delivery.updates

delivery_picked received on delivery.updates
    │
    ├─ validate: delivery_picked_completed_schema v1.0
    ├─ process_with_circuit_breaker → INSERT into eda.delivery (Postgres)
    └─ complete_delivery(event) → publishes delivery_completed to delivery.updates
```

### Error handling

| Situation | Action |
|-----------|--------|
| Schema validation fails | Event sent to `dead.letter`, offset committed |
| JSON decode error | Event sent to `dead.letter`, offset committed |
| Postgres down | `CircuitBreakerError` after 5 failures, event sent to `dead.letter` |
| Any other exception | Event sent to `dead.letter`, offset committed |
| Kafka delivers duplicate | `is_duplicate()` check in Postgres skips reprocessing |

---

## Verify events published to `delivery.updates`

```bash
cd /home/ubuntu/src/Ultimate-Event-Driven-Architecture-with-Python-and-Apache-Kafka/Chapter09
python3 -c "
from kafka import KafkaConsumer
import base_consumer_config

config = base_consumer_config.consumer_config.copy()
config['group_id']           = 'delivery-verify-$(date +%s)'
config['auto_offset_reset']  = 'earliest'
config['consumer_timeout_ms'] = 8000

consumer = KafkaConsumer(**config)
consumer.subscribe(['delivery.updates'])

for msg in consumer:
    headers    = dict(msg.headers)
    event_name = headers.get('event_name', b'?').decode()
    order_id   = msg.value.get('order_id', '?')
    status     = msg.value.get('status', '?')
    print(f'  event={event_name:<22}  order_id={order_id}  status={status}')
    consumer.commit()

consumer.close()
print('Done.')
"
```

Expected output:
```
  event=delivery_picked          order_id=ORDER-001  status=PICKEDUP
  event=delivery_completed       order_id=ORDER-001  status=COMPLETED
Done.
```
