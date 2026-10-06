# Consumer & Producer Testing Guide

## How to Test Any Service

All terminals must start with the same setup from **Chapter09 root**:

```bash
cd /home/ubuntu/src/Ultimate-Event-Driven-Architecture-with-Python-and-Apache-Kafka/Chapter09
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
```

> Without `PYTHONPATH` set, Python cannot find `base_producer_config`, `kafka_util`,
> and `validators` — which are in the Chapter09 root, not the service subdirectory.

---

## Delivery Service

### How it works

```
order_ready event on order.updates
        │
        ▼
delivery_service_consumer.py
  handle_order_ready()
        │  calls
        ▼
delivery_service_producer.pickup_delivery()
  validates: delivery_picked_completed_schema v1.0
  publishes → delivery.updates  (event_name: delivery_picked)
        │
        ▼
delivery_service_consumer.py
  handle_pickup_delivery()
        │  calls
        ▼
delivery_service_producer.complete_delivery()
  validates: delivery_picked_completed_schema v1.0
  publishes → delivery.updates  (event_name: delivery_completed)
```

### Event input the consumer expects

| Event name    | Topic          | Required fields                                                    |
|---------------|----------------|--------------------------------------------------------------------|
| `order_ready` | `order.updates`| `order_id`, `customer_id`, `restaurant_id`, `status`, `timestamp` |

### Events the producer publishes

| Function           | Event name           | Topic              |
|--------------------|----------------------|--------------------|
| `pickup_delivery`  | `delivery_picked`    | `delivery.updates` |
| `complete_delivery`| `delivery_completed` | `delivery.updates` |

---

### Step 1 — Setup (run in every terminal)

```bash
cd /home/ubuntu/src/Ultimate-Event-Driven-Architecture-with-Python-and-Apache-Kafka/Chapter09
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
```

---

### Step 2 — Terminal A: start the delivery consumer

```bash
cd delivery
python3 delivery_service_consumer.py
```

Leave it running. It subscribes to `order.updates` and `delivery.updates`.

Expected startup output:
```
Partitions revoked: set()
Partitions assigned: {TopicPartition(topic='order.updates', partition=0),
                      TopicPartition(topic='delivery.updates', partition=0)}
```

---

### Step 3 — Terminal B: test the producer functions directly

Call `pickup_delivery()` and `complete_delivery()` without Kafka — useful to verify the schema validation and publish path in isolation.

```bash
cd delivery
python3 -c "
from delivery_service_producer import pickup_delivery, complete_delivery

event = {
    'order_id':      'ORDER-DEL-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T13:00:00.000'
}

print('--- calling pickup_delivery ---')
pickup_delivery(event)

print('--- calling complete_delivery ---')
complete_delivery(event)

print('Done.')
"
```

Expected output:
```
--- calling pickup_delivery ---
--- calling complete_delivery ---
Done.
```

Both functions validate the event against `delivery_picked_completed_schema v1.0`
and publish to `delivery.updates`.

---

### Step 4 — Terminal C: end-to-end trigger via Kafka

Publish an `order_ready` event to `order.updates` — this is the event the consumer
listens for to start the delivery chain.

```bash
python3 -c "
import uuid
from kafka import KafkaProducer
import base_producer_config

producer = KafkaProducer(**base_producer_config.producer_config)

event = {
    'order_id':      'ORDER-DEL-001',
    'customer_id':   'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status':        'READY',
    'timestamp':     '2026-10-06T13:00:00.000'
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

Expected output:
```
Published order_ready -> partition=0 offset=<n>
```

---

### Step 5 — What to watch in Terminal A

After the `order_ready` message lands, the consumer processes it and the full
delivery chain fires automatically:

```
[consumer] Received: event_name=order_ready      order_id=ORDER-DEL-001
           → handle_order_ready() called
           → pickup_delivery() called
           → publishes delivery_picked to delivery.updates

[consumer] Received: event_name=delivery_picked  order_id=ORDER-DEL-001
           → handle_pickup_delivery() called
           → complete_delivery() called
           → publishes delivery_completed to delivery.updates
```

> **Note:** PostgreSQL `connection refused` errors are expected — the circuit breaker
> tries to insert into Postgres (not running here). The Kafka produce/consume chain
> works correctly regardless.

---

## Verify Events Landed on delivery.updates

Consume from `delivery.updates` to confirm both events were published:

```bash
python3 -c "
from kafka import KafkaConsumer
import base_consumer_config

config = base_consumer_config.consumer_config.copy()
config['group_id']          = 'delivery-verify-group'
config['auto_offset_reset'] = 'earliest'
config['consumer_timeout_ms'] = 8000

consumer = KafkaConsumer(**config)
consumer.subscribe(['delivery.updates'])

for msg in consumer:
    headers    = dict(msg.headers)
    event_name = headers.get('event_name', b'?').decode()
    order_id   = msg.value.get('order_id', '?')
    status     = msg.value.get('status', '?')
    print(f'  event_name={event_name:<22} order_id={order_id}  status={status}')
    consumer.commit()

consumer.close()
print('Done.')
"
```

Expected output:
```
  event_name=delivery_picked       order_id=ORDER-DEL-001  status=PICKEDUP
  event_name=delivery_completed    order_id=ORDER-DEL-001  status=COMPLETED
Done.
```

---

## All Other Services — Same Pattern

Every service follows the same test pattern:

| Service    | Consumer file                      | Trigger event      | Trigger topic      |
|------------|------------------------------------|--------------------|--------------------|
| Order      | `order/order_service_consumer.py`  | `order_initiated`  | `order.updates`    |
| Payment    | `payment/payment_service_consumer.py` | `order_created` | `order.updates`    |
| Food       | `food/food_service_consumer.py`    | `order_confirmed`  | `order.updates`    |
| Delivery   | `delivery/delivery_service_consumer.py` | `order_ready` | `order.updates`    |
| Notification | `notification/notification_service_consumer.py` | `order_completed` | `order.updates` |
| Logging    | `logging/logging_service_consumer.py` | any event       | all topics         |

**General test steps for any service:**

1. `cd <service>/` and run `python3 <service>_consumer.py` — keep running
2. In another terminal, publish the trigger event to the correct topic
3. Watch the consumer terminal react
4. Optionally consume the output topic to confirm the downstream event was published
