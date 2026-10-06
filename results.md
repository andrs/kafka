# Schema Examples — Execution Results

All examples were run from `Chapter09/` with:

```bash
source .venv/bin/activate
export PYTHONPATH=$(pwd)
```

Date: 2026-10-06

---

## Example 1 — Validate a Correct Order Event

**Command:**
```bash
python3 -c "
from validators import AvroEventValidator

order = {
    'order_id': 'ORDER-001',
    'customer_id': 'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status': 'INITIATED',
    'order_items': [
        {'item_id': 'pizza',  'quantity': 2, 'price': 15.99},
        {'item_id': 'salad',  'quantity': 1, 'price': 7.49}
    ],
    'delivery_address': '123 Main Street',
    'payment_method': 'credit_card',
    'total_amount': 39.47,
    'timestamp': '2026-10-06T12:00:00.000'
}

validator = AvroEventValidator()
result = validator.validate_event('schema/order_created_schema.avsc', 1.0, order)
print(f'Valid: {result}')
"
```

**Result:**
```
Valid: True
```

**Conclusion:** A fully-populated order dict that satisfies all required fields and correct types passes validation without errors.

---

## Example 2 — Catch a Validation Error (Missing Required Field)

**Command:**
```bash
python3 -c "
from fastavro._validation import ValidationError
from validators import AvroEventValidator

bad_order = {
    'order_id': 'ORDER-002',
    'customer_id': 'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202',
    'status': 'INITIATED',
    'order_items': [{'item_id': 'burger', 'quantity': 1, 'price': 9.99}],
    # delivery_address intentionally omitted
    'payment_method': 'cash',
    'total_amount': 9.99,
    'timestamp': '2026-10-06T12:00:00.000'
}

validator = AvroEventValidator()
try:
    validator.validate_event('schema/order_created_schema.avsc', 1.0, bad_order)
except ValidationError as e:
    print(f'Caught expected error: {e}')
"
```

**Result:**
```
ERROR:validators:Validation error in AvroEventValidator: [
  "Field(com.fandutech.events.order_created.delivery_address) is None expected string"
]
Caught expected error: [
  "Validation error in AvroEventValidator: [\n  \"Field(com.fandutech.events.order_created.delivery_address) is None expected string\"\n]"
]
```

**Conclusion:** fastavro identifies exactly which field is missing (`delivery_address`) and its expected type (`string`). The `ValidationError` is raised and caught — the event would be routed to the dead-letter queue in a real service.

---

## Example 3 — Catch a Version Mismatch

**Command:**
```bash
python3 -c "
from fastavro._validation import ValidationError
from validators import AvroEventValidator

order = { ... }  # valid order dict

validator = AvroEventValidator()
try:
    validator.validate_event('schema/order_created_schema.avsc', 2.0, order)  # wrong version
except ValidationError as e:
    print(f'Version error: {e}')
"
```

**Result:**
```
ERROR:validators:Validation error in AvroEventValidator: [
  "Schema Version mismatch!"
]
Version error: [
  "Validation error in AvroEventValidator: [\n  \"Schema Version mismatch!\"\n]"
]
```

**Conclusion:** When version `2.0` is passed but the schema file declares `1.0`, the validator raises a `ValidationError` before even checking the fields. This prevents stale producers from sending events that new consumers cannot process.

---

## Example 4 — Validate a Payment Event (with Enum)

**Command:**
```bash
python3 -c "
from validators import AvroEventValidator
from fastavro._validation import ValidationError

# Valid payment
payment = {
    'payment_id': 'PAY-9001', 'order_id': 'ORDER-001',
    'customer_id': 'CUSTOMER-101', 'restaurant_id': 'RESTAUR-202',
    'amount_paid': 39.47, 'payment_status': 'PROCESSED',
    'payment_timestamp': '2026-10-06T12:05:00.000'
}
validator = AvroEventValidator()
print(validator.validate_event('schema/payment_schema.avsc', 1.0, payment))

# Invalid enum value
payment_bad = {**payment, 'payment_status': 'REFUNDED'}
try:
    validator.validate_event('schema/payment_schema.avsc', 1.0, payment_bad)
except ValidationError as e:
    print(f'Enum error: {e}')
"
```

**Result:**
```
True
ERROR:validators:Validation error in AvroEventValidator: [
  "com.fandutech.events.payment.payment_status is <REFUNDED> of type <class 'str'> expected {'type': 'enum', 'name': 'com.fandutech.events.PaymentStatus', 'symbols': ['INITIATED', 'PROCESSED', 'FAILED']}"
]
Enum error: [
  "Validation error in AvroEventValidator: [\n  \"com.fandutech.events.payment.payment_status is <REFUNDED> of type <class 'str'> expected {'type': 'enum', 'name': 'com.fandutech.events.PaymentStatus', 'symbols': ['INITIATED', 'PROCESSED', 'FAILED']}\"\n]"
]
```

**Conclusion:** `"PROCESSED"` is a valid enum symbol and passes. `"REFUNDED"` is not in `["INITIATED", "PROCESSED", "FAILED"]` so fastavro rejects it and shows exactly which symbols are allowed.

---

## Example 5 — Validate a Delivery Event

**Command:**
```bash
python3 -c "
from validators import AvroEventValidator

delivery = {
    'order_id': 'ORDER-001', 'customer_id': 'CUSTOMER-101',
    'delivery_person_id': 'DRIVER-55', 'status': 'DELIVERED',
    'timestamp': '2026-10-06T13:30:00.000'
}

validator = AvroEventValidator()
result = validator.validate_event('schema/delivery_picked_completed_schema.avsc', 1.0, delivery)
print(f'Valid: {result}')
"
```

**Result:**
```
Valid: True
```

**Conclusion:** A flat 5-field delivery event validates cleanly. All fields are present with correct types.

---

## Example 6 — Validate and Produce an Event to Kafka

**Command:**
```bash
python3 -c "
import uuid
from datetime import datetime
from kafka import KafkaProducer
import base_producer_config, kafka_util
from validators import AvroEventValidator
from fastavro._validation import ValidationError

producer = KafkaProducer(**base_producer_config.producer_config)
validator = AvroEventValidator()

order = {
    'order_id': 'ORDER-LIVE-001', 'customer_id': 'CUSTOMER-101',
    'restaurant_id': 'RESTAUR-202', 'status': 'INITIATED',
    'order_items': [{'item_id': 'pizza', 'quantity': 2, 'price': 15.99}],
    'delivery_address': '123 Main Street', 'payment_method': 'credit_card',
    'total_amount': 31.98, 'timestamp': str(datetime.now())
}

validated = validator.validate_event('schema/order_created_schema.avsc', 1.0, order)
if validated:
    event_id = str(uuid.uuid4()).encode('utf-8')
    headers = [('event_name', b'order_initiated'), ('version', b'1.0'), ('event_id', event_id)]
    result = producer.send('order.updates', key=order['order_id'], value=order, headers=headers).get(timeout=10)
    producer.flush()
    print(f'Sent to partition={result.partition} offset={result.offset}')
producer.close()
"
```

**Result:**
```
Sent to partition=0 offset=4
```

**Conclusion:** Schema validation passed and the event was physically delivered to Kafka broker at `partition=0, offset=4`. The `get(timeout=10)` call confirms the broker acknowledged receipt.

---

## Example 7 — Consume and Validate Incoming Events from Kafka

**Command:**
```bash
python3 -c "
from kafka import KafkaConsumer
from fastavro._validation import ValidationError
import base_consumer_config
from validators import AvroEventValidator

config = base_consumer_config.consumer_config.copy()
config['group_id'] = 'example-consumer-group'
config['consumer_timeout_ms'] = 8000
config['auto_offset_reset'] = 'earliest'

consumer = KafkaConsumer(**config)
consumer.subscribe(['order.updates'])
validator = AvroEventValidator()

schema_map = {
    'order_initiated': 'schema/order_created_schema.avsc',
    'order_created':   'schema/order_created_schema.avsc',
    'order_confirmed': 'schema/order_prepared_confirmed_schema.avsc',
    'order_ready':     'schema/order_prepared_confirmed_schema.avsc',
    'order_completed': 'schema/order_complete_schema.avsc',
}

count = 0
for message in consumer:
    headers = dict(message.headers)
    event_name = headers.get('event_name', b'unknown').decode('utf-8')
    event = message.value
    schema_path = schema_map.get(event_name)
    if schema_path:
        validator.validate_event(schema_path, 1.0, event)
        print(f'Valid event received: {event_name} -> {event[\"order_id\"]}')
        consumer.commit()
    count += 1
    if count >= 5:
        break

consumer.close()
print(f'Total messages consumed: {count}')
"
```

**Result:**
```
Valid event received: order_initiated -> ORDER-TEST-001
Valid event received: order_initiated -> ORDER-123
Valid event received: order_created   -> ORDER-123
Valid event received: order_initiated -> ORDER-VERIFY-002
Valid event received: order_initiated -> ORDER-LIVE-001
Total messages consumed: 5
```

**Conclusion:** 5 events were consumed from the `order.updates` topic (messages accumulated across previous test runs). All passed schema validation. The `order_created` event for `ORDER-123` was produced by the order service consumer chain during an earlier run, confirming the full event pipeline is working.

---

## Example 8 — Load and Inspect a Schema Programmatically

**Command:**
```bash
python3 -c "
from fastavro.schema import load_schema

schema = load_schema('schema/order_created_schema.avsc')

full_name = schema['name']
short_name = full_name.split('.')[-1]
namespace  = '.'.join(full_name.split('.')[:-1])

print(f'Name      : {short_name}')
print(f'Namespace : {namespace}')
print(f'Version   : {schema.get(\"version\")}')
print(f'Fields    :')
for field in schema['fields']:
    print(f'  - {field[\"name\"]:<20s} type={field[\"type\"]}')
"
```

**Result:**
```
Name      : order_created
Namespace : com.fandutech.events
Version   : 1.0
Fields    :
  - order_id             type=string
  - customer_id          type=string
  - restaurant_id        type=string
  - status               type=string
  - order_items          type={'type': 'array', 'items': {'type': 'record', 'name': 'com.fandutech.events.order_item', 'fields': [{'name': 'item_id', 'type': 'string'}, {'name': 'quantity', 'type': 'int'}, {'name': 'price', 'type': 'double'}]}}
  - delivery_address     type=string
  - payment_method       type=string
  - total_amount         type=double
  - timestamp            type=string
```

**Note:** fastavro merges the `namespace` into `name` at parse time, producing `com.fandutech.events.order_created` instead of a separate `namespace` key. The `name` must be split on `.` to recover the short name and namespace.

**Conclusion:** The `order_items` field expands to its full inline record definition, showing the nested `order_item` record with fields `item_id (string)`, `quantity (int)`, and `price (double)`. All other fields are primitive types.

---

## Example 9 — Add a New Schema for a New Event Type

**Command:** Created `schema/rating_submitted.avsc` with an optional `comment` field (`["null", "string"]` union) and validated three scenarios.

**Result:**
```
Schema file written: schema/rating_submitted.avsc
With comment    -> Valid: True
Without comment -> Valid: True
Bad type error  -> [
  "Validation error in AvroEventValidator: [\n  \"com.fandutech.events.rating_submitted.rating is <five> of type <class 'str'> expected int\"\n]"
]
```

**Conclusion:**
- A rating event with `comment: "Great pizza!"` passes.
- A rating event with `comment: None` also passes — the `["null", "string"]` union makes it optional.
- Passing `rating: "five"` (a string instead of `int`) is caught with a clear message identifying the field name, the bad value, and the expected type.

---

## Example 10 — Smoke Test All Schema Files

**Command:**
```bash
python3 -c "
import os
from fastavro.schema import load_schema

SCHEMA_DIR = 'schema'
for filename in sorted(os.listdir(SCHEMA_DIR)):
    if not filename.endswith('.avsc'):
        continue
    path = os.path.join(SCHEMA_DIR, filename)
    try:
        schema = load_schema(path)
        version = schema.get('version', 'n/a')
        fields  = [f['name'] for f in schema.get('fields', [])]
        print(f'OK  {filename:<45} v{version}  fields={fields}')
    except Exception as e:
        print(f'ERR {filename}: {e}')
"
```

**Result:**
```
OK  delivery_picked_completed_schema.avsc         v1.0  fields=['order_id', 'customer_id', 'delivery_person_id', 'status', 'timestamp']
OK  inventory_updated.avsc                        v1.0  fields=['item_id', 'quantity_change', 'updated_timestamp']
OK  order_cancel_schema.avsc                      v1.0  fields=['order_id', 'restaurant_id', 'cancellation_reason', 'canceled_timestamp']
OK  order_complete_schema.avsc                    v1.0  fields=['order_id', 'delivery_person_id', 'status', 'delivered_timestamp']
OK  order_created_schema.avsc                     v1.0  fields=['order_id', 'customer_id', 'restaurant_id', 'status', 'order_items', 'delivery_address', 'payment_method', 'total_amount', 'timestamp']
OK  order_prepared_confirmed_schema.avsc          v1.0  fields=['order_id', 'restaurant_id', 'customer_id', 'status', 'timestamp']
OK  payment_schema.avsc                           v1.0  fields=['payment_id', 'order_id', 'customer_id', 'restaurant_id', 'amount_paid', 'payment_status', 'payment_timestamp']
OK  rating_submitted.avsc                         v1.0  fields=['order_id', 'customer_id', 'rating', 'comment', 'timestamp']
```

**Conclusion:** All 8 schema files (7 original + 1 new `rating_submitted.avsc` created in Example 9) load without errors. Every schema is at version `1.0`. No `ERR` lines — the schema directory is healthy.

---

## Summary

| # | Example                              | Status | Key Finding                                                          |
|---|--------------------------------------|--------|----------------------------------------------------------------------|
| 1 | Valid order event                    | PASS   | All fields present and typed correctly → `True`                      |
| 2 | Missing required field               | PASS   | fastavro names the missing field exactly: `delivery_address`         |
| 3 | Version mismatch                     | PASS   | Version guard fires before field checks                              |
| 4 | Payment enum — valid + invalid       | PASS   | Valid symbol passes; `REFUNDED` rejected with full enum symbol list  |
| 5 | Delivery event                       | PASS   | Flat record validates cleanly                                        |
| 6 | Validate and produce to Kafka        | PASS   | Event landed at `partition=0 offset=4` on live broker               |
| 7 | Consume and validate from Kafka      | PASS   | 5 real events consumed and validated from `order.updates` topic      |
| 8 | Inspect schema programmatically      | PASS   | Nested `order_items` array expands to its full record definition     |
| 9 | Add new schema + optional field      | PASS   | Union `["null","string"]` allows optional comment; wrong type caught |
| 10| Smoke test all schemas               | PASS   | All 8 schemas load — 0 errors                                        |
