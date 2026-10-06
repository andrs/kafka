# Schemas — How They Work in This Project

## What Is a Schema?

A **schema** is a contract that defines the exact structure an event must have before it can be sent or consumed. In this project every Kafka message carries a payload that is validated against an Avro schema before it is produced or processed.

If the payload does not match the schema the event is rejected and sent to the **dead-letter queue** instead of being processed.

---

## Format: Apache Avro (`.avsc` files)

All schemas live in the `schema/` folder and use the **Apache Avro** format (`.avsc` extension, valid JSON). Avro was chosen because:

- It is strongly typed (every field declares its type)
- It is compact and efficient to serialize/deserialize
- It supports nested structures, arrays and enums
- It has a built-in versioning concept

---

## Schema File Anatomy

Every `.avsc` file in this project shares the same top-level structure:

```json
{
  "type": "record",        // always "record" — a named group of fields
  "name": "...",           // unique name for this event type
  "version": 1.0,          // custom field checked by AvroEventValidator
  "namespace": "com.fandutech.events",  // logical grouping / package
  "fields": [ ... ]        // the actual list of fields
}
```

### Avro Primitive Types Used Here

| Type     | Description                        | Example field             |
|----------|------------------------------------|---------------------------|
| `string` | UTF-8 text                         | `order_id`, `customer_id` |
| `int`    | 32-bit integer                     | `quantity`                |
| `double` | 64-bit floating point              | `price`, `total_amount`   |
| `enum`   | Fixed set of allowed string values | `PaymentStatus`           |
| `array`  | Ordered list of typed items        | `order_items`             |

### Logical Types

Some `string` fields also carry a `logicalType` annotation:

```json
{ "name": "timestamp", "type": "string", "logicalType": "timestamp-millis" }
```

The `logicalType` is a hint to consumers about how to interpret the value. It does **not** change runtime validation — the field is still stored and validated as a plain string — but it documents the expected format (a millisecond-precision ISO timestamp string).

---

## All Schemas in This Project

### 1. `order_created_schema.avsc`
Triggered when a customer places a new order through the UI service.

```
Topic        : order.updates
Event header : event_name = "order_initiated" / "order_created"
Version      : 1.0
```

| Field              | Type                       | Description                            |
|--------------------|----------------------------|----------------------------------------|
| `order_id`         | string                     | Unique identifier for the order        |
| `customer_id`      | string                     | Customer who placed the order          |
| `restaurant_id`    | string                     | Restaurant that will prepare the food  |
| `status`           | string                     | Current state (e.g. INITIATED, CREATED)|
| `order_items`      | array of `order_item`      | List of items in the order             |
| `delivery_address` | string                     | Where to deliver the order             |
| `payment_method`   | string                     | How the customer will pay              |
| `total_amount`     | double                     | Total price of the order               |
| `timestamp`        | string (timestamp-millis)  | When the order was created             |

The `order_items` field is a **nested record array**:

```json
"order_items": [
  { "item_id": "pizza", "quantity": 2, "price": 15.99 }
]
```

Each item follows the embedded `order_item` record:

| Field      | Type   |
|------------|--------|
| `item_id`  | string |
| `quantity` | int    |
| `price`    | double |

---

### 2. `payment_schema.avsc`
Produced by the payment service after a payment is processed.

```
Topic        : payment.updates
Event header : event_name = "payment_processed"
Version      : 1.0
```

| Field               | Type                      | Description                            |
|---------------------|---------------------------|----------------------------------------|
| `payment_id`        | string                    | Unique identifier for this payment     |
| `order_id`          | string                    | The order this payment belongs to      |
| `customer_id`       | string                    | Customer who paid                      |
| `restaurant_id`     | string                    | Restaurant receiving the payment       |
| `amount_paid`       | double                    | Actual amount charged                  |
| `payment_status`    | enum `PaymentStatus`      | One of: INITIATED, PROCESSED, FAILED   |
| `payment_timestamp` | string (timestamp-millis) | When the payment was processed         |

The `payment_status` field uses an **Avro enum**. Only the three declared symbols are valid — any other value will fail validation:

```json
"payment_status": {
  "type": "enum",
  "name": "PaymentStatus",
  "symbols": ["INITIATED", "PROCESSED", "FAILED"]
}
```

---

### 3. `order_prepared_confirmed_schema.avsc`
Used in two stages: when a restaurant confirms an order, and when it marks the order as ready for pickup.

```
Topic        : order.updates
Event header : event_name = "order_confirmed" / "order_ready"
Version      : 1.0
```

| Field           | Type                      | Description                         |
|-----------------|---------------------------|-------------------------------------|
| `order_id`      | string                    | The order being confirmed/readied   |
| `restaurant_id` | string                    | Restaurant processing the order     |
| `customer_id`   | string                    | Customer who placed the order       |
| `status`        | string                    | CONFIRMED or READY                  |
| `timestamp`     | string (timestamp-millis) | When the status change happened     |

---

### 4. `delivery_picked_completed_schema.avsc`
Produced by the delivery service when a driver picks up or delivers an order.

```
Topic        : delivery.updates
Event header : event_name = "delivery_completed"
Version      : 1.0
```

| Field                | Type                      | Description                         |
|----------------------|---------------------------|-------------------------------------|
| `order_id`           | string                    | The order being delivered           |
| `customer_id`        | string                    | Customer receiving the delivery     |
| `delivery_person_id` | string                    | Driver assigned to the delivery     |
| `status`             | string                    | PICKED_UP or DELIVERED              |
| `timestamp`          | string (timestamp-millis) | When the delivery event occurred    |

---

### 5. `order_complete_schema.avsc`
Produced by the order service as the final confirmation of a completed lifecycle.

```
Topic        : order.updates
Event header : event_name = "order_completed"
Version      : 1.0
```

| Field                 | Type                      | Description                      |
|-----------------------|---------------------------|----------------------------------|
| `order_id`            | string                    | The completed order              |
| `delivery_person_id`  | string                    | Driver who delivered it          |
| `status`              | string                    | Always "COMPLETED"               |
| `delivered_timestamp` | string (timestamp-millis) | When delivery was confirmed      |

---

### 6. `order_cancel_schema.avsc`
Produced when an order is cancelled by the restaurant or the system.

```
Topic        : order.updates
Event header : event_name = "order_canceled"
Version      : 1.0
```

| Field                  | Type                      | Description                        |
|------------------------|---------------------------|------------------------------------|
| `order_id`             | string                    | The cancelled order                |
| `restaurant_id`        | string                    | Restaurant that cancelled it       |
| `cancellation_reason`  | string                    | Why the order was cancelled        |
| `canceled_timestamp`   | string (timestamp-millis) | When the cancellation happened     |

---

### 7. `inventory_updated.avsc`
Produced when the restaurant's ingredient stock changes.

```
Topic        : (not directly wired in current services — reserved for future use)
Event header : event_name = "inventory_updated"
Version      : 1.0
```

| Field               | Type                      | Description                              |
|---------------------|---------------------------|------------------------------------------|
| `item_id`           | string                    | The ingredient or item being updated     |
| `quantity_change`   | int                       | Positive = stock added, Negative = used  |
| `updated_timestamp` | string (timestamp-millis) | When the inventory change happened       |

---

## How Validation Works — `AvroEventValidator`

The class `AvroEventValidator` in `validators.py` is called by every producer and consumer before an event is handled.

### Validation Flow

```
Producer/Consumer
       |
       v
AvroEventValidator.validate_event(schema_path, version, event_data)
       |
       |-- 1. Load .avsc file from disk  (fastavro.schema.load_schema)
       |-- 2. Check version field matches (schema["version"] == version)
       |-- 3. Validate all fields        (fastavro.validate)
       |
       |-- OK  --> return True, event proceeds
       |-- FAIL --> raise ValidationError, event goes to dead-letter queue
```

### Code

```python
# validators.py
class AvroEventValidator:
    @staticmethod
    def validate_event(event_schema: str, version: float, event_data: dict) -> bool:
        event_schema = load_schema(event_schema)       # parse the .avsc file
        schema_version = event_schema.get('version')   # read the custom version field
        if schema_version != version:
            raise ValidationError("Schema Version mismatch!")
        validate(event_data, event_schema, raise_errors=True)  # fastavro check
        return True
```

### Where It Is Called

| Service / File                     | Schema Used                              | Version |
|------------------------------------|------------------------------------------|---------|
| `user/ui_service_producer.py`      | `order_created_schema.avsc`              | 1.0     |
| `order/order_service_producer.py`  | `order_created_schema.avsc`              | 1.0     |
| `order/order_service_producer.py`  | `order_prepared_confirmed_schema.avsc`   | 1.0     |
| `order/order_service_producer.py`  | `order_complete_schema.avsc`             | 1.0     |
| `order/order_service_consumer.py`  | `order_created_schema.avsc`              | 1.0     |
| `order/order_service_consumer.py`  | `payment_schema.avsc`                    | 1.0     |
| `order/order_service_consumer.py`  | `order_prepared_confirmed_schema.avsc`   | 1.0     |
| `order/order_service_consumer.py`  | `delivery_picked_completed_schema.avsc`  | 1.0     |

---

## Schema Path Resolution

Schema paths in code are written as **relative paths** from the service subdirectory:

```python
validator.validate_event("../schema/order_created_schema.avsc", 1.0, order)
```

This means every service script **must be run from its own subdirectory** (e.g. `order/`, `user/`) so that `../schema/` correctly resolves to the `schema/` folder at the Chapter09 root.

```
Chapter09/
├── schema/
│   ├── order_created_schema.avsc
│   └── ...
├── order/
│   └── order_service_producer.py   <-- runs from here, uses ../schema/
└── user/
    └── ui_service_producer.py      <-- runs from here, uses ../schema/
```

---

## What Happens on Validation Failure

When `validate_event()` raises a `ValidationError` the calling code catches it and routes the message to the **dead-letter queue** topic (`dead.letter`):

```python
except ValidationError as e:
    kafka_util.send_to_dead_letter_queue(key, event, str(e), producer)
```

The dead-letter payload wraps the original event with error metadata:

```json
{
  "event": { <original event dict> },
  "error": "Validation error: field X is required",
  "timestamp": "2026-10-06T12:45:00.000"
}
```

This guarantees that **no invalid event is ever persisted or acted on**, and that failed events are never silently dropped — they are always recoverable from `dead.letter`.

---

## Full Event Lifecycle with Schema Checkpoints

```
[User HTTP POST]
      |
      v
ui_service_producer.py
  validate: order_created_schema v1.0
      |
      v
Topic: order.updates  (event_name: order_initiated)
      |
      +--------> logging_service_consumer.py   (logs all events, no schema check)
      |
      v
order_service_consumer.py
  validate: order_created_schema v1.0
      |
      v
order_service_producer.py  --> Topic: order.updates  (event_name: order_created)
      |
      v
[payment service receives payment.updates]
  validate: payment_schema v1.0
      |
      v
order_service_consumer.py
  validate: payment_schema v1.0
      |
      v
order_service_producer.py  --> Topic: order.updates  (event_name: order_confirmed)
      |
      v
[food service confirms preparation]
  validate: order_prepared_confirmed_schema v1.0
      |
      v
order_service_producer.py  --> Topic: order.updates  (event_name: order_ready)
      |
      v
[delivery service picks up and delivers]
  validate: delivery_picked_completed_schema v1.0
      |
      v
order_service_producer.py  --> Topic: order.updates  (event_name: order_completed)
```

At every arrow crossing a topic boundary, the payload is validated against its schema before it is sent and again when it is received. Any failure at any step diverts to `dead.letter`.

---

## Practical Examples

All examples below assume you are inside the `Chapter09/` directory with the virtual environment active and `PYTHONPATH` set:

```bash
cd ./Chapter09
source .venv/bin/activate
export PYTHONPATH=$(pwd)
```

---

### Example 1 — Validate a Correct Order Event

This is the most basic usage: build a dict that matches the schema and run it through `AvroEventValidator`.

```python
from validators import AvroEventValidator

order = {
    "order_id": "ORDER-001",
    "customer_id": "CUSTOMER-101",
    "restaurant_id": "RESTAUR-202",
    "status": "INITIATED",
    "order_items": [
        {"item_id": "pizza",  "quantity": 2, "price": 15.99},
        {"item_id": "salad",  "quantity": 1, "price": 7.49}
    ],
    "delivery_address": "123 Main Street",
    "payment_method": "credit_card",
    "total_amount": 39.47,
    "timestamp": "2026-10-06T12:00:00.000"
}

validator = AvroEventValidator()
result = validator.validate_event("schema/order_created_schema.avsc", 1.0, order)
print(f"Valid: {result}")   # Valid: True
```

> **Note:** When running from the `Chapter09/` root use `"schema/..."`.
> When running from inside `order/` or `user/` use `"../schema/..."`.

---

### Example 2 — Catch a Validation Error (Missing Required Field)

Remove any required field and the validator raises `ValidationError`.

```python
from fastavro._validation import ValidationError
from validators import AvroEventValidator

# Missing "delivery_address" — a required field
bad_order = {
    "order_id": "ORDER-002",
    "customer_id": "CUSTOMER-101",
    "restaurant_id": "RESTAUR-202",
    "status": "INITIATED",
    "order_items": [{"item_id": "burger", "quantity": 1, "price": 9.99}],
    # "delivery_address" intentionally omitted
    "payment_method": "cash",
    "total_amount": 9.99,
    "timestamp": "2026-10-06T12:00:00.000"
}

validator = AvroEventValidator()
try:
    validator.validate_event("schema/order_created_schema.avsc", 1.0, bad_order)
except ValidationError as e:
    print(f"Caught expected error: {e}")
# Output: Caught expected error: Validation error in AvroEventValidator: ...
```

---

### Example 3 — Catch a Version Mismatch

If the version passed to `validate_event` does not match the `"version"` field inside the `.avsc` file, validation stops immediately.

```python
from fastavro._validation import ValidationError
from validators import AvroEventValidator

order = {
    "order_id": "ORDER-003",
    "customer_id": "CUSTOMER-101",
    "restaurant_id": "RESTAUR-202",
    "status": "INITIATED",
    "order_items": [{"item_id": "pasta", "quantity": 1, "price": 12.50}],
    "delivery_address": "456 Oak Avenue",
    "payment_method": "debit_card",
    "total_amount": 12.50,
    "timestamp": "2026-10-06T12:00:00.000"
}

validator = AvroEventValidator()
try:
    # Schema file has version 1.0 — passing 2.0 triggers the mismatch check
    validator.validate_event("schema/order_created_schema.avsc", 2.0, order)
except ValidationError as e:
    print(f"Version error: {e}")
# Output: Version error: Validation error in AvroEventValidator: Schema Version mismatch!
```

---

### Example 4 — Validate a Payment Event (with Enum)

The `payment_status` field only accepts `"INITIATED"`, `"PROCESSED"`, or `"FAILED"`.

```python
from validators import AvroEventValidator
from fastavro._validation import ValidationError

# Valid payment
payment = {
    "payment_id": "PAY-9001",
    "order_id": "ORDER-001",
    "customer_id": "CUSTOMER-101",
    "restaurant_id": "RESTAUR-202",
    "amount_paid": 39.47,
    "payment_status": "PROCESSED",       # valid enum value
    "payment_timestamp": "2026-10-06T12:05:00.000"
}

validator = AvroEventValidator()
print(validator.validate_event("schema/payment_schema.avsc", 1.0, payment))
# Output: True

# Invalid enum value
payment_bad = {**payment, "payment_status": "REFUNDED"}  # not in enum symbols
try:
    validator.validate_event("schema/payment_schema.avsc", 1.0, payment_bad)
except ValidationError as e:
    print(f"Enum error: {e}")
# Output: Enum error: Validation error in AvroEventValidator: ...
```

---

### Example 5 — Validate a Delivery Event

```python
from validators import AvroEventValidator

delivery = {
    "order_id": "ORDER-001",
    "customer_id": "CUSTOMER-101",
    "delivery_person_id": "DRIVER-55",
    "status": "DELIVERED",
    "timestamp": "2026-10-06T13:30:00.000"
}

validator = AvroEventValidator()
result = validator.validate_event("schema/delivery_picked_completed_schema.avsc", 1.0, delivery)
print(f"Valid: {result}")   # Valid: True
```

---

### Example 6 — Validate and Produce an Event to Kafka

This combines schema validation with an actual Kafka send. If validation fails the event goes to the dead-letter queue instead of the target topic.

```python
import uuid
from datetime import datetime
from fastavro._validation import ValidationError
from kafka import KafkaProducer
import base_producer_config
import kafka_util
from validators import AvroEventValidator

producer = KafkaProducer(**base_producer_config.producer_config)
validator = AvroEventValidator()

order = {
    "order_id": "ORDER-LIVE-001",
    "customer_id": "CUSTOMER-101",
    "restaurant_id": "RESTAUR-202",
    "status": "INITIATED",
    "order_items": [{"item_id": "pizza", "quantity": 2, "price": 15.99}],
    "delivery_address": "123 Main Street",
    "payment_method": "credit_card",
    "total_amount": 31.98,
    "timestamp": str(datetime.now())
}

try:
    validated = validator.validate_event("schema/order_created_schema.avsc", 1.0, order)
    if validated:
        event_id = str(uuid.uuid4()).encode("utf-8")
        headers = [
            ("event_name", b"order_initiated"),
            ("version",    b"1.0"),
            ("event_id",   event_id),
        ]
        result = producer.send(
            "order.updates",
            key=order["order_id"],
            value=order,
            headers=headers
        ).get(timeout=10)
        producer.flush()
        print(f"Sent to partition={result.partition} offset={result.offset}")
except ValidationError as e:
    print(f"Validation failed, routing to DLQ: {e}")
    kafka_util.send_to_dead_letter_queue(
        order["order_id"], order, str(e), producer
    )
finally:
    producer.close()
```

---

### Example 7 — Consume and Validate an Incoming Event

This is the consumer-side pattern: read the event from Kafka, then validate it against its schema before acting on it.

```python
from kafka import KafkaConsumer
from fastavro._validation import ValidationError
import base_consumer_config
from validators import AvroEventValidator

config = base_consumer_config.consumer_config.copy()
config["group_id"] = "example-consumer-group"
config["consumer_timeout_ms"] = 10000

consumer = KafkaConsumer(**config)
consumer.subscribe(["order.updates"])

validator = AvroEventValidator()

for message in consumer:
    headers = dict(message.headers)
    event_name = headers.get("event_name", b"unknown").decode("utf-8")
    event = message.value

    # Choose the right schema based on the event_name header
    schema_map = {
        "order_initiated": "schema/order_created_schema.avsc",
        "order_created":   "schema/order_created_schema.avsc",
        "order_confirmed": "schema/order_prepared_confirmed_schema.avsc",
        "order_ready":     "schema/order_prepared_confirmed_schema.avsc",
        "order_completed": "schema/order_complete_schema.avsc",
    }

    schema_path = schema_map.get(event_name)
    if schema_path:
        try:
            validator.validate_event(schema_path, 1.0, event)
            print(f"Valid event received: {event_name} -> {event['order_id']}")
            consumer.commit()
        except ValidationError as e:
            print(f"Invalid event {event_name}: {e}")
            consumer.commit()   # commit to avoid re-processing poison pill
    else:
        print(f"Unknown event type: {event_name}, skipping")
        consumer.commit()

consumer.close()
```

---

### Example 8 — Load and Inspect a Schema Programmatically

Use `fastavro` directly to inspect a schema's fields at runtime without running a full validation.

```python
from fastavro.schema import load_schema

schema = load_schema("schema/order_created_schema.avsc")

print(f"Name      : {schema['name']}")
print(f"Namespace : {schema['namespace']}")
print(f"Version   : {schema.get('version')}")
print(f"Fields    :")
for field in schema["fields"]:
    print(f"  - {field['name']:20s} type={field['type']}")
```

Expected output:

```
Name      : order_created
Namespace : com.fandutech.events
Version   : 1.0
Fields    :
  - order_id              type=string
  - customer_id           type=string
  - restaurant_id         type=string
  - status                type=string
  - order_items           type={'type': 'array', 'items': ...}
  - delivery_address      type=string
  - payment_method        type=string
  - total_amount          type=double
  - timestamp             type=string
```

---

### Example 9 — Add a New Schema for a New Event Type

Follow these steps to add a new Avro schema for a new event (e.g. a `rating_submitted` event).

**Step 1 — Create the `.avsc` file** at `schema/rating_submitted.avsc`:

```json
{
  "type": "record",
  "name": "rating_submitted",
  "version": 1.0,
  "namespace": "com.fandutech.events",
  "fields": [
    { "name": "order_id",    "type": "string" },
    { "name": "customer_id", "type": "string" },
    { "name": "rating",      "type": "int"    },
    { "name": "comment",     "type": ["null", "string"], "default": null },
    { "name": "timestamp",   "type": "string", "logicalType": "timestamp-millis" }
  ]
}
```

> The `["null", "string"]` union type makes `comment` optional — it can be `null` or a string.

**Step 2 — Validate in your producer**:

```python
from validators import AvroEventValidator

rating_event = {
    "order_id":    "ORDER-001",
    "customer_id": "CUSTOMER-101",
    "rating":      5,
    "comment":     "Great pizza!",
    "timestamp":   "2026-10-06T14:00:00.000"
}

validator = AvroEventValidator()
validator.validate_event("schema/rating_submitted.avsc", 1.0, rating_event)
```

**Step 3 — Validate in your consumer**:

```python
from validators import AvroEventValidator
from fastavro._validation import ValidationError

def handle_rating_submitted(event, key, event_id, event_name, version):
    validator = AvroEventValidator()
    try:
        validated = validator.validate_event(
            "../schema/rating_submitted.avsc", version, event
        )
        if validated:
            print(f"Rating {event['rating']}/5 for order {event['order_id']}")
    except ValidationError as e:
        print(f"Invalid rating event: {e}")
```

---

### Example 10 — Run All Schema Validations as a Quick Smoke Test

Use this script to verify that all schema files in `schema/` can be loaded without errors. Run it after any change to a `.avsc` file.

```python
import os
from fastavro.schema import load_schema

SCHEMA_DIR = "schema"

for filename in sorted(os.listdir(SCHEMA_DIR)):
    if not filename.endswith(".avsc"):
        continue
    path = os.path.join(SCHEMA_DIR, filename)
    try:
        schema = load_schema(path)
        version = schema.get("version", "n/a")
        fields  = [f["name"] for f in schema.get("fields", [])]
        print(f"OK  {filename:<45} v{version}  fields={fields}")
    except Exception as e:
        print(f"ERR {filename}: {e}")
```

Expected output:

```
OK  delivery_picked_completed_schema.avsc         v1.0  fields=['order_id', 'customer_id', 'delivery_person_id', 'status', 'timestamp']
OK  inventory_updated.avsc                        v1.0  fields=['item_id', 'quantity_change', 'updated_timestamp']
OK  order_cancel_schema.avsc                      v1.0  fields=['order_id', 'restaurant_id', 'cancellation_reason', 'canceled_timestamp']
OK  order_complete_schema.avsc                    v1.0  fields=['order_id', 'delivery_person_id', 'status', 'delivered_timestamp']
OK  order_created_schema.avsc                     v1.0  fields=['order_id', 'customer_id', 'restaurant_id', 'status', 'order_items', 'delivery_address', 'payment_method', 'total_amount', 'timestamp']
OK  order_prepared_confirmed_schema.avsc          v1.0  fields=['order_id', 'restaurant_id', 'customer_id', 'status', 'timestamp']
OK  payment_schema.avsc                           v1.0  fields=['payment_id', 'order_id', 'customer_id', 'restaurant_id', 'amount_paid', 'payment_status', 'payment_timestamp']
```
