# Commands Used — Chapter09 Kafka SSL Setup

> All commands assume you start from the **Chapter09** directory:
> ```bash
> cd ./Chapter09
> ```

## 1. Inspect Running Docker Containers

```bash
sudo docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"
sudo docker ps --format "table {{.Names}}\t{{.Ports}}" --no-trunc
```

## 2. Inspect Directories and SSL Certificates

```bash
ls -la ./docker/
ls -la ./docker/secrets/
ls -la ./
```

## 3. Install Python Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Verify SSL Context and Certificate Paths

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"

python3 -c "
import kafka_util, os
print(f'Secrets dir: {kafka_util._SECRETS_DIR}')
print(f'CA exists: {os.path.exists(os.path.join(kafka_util._SECRETS_DIR, \"snakeoil-ca-1.crt\"))}')
print(f'ssl_context created: {kafka_util.ssl_context is not None}')
"
```

## 5. Test Kafka Connectivity (Producer + Topic List)

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"

python3 -c "
from kafka import KafkaProducer, KafkaConsumer
import kafka_util, base_producer_config, base_consumer_config

# Test producer
producer = KafkaProducer(**base_producer_config.producer_config)
print('Producer connected:', producer.bootstrap_connected())

# List topics
cconfig = base_consumer_config.consumer_config.copy()
cconfig['group_id'] = 'test-connectivity'
consumer = KafkaConsumer(**cconfig)
print('Available topics:', consumer.topics())
consumer.close()
producer.close()
"
```

## 6. Create Required Kafka Topics

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"

python3 -c "
from kafka.admin import KafkaAdminClient, NewTopic
import kafka_util

admin = KafkaAdminClient(
    bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
    security_protocol='SSL',
    ssl_context=kafka_util.ssl_context,
    api_version=(3, 7, 0)
)
existing = admin.list_topics()
topics_needed = ['order.updates', 'payment.updates', 'food.updates', 'delivery.updates', 'dead.letter']
to_create = [t for t in topics_needed if t not in existing]
if to_create:
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=3) for t in to_create]
    admin.create_topics(new_topics)
    print(f'Created: {to_create}')
print('All topics:', admin.list_topics())
admin.close()
"
```

## 7. Run the Logging Consumer

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"
cd ./logging

python3 logging_service_consumer.py
```

## 8. Run the Order Consumer

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"
cd ./order

python3 order_service_consumer.py
```

## 9. Run the UI Service (Flask Producer)

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"
cd ./user

python3 ui_service_producer.py
```

## 10. Send a Test Order via the Flask API

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

## 11. End-to-End Produce and Consume Test

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"

python3 -c "
import uuid, json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import kafka_util, base_producer_config
from base_consumer_config import consumer_config

# Produce
producer = KafkaProducer(**base_producer_config.producer_config)
order = {
    'order_id': 'ORDER-TEST-001',
    'order_items': [{'item_id': 'pizza', 'quantity': 2, 'price': 15.99}],
    'customer_id': 'CUSTOMER-456',
    'restaurant_id': 'RESTAUR-123',
    'delivery_address': '123 Main St',
    'payment_method': 'credit_card',
    'total_amount': 31.98,
    'timestamp': str(datetime.now()),
    'status': 'INITIATED'
}
event_id = str(uuid.uuid4()).encode('utf-8')
headers = [('event_name', b'order_initiated'), ('version', b'1.0'), ('event_id', event_id)]
result = producer.send('order.updates', key='ORDER-TEST-001', value=order, headers=headers).get(timeout=10)
producer.flush()
producer.close()
print(f'Sent: topic={result.topic}, partition={result.partition}, offset={result.offset}')

# Consume
cconfig = consumer_config.copy()
cconfig['group_id'] = 'direct-test-group'
cconfig['consumer_timeout_ms'] = 10000
consumer = KafkaConsumer(**cconfig)
consumer.subscribe(['order.updates'])
for msg in consumer:
    headers = dict(msg.headers)
    print(f'Received: event_name={headers.get(\"event_name\", b\"\").decode()}, value={msg.value}')
    consumer.commit()
    break
consumer.close()
"
```

## 12. Full Multi-Service Background Test (consumers + producer)

```bash
export PYTHONPATH="$(pwd):$PYTHONPATH"
CH09="$(pwd)"
PYTHON="$CH09/.venv/bin/python"

# Start logging consumer in background
cd "$CH09/logging" && $PYTHON logging_service_consumer.py > /tmp/logging_consumer.log 2>&1 &
LOG_PID=$!

# Start order consumer in background
cd "$CH09/order" && $PYTHON order_service_consumer.py > /tmp/order_consumer.log 2>&1 &
ORDER_PID=$!

# Start Flask UI service in background
cd "$CH09/user" && $PYTHON ui_service_producer.py > /tmp/ui_service.log 2>&1 &
UI_PID=$!

# Wait for services to be ready
sleep 6

# Send test order
curl -s -X POST http://localhost:5000/initiate_orders \
  -H "Content-Type: application/json" \
  -d '{"items": [], "customer_id": "C1", "restaurant_id": "R1", "delivery_address": "addr", "payment_method": "cash", "total_amount": 10.0}'

# Check outputs
sleep 5
cat /tmp/logging_consumer.log
cat /tmp/order_consumer.log

# Cleanup
kill $LOG_PID $ORDER_PID $UI_PID
```

## Notes

- **Brokers**: `localhost:19092`, `localhost:29092`, `localhost:39092`
- **Security protocol**: `SSL` (no SASL)
- **TLS version**: Forced to TLS 1.2 (`ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2`) to avoid TLS 1.3 post-handshake socket bug in kafka-python
- **SSL certs location**: `./docker/secrets/` (inside Chapter09)
  - CA: `snakeoil-ca-1.crt`
  - Client cert: `producer.crt`
  - Client key: `producer.key`
- **PYTHONPATH** must include the Chapter09 root for module imports to resolve
- **Working directory** for each service must be its own subdirectory (for `../schema/` relative paths to resolve correctly)
