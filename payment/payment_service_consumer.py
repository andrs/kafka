import json
import logging
from datetime import datetime

from fastavro._validation import ValidationError
from kafka import KafkaConsumer, KafkaProducer

import base_producer_config
import kafka_util
from base_consumer_config import consumer_config
from payment.payment_service_producer import update_payment
from validators import AvroEventValidator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Kafka consumer
config = consumer_config
group_id = 'payment-consumer-group'
config['group_id'] = group_id
consumer = KafkaConsumer(**config)
consumer.subscribe(['order.updates'], listener=kafka_util.RebalanceListener())
# Initialize producer for dlq
producer_config = base_producer_config.producer_config
producer = KafkaProducer(**producer_config)


def handle_create_order(event, key, event_id, event_name, version):
    logger.debug(f"In handle_create_order: {event_name}, {event}")
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/order_created_schema.avsc", version, event)
        if validated:
            kafka_util.process_with_circuit_breaker(producer, event, key, event_id, event_name, process_payment_event)
            # Initiate payment process. Third-party service might be involved.
            payment_id = 'PAYMENT-123'
            order_id = event['order_id']
            customer_id = event['customer_id']
            restaurant_id = event['restaurant_id']
            amount_paid = event['total_amount']
            payment_status = 'PROCESSED'
            timestamp = str(datetime.now())
            # Once payment successful, publish an event.
            update_payment(order_id, payment_id, customer_id, restaurant_id, amount_paid, payment_status, timestamp)
    except ValidationError as e:
        logger.error(f"Validation error in handle_create_order: {e}")
        kafka_util.send_to_dead_letter_queue(key, event, f"Validation error in handle_create_order: {e}", producer)


def handle_unknown_event(event, key, event_id, event_name, version):
    logger.debug(f"Unknown event type: unknown")
    # Add fallback logic here
    pass


def process_payment_event(event_id, event_name, order_id, event_data):
    kafka_util.cursor.execute(
        f"INSERT INTO eda.orders (event_id, event_name, order_id, event_data) VALUES (%s, %s, %s, %s)",
        (event_id, event_name, order_id, json.dumps(event_data)))


# Create a mapping dictionary
EVENT_HANDLERS = {
    'order_created': handle_create_order
}

try:
    for event in consumer:
        try:
            kafka_util.process_event(event, EVENT_HANDLERS)  # Pass the whole event.
            consumer.commit()  # Commit offset after successful processing.  +1 because it commits *next* offset.

        except json.JSONDecodeError:
            logger.error(
                f"Error decoding JSON (group {group_id} topic {event.topic}, offset {event.offset}): {event.value}"
            )
            # Consider how to handle bad messages (e.g., send to a dead-letter queue)
            kafka_util.send_to_dead_letter_queue(event.key, event.value,
                                                 f"Error decoding JSON (group {group_id} topic {event.topic}, offset {event.offset}): {event.value}",
                                                 producer)
            consumer.commit()  # Commit the offset to prevent the consumer from getting stuck.
        except Exception as e:
            logger.error(
                f"Error processing message (group {group_id} topic {event.topic}, offset {event.offset}): {e}"
            )
            # Consider error handling strategy (e.g., retry, send to dead-letter queue)
            kafka_util.send_to_dead_letter_queue(event.key, event.value,
                                                 f"Error processing message (group {group_id} topic {event.topic}, offset {event.offset}): {e}",
                                                 producer)
            consumer.commit()  # Commit the offset to prevent the consumer from getting stuck.

except KeyboardInterrupt:
    logger.error(f"Consumer interrupted by user.")
except Exception as e:
    logger.error(f"Consumer encountered an error (group {group_id} topic {event.topic}, offset {event.offset}): {e}")
finally:
    consumer.close()
    logger.debug(f"Consumer closed.")
