import json
import logging

from fastavro._validation import ValidationError
from kafka import KafkaConsumer, KafkaProducer

import base_producer_config
import kafka_util
from base_consumer_config import consumer_config
from delivery.delivery_service_producer import pickup_delivery, complete_delivery
from validators import AvroEventValidator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Kafka consumer
config = consumer_config
group_id = 'delivery-consumer-group'
config['group_id'] = group_id
consumer = KafkaConsumer(**config)
consumer.subscribe(['order.updates', 'delivery.updates'], listener=kafka_util.RebalanceListener())

# Initialize producer for dlq
producer_config = base_producer_config.producer_config
producer = KafkaProducer(**producer_config)

def handle_order_ready(event, key, event_id, event_name, version):
    logger.debug(f"In handle_order_ready: {event_name}, {event}")
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/order_prepared_confirmed_schema.avsc", version, event)
        if validated:
            kafka_util.process_with_circuit_breaker(producer, event, key, event_id, event_name, process_order_event)
            pickup_delivery(event)
    except ValidationError as e:
        logger.error(f"Validation error in handle_order_ready: {e}")
        kafka_util.send_to_dead_letter_queue(key, event, f"Validation error in handle_order_ready: {e}", producer)

def handle_pickup_delivery(event, key, event_id, event_name, version):
    logger.debug(f"In handle_pickup_delivery: {event_name}, {event}")
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/delivery_picked_completed_schema.avsc", version, event)
        if validated:
            kafka_util.process_with_circuit_breaker(producer, event, key, event_id, event_name, process_delivery_event)
            complete_delivery(event)
    except ValidationError as e:
        logger.error(f"Validation error in handle_pickup_delivery: {e}")
        kafka_util.send_to_dead_letter_queue(key, event, f"Validation error in handle_pickup_delivery: {e}", producer)

def handle_unknown_event(event, key, event_id, event_name, version):
    logger.debug(f"Unknown event type: unknown")
    # Add fallback logic here
    pass

# Create a mapping dictionary
EVENT_HANDLERS = {
    'order_ready': handle_order_ready,
    'delivery_picked': handle_pickup_delivery
}


def process_order_event(event_id, event_name, order_id, event_data):
    kafka_util.cursor.execute(
        f"INSERT INTO eda.orders (event_id, event_name, order_id, event_data) VALUES (%s, %s, %s, %s)",
        (event_id, event_name, order_id, json.dumps(event_data)))

def process_delivery_event(event_id, event_name, order_id, event_data):
    kafka_util.cursor.execute(
        f"INSERT INTO eda.delivery (event_id, event_name, order_id, delivery_data) VALUES (%s, %s, %s, %s)",
        (event_id, event_name, order_id, json.dumps(event_data)))


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
    logger.error(f"Consumer encountered an error (group {group_id}: {e}")
finally:
    consumer.close()
    logger.debug(f"Consumer closed.")