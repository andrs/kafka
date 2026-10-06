import logging
import uuid
from datetime import datetime

from fastavro._validation import ValidationError
from kafka import KafkaProducer

import base_producer_config
import kafka_util
from validators import AvroEventValidator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Kafka Producer
producer_config = base_producer_config.producer_config
producer_config['acks'] = 1
producer_config['linger_ms'] = 100

producer = KafkaProducer(**producer_config)


def create_order(event):
    try:
        order_id = event['order_id']
        order = event
        order['status'] = 'CREATED'
        order['timestamp'] = str(datetime.now())
        try:
            validator = AvroEventValidator()
            validated = validator.validate_event("../schema/order_created_schema.avsc", 1.0, order)
            if (validated):
                event_id = str(uuid.uuid4()).encode('utf-8')
                headers = [
                    ('event_name', b'order_created'),  # Header value must be bytes
                    ('version', b'1.0'),
                    ('event_id', event_id)
                ]
                kafka_util.produce_event(producer, "order.updates", order_id, order, headers,2)
                logger.debug(f"Order {order_id} for create_order published")
        except ValidationError as e:
            kafka_util.send_to_dead_letter_queue(order_id, order,
                                                 "Validation error in Order Service Producer for create order: {e}",
                                                 producer)
            logger.error(f"Validation error in Order Service Producer for create order: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for create order: {e}")


def update_order(event):
    try:
        order_id = event['order_id']
        restaurant_id = event['restaurant_id']
        customer_id = event['customer_id']
        status = 'CONFIRMED'
        timestamp = str(datetime.now())
        update_order = {
            'order_id': order_id,
            'restaurant_id': restaurant_id,
            'customer_id': customer_id,
            'status': status,
            'timestamp': timestamp
        }
        try:
            validator = AvroEventValidator()
            validated = validator.validate_event("../schema/order_prepared_confirmed_schema.avsc", 1.0, update_order)
            if (validated):
                event_id = str(uuid.uuid4()).encode('utf-8')
                headers = [
                    ('event_name', b'order_confirmed'),  # Header value must be bytes
                    ('version', b'1.0'),
                    ('event_id', event_id)
                ]
                kafka_util.produce_event(producer, "order.updates", order_id, update_order, headers, 2)
                logger.debug(f"Order {order_id} for event order_confirmed published")
        except ValidationError as e:
            kafka_util.send_to_dead_letter_queue(order_id, update_order,
                                                 "Validation error in Order Service Producer for update order: {e}",
                                                 producer)
            logger.error(f"Validation error in Order Service Producer for confirm order: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for confirm order: {e}")


def complete_order(event):
    try:
        order_id = event['order_id']
        complete_order = {
            'order_id': order_id,
            'delivery_person_id': event['delivery_person_id'],
            'status': "COMPLETED",
            'delivered_timestamp': str(datetime.now())
        }
        try:
            validator = AvroEventValidator()
            validated = validator.validate_event("../schema/order_complete_schema.avsc", 1.0, complete_order)
            if (validated):
                event_id = str(uuid.uuid4()).encode('utf-8')
                headers = [
                    ('event_name', b'order_completed'),  # Header value must be bytes
                    ('version', b'1.0'),
                    ('event_id', event_id)
                ]
                kafka_util.produce_event(producer, "order.updates", order_id, complete_order, headers, 2)
                logger.debug(f"Order {order_id} for event complete_order published")
        except ValidationError as e:
            kafka_util.send_to_dead_letter_queue(order_id, complete_order,
                                                 "Validation error in Order Service Producer for complete order: {e}",
                                                 producer)
            logger.error(f"Validation error in Order Service Producer for complete order: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for complete order: {e}")

def ready_order(event):
    try:
        order_id = event['order_id']
        restaurant_id = event['restaurant_id']
        customer_id = event['customer_id']
        status = 'READY'
        timestamp = str(datetime.now())
        ready_order = {
            'order_id': order_id,
            'restaurant_id': restaurant_id,
            'customer_id': customer_id,
            'status': status,
            'timestamp': timestamp
        }
        try:
            validator = AvroEventValidator()
            validated = validator.validate_event("../schema/order_prepared_confirmed_schema.avsc", 1.0, ready_order)
            if (validated):
                event_id = str(uuid.uuid4()).encode('utf-8')
                headers = [
                    ('event_name', b'order_ready'),  # Header value must be bytes
                    ('version', b'1.0'),
                    ('event_id', event_id)
                ]
                kafka_util.produce_event(producer, "order.updates", order_id, ready_order, headers, 2)
                logger.debug(f"Order {order_id} for event order_ready published")
        except ValidationError as e:
            kafka_util.send_to_dead_letter_queue(order_id, ready_order,
                                                 "Validation error in Order Service Producer for ready order: {e}",
                                                 producer)
            logger.error(f"Validation error in Order Service Producer for ready order: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for ready order: {e}")
