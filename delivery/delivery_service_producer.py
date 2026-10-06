import logging
import uuid
from datetime import datetime

from fastavro._validation import ValidationError
from kafka import KafkaProducer

import base_producer_config
import kafka_util
from validators import AvroEventValidator

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Initialize Kafka Producer
producer_config = base_producer_config.producer_config
producer_config['acks'] = 1
producer_config['linger_ms'] = 5
producer = KafkaProducer(**producer_config)

def pickup_delivery(event):
    order_id = event['order_id']
    delivery = {
        'order_id': order_id,
        'customer_id': event['customer_id'],
        'delivery_person_id': 'delivery_person_id',
        'status': "PICKEDUP",
        'timestamp': str(datetime.now())
    }
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/delivery_picked_completed_schema.avsc", 1.0, delivery)
        if (validated):
            event_id = str(uuid.uuid4()).encode('utf-8')
            headers = [
                ('event_name', b'delivery_picked'),  # Header value must be bytes
                ('version', b'1.0'),
                ('event_id', event_id)
            ]
            kafka_util.produce_event(producer, "delivery.updates", order_id, delivery, headers, 2)
            logger.debug(f"Order {order_id} with event delivery_picked published")
    except ValidationError as e:
        kafka_util.send_to_dead_letter_queue(order_id, delivery,
                                             "Validation error in Delivery Service Producer for pickup delivery: {e}",
                                             producer)
        logger.error(f"Validation error in Delivery Service Producer for pickup delivery: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for pickup delivery: {e}")


def complete_delivery(event):
    order_id = event['order_id']
    delivery = {
        'order_id': order_id,
        'customer_id': event['customer_id'],
        'delivery_person_id': 'delivery_person_id',
        'status': "COMPLETED",
        'timestamp': str(datetime.now())
    }
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/delivery_picked_completed_schema.avsc", 1.0, delivery)
        if (validated):
            event_id = str(uuid.uuid4()).encode('utf-8')
            headers = [
                ('event_name', b'delivery_completed'),  # Header value must be bytes
                ('version', b'1.0'),
                ('event_id', event_id)
            ]
            kafka_util.produce_event(producer, "delivery.updates", order_id, delivery, headers, 2)
            logger.debug(f"Order {order_id} with event delivery_completed published")
    except ValidationError as e:
        kafka_util.send_to_dead_letter_queue(order_id, delivery,
                                             "Validation error in Delivery Service Producer for complete delivery: {e}",
                                             producer)
        logger.error(f"Validation error in Delivery Service Producer for complete delivery: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for complete delivery: {e}")