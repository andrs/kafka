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
producer_config['linger_ms'] = 5

producer = KafkaProducer(**producer_config)

def prepare_food(event):
    order_id = event['order_id']
    restaurant_id = event['restaurant_id']
    customer_id = event['customer_id']
    status = 'PREPARED'
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
                ('event_name', b'order_prepared'),  # Header value must be bytes
                ('version', b'1.0'),
                ('event_id', event_id)
            ]
            kafka_util.produce_event(producer, "food.updates", order_id, update_order, headers, 2)
            logger.debug(f"Order {order_id} published")
    except ValidationError as e:
        kafka_util.send_to_dead_letter_queue(order_id, update_order,
                                             "Validation error in Food Service Producer for prepare order: {e}",
                                             producer)
        logger.error(f"Validation error in Food Service Producer for prepare order: {e}")
    except Exception as e:
        logger.error(f"Failed to publish order for prepare order: {e}")