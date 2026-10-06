import logging
import uuid

from fastavro._validation import ValidationError
from kafka import KafkaProducer

import base_producer_config
import kafka_util
from validators import AvroEventValidator

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Initialize Kafka Producer
producer_config = base_producer_config.producer_config
producer_config['acks'] = all
producer = KafkaProducer(**producer_config)


def update_payment(order_id, payment_id, customer_id, restaurant_id, amount_paid, payment_status, timestamp):
    payment = {
        'payment_id': payment_id,
        'order_id': order_id,
        'customer_id': customer_id,
        'restaurant_id': restaurant_id,
        'amount_paid': amount_paid,
        'payment_status': payment_status,
        'payment_timestamp': timestamp
    }

    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/payment_schema.avsc", 1.0, payment)
        if (validated):
            event_id = str(uuid.uuid4()).encode('utf-8')
            headers = [
                ('event_name', b'payment_processed'),  # Header value must be bytes
                ('version', b'1.0'),
                ('event_id', event_id)
            ]
            kafka_util.produce_event(producer, "payment.updates", order_id, payment, headers, 3)
            logger.debug(f"Payment {payment_id} published for Order {order_id}")
    except ValidationError as e:
        logger.error(f"Validation error in Payment Service Producer for {payment_status} payment: {e}")
    except Exception as e:
        logger.error(f"Failed to publish payment event for {payment_status} payment: {e}")


