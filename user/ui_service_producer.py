import logging
import uuid
from datetime import datetime

from flask import Flask, request
from fastavro._validation import ValidationError
from kafka import KafkaProducer

import base_producer_config
import kafka_util
from validators import AvroEventValidator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize Kafka Producer
producer_config = base_producer_config.producer_config

producer = KafkaProducer(**producer_config)

@app.route('/initiate_orders', methods=['POST'])
def initiate_order():
    order_id = 'ORDER-123'
    order_data = request.get_json()
    timestamp = str(datetime.now())
    order = {
        'order_id': order_id,
        'order_items': order_data.get('items'),
        'customer_id': order_data.get('customer_id'),
        'restaurant_id': order_data.get('restaurant_id'),
        'delivery_address': order_data.get('delivery_address'),
        'payment_method': order_data.get('payment_method'),
        'total_amount': order_data.get('total_amount'),
        'timestamp': timestamp,
        'status': 'INITIATED'
    }
    try:
        validator = AvroEventValidator()
        validated = validator.validate_event("../schema/order_created_schema.avsc", 1.0, order)
        if (validated):
            event_id = str(uuid.uuid4()).encode('utf-8')
            headers = [
                ('event_name', b'order_initiated'),  # Header value must be bytes
                ('version', b'1.0'),
                ('event_id', event_id)
            ]
            kafka_util.produce_event(producer, "order.updates", order_id, order, headers)
            logger.debug(f"Order {order_id} published")
            return {"status": "order_initiated", "order_id": order_id}, 201
    except ValidationError as e:
        kafka_util.send_to_dead_letter_queue(order_id, order,
                                             "Validation error in UI Service Producer for initiate order: {e}",
                                             producer)
        logger.error(f"Validation error in UI Service Producer for initiate order: {e}")
        return {"error": str(e)}, 400
    except Exception as e:
        kafka_util.send_to_dead_letter_queue(order_id, order,
                                             "Failed to publish order for initiate order: {e}", producer)
        logger.error(f"Failed to publish order for initiate order: {e}")
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
    # items = [
    #     {"item_id": "pizza", "quantity": 2, "price": 15.99},
    #     {"item_id": "salad", "quantity": 1, "price": 7.49}
    # ]
    # initiate_order("ORDER-123", items, "CUSTOMER-456", "RESTAUR-123", "some_address", "some_payment", 120.50)
