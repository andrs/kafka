import logging

from kafka import KafkaConsumer, KafkaProducer

import base_producer_config
import kafka_util
from base_consumer_config import consumer_config

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Kafka consumer
config = consumer_config
group_id = 'logging-consumer-group'
config['group_id'] = group_id
consumer = KafkaConsumer(**config)
# Subscribe to all topics
consumer.subscribe(['order.updates', 'payment.updates', 'delivery.updates', 'food.updates'], listener=kafka_util.RebalanceListener())

# Initialize producer for dlq
producer_config = base_producer_config.producer_config
producer = KafkaProducer(**producer_config)

try:
    # Consume all events
    for event in consumer:
        try:
            headers = dict(event.headers)
            event_name = headers.get('event_name', b'unknown').decode('utf-8')
            event_id = headers.get('event_id', b'unknown').decode('utf-8')
            event_value = event.value
            logger.debug((f"Event name, Event ID, Event produced: {event_name}, {event_id}, {event_value}"))
            print(f"Event name, Event ID, Event produced: {event_name}, {event_id}, {event_value}")
            consumer.commit()  # Commit offset after successful processing.  +1 because it commits *next* offset.

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