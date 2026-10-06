import json
import logging
import os
import ssl
import time
from datetime import datetime

import psycopg2
import pybreaker
from fastavro._validation import ValidationError
from kafka import ConsumerRebalanceListener
from kafka.errors import KafkaError
from psycopg2 import DatabaseError

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_SECRETS_DIR = os.path.join(_BASE_DIR, 'docker', 'secrets')


# Add rebalance listener for debugging
class RebalanceListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print(f"Partitions revoked: {revoked}")

    def on_partitions_assigned(self, assigned):
        print(f"Partitions assigned: {assigned}")


# PostgreSQL Connection Configuration
postgres_config = {
    'host': 'localhost',  # Replace with your PostgreSQL host
    'port': 5432,  # Replace with your PostgreSQL port
    'database': 'fandutech',  # Replace with your database name
    'user': 'fandutech',  # Replace with your username
    'password': 'fandutech',  # Replace with your password
}

ssl_context = ssl.create_default_context(
    purpose=ssl.Purpose.SERVER_AUTH,
    cafile=os.path.join(_SECRETS_DIR, 'snakeoil-ca-1.crt')
)
ssl_context.load_cert_chain(
    certfile=os.path.join(_SECRETS_DIR, 'producer.crt'),
    keyfile=os.path.join(_SECRETS_DIR, 'producer.key')
)

ssl_context.check_hostname = False
ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2


def string_serializer(value):
    if isinstance(value, str):
        return value.encode('utf-8')
    raise TypeError('Expected a string')


def string_deserializer(data):
    if data is None:
        return None
    return data.decode('utf-8')


def json_serializer(value):
    if isinstance(value, dict):
        return json.dumps(value).encode('utf-8')
    raise TypeError('Expected a dictionary')


def json_deserializer(data):
    if data is None:
        return None
    text = data.decode('utf-8')
    return json.loads(text)


def send_to_dead_letter_queue(key, value, error, producer=None):
    dlq_event = {"event": value, "error": str(error), "timestamp": datetime.now().isoformat(), }
    producer.send('dead.letter', key=key, value=dlq_event)
    producer.flush()
    producer.close()


def send_with_retry(producer, topic, key, value, headers, retries):
    for attempt in range(retries):
        try:
            producer.send(topic, key=key, value=value, headers=headers).get(timeout=100)
            producer.flush()
            logger.debug(f"Event: {value} sent to kafka topic: {topic}")
            return  # Successful send
        except KafkaError as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            # print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    # raise Exception("All retries failed")
    logger.debug("All retries failed, sending to dlq")
    send_to_dead_letter_queue(key, value, "All retries failed", producer)


circuit_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=30)


@circuit_breaker
def produce_event(producer, topic, key, value, headers, retries=3):
    send_with_retry(producer, topic, key, value, headers, retries)


# Consumer utility methods

def is_duplicate(event_id, event_name, cursor):
    try:
        cursor.execute("SELECT event_id FROM eda.processed_events WHERE event_id = %s and event_name= %s",
                       (event_id, event_name))
        return cursor.fetchone() is not None
    except DatabaseError as e:
        raise DatabaseError(e)

def consume_event(event, event_id, event_name, domain_method):
    global cursor, conn
    logger.debug(f"Consumed event: {event}")

    try:
        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()
        with psycopg2.connect(**postgres_config) as conn:
            with conn.cursor() as cursor:
                if is_duplicate(event_id, event_name, cursor):
                    logger.debug(f"Duplicate event detected: {event_id}, {event_name}")
                    return

                order_id = event['order_id']
                timestamp = datetime.now()
                # Insert into event metadata table
                cursor.execute(
                    "INSERT INTO eda.processed_events (event_id, event_name, timestamp, order_id) VALUES (%s, %s, %s, %s)",
                    (event_id, event_name, timestamp, order_id))

                # Execute domain method for event consumption.
                # Insert event processing logic here, like updating a database or triggering an API call
                domain_method(event_id, event_name, order_id, event)
                conn.commit()
                logger.debug(f"Event processed in kafka utility: {event_id}, {event_name}")
    except psycopg2.Error as e:
        logger.error(f"Database error with event_id {event_id}, event_name {event_name}: {e}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Error in consuming event with event_id {event_id}, event_name {event_name}: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def retry_event(producer, event, key, event_id, event_name, domain_method, retries, backoff):
    for attempt in range(retries):
        try:
            consume_event(event, event_id, event_name, domain_method)
            return
        except DatabaseError as e:
            time.sleep(backoff ** attempt)
            logger.error(f"Database error: {e}")
        except ValidationError as e:
            logger.error(f"Validation error: {e}")
            send_to_dead_letter_queue(key, event, "Validation Error", producer)
            return
        except Exception as e:
            logger.error(f"Exception: {e}")
            send_to_dead_letter_queue(key, event, e, producer)
            return
    # Send to DLQ if retries fail
    send_to_dead_letter_queue(key, event, "MaxRetriesExceeded", producer)


circuit_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=30)


@circuit_breaker
def process_with_circuit_breaker(producer, event, key, event_id, event_name, domain_method, retries=3, backoff=2):
    print(f"EVent {event}, event_name {event_name}")
    retry_event(producer, event, key, event_id, event_name, domain_method, retries, backoff)


def handle_irrelevant_event(event, key, event_id, event_name, version):
    logger.debug(f"Irrelevant event type: {event_name}")
    # Add fallback logic here
    pass


# Process each event
def process_event(event, event_handlers):
    # Check headers for event type
    headers = dict(event.headers)
    event_name = headers.get('event_name', b'unknown').decode('utf-8')
    event_id = headers.get('event_id', b'unknown').decode('utf-8')
    version = float(headers.get('version', b'unknown').decode('utf-8'))
    event_value = event.value
    key = event.key
    # Get handler based on event name
    handler = event_handlers.get(
        event_name,
        handle_irrelevant_event
    )

    # Execute the handler
    handler(event_value, key, event_id, event_name, version)
