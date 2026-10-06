# SSL/TLS configuration

import kafka_util

# Common Producer Configuration
producer_config = {
    'bootstrap_servers': ['localhost:19092', 'localhost:29092', 'localhost:39092'],
    'acks': 'all',  # Wait for all ISR replicas to acknowledge
    'retries': 3,  # Retry failed sends
    'linger_ms': 5,
    'compression_type': 'gzip',  # Compress messages
    'max_in_flight_requests_per_connection': 1,
    # enable_idempotence=True,  # Ensure exactly-once semantics
    'key_serializer': kafka_util.string_serializer,
    'value_serializer': kafka_util.json_serializer,
    'security_protocol': 'SSL',
    'ssl_context': kafka_util.ssl_context,
    'api_version': (3, 7, 0),
    # 'security_protocol': 'SASL_SSL',
    # 'sasl_mechanism': 'SCRAM-SHA-256',
    # 'ssl_context': kafka_util.ssl_context,
    # 'sasl_plain_username': 'alice',
    # 'sasl_plain_password': 'eda_kafka'
}
