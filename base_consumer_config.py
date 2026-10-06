from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor

import kafka_util

# Common Consumer Config
consumer_config = {
    'bootstrap_servers': ['localhost:19092', 'localhost:29092', 'localhost:39092'],
    'security_protocol': 'SSL',
    'ssl_context': kafka_util.ssl_context,
    'api_version': (3, 7, 0),
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'key_deserializer': kafka_util.string_deserializer,
    'value_deserializer': kafka_util.json_deserializer,
    'max_poll_interval_ms': 420000,
    'partition_assignment_strategy': [StickyPartitionAssignor()]
}
