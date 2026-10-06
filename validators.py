import logging

from fastavro import validate
from fastavro._validation import ValidationError
from fastavro.schema import load_schema

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


class AvroEventValidator:
    """Validates event schemas"""

    @staticmethod
    def validate_event(event_schema: str, version: float, event_data: dict) -> bool:
        event_schema = load_schema(event_schema)
        try:
            schema_version = event_schema.get('version')
            if schema_version != version:
                raise ValidationError("Schema Version mismatch!")
            validate(event_data, event_schema, raise_errors=True)
            return True
        except ValidationError as e:
            logger.error(f"Validation error in AvroEventValidator: {e}")
            raise ValidationError(f"Validation error in AvroEventValidator: {e}")
