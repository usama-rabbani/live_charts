from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaProducerSingleton:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            try:
                cls._instance = KafkaProducer(
                    bootstrap_servers=['108.143.249.139:9092'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    max_block_ms=10000
                )
                # logger.info("Kafka Producer initialized successfully")
            except Exception as e:
                # logger.error(f"Failed to initialize Kafka Producer: {e}")
                raise
        return cls._instance

def send_execution_to_kafka(topic, data):
    producer = KafkaProducerSingleton.get_instance()
    try:
        for row in data:
            # Convert Result_On to ISO format string if it's a datetime object
            result_on = row.get('Result_On', datetime.now())
            if isinstance(result_on, datetime):
                result_on = result_on.isoformat()
            else:
            
                result_on = str(result_on)  # Ensure it's a string if not datetime

            message = {
                'ResultOn': result_on,
                'Result': row.get('Result', 0)
            }
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=100)
            # logger.info(f"Sent message to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
        producer.flush()
    except KafkaError as e:
        # logger.error(f"Error sending message to Kafka: {e}")
        raise
    except Exception as e:
        # logger.error(f"Unexpected error: {e}")
        raise