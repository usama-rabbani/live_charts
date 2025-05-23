from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_executions(topic, max_messages=10, timeout_ms=5000, max_retries=3, last_timestamp=None):
    retry_count = 0
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['108.143.249.139:9092'],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='dashboard-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=timeout_ms,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=60000
            )
            
            messages = []
            for message in consumer:
                message_data = message.value
                message_time = datetime.fromisoformat(message_data['ResultOn'].replace('Z', '+00:00'))
                if last_timestamp and message_time <= last_timestamp:
                    continue  # Skip messages older than last_timestamp
                if len(messages) >= max_messages:
                    break
                messages.append(message_data)
                logger.debug(f"Received message: offset={message.offset}, value={message_data}")
            
            logger.info(f"Consumed {len(messages)} messages")
            consumer.close()
            return messages
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"Retrying ({retry_count}/{max_retries}) after 2 seconds...")
                time.sleep(2)
            else:
                logger.error(f"Max retries reached. Failed to consume messages.")
                return []
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}")
            return []
        finally:
            try:
                consumer.close()
            except:
                pass