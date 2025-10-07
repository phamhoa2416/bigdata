import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from typing import Any, Dict, List

class JobDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic_name='jobs_topic'):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = None
        self.setup_producer()

    def setup_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=5,
                buffer_memory=33554432,
                max_block_ms=60000
            )
            logging.info(f"Kafka producer connected to {self.bootstrap_servers}")

            self.producer.send(self.topic_name, {'message': 'Producer initialized successfully'})
            self.producer.flush(timeout=10)
            logging.info("Kafka connection test successful.")
        except KafkaError as e:
            logging.error(f"Failed to connect to Kafka: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error setting up producer: {e}")
            raise

    def send_job_data(self, job_data: Dict[str, Any]) -> bool:
        try:
            key = job_data.get('company', 'unknown').replace(' ', '_').lower()

            future = self.producer.send(
                topic=self.topic_name,
                value=job_data,
                key=key
            )

            record_metadata = future.get(timeout=10)

            logging.debug(
                f"Message sent to topic: {record_metadata.topic}, "
                f"partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset}"
            )

            return True
        except KafkaError as e:
            logging.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logging.error(f"Error sending job data to Kafka: {e}")
            return False

    def send_batch_job_data(self, jobs_data: List[Dict[str, Any]]) -> Dict[str, int]:
        results = {
            'successful': 0,
            'failed': 0,
            'total': len(jobs_data)
        }

        for job_data in jobs_data:
            success = self.send_job_data(job_data)
            if success:
                results['successful'] += 1
                logging.info(f"Sent job to Kafka: {job_data['title']}")
            else:
                results['failed'] += 1
                logging.error(f"Failed to send job: {job_data['title']}")

            time.sleep(0.1)

        logging.info(
            f"Batch send completed: {results['successful']}/"
            f"{results['total']} successful, {results['failed']} failed"
        )
        return results

    def flush_messages(self):
        try:
            self.producer.flush(timeout=30)
            logging.info("All pending messages flushed to Kafka")
        except Exception as e:
            logging.error(f"Error flushing messages: {e}")

    def close(self):
        if self.producer:
            self.flush_messages()
            self.producer.close(timeout=10)
            logging.info("Kafka producer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()