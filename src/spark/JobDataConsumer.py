import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobDataConsumer:
    def __init__(
        self,
        app_name="JobDataKafkaToHDFS",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="jobs_topic",
        hdfs_output_path="hdfs://localhost:9000/jobs_data",
        checkpoint_location="hdfs://localhost:9000/checkpoints/jobs"
    ):
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.hdfs_output_path = hdfs_output_path
        self.checkpoint_location = checkpoint_location
        self.spark = None
        self.setup_spark()

    def setup_spark(self):
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation",
                        self.checkpoint_location) \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Spark session: {e}")
            raise