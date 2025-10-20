import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, trim, when, regexp_extract, lower, \
    current_timestamp, concat_ws, split, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class JobDataProcessor:
    def __init__(
            self,
            app_name="JobDataKafkaToHDFS",
            kafka_bootstrap_servers=None,
            kafka_topic="jobs_topic",
            hdfs_output_path=None,
            checkpoint_location=None,
    ):
        self.app_name = app_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'
        )
        self.kafka_topic = kafka_topic
        self.hdfs_output_path = hdfs_output_path or os.getenv(
            'HDFS_OUTPUT_PATH', 'hdfs://namenode:9000/jobs/processed'
        )
        self.checkpoint_location = checkpoint_location or os.getenv(
            'CHECKPOINT_LOCATION', 'hdfs://namenode:9000/jobs/checkpoints'
        )
        self.spark = None

        logger.info(f"Initializing JobDataProcessor with:")
        logger.info(f"  Kafka Bootstrap Servers: {self.kafka_bootstrap_servers}")
        logger.info(f"  Kafka Topic: {self.kafka_topic}")
        logger.info(f"  HDFS Output Path: {self.hdfs_output_path}")
        logger.info(f"  Checkpoint Location: {self.checkpoint_location}")

    def setup_spark(self):
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master("local[*]") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
                .config("spark.executor.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .getOrCreate()

            logger.info("Spark session initialized successfully")

        except Exception as e:
            logger.error(f"Error initializing Spark session: {e}")
            raise

    def create_schema(self):
        return StructType([
            StructField("Title", StringType(), True),
            StructField("Salary", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("Experience", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("ingestion_time", DoubleType(), True)
        ])

    def read_from_kafka(self):
        try:
            logger.info(f"Connecting to Kafka at {self.kafka_bootstrap_servers}")

            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.session.timeout.ms", "30000") \
                .option("kafka.request.timeout.ms", "40000") \
                .load()

            logger.info(f"Successfully connected to Kafka topic: {self.kafka_topic}")
            return df
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise

    def parse_kafka_data(self, df):
        schema = self.create_schema()

        parsed_df = df.select(
            col("key").cast("string").alias("job_key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            "job_key",
            "data.*",
            "kafka_timestamp"
        )

        logger.info("Kafka data parsed successfully")
        return parsed_df

    def clean_and_transform_data(self, df):
        # Title
        df = df.withColumn("title_cleaned", regexp_replace(trim(col("Title")), r'\s+', ' '))

        # Salary
        df = df.withColumn("salary_cleaned", regexp_replace(trim(col("Salary")), r'\s+', ' '))
        df = df.withColumn("salary_min",
                           when(col("salary_cleaned").contains("triệu"),
                                regexp_extract(col("salary_cleaned"), r'(\d+)\s*-?\s*\d*\s*triệu', 1).cast(IntegerType()))
                           .when(col("salary_cleaned").rlike(r'\d+'),
                                 regexp_extract(col("salary_cleaned"), r'(\d+)', 1).cast(IntegerType()))
                           .otherwise(None)
                           )

        df = df.withColumn("salary_max",
                           when(col("salary_cleaned").contains("-"),
                                regexp_extract(col("salary_cleaned"), r'-\s*(\d+)\s*triệu', 1).cast(IntegerType()))
                           .otherwise(col("salary_min"))
                           )

        df = df.withColumn("salary_negotiable",
                           when(lower(col("salary_cleaned")).contains("thỏa thuận") |
                                lower(col("salary_cleaned")).contains("thoa thuan"), True)
                           .otherwise(False)
                           )

        # Location
        df = df.withColumn("location_cleaned", regexp_replace(trim(col("Location")), r'\s+', ' '))
        df = df.withColumn("primary_location",
                           when(col("location_cleaned").contains("&"),
                                trim(split(col("location_cleaned"), "&")[0]))
                           .when(col("location_cleaned").contains(","),
                                 trim(split(col("location_cleaned"), ",")[0]))
                           .otherwise(col("location_cleaned"))
                           )
        df = df.withColumn("is_major_city",
                           when(lower(col("location_cleaned")).contains("hồ chí minh") |
                                lower(col("location_cleaned")).contains("hà nội") |
                                lower(col("location_cleaned")).contains("đà nẵng"), True)
                           .otherwise(False)
                           )

        # Experience
        df = df.withColumn("experience_cleaned", regexp_replace(trim(col("Experience")), r'\s+', ' '))
        df = df.withColumn("experience_years",
                           when(lower(col("experience_cleaned")).contains("không yêu cầu"), 0)
                           .when(col("experience_cleaned").rlike(r'(\d+)\s*năm'),
                                 regexp_extract(col("experience_cleaned"), r'(\d+)\s*năm', 1).cast(IntegerType()))
                           .otherwise(0)
                           )
        df = df.withColumn("experience_level",
                           when(col("experience_years") == 0, "Fresher")
                           .when(col("experience_years") <= 2, "Junior")
                           .when(col("experience_years") <= 5, "Mid-Level")
                           .otherwise("Senior")
                           )

        # Company info
        df = df.withColumn("company_name",
                           regexp_extract(col("Description"), r'(CÔNG TY [^\n]+)', 1))
        df = df.withColumn("company_size",
                           regexp_extract(col("Description"), r'(\d+-\d+ nhân viên|\d+ nhân viên)', 1))

        # Language & work type
        df = df.withColumn("requires_english",
                           when(lower(col("Description")).contains("tiếng anh") |
                                lower(col("Description")).contains("english"), True)
                           .otherwise(False)
                           )
        df = df.withColumn("remote_work",
                           when(lower(col("Description")).contains("remote") |
                                lower(col("Description")).contains("làm việc tại nhà") |
                                lower(col("Description")).contains("online"), True)
                           .otherwise(False)
                           )

        # Industry
        df = df.withColumn("industry",
                           when(lower(col("title_cleaned")).contains("logistics") |
                                lower(col("Description")).contains("logistics"), "Logistics")
                           .when(lower(col("title_cleaned")).contains("telesales") |
                                 lower(col("title_cleaned")).contains("sales"), "Sales")
                           .when(lower(col("title_cleaned")).contains("kinh doanh"), "Business Development")
                           .otherwise("Other")
                           )

        # Processing timestamp + Job ID
        df = df.withColumn("processing_timestamp", current_timestamp())
        df = df.withColumn("job_id",
                           concat_ws("_",
                                     col("company_name"),
                                     col("primary_location"),
                                     col("kafka_timestamp").cast("long")))

        logger.info("Data cleaning and transformation completed")
        return df

    def enrich_data(self, df):
        df = df.withColumn("salary_category",
                           when(col("salary_negotiable"), "Negotiable")
                           .when((col("salary_min") >= 20) | (col("salary_max") >= 20), "High")
                           .when((col("salary_min") >= 10) | (col("salary_max") >= 10), "Medium")
                           .when(col("salary_min").isNotNull(), "Low")
                           .otherwise("Not Specified")
                           )

        df = df.withColumn("salary_average",
                           when(col("salary_min").isNotNull() & col("salary_max").isNotNull(),
                                round((col("salary_min") + col("salary_max")) / 2, 1))
                           .when(col("salary_min").isNotNull(), col("salary_min").cast(FloatType()))
                           .otherwise(None)
                           )

        logger.info("Data enrichment completed")
        return df

    def select_final_schema(self, df):
        return df.select(
            "job_id",
            "title_cleaned",
            "salary_cleaned",
            "salary_min",
            "salary_max",
            "salary_average",
            "salary_negotiable",
            "salary_category",
            "location_cleaned",
            "primary_location",
            "is_major_city",
            "experience_cleaned",
            "experience_years",
            "experience_level",
            "company_name",
            "company_size",
            "industry",
            "requires_english",
            "remote_work",
            "kafka_timestamp",
            "processing_timestamp"
        )

    # def process_batch_analytics(self, batch_df, batch_id):
    #     logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
    #
    #     try:
    #         # Location analysis
    #         location_count = batch_df.groupBy("Primary_Location").count()
    #         logger.info("Job count by location:")
    #         location_count.show(truncate=False)
    #
    #         # Salary analysis
    #         salary_stats = batch_df.select("Salary_Category").groupBy("Salary_Category").count()
    #         logger.info("Salary distribution:")
    #         salary_stats.show(truncate=False)
    #
    #         # Industry analysis
    #         industry_count = batch_df.groupBy("Industry").count().orderBy(col("count").desc())
    #         logger.info("Top industries:")
    #         industry_count.show(truncate=False)
    #
    #     except Exception as e:
    #         logger.error(f"Error in batch analytics: {e}")
    #
    # def _process_and_write_batch(self, batch_df, batch_id):
    #     try:
    #         if batch_df.count() > 0:
    #             # Run analytics
    #             self.process_batch_analytics(batch_df, batch_id)
    #
    #             # Write to HDFS
    #             batch_df.write \
    #                 .mode("append") \
    #                 .partitionBy("Primary_Location", "Industry") \
    #                 .parquet(self.hdfs_output_path)
    #
    #             logger.info(f"Batch {batch_id} written to HDFS successfully")
    #         else:
    #             logger.info(f"Batch {batch_id} is empty, skipping write")
    #
    #     except Exception as e:
    #         logger.error(f"Error processing batch {batch_id}: {e}")
    #         # Don't raise - allow stream to continue

    def write_to_hdfs_with_analytics(self, df):
        try:
            query = df.writeStream \
                .foreachBatch(lambda batch_df, batch_id:
                              self._process_and_write_batch(batch_df, batch_id)) \
                .option("checkpointLocation", self.checkpoint_location) \
                .trigger(processingTime="30 seconds") \
                .start()

            logger.info("Write stream started successfully")
            return query
        except Exception as e:
            logger.error(f"Error in write stream: {e}")
            raise

    def run(self):
        try:
            logger.info("Starting Job Data Processing Pipeline...")

            # 1. Setup Spark
            self.setup_spark()

            # 2. Read from Kafka
            kafka_df = self.read_from_kafka()

            # 3. Parse data
            parsed_df = self.parse_kafka_data(kafka_df)

            # 4. Clean and transform
            cleaned_df = self.clean_and_transform_data(parsed_df)

            # 5. Enrich data
            enriched_df = self.enrich_data(cleaned_df)

            # 6. Select final schema
            final_df = self.select_final_schema(enriched_df)

            # 7. Write to HDFS with analytics
            query = self.write_to_hdfs_with_analytics(final_df)

            logger.info("Streaming pipeline started successfully")

            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")


if __name__ == "__main__":
    processor = JobDataProcessor(
        kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
        kafka_topic=os.getenv('KAFKA_TOPIC', 'jobs_topic'),
        hdfs_output_path=os.getenv('HDFS_OUTPUT_PATH', 'hdfs://namenode:9000/jobs/processed'),
        checkpoint_location=os.getenv('CHECKPOINT_LOCATION', 'hdfs://namenode:9000/jobs/checkpoints'),
    )

    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
