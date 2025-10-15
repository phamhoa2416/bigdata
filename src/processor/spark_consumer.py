import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, trim, length, when, regexp_extract, lower, \
    current_timestamp, concat_ws, split, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobDataProcessor:
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
                .master("local[*]") \
                .config("spark.jars.packages",
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation",
                        self.checkpoint_location) \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
                .config("spark.executor.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
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
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "earliest") \
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

        logger.info("Kafka data parsed automatically")
        return parsed_df

    def clean_and_transform_data(self, df):
        df = df.withColumn("Title_Cleaned",
                           regexp_replace(trim(col("Title")), r'\s+', ' '))

        df = df.withColumn("Title_Length", length(col("Title")))

        df = df.withColumn("Salary_Cleaned",
                           regexp_replace(trim(col("Salary")), r'\s+', ' '))

        df = df.withColumn("Salary_Min",
                           when(col("Salary_Cleaned").contains("triệu"),
                                regexp_extract(col("Salary_Cleaned"), r'(\d+)\s*-?\s*\d*\s*triệu', 1).cast(
                                    IntegerType())).when(col("Salary_Cleaned").rlike(r'\d+'),
                                       regexp_extract(col("Salary_Cleaned"), r'(\d+)', 1).cast(IntegerType())
                                       ).otherwise(None)
                           )

        df = df.withColumn("Salary_Max",
                           when(col("Salary_Cleaned").contains("-"),
                                regexp_extract(col("Salary_Cleaned"), r'-\s*(\d+)\s*triệu', 1).cast(IntegerType())
                                ).otherwise(col("Salary_Min"))
                           )

        df = df.withColumn("Salary_Negotiable",
                           when(lower(col("Salary_Cleaned")).contains("thoa thuan") |
                                lower(col("Salary_Cleaned")).contains("thoả thuận"), True)
                           .otherwise(False)
                           )

        df = df.withColumn("Location_Cleaned",
                           regexp_replace(trim(col("Location")), r'\s+', ' '))

        df = df.withColumn("Primary_Location",
                           when(col("Location_Cleaned").contains("&"),
                                trim(split(col("Location_Cleaned"), "&")[0])
                                ).when(col("Location_Cleaned").contains(","),
                                       trim(split(col("Location_Cleaned"), ",")[0])
                                       ).otherwise(col("Location_Cleaned"))
                           )

        df = df.withColumn("Number_of_Locations",
                           when(col("Location_Cleaned").rlike(r'(\d+)\s*nơi\s*khác'),
                                regexp_extract(col("Location_Cleaned"), r'(\d+)\s*nơi\s*khác', 1).cast(
                                    IntegerType()) + 1
                                ).otherwise(1)
                           )

        # Xác định các thành phố lớn
        df = df.withColumn("Is_Major_City",
                           when(lower(col("Location_Cleaned")).contains("hồ chí minh") |
                                lower(col("Location_Cleaned")).contains("hà nội") |
                                lower(col("Location_Cleaned")).contains("đà nẵng"), True)
                           .otherwise(False)
                           )

        # 4. Xử lý Experience
        df = df.withColumn("Experience_Cleaned",
                           regexp_replace(trim(col("Experience")), r'\s+', ' '))

        df = df.withColumn("Experience_Years",
                           when(lower(col("Experience_Cleaned")).contains("không yêu cầu"), 0)
                           .when(col("Experience_Cleaned").rlike(r'(\d+)\s*năm'),
                                 regexp_extract(col("Experience_Cleaned"), r'(\d+)\s*năm', 1).cast(IntegerType()))
                           .otherwise(0)
                           )

        df = df.withColumn("Experience_Level",
                           when(col("Experience_Years") == 0, "Fresher")
                           .when(col("Experience_Years") <= 2, "Junior")
                           .when(col("Experience_Years") <= 5, "Mid-Level")
                           .otherwise("Senior")
                           )

        # 5. Phân tích Description
        df = df.withColumn("Description_Length",
                           length(col("Description")))

        # Trích xuất thông tin công ty
        df = df.withColumn("Company_Name",
                           regexp_extract(col("Description"),
                                          r'(CÔNG TY [^\\n]+)', 1)
                           )

        df = df.withColumn("Company_Size",
                           regexp_extract(col("Description"),
                                          r'(\\d+-\\d+ nhân viên|\\d+ nhân viên)', 1)
                           )

        # Trích xuất yêu cầu kỹ năng từ mô tả
        df = df.withColumn("Requires_English",
                           when(lower(col("Description")).contains("tiếng anh") |
                                lower(col("Description")).contains("english"), True)
                           .otherwise(False)
                           )

        df = df.withColumn("Requires_Chinese",
                           when(lower(col("Description")).contains("tiếng trung") |
                                lower(col("Description")).contains("chinese"), True)
                           .otherwise(False)
                           )

        df = df.withColumn("Remote_Work",
                           when(lower(col("Description")).contains("remote") |
                                lower(col("Description")).contains("làm việc tại nhà") |
                                lower(col("Description")).contains("online"), True)
                           .otherwise(False)
                           )

        # 6. Phân loại ngành nghề
        df = df.withColumn("Industry",
                           when(lower(col("Title_Cleaned")).contains("logistics") |
                                lower(col("Description")).contains("logistics"), "Logistics")
                           .when(lower(col("Title_Cleaned")).contains("telesales") |
                                 lower(col("Title_Cleaned")).contains("sales"), "Sales")
                           .when(lower(col("Title_Cleaned")).contains("kinh doanh"), "Business Development")
                           .otherwise("Other")
                           )

        # 7. Tính toán Score cho công việc (dựa trên độ đầy đủ thông tin)
        df = df.withColumn("Job_Completeness_Score",
                           (when(col("Title_Cleaned").isNotNull(), 1).otherwise(0) +
                            when(col("Salary_Min").isNotNull(), 1).otherwise(0) +
                            when(col("Location_Cleaned").isNotNull(), 1).otherwise(0) +
                            when(col("Experience_Years").isNotNull(), 1).otherwise(0) +
                            when(col("Company_Name").isNotNull(), 1).otherwise(0)) * 20
                           )

        # 8. Thêm timestamp xử lý
        df = df.withColumn("Processing_Timestamp", current_timestamp())

        # 9. Tạo ID duy nhất cho mỗi công việc
        df = df.withColumn("Job_ID",
                           concat_ws("_",
                                     col("Company_Name"),
                                     col("Primary_Location"),
                                     col("kafka_timestamp").cast("long")))

        logger.info("Data cleaning and transformation completed")
        return df

    def enrich_data(self, df):
        """Làm giàu dữ liệu với các phân tích bổ sung"""

        # Phân tích xu hướng lương
        df = df.withColumn("Salary_Category",
                           when(col("Salary_Negotiable"), "Negotiable")
                           .when((col("Salary_Min") >= 20) | (col("Salary_Max") >= 20), "High")
                           .when((col("Salary_Min") >= 10) | (col("Salary_Max") >= 10), "Medium")
                           .when(col("Salary_Min").isNotNull(), "Low")
                           .otherwise("Not Specified")
                           )

        # Tính lương trung bình
        df = df.withColumn("Salary_Average",
                           when(col("Salary_Min").isNotNull() & col("Salary_Max").isNotNull(),
                                round((col("Salary_Min") + col("Salary_Max")) / 2, 1))
                           .when(col("Salary_Min").isNotNull(), col("Salary_Min").cast(FloatType()))
                           .otherwise(None)
                           )

        # Phân loại độ hấp dẫn của công việc
        df = df.withColumn("Job_Attractiveness",
                           when((col("Salary_Category") == "High") &
                                (col("Is_Major_City") == True) &
                                (col("Remote_Work") == True), "Very Attractive")
                           .when((col("Salary_Category").isin(["High", "Medium"])) &
                                 (col("Is_Major_City") == True), "Attractive")
                           .when(col("Salary_Category") == "Medium", "Moderate")
                           .otherwise("Less Attractive")
                           )

        # Phân tích yêu cầu kỹ năng
        df = df.withColumn("Language_Requirements",
                           when(col("Requires_English") & col("Requires_Chinese"), "English & Chinese")
                           .when(col("Requires_English"), "English")
                           .when(col("Requires_Chinese"), "Chinese")
                           .otherwise("None Specified")
                           )

        logger.info("Data enrichment completed")
        return df

    def select_final_schema(self, df):
        """Chọn các cột cuối cùng cho output schema"""
        return df.select(
            "Job_ID",
            "Title_Cleaned",
            "Title_Length",
            "Salary_Cleaned",
            "Salary_Min",
            "Salary_Max",
            "Salary_Average",
            "Salary_Negotiable",
            "Salary_Category",
            "Location_Cleaned",
            "Primary_Location",
            "Number_of_Locations",
            "Is_Major_City",
            "Experience_Cleaned",
            "Experience_Years",
            "Experience_Level",
            "Company_Name",
            "Company_Size",
            "Industry",
            "Requires_English",
            "Requires_Chinese",
            "Language_Requirements",
            "Remote_Work",
            "Job_Completeness_Score",
            "Job_Attractiveness",
            "Description",
            "Description_Length",
            "kafka_timestamp",
            "Processing_Timestamp"
        )

    def write_to_hdfs(self, df):
        """Ghi dữ liệu đã xử lý vào HDFS"""
        try:
            query = df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", self.hdfs_output_path) \
                .option("checkpointLocation", self.checkpoint_location) \
                .partitionBy("Primary_Location", "Industry") \
                .trigger(processingTime="30 seconds") \
                .start()

            logger.info(f"Writing data to HDFS: {self.hdfs_output_path}")
            return query
        except Exception as e:
            logger.error(f"Error writing to HDFS: {e}")
            raise

    def process_batch_analytics(self, batch_df, batch_id):
        """Xử lý phân tích cho mỗi batch"""
        logger.info(f"Processing batch {batch_id}")

        location_count = batch_df.groupBy("Primary_Location").count()
        logger.info("Job count by location:")
        location_count.show()

        # Phân tích salary
        salary_stats = batch_df.select(
            "Salary_Category"
        ).groupBy("Salary_Category").count()
        logger.info("Salary distribution:")
        salary_stats.show()

        # Top industries
        industry_count = batch_df.groupBy("Industry").count().orderBy(col("count").desc())
        logger.info("Top industries:")
        industry_count.show()

    def write_to_hdfs_with_analytics(self, df):
        """Ghi vào HDFS với phân tích realtime"""
        try:
            query = df.writeStream \
                .foreachBatch(lambda batch_df, batch_id:
                              self._process_and_write_batch(batch_df, batch_id)) \
                .option("checkpointLocation", self.checkpoint_location) \
                .trigger(processingTime="30 seconds") \
                .start()

            return query
        except Exception as e:
            logger.error(f"Error in write stream: {e}")
            raise

    def _process_and_write_batch(self, batch_df, batch_id):
        """Xử lý và ghi từng batch"""
        if batch_df.count() > 0:
            # Analytics
            self.process_batch_analytics(batch_df, batch_id)

            # Write to HDFS
            batch_df.write \
                .mode("append") \
                .partitionBy("Primary_Location", "Industry") \
                .parquet(self.hdfs_output_path)

            logger.info(f"Batch {batch_id} written to HDFS successfully")

    def run(self):
        """Chạy toàn bộ pipeline"""
        try:
            # 1. Khởi tạo Spark
            self.setup_spark()

            # 2. Đọc từ Kafka
            kafka_df = self.read_from_kafka()

            # 3. Parse dữ liệu
            parsed_df = self.parse_kafka_data(kafka_df)

            cleaned_df = self.clean_and_transform_data(parsed_df)

            enriched_df = self.enrich_data(cleaned_df)

            final_df = self.select_final_schema(enriched_df)

            # 7. Ghi vào HDFS với analytics
            query = self.write_to_hdfs_with_analytics(final_df)

            logger.info("Streaming pipeline started successfully")

            # Chờ cho đến khi bị terminate
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

if __name__ == "__main__":
    processor = JobDataProcessor(
        kafka_bootstrap_servers='localhost:9092',
        kafka_topic='jobs_topic',
        hdfs_output_path='hdfs://localhost:9000/jobs/processed',
        checkpoint_location='hdfs://localhost:9000/jobs/checkpoints'
    )

    try:
        processor.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise