"""
Spark Streaming processor for Wikipedia events.
"""
import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "input")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "processed")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark/work-dir/checkpoints/processor")

# Spark Master URL
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# Define the allowed domains for filtering
ALLOWED_DOMAINS = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]


def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.

    Returns:
        SparkSession: The configured Spark session
    """
    spark = (SparkSession.builder
             .appName("WikipediaStreamProcessor")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")
             .config("spark.cores.max", "1")
             .config("spark.executor.memory", "1g")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
             # .master(SPARK_MASTER_URL)
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def define_schema() -> StructType:
    """
    Define the schema for Wikipedia events.

    Returns:
        StructType: The schema for parsing JSON data
    """
    schema = StructType([
        StructField("meta", StructType([
            StructField("id", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("stream", StringType(), True),
            StructField("dt", StringType(), True),
        ]), True),
        StructField("page_title", StringType(), True),
        StructField("dt", StringType(), True),  # created_at
        StructField("performer", StructType([
            StructField("user_is_bot", BooleanType(), True),
            StructField("user_id", LongType(), True)
        ]), True)
    ])
    return schema


def process_wikipedia_stream():
    """
    Process the Wikipedia event stream from Kafka.
    """
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Created Spark session")

        # Define schema for JSON parsing
        wiki_schema = define_schema()

        # Read from Kafka
        df_kafka = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("subscribe", KAFKA_INPUT_TOPIC)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    # .option("startingOffsets", "earliest")
                    .load())

        logger.info(f"Kafka schema: {df_kafka.schema.simpleString()}")

        # Parse JSON data with schema
        parsed_df = df_kafka.selectExpr("CAST(value AS STRING) AS json_data")
        json_df = parsed_df.select(from_json(col("json_data"), wiki_schema).alias("data"))

        # Extract and filter data based on requirements
        filtered_df = (json_df
                       .select("data.*")
                       .filter(col("meta.domain").isin(ALLOWED_DOMAINS))
                       .filter((col("performer.user_is_bot") == False) | col("performer.user_is_bot").isNull()))

        # Prepare output structure
        output_df = (filtered_df
        .select(
            col("performer.user_id").alias("user_id"),
            col("performer.user_is_bot").alias("user_is_bot"),
            col("meta.domain").alias("domain"),
            col("dt").alias("created_at"),
            col("page_title")
        ))

        # Convert to JSON for Kafka output
        kafka_output = (output_df
                        .select(to_json(struct("*")).alias("value")))

        query = (kafka_output
                 .writeStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                 .option("topic", KAFKA_OUTPUT_TOPIC)
                 .option("checkpointLocation", CHECKPOINT_LOCATION)
                 .start())

        # query = (output_df  # Використовуємо DataFrame перед перетворенням в JSON
        #          .writeStream
        #          .outputMode("append")
        #          .format("console")
        #          .start())

        logger.info(f"Stream processing started. Writing filtered events to topic: {KAFKA_OUTPUT_TOPIC}")

        # Wait for the query to terminate
        query.awaitTermination()


    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")


def main():
    """Main entry point for the Spark Streaming processor."""
    try:
        logger.info("Starting Wikipedia stream processor")
        process_wikipedia_stream()
    except KeyboardInterrupt:
        logger.info("Stream processor stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
