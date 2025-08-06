"""
Spark Streaming writer for Wikipedia events to Cassandra.
"""
import logging
import os
import time

from cassandra.cluster import Cluster, Session
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

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
KAFKA_PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "processed")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark/work-dir/checkpoints/writer")

# Spark Master URL
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# Cassandra configuration
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wikipedia")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "page_creations")


def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.

    Returns:
        SparkSession: The configured Spark session
    """
    return (SparkSession.builder
            .appName("WikipediaCassandraWriter")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.cores.max", "1")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
            # .master(SPARK_MASTER_URL)
            .getOrCreate())


def define_schema() -> StructType:
    """
    Define the schema for processed Wikipedia events.

    Returns:
        StructType: The schema for parsing JSON data
    """
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("page_title", StringType(), True)
    ])


def initialize_cassandra() -> Session:
    """
    Initialize the Cassandra connection and create the required keyspace and table.

    Returns:
        Session: Cassandra session
    """
    # Wait for Cassandra to be ready
    max_retries = 10
    retry_interval = 5  # seconds

    for i in range(max_retries):
        try:
            # Connect to Cassandra
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            logger.info(f"Connected to Cassandra at {CASSANDRA_HOST}")

            # Create keyspace if it doesn't exist
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)

            # Use the keyspace
            session.execute(f"USE {CASSANDRA_KEYSPACE}")

            # Create table if it doesn't exist
            session.execute(f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
                    user_id TEXT,
                    domain TEXT,
                    created_at TEXT,
                    page_title TEXT,
                    PRIMARY KEY (user_id, created_at)
                )
            """)

            logger.info(f"Created keyspace {CASSANDRA_KEYSPACE} and table {CASSANDRA_TABLE}")
            return session

        except Exception as e:
            if i < max_retries - 1:
                logger.warning(f"Failed to connect to Cassandra, retrying in {retry_interval} seconds: {str(e)}")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to initialize Cassandra after {max_retries} attempts: {str(e)}")
                raise


def write_to_cassandra(batch_df, batch_id):
    """
    Write the batch DataFrame to Cassandra.

    Args:
        batch_df: The batch DataFrame to write
        batch_id: The batch ID
    """
    try:
        # Write to Cassandra
        (batch_df
         .write
         .format("org.apache.spark.sql.cassandra")
         .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
         .mode("append")
         .save())

        logger.info(f"Successfully wrote batch {batch_id} to Cassandra")
    except Exception as e:
        logger.error(f"Failed to write batch {batch_id} to Cassandra: {str(e)}")


def process_and_save_stream():
    """
    Process the Wikipedia event stream from Kafka and save to Cassandra.
    """
    try:
        # Initialize Cassandra
        initialize_cassandra()

        # Create Spark session
        spark = create_spark_session()
        logger.info("Created Spark session")

        # Define schema for JSON parsing
        wiki_schema = define_schema()

        # Read from Kafka
        df_kafka = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("subscribe", KAFKA_PROCESSED_TOPIC)
                    .option("startingOffsets", "latest")
                    .load())

        # Parse JSON data with schema
        parsed_df = df_kafka.selectExpr("CAST(value AS STRING)")
        json_df = parsed_df.select(from_json(col("value"), wiki_schema).alias("data"))

        # Extract fields
        output_df = json_df.select(
            col("data.user_id"),
            col("data.domain"),
            col("data.created_at"),
            col("data.page_title")
        )

        # Write to Cassandra
        query = (output_df.writeStream
                 .foreachBatch(write_to_cassandra)
                 .option("checkpointLocation", CHECKPOINT_LOCATION)
                 .start())

        logger.info(
            f"Stream processing started. Writing events to Cassandra table: {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")

        # Wait for the query to terminate
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")


def main():
    """Main entry point for the Spark Streaming Cassandra writer."""
    try:
        logger.info("Starting Wikipedia stream Cassandra writer")
        process_and_save_stream()
    except KeyboardInterrupt:
        logger.info("Stream Cassandra writer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()
