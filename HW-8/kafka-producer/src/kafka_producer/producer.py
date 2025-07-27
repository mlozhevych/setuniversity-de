import os
from pathlib import Path

from kafka_producer.utils import download_twitter_dataset, get_producer, send_data

# --- Configuration ---
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "tweets")
SLEEP_TIME = 1 / int(os.environ.get("MESSAGES_PER_SECOND", 15))


def main():
    data_dir = Path(__file__).resolve().parent / "data"
    download_twitter_dataset(data_dir)

    # Create Kafka producer
    kafka_producer = get_producer(KAFKA_BROKER_URL)

    file_path = data_dir / "twcs" / "twcs.csv"
    # Send data to Kafka
    send_data(kafka_producer, file_path, TOPIC_NAME, SLEEP_TIME)


if __name__ == "__main__":
    main()
