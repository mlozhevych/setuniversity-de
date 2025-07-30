# Kafka Consumer for tweets topic
import os
from pathlib import Path

from kafka_consumer.utils import get_consumer, process_messages

# --- Configuration ---
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "tweets")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "data"))


def main():
    consumer = get_consumer(KAFKA_BROKER_URL, TOPIC_NAME)
    data_dir = Path(__file__).resolve().parent / OUTPUT_DIR
    if consumer:
        process_messages(consumer, data_dir)


if __name__ == "__main__":
    main()
