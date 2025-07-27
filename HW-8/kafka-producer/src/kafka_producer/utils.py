import csv
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import kagglehub
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def download_twitter_dataset(data_dir: Path) -> None:
    """Downloads the Twitter Customer Support dataset from Kaggle and saves it to the specified directory."""
    try:
        data_dir.mkdir(parents=True, exist_ok=True)

        dest_subdir = data_dir / "twcs"
        dest_subdir.mkdir(parents=True, exist_ok=True)
        dest_csv = dest_subdir / "twcs.csv"

        if dest_csv.exists():
            print(f"Dataset already exists at: {dest_csv}")
            print("✅ Using existing dataset.")
            return

        print("📥 Downloading dataset via kagglehub...")
        dataset_path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")
        dataset_path = Path(dataset_path)
        print(f"📂 Dataset cache path: {dataset_path}")

        source_csv = dataset_path / "twcs" / "twcs.csv"

        if not source_csv.exists():
            raise FileNotFoundError(f"File not found: {source_csv}")

        shutil.copy2(source_csv, dest_csv)
        print(f"Dataset copied to: {dest_csv}")
        print("✅ Dataset download completed.")
    except Exception as e:
        print(f"An unexpected error occurred while download file : {e}")


def get_producer(kafka_broker_url: str) -> KafkaProducer:
    """
    Tries to connect to Kafka and returns a producer instance.
    Retries connection every 10 seconds if the broker is not available.
    """
    while True:
        try:
            # The value_serializer encodes our dictionary as JSON and then as bytes
            producer = KafkaProducer(
                bootstrap_servers=kafka_broker_url,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id="tweet-producer"
            )
            print("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker at {kafka_broker_url} not available. Retrying in 10 seconds...")
            time.sleep(10)
        except Exception as e:
            print(f"An unexpected error occurred while connecting to Kafka: {e}")
            time.sleep(10)


def send_data(producer, file_path, topic: str, sleep_time) -> None:
    """
    Reads tweets from the twcs.csv file, formats a message, updates
    the timestamp, and sends them to Kafka.
    The function will loop indefinitely, resending the tweets from the file.
    """
    print(f"Starting to stream data from '{file_path}' to topic '{topic}'...")
    while True:
        try:
            with open(file_path, mode='r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    message = {
                        'tweet_id': row.get('tweet_id'),
                        'author_id': row.get('author_id'),
                        'inbound': row.get('inbound'),
                        'created_at': datetime.now(timezone.utc).isoformat(),
                        'text': row.get('text'),
                        'response_tweet_id': row.get('response_tweet_id'),
                        'in_response_to_tweet_id': row.get('in_response_to_tweet_id')
                    }

                    # Send the message to the Kafka topic
                    producer.send(topic, value=message)

                    # Log to console for verification
                    print(f"Sent: {message['tweet_id']}")

                    # Wait for a short period to control the message rate
                    time.sleep(sleep_time)

            print("Finished reading file. Restarting from the beginning in 15 seconds...")
            time.sleep(15)

        except FileNotFoundError:
            print(f"CRITICAL ERROR: The file '{file_path}' was not found.")
            print("Please download 'twcs.csv' from Kaggle and place it in the 'producer' directory.")
            break  # Exit if the file doesn't exist
        except Exception as e:
            print(f"An error occurred while sending messages: {e}")
            # In case of a transient error, wait before retrying
            time.sleep(5)
