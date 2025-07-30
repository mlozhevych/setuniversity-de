import csv
import json
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


def get_consumer(kafka_broker_url: str, topic_name: str) -> KafkaConsumer:
    """
    Tries to connect to Kafka and returns a consumer instance.
    Retries connection every 10 seconds if the broker is not available.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=kafka_broker_url,
                auto_offset_reset='earliest',  # Start reading from the beginning of the topic
                group_id='tweet-csv-writer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                client_id="tweet-consumer"
            )
            print("Successfully connected to Kafka broker.")
            return consumer
        except NoBrokersAvailable:
            print(f" Kafka broker at {kafka_broker_url} not available. Retrying in 10 seconds...")
            time.sleep(10)
        except Exception as e:
            print(f" An unexpected error occurred while connecting to Kafka: {e}")
            time.sleep(10)


def process_messages(consumer: KafkaConsumer, output_dir: Path):
    """
    Continuously reads messages from the consumer, extracts required fields,
    and writes them to timestamped CSV files.
    """
    print(f" Writing CSV files to: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    open_files = {}  # Dictionary to manage open file handlers

    try:
        for message in consumer:
            try:
                data = message.value
                created_at_str = data.get('created_at')
                author_id = data.get('author_id')
                text = data.get('text')

                if not all([created_at_str, author_id, text]):
                    print(f" Skipping message with missing fields: {data}")
                    continue

                # Parse timestamp and format filename
                created_at = datetime.fromisoformat(created_at_str)
                filename = f"tweets_{created_at.strftime('%d_%m_%Y_%H_%M')}.csv"
                filepath = output_dir / filename

                # Check if we need to switch files
                if filepath not in open_files:
                    # Close all previously opened files
                    for f in open_files.values():
                        f['writer'] = None
                        f['file'].close()
                    open_files.clear()

                    # Open new file and create writer
                    file_handle = open(filepath, 'a', newline='', encoding='utf-8')
                    writer = csv.writer(file_handle)
                    # Write header if the file is new
                    if file_handle.tell() == 0:
                        writer.writerow(['author_id', 'created_at', 'text'])

                    open_files[filepath] = {'file': file_handle, 'writer': writer}

                # Write data row
                writer = open_files[filepath]['writer']
                writer.writerow([author_id, created_at, text])
                # We don't manually flush for performance reasons, relying on file buffer.
                print(f" Wrote message from author {author_id} to {filename}")

            except (json.JSONDecodeError, AttributeError, KeyError) as e:
                print(f" Could not process message: {message}. Error: {e}")
            except Exception as e:
                print(f" An unexpected error occurred during message processing: {e}")

    except KeyboardInterrupt:
        print(" Consumer process interrupted. Closing files...")
    finally:
        # Final cleanup
        for f in open_files.values():
            f['file'].close()
        consumer.close()
        print(" Kafka consumer closed.")
