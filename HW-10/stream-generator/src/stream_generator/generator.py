"""
Stream generator for Wikipedia page creation events.
"""
import json
import logging
import os
import sys
import time
from typing import Dict, Any

import requests
import sseclient
from dotenv import load_dotenv
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "input")
WIKI_STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/page-create")


def create_kafka_producer() -> KafkaProducer:
    """
    Create and configure the Kafka producer.

    Returns:
        KafkaProducer: The configured Kafka producer instance
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            retries=5
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {str(e)}")
        raise


def parse_wiki_event(event_data: str) -> Dict[str, Any]:
    """
    Parse the Wikipedia event data from SSE.

    Args:
        event_data: The raw event data as string

    Returns:
        Dict containing parsed event data
    """
    try:
        return json.loads(event_data)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse event data: {str(e)}")
        return {}


def stream_wikipedia_events(producer: KafkaProducer):
    """
    Stream Wikipedia page creation events and send to Kafka.

    Args:
        producer: The Kafka producer to use for sending events
    """
    try:
        headers = {'Accept': 'text/event-stream'}
        response = requests.get(WIKI_STREAM_URL, headers=headers, stream=True, timeout=(5, 60))
        response.raise_for_status()
        client = sseclient.SSEClient(response)

        logger.info(f"Connected to Wikipedia event stream: {WIKI_STREAM_URL}")

        for event in client.events():
            if event.data:
                event_data = parse_wiki_event(event.data)

                if event_data:
                    # Send to Kafka
                    producer.send(
                        KAFKA_TOPIC,
                        key=event_data.get('meta', {}).get('id'),
                        value=event_data
                    )
                    logger.info(f"Sent event to Kafka: {event_data.get('meta', {}).get('id', 'unknown')}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Connection error: {str(e)}")
    except Exception as e:
        logger.error(f"Error streaming Wikipedia events: {str(e)}")


def main():
    """Main entry point for the Wikipedia stream generator."""
    try:
        # Create Kafka producer
        producer = create_kafka_producer()

        # Test connection to Kafka
        producer.send(KAFKA_TOPIC, key="test", value={"message": "test_connection"})
        producer.flush()
        logger.info(f"Successfully connected and sent test message to Kafka topic: {KAFKA_TOPIC}")

        # Stream Wikipedia events
        while True:
            try:
                stream_wikipedia_events(producer)
            except Exception as e:
                logger.error(f"Stream error, reconnecting in 5 seconds: {str(e)}")
                time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Stream generator stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    main()
