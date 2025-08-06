#!/bin/bash

# Start services using docker-compose
echo "Starting Kafka, Cassandra and Spark..."
docker compose build && docker-compose up -d zookeeper kafka cassandra spark-master spark-worker

# Wait for Kafka and Cassandra to be ready
echo "Waiting for Kafka and Cassandra to be ready..."
sleep 20

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --topic input
docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --topic processed

# Start the Wikipedia stream generator
echo "Starting Wikipedia stream generator..."
docker compose build && docker-compose up -d stream-generator

# Wait for some data to arrive in Kafka
echo "Waiting for stream data..."
sleep 10

# Start Spark streaming processors
echo "Starting Spark streaming processors..."
docker compose build && docker-compose up -d spark-processor spark-cassandra-writer

echo "All services started successfully!"
echo "Run './show-results.sh' to check the results after a few minutes."
