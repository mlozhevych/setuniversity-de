#!/bin/bash
echo "Stopping Kafka and Zookeeper..."
docker-compose down

echo "Removing Kafka data..."
rm -rf ./data/*

echo "Starting Kafka and Zookeeper..."
docker compose build && docker-compose up -d
