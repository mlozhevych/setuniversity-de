#!/bin/bash
echo "Stopping Kafka and Zookeeper..."
docker compose down -v

echo "Removing lingering Zookeeper container..."
docker rm -f zookeeper 2>/dev/null

echo "Removing lingering Kafka container..."
docker rm -f kafka 2>/dev/null

echo "Removing lingering tweet-producer container..."
docker rm -f tweet-producer 2>/dev/null

echo "Removing Kafka data..."
rm -rf ./data/*

echo "Starting Kafka and Zookeeper..."
docker compose build && docker-compose up -d
