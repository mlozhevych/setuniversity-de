#!/bin/bash

# Stop all services and remove containers
echo "Stopping and removing all containers..."
docker-compose down -v

echo "Removing lingering Zookeeper container..."
docker rm -f zookeeper 2>/dev/null

echo "Removing lingering Kafka container..."
docker rm -f kafka 2>/dev/null

echo "Removing Kafka data..."
rm -rf ./kafka/data

# Remove volumes and networks if needed
# docker-compose down --volumes --remove-orphans

echo "All services stopped and removed."
