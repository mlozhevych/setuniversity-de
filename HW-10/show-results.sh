#!/bin/bash

echo "==== Active containers ===="
docker ps

echo -e "\n==== Kafka topics status ===="
echo "Input topic:"
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic input --from-beginning --max-messages 5

echo -e "\nProcessed topic:"
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic processed --from-beginning --max-messages 5

echo -e "\n==== Cassandra data ===="
docker exec cassandra cqlsh -e "SELECT * FROM wikipedia.page_creations LIMIT 10;"
