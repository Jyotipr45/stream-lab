#!/usr/bin/env bash
# Runs a Kafka CLI tool inside the container. Examples:
#   infrastructure/kafka.sh kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
#   infrastructure/kafka.sh kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
tool="$1"; shift
docker exec -it streamlab-kafka "/opt/kafka/bin/${tool}" "$@"
