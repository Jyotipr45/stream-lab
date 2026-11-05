#!/usr/bin/env bash
# Usage: ./run.sh [experiment-number]     e.g.  ./run.sh 1
# Starts Kafka (if not running), waits until healthy, then runs the experiment.
set -euo pipefail
EXP="${1:-1}"
cd "$(dirname "$0")"

echo "Starting Kafka..."
docker compose -f infrastructure/docker-compose.yml up -d

echo "Waiting for Kafka to be healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' streamlab-kafka)" = "healthy" ]; do
  sleep 2
done
echo "Kafka is healthy."

./gradlew :event-producer:bootRun --args="--streamlab.experiment=${EXP}"
