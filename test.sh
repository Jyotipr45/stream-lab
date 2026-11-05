#!/usr/bin/env bash
# Runs all unit and integration tests. Docker must be running (Testcontainers starts its own Kafka).
set -euo pipefail
cd "$(dirname "$0")"
./gradlew clean test
