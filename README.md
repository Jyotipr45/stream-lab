# StreamLab - Real-Time Kafka Event Processing System

A small, hands-on lab for learning Apache Kafka and Kafka Streams through working experiments on simulated e-commerce order events.

**Stack:** Java 17 · Spring Boot 3.5.16 · Gradle 8.11.1 · Spring for Apache Kafka · Apache Kafka 3.9 (KRaft) · Kafka Streams · Docker Compose · JUnit 5 · Mockito · Testcontainers

## Modules

| Module | Purpose | Status |
|---|---|---|
| `event-producer` | Generates order events and publishes them to Kafka | ✅ Experiment 1 |
| `event-consumer` | Partitions, offsets, consumer groups, rebalancing | ⏳ Experiment 4+ |
| `streams-processor` | Aggregation, windowing, fraud detection | ⏳ Experiment 10+ |
| `infrastructure` | Docker Compose for Kafka (KRaft) and CLI helpers | ✅ |
| `experiments` | One step-by-step guide per experiment | ✅ 01 |
| `docs` | Glossary and concept notes | ✅ |

## Quick start

Requires Docker Desktop running. Gradle and JDK 17 are fetched automatically.

```bat
run.bat 1        :: Windows
./run.sh 1       # macOS / Linux / Git Bash
```

Run tests (Docker must be running):
```bat
test.bat
```

## Experiments

| # | Experiment | Guide |
|---|---|---|
| 1 | Kafka in KRaft mode, 3-partition topic, partitions and offsets | [experiments/01](experiments/01-kafka-setup-topic-partitions-offsets.md) |
| 2 | Publish 100 messages and inspect partition distribution | — |
| 3 | Identical vs different keys | — |
| 4 | Offsets and committed offsets | — |
| 5 | Multiple consumers in one group | — |
| 6 | Rebalancing | — |
| 7 | Consumer failure | — |
| 8 | Manual offset commits | — |
| 9 | Retries and Dead Letter Topics | — |
| 10 | Kafka Streams aggregation | — |
| 11 | One-minute sales windows | — |
| 12 | Fraud: > 5 orders per customer in 30 s | — |
| 13 | Streams restart and state restoration | — |

New to a term? Start with the [glossary](docs/glossary.md).
