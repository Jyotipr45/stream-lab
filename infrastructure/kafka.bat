@echo off
REM Runs a Kafka CLI tool inside the container. Examples:
REM   infrastructure\kafka.bat kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
REM   infrastructure\kafka.bat kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
docker exec -it streamlab-kafka /opt/kafka/bin/%*
