@echo off
REM Runs all unit and integration tests. Docker Desktop must be running (Testcontainers starts its own Kafka).
cd /d "%~dp0"
call gradlew.bat clean test
