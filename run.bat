@echo off
REM Usage: run.bat [experiment-number]     e.g.  run.bat 1
REM Starts Kafka (if not running), waits until healthy, then runs the experiment.
setlocal
set EXP=%1
if "%EXP%"=="" set EXP=1

cd /d "%~dp0"

echo Starting Kafka...
docker compose -f infrastructure\docker-compose.yml up -d || exit /b 1

echo Waiting for Kafka to be healthy...
:wait
for /f %%s in ('docker inspect -f "{{.State.Health.Status}}" streamlab-kafka') do set STATUS=%%s
if not "%STATUS%"=="healthy" (
    timeout /t 2 /nobreak >nul
    goto wait
)
echo Kafka is healthy.

call gradlew.bat :event-producer:bootRun --args="--streamlab.experiment=%EXP%"
